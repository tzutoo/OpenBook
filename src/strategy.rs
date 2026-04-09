//! Trading strategy module built on top of [`SignalSample`] data.
//!
//! This module provides a complete trading strategy framework including bar
//! aggregation, position management, signal generation, risk controls, and
//! performance reporting. It is designed to be used in conjunction with
//! [`crate::signal::SignalExtractor`] for real-time or backtested trading.
//!
//! # Architecture
//!
//! The data flow is:
//!
//! 1. `SignalExtractor::sample()` produces `SignalSample`s every second
//! 2. `SignalBarAggregator` rolls up samples into `AggregatedBar`s (e.g., 1-minute)
//! 3. `StrategyEngine::on_bar()` processes each bar and produces `Signal`s
//! 4. `TradeLogger` optionally records trades to CSV
//! 5. `PerformanceReport` summarizes trading results
//!
//! # Example
//!
//! ```rust,ignore
//! use crate::signal::{SignalExtractor, SignalConfig};
//! use crate::strategy::{SignalBarAggregator, StrategyEngine, StrategyConfig, TradeLogger};
//! use std::sync::{Arc, Mutex};
//!
//! // Setup (typically done once at startup)
//! let config = StrategyConfig::default();
//! let mut aggregator = SignalBarAggregator::new(config.bar_duration_ms);
//! let mut engine = StrategyEngine::new(config.clone(), 10_000.0);
//! let mut logger = TradeLogger::open("trades.csv").ok();
//!
//! // Main loop (called every second)
//! loop {
//!     let sample = extractor.sample();
//!     if let Some(bar) = aggregator.push(sample) {
//!         let signal = engine.on_bar(&bar);
//!         println!("Signal: {:?}", signal);
//!
//!         // Log completed trades
//!         if let Some(ref mut l) = logger {
//!             for trade in engine.trade_history().iter().rev() {
//!                 let _ = l.write_trade(trade);
//!             }
//!         }
//!     }
//! }
//!
//! // At shutdown, flush any partial bar
//! if let Some(bar) = aggregator.flush() {
//!     let _ = engine.on_bar(&bar);
//! }
//!
//! // Print performance report
//! let report = PerformanceReport::from_trades(engine.trade_history(), 10_000.0);
//! println!("{:#?}", report);
//! ```

use crate::execution::BinanceTestnetClient;
use crate::signal::*;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;

// =============================================================================
// AggregatedBar & SignalBarAggregator
// =============================================================================

/// A rolled-up 1-minute (or configurable duration) bar aggregating multiple
/// [`SignalSample`]s into summary statistics.
///
/// Contains price statistics, order book metrics, fill:kill aggregates,
/// aggression indicators, absorption scores, and trade flow summaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregatedBar {
    /// Timestamp of the first sample in the bar.
    pub bar_start_ms: u64,
    /// Timestamp of the last sample in the bar.
    pub bar_end_ms: u64,
    /// Number of samples that contributed to this bar.
    pub sample_count: usize,

    // -- Price --
    /// Mid price at bar start.
    pub open_price: f64,
    /// Mid price at bar end.
    pub close_price: f64,
    /// Highest mid price in the bar.
    pub high_price: f64,
    /// Lowest mid price in the bar.
    pub low_price: f64,
    /// Average mid price across all samples.
    pub avg_mid_price: f64,

    // -- Order book imbalance --
    /// Mean order book imbalance ([-1, 1], positive = bid-heavy).
    pub ob_imbalance_mean: f64,
    /// Maximum order book imbalance in the bar.
    pub ob_imbalance_max: f64,
    /// Minimum order book imbalance in the bar.
    pub ob_imbalance_min: f64,
    /// Linear regression slope of imbalance over samples (trend indicator).
    pub ob_imbalance_trend: f64,

    // -- Spread & Session Trend --
    /// Average spread in basis points.
    pub avg_spread_bps: f64,
    /// Final cumulative net quantity at bar close.
    pub cum_net_qty_final: f64,
    /// Final cumulative fill:kill ratio at bar close.
    pub cum_ratio_final: f64,

    // -- Fill:Kill --
    /// Mean net fill:kill direction ([-1, 1], positive = buy-aggressive).
    pub fk_net_direction_mean: f64,
    /// Maximum net direction in the bar.
    pub fk_net_direction_max: f64,
    /// Minimum net direction in the bar.
    pub fk_net_direction_min: f64,
    /// Net direction from the final sample.
    pub fk_net_direction_final: f64,
    /// Total buy fill quantity.
    pub fk_buy_fill_total: f64,
    /// Total sell fill quantity.
    pub fk_sell_fill_total: f64,
    /// Total buy kill quantity.
    pub fk_buy_kill_total: f64,
    /// Total sell kill quantity.
    pub fk_sell_kill_total: f64,

    // -- Aggression --
    /// Mean aggression shift (~[-2, 2], positive = shift to buying).
    pub aggression_shift_mean: f64,
    /// Maximum aggression shift in the bar.
    pub aggression_shift_max: f64,
    /// Minimum aggression shift in the bar.
    pub aggression_shift_min: f64,
    /// Aggression shift from the final sample.
    pub aggression_shift_final: f64,

    // -- Absorption --
    /// Maximum absorption score in the bar ([0, 1]).
    pub absorption_score_max: f64,
    /// Mean absorption score in the bar.
    pub absorption_score_mean: f64,
    /// Absorption score from the final sample.
    pub absorption_score_final: f64,

    // -- Liquidity Void --
    /// Maximum levels moved by a single buy burst.
    pub vd_buy_levels_max: u64,
    /// Maximum levels moved by a single sell burst.
    pub vd_sell_levels_max: u64,

    // -- Market impact --
    /// Average buy slippage in basis points.
    pub avg_impact_buy_slippage_bps: f64,
    /// Average sell slippage in basis points.
    pub avg_impact_sell_slippage_bps: f64,

    // -- Trade flow --
    /// Total trade volume (notional) in the bar.
    pub total_trade_volume: f64,
    /// Average buy volume percentage.
    pub avg_buy_volume_pct: f64,
    /// Average trades per second.
    pub avg_trade_count_per_sec: f64,
}

/// Aggregates [`SignalSample`]s into [`AggregatedBar`]s based on time boundaries.
///
/// The aggregator tracks the current bar's start time and closes the bar when
/// a sample's timestamp crosses the next boundary. Partial bars can be flushed
/// manually (e.g., at shutdown or symbol switch).
pub struct SignalBarAggregator {
    /// Duration of each bar in milliseconds.
    bar_duration_ms: u64,
    /// Samples accumulated for the current (incomplete) bar.
    samples: Vec<SignalSample>,
    /// Start timestamp of the current bar.
    current_bar_start_ms: u64,
}

impl SignalBarAggregator {
    /// Creates a new aggregator with the specified bar duration.
    ///
    /// # Arguments
    ///
    /// * `bar_duration_ms` - Bar duration in milliseconds (e.g., 60_000 for 1 minute).
    pub fn new(bar_duration_ms: u64) -> Self {
        Self {
            bar_duration_ms,
            samples: Vec::new(),
            current_bar_start_ms: 0,
        }
    }

    /// Push a sample into the aggregator.
    ///
    /// Returns `Some(AggregatedBar)` when the sample's timestamp crosses the
    /// next bar boundary, completing the current bar. The returned bar contains
    /// all samples up to (but not including) the new sample.
    ///
    /// The first sample pushed sets the initial bar start time.
    pub fn push(&mut self, sample: SignalSample) -> Option<AggregatedBar> {
        // Initialize bar start on first sample
        if self.samples.is_empty() {
            self.current_bar_start_ms = sample.timestamp_ms;
        }

        let next_boundary = self.current_bar_start_ms + self.bar_duration_ms;

        // Check if sample crosses into a new bar
        let mut completed_bar = None;
        if sample.timestamp_ms >= next_boundary && !self.samples.is_empty() {
            completed_bar = Some(Self::aggregate(&self.samples));
            self.samples.clear();
            self.current_bar_start_ms = sample.timestamp_ms;
        }

        self.samples.push(sample);
        completed_bar
    }

    /// Force-close the current bar, returning any accumulated samples as a bar.
    ///
    /// Returns `None` if no samples have been accumulated. Call this at shutdown
    /// or when switching symbols to avoid losing partial bar data.
    pub fn flush(&mut self) -> Option<AggregatedBar> {
        if self.samples.is_empty() {
            return None;
        }
        let bar = Self::aggregate(&self.samples);
        self.samples.clear();
        Some(bar)
    }

    /// Aggregate a slice of samples into an [`AggregatedBar`].
    fn aggregate(samples: &[SignalSample]) -> AggregatedBar {
        debug_assert!(!samples.is_empty(), "Cannot aggregate empty samples");

        let bar_start_ms = samples.first().unwrap().timestamp_ms;
        let bar_end_ms = samples.last().unwrap().timestamp_ms;
        let sample_count = samples.len();

        // Price stats
        let open_price = samples.first().unwrap().mid_price;
        let close_price = samples.last().unwrap().mid_price;
        let high_price = samples.iter().map(|s| s.mid_price).fold(f64::NEG_INFINITY, f64::max);
        let low_price = samples.iter().map(|s| s.mid_price).fold(f64::INFINITY, f64::min);
        let avg_mid_price = samples.iter().map(|s| s.mid_price).sum::<f64>() / sample_count as f64;

        // Order book imbalance stats
        let ob_imbalances: Vec<f64> = samples.iter().map(|s| s.ob_imbalance).collect();
        let ob_imbalance_mean = ob_imbalances.iter().sum::<f64>() / sample_count as f64;
        let ob_imbalance_max = ob_imbalances.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let ob_imbalance_min = ob_imbalances.iter().copied().fold(f64::INFINITY, f64::min);
        let ob_imbalance_trend = Self::linear_slope(&ob_imbalances);

        // Spread & Session Trend
        let avg_spread_bps = samples.iter().map(|s| s.spread_bps).sum::<f64>() / sample_count as f64;
        let cum_net_qty_final = samples.last().unwrap().cum_net_qty;
        let cum_ratio_final = samples.last().unwrap().cum_ratio;

        // Fill:Kill stats
        let fk_net_directions: Vec<f64> = samples.iter().map(|s| s.fk_net_direction).collect();
        let fk_net_direction_mean = fk_net_directions.iter().sum::<f64>() / sample_count as f64;
        let fk_net_direction_max = fk_net_directions.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let fk_net_direction_min = fk_net_directions.iter().copied().fold(f64::INFINITY, f64::min);
        let fk_net_direction_final = samples.last().unwrap().fk_net_direction;

        let fk_buy_fill_total: f64 = samples.iter().map(|s| s.fk_buy_fill_qty).sum();
        let fk_sell_fill_total: f64 = samples.iter().map(|s| s.fk_sell_fill_qty).sum();
        let fk_buy_kill_total: f64 = samples.iter().map(|s| s.fk_buy_kill_qty).sum();
        let fk_sell_kill_total: f64 = samples.iter().map(|s| s.fk_sell_kill_qty).sum();

        // Aggression shift stats
        let aggression_shifts: Vec<f64> = samples.iter().map(|s| s.aggression_shift).collect();
        let aggression_shift_mean = aggression_shifts.iter().sum::<f64>() / sample_count as f64;
        let aggression_shift_max = aggression_shifts.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let aggression_shift_min = aggression_shifts.iter().copied().fold(f64::INFINITY, f64::min);
        let aggression_shift_final = samples.last().unwrap().aggression_shift;

        // Absorption stats
        let absorption_scores: Vec<f64> = samples.iter().map(|s| s.absorption_score).collect();
        let absorption_score_max = absorption_scores.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let absorption_score_mean = absorption_scores.iter().sum::<f64>() / sample_count as f64;
        let absorption_score_final = samples.last().unwrap().absorption_score;

        // Liquidity Void stats
        let vd_buy_levels_max = samples.iter().map(|s| s.vd_buy_levels).max().unwrap_or(0);
        let vd_sell_levels_max = samples.iter().map(|s| s.vd_sell_levels).max().unwrap_or(0);

        // Market impact stats
        let avg_impact_buy_slippage_bps =
            samples.iter().map(|s| s.impact_buy_slippage_bps).sum::<f64>() / sample_count as f64;
        let avg_impact_sell_slippage_bps =
            samples.iter().map(|s| s.impact_sell_slippage_bps).sum::<f64>() / sample_count as f64;

        // Trade flow stats
        let total_trade_volume: f64 = samples.iter().map(|s| s.trade_volume_1s).sum();
        let avg_buy_volume_pct =
            samples.iter().map(|s| s.buy_volume_pct_1s).sum::<f64>() / sample_count as f64;
        let avg_trade_count_per_sec =
            samples.iter().map(|s| s.trade_count_1s as f64).sum::<f64>() / sample_count as f64;

        AggregatedBar {
            bar_start_ms,
            bar_end_ms,
            sample_count,
            open_price,
            close_price,
            high_price,
            low_price,
            avg_mid_price,
            ob_imbalance_mean,
            ob_imbalance_max,
            ob_imbalance_min,
            ob_imbalance_trend,
            avg_spread_bps,
            cum_net_qty_final,
            cum_ratio_final,
            fk_net_direction_mean,
            fk_net_direction_max,
            fk_net_direction_min,
            fk_net_direction_final,
            fk_buy_fill_total,
            fk_sell_fill_total,
            fk_buy_kill_total,
            fk_sell_kill_total,
            aggression_shift_mean,
            aggression_shift_max,
            aggression_shift_min,
            aggression_shift_final,
            absorption_score_max,
            absorption_score_mean,
            absorption_score_final,
            vd_buy_levels_max,
            vd_sell_levels_max,
            avg_impact_buy_slippage_bps,
            avg_impact_sell_slippage_bps,
            total_trade_volume,
            avg_buy_volume_pct,
            avg_trade_count_per_sec,
        }
    }

    /// Compute the linear regression slope for a series of values.
    ///
    /// Uses the formula: `slope = (n * Σ(x*y) - Σx * Σy) / (n * Σ(x²) - (Σx)²)`
    /// where x is the index (0, 1, 2, ...) and y is the value.
    ///
    /// Returns 0.0 if there are fewer than 2 values or if the denominator is zero.
    fn linear_slope(values: &[f64]) -> f64 {
        let n = values.len();
        if n < 2 {
            return 0.0;
        }

        let sum_x: f64 = (0..n).map(|i| i as f64).sum();
        let sum_y: f64 = values.iter().sum();
        let sum_xy: f64 = values.iter().enumerate().map(|(i, y)| i as f64 * y).sum();
        let sum_x2: f64 = (0..n).map(|i| (i as f64).powi(2)).sum();

        let denominator = n as f64 * sum_x2 - sum_x * sum_x;
        if denominator.abs() < 1e-10 {
            return 0.0;
        }

        (n as f64 * sum_xy - sum_x * sum_y) / denominator
    }
}

// =============================================================================
// Position & Trade Records
// =============================================================================

/// Side of a trade or position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    /// Buy side (long position or closing short).
    Buy,
    /// Sell side (short position or closing long).
    Sell,
}

/// Current state of a position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PositionState {
    /// No open position.
    Flat,
    /// Holding a long position.
    Long,
    /// Holding a short position.
    Short,
}

/// Represents an open trading position with P&L tracking.
///
/// Tracks entry details, current unrealized P&L, and the maximum favorable
/// and adverse excursions since entry (useful for trailing stops and analysis).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// Current position state.
    pub state: PositionState,
    /// Entry price of the position.
    pub entry_price: f64,
    /// Timestamp (ms) when the position was opened.
    pub entry_time_ms: u64,
    /// Position size in base units.
    pub quantity: f64,
    /// Current unrealized profit/loss in quote currency (e.g., USDT).
    pub unrealized_pnl: f64,
    /// Highest unrealized profit since entry (positive value).
    pub max_favorable: f64,
    /// Deepest unrealized loss since entry (negative value).
    pub max_adverse: f64,
}

impl Position {
    /// Creates a new flat (no position) state.
    pub fn flat() -> Self {
        Self {
            state: PositionState::Flat,
            entry_price: 0.0,
            entry_time_ms: 0,
            quantity: 0.0,
            unrealized_pnl: 0.0,
            max_favorable: 0.0,
            max_adverse: 0.0,
        }
    }

    /// Returns `true` if the position is flat (no open position).
    pub fn is_flat(&self) -> bool {
        self.state == PositionState::Flat
    }

    /// Update the position's unrealized P&L based on the current market price.
    ///
    /// Also updates `max_favorable` and `max_adverse` excursion tracking.
    /// Does nothing if the position is flat.
    pub fn update_market(&mut self, current_price: f64) {
        if self.is_flat() || self.quantity == 0.0 {
            return;
        }

        match self.state {
            PositionState::Long => {
                self.unrealized_pnl = (current_price - self.entry_price) * self.quantity;
            }
            PositionState::Short => {
                self.unrealized_pnl = (self.entry_price - current_price) * self.quantity;
            }
            PositionState::Flat => {}
        }

        // Track maximum favorable excursion
        if self.unrealized_pnl > self.max_favorable {
            self.max_favorable = self.unrealized_pnl;
        }

        // Track maximum adverse excursion (more negative = worse)
        if self.unrealized_pnl < self.max_adverse {
            self.max_adverse = self.unrealized_pnl;
        }
    }
}

impl Default for Position {
    fn default() -> Self {
        Self::flat()
    }
}

/// A completed trade record with full entry/exit details.
///
/// Created when a position is closed. Used for performance analysis,
/// trade logging, and strategy evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    /// Unique trade identifier (incrementing).
    pub id: u64,
    /// Trading symbol (e.g., "BTCUSDT").
    pub symbol: String,
    /// Side of the entry (Buy = long, Sell = short).
    pub side: Side,
    /// Entry timestamp in milliseconds.
    pub entry_time_ms: u64,
    /// Exit timestamp in milliseconds.
    pub exit_time_ms: u64,
    /// Entry price.
    pub entry_price: f64,
    /// Exit price.
    pub exit_price: f64,
    /// Position size in base units.
    pub quantity: f64,
    /// Realized profit/loss in quote currency.
    pub pnl: f64,
    /// Realized profit/loss as a percentage.
    pub pnl_pct: f64,
    /// Duration the position was held in milliseconds.
    pub hold_duration_ms: u64,
    /// Reason for exiting the position.
    pub exit_reason: String,
}

// =============================================================================
// Signal
// =============================================================================

/// Trading signal produced by the strategy engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Signal {
    /// Enter a long position.
    GoLong,
    /// Enter a short position.
    GoShort,
    /// Exit an existing long position.
    ExitLong,
    /// Exit an existing short position.
    ExitShort,
    /// No action; maintain current state.
    Hold,
}

// =============================================================================
// StrategyConfig
// =============================================================================

/// Configuration for the [`StrategyEngine`].
///
/// Contains all tunable parameters for entry/exit logic, risk management,
/// and circuit breaker thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyConfig {
    // -- General --
    /// Trading symbol (e.g., "BTCUSDT").
    pub symbol: String,
    /// Bar duration in milliseconds for aggregation (default: 30_000 = 30 seconds).
    pub bar_duration_ms: u64,

    // -- Entry thresholds --
    /// Composite score threshold for long entry (default: 0.5).
    pub long_entry_threshold: f64,
    /// Composite score threshold for short entry (default: -0.5).
    pub short_entry_threshold: f64,
    /// Minimum absorption score for reversal entries (default: 0.7).
    pub min_absorption_for_reversal: f64,
    /// Minimum fill:kill net direction for momentum-based entries (default: 0.15).
    pub min_momentum_threshold: f64,
    /// Minimum order book imbalance to confirm absorption direction in score (default: 0.1).
    pub absorption_ob_imbalance_threshold: f64,

    // -- Exit thresholds --
    /// Composite score threshold to exit long (default: -0.2).
    pub long_exit_threshold: f64,
    /// Composite score threshold to exit short (default: 0.2).
    pub short_exit_threshold: f64,

    // -- Advanced Microstructure Filters --
    /// Maximum allowed average spread in bps to enter a trade (default: 10.0).
    pub max_spread_bps: f64,
    /// Minimum ratio of opposite-side slippage vs same-side slippage (default: 1.2).
    pub min_liquidity_skew_ratio: f64,
    /// Maximum allowed adverse limit book trend (slope) to prevent spoofing traps (default: -0.05).
    pub trend_divergence_threshold: f64,
    /// Require cumulative session volume to align with trade direction (default: true).
    pub enforce_macro_trend: bool,
    /// Minimum levels moved by a single burst to trigger a Liquidity Void Snap-back (default: 20).
    pub min_void_levels_for_reversion: u64,
    /// Price recovery from bar low as multiplier for void long confirmation (default: 1.0005 = 0.05%).
    pub void_price_recovery_pct: f64,
    /// Minimum order book imbalance to confirm void entry (default: 0.05).
    pub void_ob_imbalance_threshold: f64,
    /// Spread must be below this fraction of max_spread_bps for void entry (default: 0.7).
    pub void_spread_tightness_pct: f64,
    /// Minimum average trades per second to enter a trade (default: 0.5).
    /// Filters out entries during low-activity periods where signals are often noise.
    pub min_trades_per_sec: f64,

    // -- Signal weights (for composite score) --
    /// Weight for fill:kill net direction (default: 0.30).
    pub weight_fk_direction: f64,
    /// Weight for aggression shift (default: 0.25).
    pub weight_aggression_shift: f64,
    /// Weight for order book imbalance (default: 0.15).
    pub weight_ob_imbalance: f64,
    /// Weight for absorption signal (default: 0.15).
    pub weight_absorption: f64,
    /// Weight for buy volume percentage (default: 0.15).
    pub weight_buy_volume_pct: f64,

    // -- Risk management --
    /// Risk per trade as percentage of equity (default: 0.5).
    pub risk_per_trade_pct: f64,
    /// Maximum position size in quote currency (default: 10_000.0).
    pub max_position_size_usd: f64,
    /// Stop loss as percentage of entry price (default: 0.15).
    pub stop_loss_pct: f64,
    /// Take profit as percentage of entry price (default: 0.30).
    pub take_profit_pct: f64,
    /// Activate trailing stop after this much profit % (default: 0.15).
    pub trailing_stop_activation_pct: f64,
    /// Trailing stop distance from peak as % (default: 0.08).
    pub trailing_stop_distance_pct: f64,
    /// Exit if no profit after this duration in ms (default: 90_000 = 1.5 min).
    pub time_stop_ms: u64,
    /// Minimum number of bars before allowing stale trade exit (default: 1).
    /// Prevents exiting too quickly before the trade has time to develop.
    pub stale_exit_min_bars: u64,
    /// Maximum number of simultaneous open positions (default: 1).
    pub max_open_positions: usize,

    // -- Circuit breakers --
    /// Daily loss limit as percentage of equity (default: 3.0).
    pub daily_loss_limit_pct: f64,
    /// Maximum consecutive losses before circuit breaker (default: 5).
    pub max_consecutive_losses: usize,
    /// Cooldown period after a loss in ms (default: 30_000 = 30 sec).
    pub cooldown_after_loss_ms: u64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        // Default uses 30s bars (balanced settings)
        Self::for_30s()
    }
}

impl StrategyConfig {
    /// Timeframe preset for 15-second bars.
    ///
    /// **Characteristics:** High noise, fast signals, smaller per-bar moves.
    /// **Adjustments:** Loosened for more trade frequency.
    /// - Lowered entry thresholds to allow more signals through noisy bars
    /// - Relaxed absorption/void thresholds to catch more reversals
    /// - Widened spread filter to allow entries on more instruments
    /// - Shortened cooldown to increase trade frequency
    pub fn for_15s() -> Self {
        Self {
            symbol: String::new(),
            bar_duration_ms: 15_000,
            // Entry: loosened from 0.55 to allow more signals through noisy bars
            long_entry_threshold: 0.40,
            short_entry_threshold: -0.40,
            // Absorption: lowered to catch more reversals
            min_absorption_for_reversal: 0.55,
            // Momentum: lowered from 0.20 to filter less
            min_momentum_threshold: 0.12,
            // Absorption OB confirmation: more lenient
            absorption_ob_imbalance_threshold: 0.08,
            // Exit: tighter to reduce whipsaw in noisy environment
            long_exit_threshold: -0.05,
            short_exit_threshold: 0.05,
            // Spread: widened from 8.0 to allow entries on more instruments
            max_spread_bps: 12.0,
            // Liquidity skew: relaxed from 1.3
            min_liquidity_skew_ratio: 1.1,
            // Divergence: relaxed from -0.03 to -0.06
            trend_divergence_threshold: -0.06,
            enforce_macro_trend: true,
            // Void: lowered from 35 to catch more void events
            min_void_levels_for_reversion: 25,
            // Void confirmation: relaxed
            void_price_recovery_pct: 1.0005,
            void_ob_imbalance_threshold: 0.04,
            void_spread_tightness_pct: 0.75,
            // Trade activity: lowered from 0.8 to allow quieter periods
            min_trades_per_sec: 0.3,
            // Weights: keep momentum emphasis (sum to 1.0)
            weight_fk_direction: 0.35,
            weight_aggression_shift: 0.25,
            weight_ob_imbalance: 0.10,
            weight_absorption: 0.10,
            weight_buy_volume_pct: 0.20,
            risk_per_trade_pct: 0.4,
            max_position_size_usd: 10_000.0,
            // Stop loss: tighter for smaller moves
            stop_loss_pct: 0.20,
            // Take profit: smaller target, more achievable
            take_profit_pct: 0.10,
            trailing_stop_activation_pct: 0.10,
            trailing_stop_distance_pct: 0.05,
            // Time stop: very fast exit (~1.3 bars)
            time_stop_ms: 20_000,
            // Stale exit: require 2 bars before allowing early exit
            stale_exit_min_bars: 2,
            max_open_positions: 1,
            daily_loss_limit_pct: 3.0,
            max_consecutive_losses: 5,
            // Cooldown: shortened from 120s to 60s (4 bars)
            cooldown_after_loss_ms: 60_000,
        }
    }

    /// Timeframe preset for 30-second bars (default).
    ///
    /// **Characteristics:** Balanced noise/signal ratio, moderate moves.
    /// **Adjustments:** Loosened for more trade frequency.
    pub fn for_30s() -> Self {
        Self {
            symbol: String::new(),
            bar_duration_ms: 30_000,
            // Entry: loosened from 0.45 to allow more signals
            long_entry_threshold: 0.30,
            short_entry_threshold: -0.30,
            // Absorption: lowered from 0.60 to catch more reversals
            min_absorption_for_reversal: 0.45,
            // Momentum: lowered from 0.15
            min_momentum_threshold: 0.10,
            // Absorption OB confirmation: more lenient
            absorption_ob_imbalance_threshold: 0.06,
            // Exit: slightly tighter reversal exit
            long_exit_threshold: -0.08,
            short_exit_threshold: 0.08,
            // Spread: widened from 10.0 to allow entries on more instruments
            max_spread_bps: 15.0,
            // Liquidity skew: relaxed from 1.2
            min_liquidity_skew_ratio: 1.05,
            // Divergence: relaxed from -0.05 to -0.08
            trend_divergence_threshold: -0.08,
            enforce_macro_trend: true,
            // Void: lowered from 30 to catch more void events
            min_void_levels_for_reversion: 20,
            // Void confirmation: relaxed
            void_price_recovery_pct: 1.0003,
            void_ob_imbalance_threshold: 0.03,
            void_spread_tightness_pct: 0.80,
            // Trade activity: lowered from 0.5 to allow quieter periods
            min_trades_per_sec: 0.2,
            // Weights: balanced (sum to 1.0)
            weight_fk_direction: 0.30,
            weight_aggression_shift: 0.25,
            weight_ob_imbalance: 0.15,
            weight_absorption: 0.15,
            weight_buy_volume_pct: 0.15,
            risk_per_trade_pct: 0.5,
            max_position_size_usd: 10_000.0,
            // Stop loss: tighter to limit per-trade damage
            stop_loss_pct: 0.30,
            // Take profit: more realistic target
            take_profit_pct: 0.15,
            trailing_stop_activation_pct: 0.15,
            trailing_stop_distance_pct: 0.08,
            // Time stop: exit faster if trade isn't working (~1.5 bars)
            time_stop_ms: 45_000,
            // Stale exit: require 1 bar before allowing early exit
            stale_exit_min_bars: 1,
            max_open_positions: 1,
            daily_loss_limit_pct: 3.0,
            max_consecutive_losses: 5,
            // Cooldown: shortened from 180s to 90s (3 bars)
            cooldown_after_loss_ms: 90_000,
        }
    }

    /// Timeframe preset for 1-minute bars.
    ///
    /// **Characteristics:** Lower noise, smoother signals, larger per-bar moves.
    /// **Adjustments:** Loosened for more trade frequency.
    pub fn for_1m() -> Self {
        Self {
            symbol: String::new(),
            bar_duration_ms: 60_000,
            // Entry: loosened from 0.40 — smoother signals allow lower threshold
            long_entry_threshold: 0.25,
            short_entry_threshold: -0.25,
            // Absorption: lowered from 0.50 to catch more reversals
            min_absorption_for_reversal: 0.40,
            // Momentum: lowered from 0.12
            min_momentum_threshold: 0.08,
            // Absorption OB confirmation: more lenient
            absorption_ob_imbalance_threshold: 0.05,
            // Exit: slightly wider reversal exit (less whipsaw on 1m)
            long_exit_threshold: -0.10,
            short_exit_threshold: 0.10,
            // Spread: widened from 12.0 to allow entries on more instruments
            max_spread_bps: 18.0,
            // Liquidity skew: relaxed from 1.1 — effectively disabled
            min_liquidity_skew_ratio: 1.0,
            // Divergence: relaxed from -0.08 to -0.10
            trend_divergence_threshold: -0.10,
            enforce_macro_trend: true,
            // Void: lowered from 25 to catch more void events
            min_void_levels_for_reversion: 15,
            // Void confirmation: more lenient on 1m bars
            void_price_recovery_pct: 1.0002,
            void_ob_imbalance_threshold: 0.02,
            void_spread_tightness_pct: 0.85,
            // Trade activity: lowered from 0.3 to allow quieter periods
            min_trades_per_sec: 0.1,
            // Weights: emphasize microstructure (sum to 1.0)
            weight_fk_direction: 0.25,
            weight_aggression_shift: 0.20,
            weight_ob_imbalance: 0.20,
            weight_absorption: 0.20,
            weight_buy_volume_pct: 0.15,
            risk_per_trade_pct: 0.6,
            max_position_size_usd: 10_000.0,
            // Stop loss: wider to allow breathing room
            stop_loss_pct: 0.40,
            // Take profit: larger target (moves are bigger)
            take_profit_pct: 0.20,
            trailing_stop_activation_pct: 0.20,
            trailing_stop_distance_pct: 0.10,
            // Time stop: longer (~1.5 bars)
            time_stop_ms: 90_000,
            // Stale exit: require 1 bar before allowing early exit
            stale_exit_min_bars: 1,
            max_open_positions: 1,
            daily_loss_limit_pct: 3.0,
            max_consecutive_losses: 5,
            // Cooldown: shortened from 240s to 120s (2 bars)
            cooldown_after_loss_ms: 120_000,
        }
    }

    /// Create config for a given bar duration, selecting the appropriate preset.
    ///
    /// - `<= 20_000` ms → 15s preset
    /// - `<= 45_000` ms → 30s preset
    /// - `> 45_000` ms → 1m preset
    pub fn for_bar_duration(bar_duration_ms: u64) -> Self {
        if bar_duration_ms <= 20_000 {
            Self::for_15s()
        } else if bar_duration_ms <= 45_000 {
            Self::for_30s()
        } else {
            Self::for_1m()
        }
    }
}

// =============================================================================
// StrategyEngine
// =============================================================================

/// Core trading strategy engine.
///
/// Processes [`AggregatedBar`]s and produces [`Signal`]s based on a composite
/// scoring system. Manages position state, enforces risk rules, and tracks
/// circuit breaker conditions.
///
/// # Entry Logic
///
/// A long entry requires:
/// - Composite score > `long_entry_threshold`
/// - Either absorption score >= `min_absorption_for_reversal` (reversal signal)
///   OR `fk_net_direction_mean` > 0.2 (momentum confirmation)
///
/// Short entries mirror this logic.
///
/// # Exit Logic
///
/// Exits are checked in priority order:
/// 1. Stop loss
/// 2. Take profit
/// 3. Trailing stop (activated after `trailing_stop_activation_pct` profit)
/// 4. Time stop (exit if no profit after `time_stop_ms`)
/// 5. Signal reversal (score crosses exit threshold)
pub struct StrategyEngine {
    pub config: StrategyConfig,
    position: Position,
    trade_history: Vec<TradeRecord>,
    next_trade_id: u64,

    // Risk state
    equity: f64,
    daily_start_equity: f64,
    daily_pnl: f64,
    consecutive_losses: usize,
    last_exit_time_ms: u64,
    session_active: bool,

    // Bar tracking
    last_bar: Option<AggregatedBar>,

    // Binance execution
    binance_client: Option<Arc<BinanceTestnetClient>>,
}

impl StrategyEngine {
    /// Creates a new strategy engine with the given configuration and starting equity.
    ///
    /// # Arguments
    ///
    /// * `config` - Strategy configuration parameters.
    /// * `equity` - Starting account equity in quote currency.
    pub fn new(config: StrategyConfig, equity: f64) -> Self {
        Self {
            config,
            position: Position::flat(),
            trade_history: Vec::new(),
            next_trade_id: 1,
            equity,
            daily_start_equity: equity,
            daily_pnl: 0.0,
            consecutive_losses: 0,
            last_exit_time_ms: 0,
            session_active: true,
            last_bar: None,
            binance_client: None,
        }
    }

    /// Set the Binance execution client. When set, all open/close operations
    /// will place real orders on Binance Demo before updating internal state.
    /// If the Binance order fails, the internal state is not updated.
    pub fn set_binance_client(&mut self, client: Arc<BinanceTestnetClient>) {
        self.binance_client = Some(client);
    }

    /// Sync the current position and equity from the Binance exchange.
    /// Call this after login or symbol switch to reconcile internal state.
    pub fn sync_from_exchange(&mut self, now_ms: u64) {
        let client = match &self.binance_client {
            Some(c) => c,
            None => return,
        };

        let symbol = self.config.symbol.to_uppercase();

        // Sync balance
        if let Ok(balance) = client.get_balance() {
            if balance > 0.0 {
                self.equity = balance;
            }
        }

        // Sync position
        if let Ok(Some(pos)) = client.get_position(&symbol) {
            if pos.position_amt.abs() > 0.0 {
                let state = if pos.position_amt > 0.0 {
                    PositionState::Long
                } else {
                    PositionState::Short
                };
                self.position = Position {
                    state,
                    entry_price: pos.entry_price,
                    entry_time_ms: now_ms, // We don't know exact entry time from exchange
                    quantity: pos.position_amt.abs(),
                    unrealized_pnl: pos.unrealized_pnl,
                    max_favorable: 0.0,
                    max_adverse: 0.0,
                };
            } else {
                self.position = Position::flat();
            }
        }
    }

    /// Process a completed bar and return a trading signal.
    ///
    /// This is the main entry point for the strategy. Call this once per
    /// completed [`AggregatedBar`]. The engine will:
    /// 1. Check circuit breakers
    /// 2. If in a position, check exit conditions
    /// 3. If flat, check entry conditions
    /// 4. Return the appropriate signal
    pub fn on_bar(&mut self, bar: &AggregatedBar) -> Signal {
        self.last_bar = Some(bar.clone());

        // Check circuit breakers first
        self.check_circuit_breakers();

        if !self.session_active {
            return Signal::Hold;
        }

        // If in a position, check exit conditions
        if !self.position.is_flat() {
            // Update unrealized PnL with bar's close price
            self.position.update_market(bar.close_price);

            let exit_signal = self.check_exit(bar);
            if exit_signal != Signal::Hold {
                // Determine exit reason by re-checking conditions
                let reason = self.determine_exit_reason(bar);
                let closed = self.close_position(bar.close_price, bar.bar_end_ms, reason);
                return if closed { exit_signal } else { Signal::Hold };
            }
            return Signal::Hold;
        }

        // If flat, check entry conditions
        let entry_signal = self.check_entry(self.compute_score(bar), bar);
        let opened = match entry_signal {
            Signal::GoLong => self.open_position(Side::Buy, bar.close_price, bar.bar_end_ms),
            Signal::GoShort => self.open_position(Side::Sell, bar.close_price, bar.bar_end_ms),
            _ => true,
        };
        if opened { entry_signal } else { Signal::Hold }
    }

    /// Compute the composite score from an aggregated bar.
    ///
    /// The score is a weighted combination of multiple signals:
    /// - Fill:kill net direction
    /// - Aggression shift
    /// - Order book imbalance
    /// - Absorption (with direction logic for reversals)
    /// - Buy volume percentage deviation from 50%
    ///
    /// Returns a value where positive = bullish, negative = bearish.
    /// Uses configurable weights from `StrategyConfig`.
    pub fn compute_score(&self, bar: &AggregatedBar) -> f64 {
        // Absorption direction logic:
        // High absorption with contrary flow AND order book confirmation = reversal signal
        let absorption_direction = if bar.absorption_score_max > self.config.min_absorption_for_reversal 
            && bar.fk_net_direction_mean < 0.0 
            && bar.ob_imbalance_mean > self.config.absorption_ob_imbalance_threshold 
        {
            // Buy absorption reversal: selling being absorbed, and limit bids are stacked = bullish
            bar.absorption_score_max
        } else if bar.absorption_score_max > self.config.min_absorption_for_reversal 
            && bar.fk_net_direction_mean > 0.0 
            && bar.ob_imbalance_mean < -self.config.absorption_ob_imbalance_threshold 
        {
            // Sell absorption reversal: buying being absorbed, and limit asks are stacked = bearish
            -bar.absorption_score_max
        } else {
            0.0
        };

        // Buy volume normalized to [-1, 1] range
        let buy_volume_normalized = (bar.avg_buy_volume_pct - 50.0) / 50.0;

        let score = bar.fk_net_direction_mean * self.config.weight_fk_direction
            + bar.aggression_shift_mean * self.config.weight_aggression_shift
            + bar.ob_imbalance_mean * self.config.weight_ob_imbalance
            + absorption_direction * self.config.weight_absorption
            + buy_volume_normalized * self.config.weight_buy_volume_pct;

        score
    }

    /// Check if entry conditions are met.
    ///
    /// Entry requires:
    /// - Session is active
    /// - Cooldown period has elapsed since last exit
    /// - Score crosses entry threshold
    /// - Either absorption or momentum confirms the signal
    fn check_entry(&self, score: f64, bar: &AggregatedBar) -> Signal {
        // Check cooldown
        if self.last_exit_time_ms > 0 {
            let time_since_exit = bar.bar_end_ms.saturating_sub(self.last_exit_time_ms);
            if time_since_exit < self.config.cooldown_after_loss_ms {
                return Signal::Hold;
            }
        }

        // 1. Spread Filter: Do not enter if the market is illiquid or volatile
        if bar.avg_spread_bps > self.config.max_spread_bps {
            return Signal::Hold;
        }

        // --- LIQUIDITY VOID SNAP-BACK (ANOMALY ENTRY) ---
        // A void is created when aggressive flow rips through many levels in <1s.
        // Requires confirmation to avoid false triggers on noise.
        let tight_spread = bar.avg_spread_bps < self.config.max_spread_bps * self.config.void_spread_tightness_pct;

        // If sellers ripped through >= N levels of bids, the path up is vacuumed out -> Snap-back Long
        if bar.vd_sell_levels_max >= self.config.min_void_levels_for_reversion {
            // Confirm: price recovering from low, bids stacked, spread tight
            let price_recovering = bar.close_price > bar.low_price * self.config.void_price_recovery_pct;
            let bids_stacked = bar.ob_imbalance_mean > self.config.void_ob_imbalance_threshold;
            if price_recovering && bids_stacked && tight_spread {
                return Signal::GoLong;
            }
        }
        // If buyers ripped through >= N levels of asks, the path down is vacuumed out -> Snap-back Short
        if bar.vd_buy_levels_max >= self.config.min_void_levels_for_reversion {
            // Confirm: price falling from high, asks stacked, spread tight
            let price_falling = bar.close_price < bar.high_price * (2.0 - self.config.void_price_recovery_pct);
            let asks_stacked = bar.ob_imbalance_mean < -self.config.void_ob_imbalance_threshold;
            if price_falling && asks_stacked && tight_spread {
                return Signal::GoShort;
            }
        }

        // --- MOMENTUM / ABSORPTION (STANDARD ENTRY) ---
        // Check long entry
        if score > self.config.long_entry_threshold {
            // 5. Trade Activity Filter: avoid entries during low-activity periods
            // Low activity often means the signal is noise, not genuine momentum
            if bar.avg_trade_count_per_sec < self.config.min_trades_per_sec {
                return Signal::Hold;
            }

            // 6. Confirmation: require either strong absorption OR strong momentum
            let has_absorption = bar.absorption_score_max >= self.config.min_absorption_for_reversal;
            let has_momentum = bar.fk_net_direction_mean > self.config.min_momentum_threshold;

            if has_absorption || has_momentum {
                return Signal::GoLong;
            }
        }

        // Check short entry
        if score < self.config.short_entry_threshold {
            // 5. Trade Activity Filter: avoid entries during low-activity periods
            if bar.avg_trade_count_per_sec < self.config.min_trades_per_sec {
                return Signal::Hold;
            }

            // 6. Confirmation: require either strong absorption OR strong momentum
            let has_absorption = bar.absorption_score_max >= self.config.min_absorption_for_reversal;
            let has_momentum = bar.fk_net_direction_mean < -self.config.min_momentum_threshold;

            if has_absorption || has_momentum {
                return Signal::GoShort;
            }
        }

        Signal::Hold
    }

    /// Check if exit conditions are met for the current position.
    ///
    /// Checks in priority order:
    /// 1. Stop loss
    /// 2. Take profit
    /// 3. Trailing stop
    /// 4. Time stop
    /// 5. Signal reversal
    fn check_exit(&self, bar: &AggregatedBar) -> Signal {
        if self.position.is_flat() || self.position.quantity == 0.0 {
            return Signal::Hold;
        }

        let entry_value = self.position.entry_price * self.position.quantity;
        if entry_value == 0.0 {
            return Signal::Hold;
        }

        let pnl_pct = (self.position.unrealized_pnl / entry_value) * 100.0;
        let max_fav_pct = (self.position.max_favorable / entry_value) * 100.0;

        // 1. Stop loss check
        if pnl_pct <= -self.config.stop_loss_pct {
            return match self.position.state {
                PositionState::Long => Signal::ExitLong,
                PositionState::Short => Signal::ExitShort,
                PositionState::Flat => Signal::Hold,
            };
        }

        // 2. Take profit check
        if pnl_pct >= self.config.take_profit_pct {
            return match self.position.state {
                PositionState::Long => Signal::ExitLong,
                PositionState::Short => Signal::ExitShort,
                PositionState::Flat => Signal::Hold,
            };
        }

        // 3. Trailing stop check
        if max_fav_pct >= self.config.trailing_stop_activation_pct {
            let trailing_stop_pct = self.config.trailing_stop_distance_pct;

            match self.position.state {
                PositionState::Long => {
                    // Best price = entry + max_favorable / quantity
                    let best_price =
                        self.position.entry_price + self.position.max_favorable / self.position.quantity;
                    let trailing_stop_price = best_price * (1.0 - trailing_stop_pct / 100.0);

                    if bar.close_price <= trailing_stop_price {
                        return Signal::ExitLong;
                    }
                }
                PositionState::Short => {
                    // Worst price = entry - max_favorable / quantity
                    let worst_price =
                        self.position.entry_price - self.position.max_favorable / self.position.quantity;
                    let trailing_stop_price = worst_price * (1.0 + trailing_stop_pct / 100.0);

                    if bar.close_price >= trailing_stop_price {
                        return Signal::ExitShort;
                    }
                }
                PositionState::Flat => {}
            }
        }

        // 4. Time stop checks
        let hold_duration = bar.bar_end_ms.saturating_sub(self.position.entry_time_ms);
        let min_hold_for_stale = self.config.stale_exit_min_bars * self.config.bar_duration_ms;

        // 4a. Stale trade: exit faster if no favorable movement at all
        // A trade that hasn't moved in your favor is likely wrong - cut it early
        // But only after minimum bars have passed to give the trade time to develop
        let no_favorable_movement = self.position.max_favorable <= 0.0;
        let stale_time_limit = self.config.time_stop_ms / 2; // Half the normal time limit
        if hold_duration > stale_time_limit && hold_duration >= min_hold_for_stale && no_favorable_movement {
            return match self.position.state {
                PositionState::Long => Signal::ExitLong,
                PositionState::Short => Signal::ExitShort,
                PositionState::Flat => Signal::Hold,
            };
        }

        // 4b. Full time stop: exit regardless of PnL if trade isn't working
        // If we've held this long and it's not hitting take profit, get out
        if hold_duration > self.config.time_stop_ms {
            return match self.position.state {
                PositionState::Long => Signal::ExitLong,
                PositionState::Short => Signal::ExitShort,
                PositionState::Flat => Signal::Hold,
            };
        }

        // 5. Signal reversal check
        let score = self.compute_score(bar);
        match self.position.state {
            PositionState::Long => {
                if score < self.config.long_exit_threshold {
                    return Signal::ExitLong;
                }
            }
            PositionState::Short => {
                if score > self.config.short_exit_threshold {
                    return Signal::ExitShort;
                }
            }
            PositionState::Flat => {}
        }

        Signal::Hold
    }

    /// Determine the reason for an exit signal by checking which condition triggered.
    fn determine_exit_reason(&self, bar: &AggregatedBar) -> &'static str {
        if self.position.is_flat() || self.position.quantity == 0.0 {
            return "unknown";
        }

        let entry_value = self.position.entry_price * self.position.quantity;
        if entry_value == 0.0 {
            return "unknown";
        }

        let pnl_pct = (self.position.unrealized_pnl / entry_value) * 100.0;
        let max_fav_pct = (self.position.max_favorable / entry_value) * 100.0;

        // 1. Stop loss
        if pnl_pct <= -self.config.stop_loss_pct {
            return "stop_loss";
        }

        // 2. Take profit
        if pnl_pct >= self.config.take_profit_pct {
            return "take_profit";
        }

        // 3. Trailing stop
        if max_fav_pct >= self.config.trailing_stop_activation_pct {
            let trailing_stop_pct = self.config.trailing_stop_distance_pct;
            match self.position.state {
                PositionState::Long => {
                    let best_price =
                        self.position.entry_price + self.position.max_favorable / self.position.quantity;
                    let trailing_stop_price = best_price * (1.0 - trailing_stop_pct / 100.0);
                    if bar.close_price <= trailing_stop_price {
                        return "trailing_stop";
                    }
                }
                PositionState::Short => {
                    let worst_price =
                        self.position.entry_price - self.position.max_favorable / self.position.quantity;
                    let trailing_stop_price = worst_price * (1.0 + trailing_stop_pct / 100.0);
                    if bar.close_price >= trailing_stop_price {
                        return "trailing_stop";
                    }
                }
                PositionState::Flat => {}
            }
        }

        // 4. Time stop checks
        let hold_duration = bar.bar_end_ms.saturating_sub(self.position.entry_time_ms);
        let min_hold_for_stale = self.config.stale_exit_min_bars * self.config.bar_duration_ms;

        // 4a. Stale trade: exit faster if no favorable movement at all
        // But only after minimum bars have passed to give the trade time to develop
        let no_favorable_movement = self.position.max_favorable <= 0.0;
        let stale_time_limit = self.config.time_stop_ms / 2;
        if hold_duration > stale_time_limit && hold_duration >= min_hold_for_stale && no_favorable_movement {
            return "time_stop_stale";
        }

        // 4b. Full time stop: exit regardless of PnL
        if hold_duration > self.config.time_stop_ms {
            return "time_stop";
        }

        // 5. Signal reversal
        "signal_reversal"
    }

    /// Open a new position.
    ///
    /// Calculates position size based on risk config and opens the position
    /// at the specified price and time.
    /// Returns `true` if the position was opened successfully.
    /// Returns `false` if the Binance order failed (position not opened).
    fn open_position(&mut self, side: Side, price: f64, time_ms: u64) -> bool {
        let quantity = self.calculate_position_size(price);
        if quantity <= 0.0 {
            return false;
        }

        // If Binance client is set, place real order first
        if let Some(ref client) = self.binance_client {
            let binance_side = match side {
                Side::Buy => "BUY",
                Side::Sell => "SELL",
            };
            let symbol = self.config.symbol.to_uppercase();
            if client.place_market_order(&symbol, binance_side, quantity, false).is_err() {
                return false;
            }
        }

        let state = match side {
            Side::Buy => PositionState::Long,
            Side::Sell => PositionState::Short,
        };

        self.position = Position {
            state,
            entry_price: price,
            entry_time_ms: time_ms,
            quantity,
            unrealized_pnl: 0.0,
            max_favorable: 0.0,
            max_adverse: 0.0,
        };
        true
    }

    /// Close the current position and record the trade.
    ///
    /// Returns `true` if the position was closed successfully.
    /// Returns `false` if the Binance order failed (position not closed).
    pub fn close_position(&mut self, exit_price: f64, time_ms: u64, reason: &str) -> bool {
        if self.position.is_flat() {
            return false;
        }

        let quantity = self.position.quantity;

        // If Binance client is set, place real order first (opposite side to close)
        if let Some(ref client) = self.binance_client {
            let binance_side = match self.position.state {
                PositionState::Long => "SELL",
                PositionState::Short => "BUY",
                PositionState::Flat => return false,
            };
            let symbol = self.config.symbol.to_uppercase();
            if client.place_market_order(&symbol, binance_side, quantity, true).is_err() {
                return false;
            }
        }

        let entry_price = self.position.entry_price;
        let quantity = self.position.quantity;
        let entry_time_ms = self.position.entry_time_ms;

        let (pnl, pnl_pct, side) = match self.position.state {
            PositionState::Long => {
                let pnl = (exit_price - entry_price) * quantity;
                let pnl_pct = if entry_price > 0.0 {
                    ((exit_price - entry_price) / entry_price) * 100.0
                } else {
                    0.0
                };
                (pnl, pnl_pct, Side::Buy)
            }
            PositionState::Short => {
                let pnl = (entry_price - exit_price) * quantity;
                let pnl_pct = if entry_price > 0.0 {
                    ((entry_price - exit_price) / entry_price) * 100.0
                } else {
                    0.0
                };
                (pnl, pnl_pct, Side::Sell)
            }
            PositionState::Flat => return false,
        };

        let hold_duration_ms = time_ms.saturating_sub(entry_time_ms);

        let trade = TradeRecord {
            id: self.next_trade_id,
            symbol: self.config.symbol.clone(),
            side,
            entry_time_ms,
            exit_time_ms: time_ms,
            entry_price,
            exit_price,
            quantity,
            pnl,
            pnl_pct,
            hold_duration_ms,
            exit_reason: reason.to_string(),
        };

        self.next_trade_id += 1;
        self.equity += pnl;
        self.daily_pnl += pnl;

        // Track consecutive losses
        if pnl < 0.0 {
            self.consecutive_losses += 1;
        } else {
            self.consecutive_losses = 0;
        }

        self.last_exit_time_ms = time_ms;
        self.trade_history.push(trade);
        self.position = Position::flat();
        true
    }

    /// Calculate position size based on risk configuration.
    ///
    /// Formula: `quantity_usd = equity * (risk_per_trade_pct / 100) / (stop_loss_pct / 100)`
    ///
    /// The result is clamped to `max_position_size_usd`.
    fn calculate_position_size(&self, entry_price: f64) -> f64 {
        if entry_price <= 0.0 || self.config.stop_loss_pct <= 0.0 {
            return 0.0;
        }

        // Calculate size based on risk
        let quantity_usd = self.equity * (self.config.risk_per_trade_pct / 100.0)
            / (self.config.stop_loss_pct / 100.0);

        // Clamp to max position size
        let quantity_usd = quantity_usd.min(self.config.max_position_size_usd);

        // Convert to base units
        quantity_usd / entry_price
    }

    /// Check and enforce circuit breaker conditions.
    ///
    /// Disables trading if:
    /// - Daily P&L loss exceeds `daily_loss_limit_pct`
    /// - Consecutive losses exceed `max_consecutive_losses`
    fn check_circuit_breakers(&mut self) {
        // Check daily loss limit
        if self.equity > 0.0 {
            let daily_pnl_pct = (self.daily_pnl / self.equity) * 100.0;
            if daily_pnl_pct < -self.config.daily_loss_limit_pct {
                self.session_active = false;
            }
        }

        // Check consecutive losses
        if self.consecutive_losses >= self.config.max_consecutive_losses {
            self.session_active = false;
        }
    }

    /// Returns a reference to the current position.
    pub fn position(&self) -> &Position {
        &self.position
    }

    /// Returns the trade history.
    pub fn trade_history(&self) -> &[TradeRecord] {
        &self.trade_history
    }

    /// Returns the current equity.
    pub fn equity(&self) -> f64 {
        self.equity
    }

    /// Returns the daily P&L.
    pub fn daily_pnl(&self) -> f64 {
        self.daily_pnl
    }

    /// Returns whether the trading session is active.
    ///
    /// Returns `false` if a circuit breaker has been triggered.
    pub fn is_session_active(&self) -> bool {
        self.session_active
    }

    /// Returns the current count of consecutive losses.
    pub fn consecutive_losses(&self) -> usize {
        self.consecutive_losses
    }

    /// Computes the win rate from trade history.
    ///
    /// Returns 0.0 if there are no trades.
    #[allow(dead_code)]
    pub fn win_rate(&self) -> f64 {
        if self.trade_history.is_empty() {
            return 0.0;
        }
        let winners = self.trade_history.iter().filter(|t| t.pnl > 0.0).count();
        (winners as f64 / self.trade_history.len() as f64) * 100.0
    }

    /// Computes the profit factor (gross profit / gross loss).
    ///
    /// Returns `f64::INFINITY` if there are no losses.
    /// Returns 0.0 if there are no trades or no profits.
    #[allow(dead_code)]
    pub fn profit_factor(&self) -> f64 {
        let gross_profit: f64 = self.trade_history.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
        let gross_loss: f64 = self.trade_history.iter().filter(|t| t.pnl < 0.0).map(|t| t.pnl.abs()).sum();

        if gross_loss == 0.0 {
            if gross_profit > 0.0 {
                return f64::INFINITY;
            }
            return 0.0;
        }
        gross_profit / gross_loss
    }

    /// Computes the total realized P&L.
    #[allow(dead_code)]
    pub fn total_pnl(&self) -> f64 {
        self.trade_history.iter().map(|t| t.pnl).sum()
    }

    /// Computes the maximum drawdown percentage from trade history.
    ///
    /// Tracks the equity curve through trades and finds the largest peak-to-trough
    /// decline as a percentage.
    #[allow(dead_code)]
    pub fn max_drawdown_pct(&self) -> f64 {
        if self.trade_history.is_empty() {
            return 0.0;
        }

        let starting_equity = self.equity - self.total_pnl();
        let mut peak = starting_equity;
        let mut max_dd = 0.0;
        let mut current_equity = starting_equity;

        for trade in &self.trade_history {
            current_equity += trade.pnl;
            if current_equity > peak {
                peak = current_equity;
            }
            if peak > 0.0 {
                let dd = (peak - current_equity) / peak * 100.0;
                if dd > max_dd {
                    max_dd = dd;
                }
            }
        }

        max_dd
    }

    /// Reset daily state for a new trading session.
    ///
    /// Resets daily P&L and circuit breaker state while preserving
    /// trade history and current position.
    #[allow(dead_code)]
    pub fn reset_daily(&mut self, current_equity: f64) {
        self.equity = current_equity;
        self.daily_start_equity = current_equity;
        self.daily_pnl = 0.0;
        self.consecutive_losses = 0;
        self.session_active = true;
    }

    /// Reset all state for a completely new trading session.
    ///
    /// Clears trade history, closes any open position (without recording),
    /// and resets all counters.
    #[allow(dead_code)]
    pub fn reset_all(&mut self, equity: f64) {
        self.position = Position::flat();
        self.trade_history.clear();
        self.next_trade_id = 1;
        self.equity = equity;
        self.daily_start_equity = equity;
        self.daily_pnl = 0.0;
        self.consecutive_losses = 0;
        self.last_exit_time_ms = 0;
        self.session_active = true;
        self.last_bar = None;
    }
}

// =============================================================================
// TradeLogger
// =============================================================================

/// CSV logger for trade records.
///
/// Appends completed trades to a CSV file with a header row on first write.
/// Useful for post-analysis and record keeping.
#[allow(dead_code)]
pub struct TradeLogger {
    writer: Option<File>,
}

impl TradeLogger {
    /// Opens a trade log file for writing.
    ///
    /// Creates the file if it doesn't exist, appends if it does.
    /// The header row is written on first open of a new file.
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file_exists = std::path::Path::new(path).exists();

        let file = OpenOptions::new().create(true).append(true).open(path)?;

        let mut logger = Self { writer: Some(file) };

        // Write header if file is new
        if !file_exists {
            logger.write_header()?;
        }

        Ok(logger)
    }

    /// Writes the CSV header row.
    fn write_header(&mut self) -> std::io::Result<()> {
        if let Some(ref mut w) = self.writer {
            writeln!(
                w,
                "id,symbol,side,entry_time_ms,exit_time_ms,entry_price,exit_price,quantity,pnl,pnl_pct,hold_duration_ms,exit_reason"
            )?;
        }
        Ok(())
    }

    /// Writes a trade record to the CSV file.
    pub fn write_trade(&mut self, trade: &TradeRecord) -> std::io::Result<()> {
        if let Some(ref mut w) = self.writer {
            let side_str = match trade.side {
                Side::Buy => "Buy",
                Side::Sell => "Sell",
            };
            writeln!(
                w,
                "{},{},{},{},{},{:.8},{:.8},{:.8},{:.8},{:.8},{},{}",
                trade.id,
                trade.symbol,
                side_str,
                trade.entry_time_ms,
                trade.exit_time_ms,
                trade.entry_price,
                trade.exit_price,
                trade.quantity,
                trade.pnl,
                trade.pnl_pct,
                trade.hold_duration_ms,
                trade.exit_reason
            )?;
        }
        Ok(())
    }
}

// =============================================================================
// PerformanceReport
// =============================================================================

/// Comprehensive performance report computed from trade history.
///
/// Contains win/loss statistics, P&L metrics, risk-adjusted returns,
/// and streak analysis.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[allow(dead_code)]
pub struct PerformanceReport {
    /// Total number of trades.
    pub total_trades: usize,
    /// Number of winning trades.
    pub winning_trades: usize,
    /// Number of losing trades.
    pub losing_trades: usize,
    /// Win rate as a percentage (0-100).
    pub win_rate: f64,
    /// Total realized P&L in quote currency.
    pub total_pnl: f64,
    /// Total realized P&L as a percentage of starting equity.
    pub total_pnl_pct: f64,
    /// Average winning trade P&L.
    pub avg_win: f64,
    /// Average losing trade P&L (negative value).
    pub avg_loss: f64,
    /// Profit factor (gross profit / gross loss).
    pub profit_factor: f64,
    /// Maximum drawdown as a percentage.
    pub max_drawdown_pct: f64,
    /// Simplified Sharpe ratio (assuming risk-free rate = 0).
    pub sharpe_ratio: f64,
    /// Average hold duration in milliseconds.
    pub avg_hold_duration_ms: u64,
    /// Best trade P&L.
    pub best_trade_pnl: f64,
    /// Worst trade P&L.
    pub worst_trade_pnl: f64,
    /// Longest winning streak.
    pub longest_win_streak: usize,
    /// Longest losing streak.
    pub longest_loss_streak: usize,
}

impl PerformanceReport {
    /// Generates a performance report from trade history.
    ///
    /// # Arguments
    ///
    /// * `trades` - Slice of completed trade records.
    /// * `starting_equity` - Initial equity before any trades.
    pub fn from_trades(trades: &[TradeRecord], starting_equity: f64) -> Self {
        if trades.is_empty() {
            return Self {
                total_trades: 0,
                winning_trades: 0,
                losing_trades: 0,
                win_rate: 0.0,
                total_pnl: 0.0,
                total_pnl_pct: 0.0,
                avg_win: 0.0,
                avg_loss: 0.0,
                profit_factor: 0.0,
                max_drawdown_pct: 0.0,
                sharpe_ratio: 0.0,
                avg_hold_duration_ms: 0,
                best_trade_pnl: 0.0,
                worst_trade_pnl: 0.0,
                longest_win_streak: 0,
                longest_loss_streak: 0,
            };
        }

        let total_trades = trades.len();
        let winning_trades = trades.iter().filter(|t| t.pnl > 0.0).count();
        let losing_trades = trades.iter().filter(|t| t.pnl < 0.0).count();
        let win_rate = (winning_trades as f64 / total_trades as f64) * 100.0;

        let total_pnl: f64 = trades.iter().map(|t| t.pnl).sum();
        let total_pnl_pct = if starting_equity > 0.0 {
            (total_pnl / starting_equity) * 100.0
        } else {
            0.0
        };

        let gross_profit: f64 = trades.iter().filter(|t| t.pnl > 0.0).map(|t| t.pnl).sum();
        let gross_loss: f64 = trades.iter().filter(|t| t.pnl < 0.0).map(|t| t.pnl.abs()).sum();

        let avg_win = if winning_trades > 0 {
            gross_profit / winning_trades as f64
        } else {
            0.0
        };

        let avg_loss = if losing_trades > 0 {
            -(gross_loss / losing_trades as f64) // Negative to indicate loss
        } else {
            0.0
        };

        let profit_factor = if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        };

        // Max drawdown calculation
        let mut peak = starting_equity;
        let mut max_dd = 0.0;
        let mut current_equity = starting_equity;

        for trade in trades {
            current_equity += trade.pnl;
            if current_equity > peak {
                peak = current_equity;
            }
            if peak > 0.0 {
                let dd = (peak - current_equity) / peak * 100.0;
                if dd > max_dd {
                    max_dd = dd;
                }
            }
        }

        // Simplified Sharpe ratio (annualized assuming bars are 1 minute, 24/7 trading)
        let pnl_values: Vec<f64> = trades.iter().map(|t| t.pnl).collect();
        let mean_pnl = pnl_values.iter().sum::<f64>() / pnl_values.len() as f64;
        let variance = pnl_values
            .iter()
            .map(|p| (p - mean_pnl).powi(2))
            .sum::<f64>()
            / pnl_values.len() as f64;
        let std_dev = variance.sqrt();

        // Annualization factor: ~525,600 minutes per year
        let annualization_factor = (525_600.0_f64).sqrt();
        let sharpe_ratio = if std_dev > 0.0 {
            (mean_pnl / std_dev) * annualization_factor
        } else {
            0.0
        };

        let avg_hold_duration_ms = if total_trades > 0 {
            trades.iter().map(|t| t.hold_duration_ms).sum::<u64>() / total_trades as u64
        } else {
            0
        };

        let best_trade_pnl = trades.iter().map(|t| t.pnl).fold(f64::NEG_INFINITY, f64::max);
        let worst_trade_pnl = trades.iter().map(|t| t.pnl).fold(f64::INFINITY, f64::min);

        // Streak calculation
        let mut longest_win_streak = 0;
        let mut longest_loss_streak = 0;
        let mut current_win_streak = 0;
        let mut current_loss_streak = 0;

        for trade in trades {
            if trade.pnl > 0.0 {
                current_win_streak += 1;
                current_loss_streak = 0;
                longest_win_streak = longest_win_streak.max(current_win_streak);
            } else if trade.pnl < 0.0 {
                current_loss_streak += 1;
                current_win_streak = 0;
                longest_loss_streak = longest_loss_streak.max(current_loss_streak);
            } else {
                // Breakeven trade resets both streaks
                current_win_streak = 0;
                current_loss_streak = 0;
            }
        }

        Self {
            total_trades,
            winning_trades,
            losing_trades,
            win_rate,
            total_pnl,
            total_pnl_pct,
            avg_win,
            avg_loss,
            profit_factor,
            max_drawdown_pct: max_dd,
            sharpe_ratio,
            avg_hold_duration_ms,
            best_trade_pnl,
            worst_trade_pnl,
            longest_win_streak,
            longest_loss_streak,
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to create a sample with specific values
    fn make_sample(timestamp_ms: u64, mid_price: f64, ob_imbalance: f64, fk_net_direction: f64,
                   aggression_shift: f64, absorption_score: f64, buy_volume_pct: f64) -> SignalSample {
        SignalSample {
            timestamp_ms,
            symbol: "BTCUSDT".to_string(),
            mid_price,
            spread_bps: 1.0,
            ob_imbalance,
            ob_bid_depth_near: 100.0,
            ob_ask_depth_near: 100.0,
            fk_buy_fill_qty: 50.0,
            fk_buy_kill_qty: 10.0,
            fk_sell_fill_qty: 50.0,
            fk_sell_kill_qty: 10.0,
            fk_net_direction,
            fk_buy_signed_log_ratio: None,
            fk_sell_signed_log_ratio: None,
            cum_fill_qty: 100.0,
            cum_kill_qty: 20.0,
            cum_net_qty: 0.0,
            cum_ratio: 1.0,
            impact_buy_slippage_bps: 0.5,
            impact_sell_slippage_bps: 0.5,
            impact_buy_levels_consumed: 1,
            impact_sell_levels_consumed: 1,
            trade_count_1s: 10,
            trade_volume_1s: 1000.0,
            buy_volume_pct_1s: buy_volume_pct,
            absorption_score,
            aggression_shift,
            vd_buy_levels: 0,
            vd_sell_levels: 0,
        }
    }

    /// Helper to create a bullish bar that should trigger long entry
    fn make_bullish_bar(bar_end_ms: u64) -> AggregatedBar {
        AggregatedBar {
            bar_start_ms: bar_end_ms - 60_000,
            bar_end_ms,
            sample_count: 60,
            open_price: 50000.0,
            close_price: 50100.0,
            high_price: 50150.0,
            low_price: 49950.0,
            avg_mid_price: 50050.0,
            ob_imbalance_mean: 0.5,
            ob_imbalance_max: 0.6,
            ob_imbalance_min: 0.4,
            ob_imbalance_trend: 0.01,
            avg_spread_bps: 1.0,
            cum_net_qty_final: 100.0,
            cum_ratio_final: 1.0,
            fk_net_direction_mean: 0.5,
            fk_net_direction_max: 0.6,
            fk_net_direction_min: 0.4,
            fk_net_direction_final: 0.5,
            fk_buy_fill_total: 3000.0,
            fk_sell_fill_total: 1000.0,
            fk_buy_kill_total: 500.0,
            fk_sell_kill_total: 200.0,
            aggression_shift_mean: 1.5,
            aggression_shift_max: 2.0,
            aggression_shift_min: 1.0,
            aggression_shift_final: 1.5,
            absorption_score_max: 0.71,
            absorption_score_mean: 0.5,
            absorption_score_final: 0.6,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 0,
            avg_impact_buy_slippage_bps: 0.5,
            avg_impact_sell_slippage_bps: 1.0,
            total_trade_volume: 60000.0,
            avg_buy_volume_pct: 80.0,
            avg_trade_count_per_sec: 10.0,
        }
    }

    /// Helper to create a bearish bar that should trigger short entry
    fn make_bearish_bar(bar_end_ms: u64) -> AggregatedBar {
        AggregatedBar {
            bar_start_ms: bar_end_ms - 60_000,
            bar_end_ms,
            sample_count: 60,
            open_price: 50000.0,
            close_price: 49900.0,
            high_price: 50050.0,
            low_price: 49850.0,
            avg_mid_price: 49950.0,
            ob_imbalance_mean: -0.5,
            ob_imbalance_max: -0.4,
            ob_imbalance_min: -0.6,
            ob_imbalance_trend: -0.01,
            avg_spread_bps: 1.0,
            cum_net_qty_final: -100.0,
            cum_ratio_final: 0.5,
            fk_net_direction_mean: -0.5,
            fk_net_direction_max: -0.4,
            fk_net_direction_min: -0.6,
            fk_net_direction_final: -0.5,
            fk_buy_fill_total: 1000.0,
            fk_sell_fill_total: 3000.0,
            fk_buy_kill_total: 200.0,
            fk_sell_kill_total: 500.0,
            aggression_shift_mean: -1.5,
            aggression_shift_max: -1.0,
            aggression_shift_min: -2.0,
            aggression_shift_final: -1.5,
            absorption_score_max: 0.71,
            absorption_score_mean: 0.5,
            absorption_score_final: 0.6,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 0,
            avg_impact_buy_slippage_bps: 1.0,
            avg_impact_sell_slippage_bps: 0.5,
            total_trade_volume: 60000.0,
            avg_buy_volume_pct: 20.0,
            avg_trade_count_per_sec: 10.0,
        }
    }

    #[test]
    fn test_bar_aggregation() {
        let mut aggregator = SignalBarAggregator::new(60_000);

        // Push 60 samples spanning 60 seconds
        for i in 0..60 {
            let sample = make_sample(
                i * 1000, // 0, 1000, 2000, ... 59000 ms
                50000.0 + i as f64 * 10.0, // Rising price
                0.1 * (i as f64 / 59.0),   // Rising imbalance
                0.2,
                0.5,
                0.3,
                60.0,
            );
            let result = aggregator.push(sample);
            // No bar should complete until we push a sample at or after 60000ms
            assert!(result.is_none(), "Bar should not complete at sample {}", i);
        }

        // Push one more sample to trigger bar completion
        let trigger_sample = make_sample(60_000, 50600.0, 0.2, 0.2, 0.5, 0.3, 60.0);
        let bar = aggregator.push(trigger_sample).expect("Bar should complete");

        // Verify bar fields
        assert_eq!(bar.sample_count, 60);
        assert_eq!(bar.bar_start_ms, 0);
        assert_eq!(bar.bar_end_ms, 59_000);
        assert_eq!(bar.open_price, 50000.0);
        assert_eq!(bar.close_price, 50590.0);
        assert!((bar.high_price - 50590.0).abs() < 0.01);
        assert!((bar.low_price - 50000.0).abs() < 0.01);
        assert!(bar.ob_imbalance_trend > 0.0, "Trend should be positive for rising imbalance");
        assert_eq!(bar.fk_buy_fill_total, 60.0 * 50.0);
    }

    #[test]
    fn test_bar_flush() {
        let mut aggregator = SignalBarAggregator::new(60_000);

        // Push only 10 samples (partial bar)
        for i in 0..10 {
            let sample = make_sample(i * 1000, 50000.0, 0.0, 0.0, 0.0, 0.0, 50.0);
            aggregator.push(sample);
        }

        // Flush should return partial bar
        let bar = aggregator.flush().expect("Flush should return partial bar");
        assert_eq!(bar.sample_count, 10);
        assert_eq!(bar.bar_start_ms, 0);
        assert_eq!(bar.bar_end_ms, 9_000);

        // Second flush should return None
        assert!(aggregator.flush().is_none());
    }

    #[test]
    fn test_position_flat() {
        let pos = Position::flat();

        assert!(pos.is_flat());
        assert_eq!(pos.state, PositionState::Flat);
        assert_eq!(pos.entry_price, 0.0);
        assert_eq!(pos.quantity, 0.0);
        assert_eq!(pos.unrealized_pnl, 0.0);
        assert_eq!(pos.max_favorable, 0.0);
        assert_eq!(pos.max_adverse, 0.0);
    }

    #[test]
    fn test_position_update_market() {
        let mut pos = Position {
            state: PositionState::Long,
            entry_price: 50000.0,
            entry_time_ms: 0,
            quantity: 1.0,
            unrealized_pnl: 0.0,
            max_favorable: 0.0,
            max_adverse: 0.0,
        };

        // Price goes up
        pos.update_market(50500.0);
        assert!((pos.unrealized_pnl - 500.0).abs() < 0.01);
        assert!((pos.max_favorable - 500.0).abs() < 0.01);
        assert_eq!(pos.max_adverse, 0.0);

        // Price goes down below entry
        pos.update_market(49500.0);
        assert!((pos.unrealized_pnl - (-500.0)).abs() < 0.01);
        assert!((pos.max_favorable - 500.0).abs() < 0.01); // Should retain max
        assert!((pos.max_adverse - (-500.0)).abs() < 0.01);

        // Test short position
        let mut short_pos = Position {
            state: PositionState::Short,
            entry_price: 50000.0,
            entry_time_ms: 0,
            quantity: 1.0,
            unrealized_pnl: 0.0,
            max_favorable: 0.0,
            max_adverse: 0.0,
        };

        // Price goes down (profit for short)
        short_pos.update_market(49500.0);
        assert!((short_pos.unrealized_pnl - 500.0).abs() < 0.01);
        assert!((short_pos.max_favorable - 500.0).abs() < 0.01);
    }

    #[test]
    fn test_strategy_long_entry() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);

        let bar = make_bullish_bar(60_000);
        let signal = engine.on_bar(&bar);

        assert_eq!(signal, Signal::GoLong, "Should trigger GoLong for bullish bar");
    }

    #[test]
    fn test_strategy_short_entry() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);

        let bar = make_bearish_bar(60_000);
        let signal = engine.on_bar(&bar);

        assert_eq!(signal, Signal::GoShort, "Should trigger GoShort for bearish bar");
    }

    #[test]
    fn test_strategy_stop_loss() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            stop_loss_pct: 0.15,
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);

        // Enter long
        let entry_bar = make_bullish_bar(60_000);
        let signal = engine.on_bar(&entry_bar);
        assert_eq!(signal, Signal::GoLong);

        // Manually open position (engine.on_bar doesn't actually open positions,
        // it just returns signals - the caller would handle execution)
        engine.open_position(Side::Buy, 50000.0, 60_000);
        assert_eq!(engine.position().state, PositionState::Long);

        // Create a bar that triggers stop loss
        // stop_loss_pct = 0.15, so need price drop of 0.15% from 50000 = 75 points
        let stop_loss_bar = AggregatedBar {
            bar_start_ms: 60_000,
            bar_end_ms: 120_000,
            sample_count: 60,
            open_price: 50000.0,
            close_price: 49920.0, // Drop of 80 points = 0.16% > 0.15% stop loss
            high_price: 50000.0,
            low_price: 49900.0,
            avg_mid_price: 49950.0,
            // Other fields don't matter for stop loss check
            ob_imbalance_mean: 0.0,
            ob_imbalance_max: 0.0,
            ob_imbalance_min: 0.0,
            ob_imbalance_trend: 0.0,
            avg_spread_bps: 0.0,
            cum_net_qty_final: 0.0,
            cum_ratio_final: 0.0,
            fk_net_direction_mean: 0.0,
            fk_net_direction_max: 0.0,
            fk_net_direction_min: 0.0,
            fk_net_direction_final: 0.0,
            fk_buy_fill_total: 0.0,
            fk_sell_fill_total: 0.0,
            fk_buy_kill_total: 0.0,
            fk_sell_kill_total: 0.0,
            aggression_shift_mean: 0.0,
            aggression_shift_max: 0.0,
            aggression_shift_min: 0.0,
            aggression_shift_final: 0.0,
            absorption_score_max: 0.0,
            absorption_score_mean: 0.0,
            absorption_score_final: 0.0,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 0,
            avg_impact_buy_slippage_bps: 0.0,
            avg_impact_sell_slippage_bps: 0.0,
            total_trade_volume: 0.0,
            avg_buy_volume_pct: 50.0,
            avg_trade_count_per_sec: 0.0,
        };

        let signal = engine.on_bar(&stop_loss_bar);
        assert_eq!(signal, Signal::ExitLong, "Should trigger stop loss exit");
    }

    #[test]
    fn test_strategy_take_profit() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            take_profit_pct: 0.30,
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);

        // Enter long
        engine.open_position(Side::Buy, 50000.0, 60_000);

        // Create a bar that triggers take profit
        // take_profit_pct = 0.30, so need price rise of 0.30% from 50000 = 150 points
        let tp_bar = AggregatedBar {
            bar_start_ms: 60_000,
            bar_end_ms: 120_000,
            sample_count: 60,
            open_price: 50000.0,
            close_price: 50160.0, // Rise of 160 points = 0.32% > 0.30% take profit
            high_price: 50200.0,
            low_price: 50000.0,
            avg_mid_price: 50080.0,
            ob_imbalance_mean: 0.5,
            ob_imbalance_max: 0.6,
            ob_imbalance_min: 0.4,
            ob_imbalance_trend: 0.0,
            avg_spread_bps: 1.0,
            cum_net_qty_final: 100.0,
            cum_ratio_final: 1.0,
            fk_net_direction_mean: 0.5,
            fk_net_direction_max: 0.6,
            fk_net_direction_min: 0.4,
            fk_net_direction_final: 0.5,
            fk_buy_fill_total: 1000.0,
            fk_sell_fill_total: 500.0,
            fk_buy_kill_total: 100.0,
            fk_sell_kill_total: 50.0,
            aggression_shift_mean: 0.5,
            aggression_shift_max: 0.6,
            aggression_shift_min: 0.4,
            aggression_shift_final: 0.5,
            absorption_score_max: 0.5,
            absorption_score_mean: 0.4,
            absorption_score_final: 0.5,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 0,
            avg_impact_buy_slippage_bps: 0.5,
            avg_impact_sell_slippage_bps: 0.5,
            total_trade_volume: 10000.0,
            avg_buy_volume_pct: 70.0,
            avg_trade_count_per_sec: 5.0,
        };

        let signal = engine.on_bar(&tp_bar);
        assert_eq!(signal, Signal::ExitLong, "Should trigger take profit exit");
    }

    #[test]
    fn test_strategy_circuit_breaker() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            daily_loss_limit_pct: 2.0,
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);

        // Simulate losses that exceed daily limit
        // Need daily_pnl / equity * 100 < -2.0
        // With equity 10000, need daily_pnl < -200

        // Manually create losing trades to trigger circuit breaker
        for i in 0..5 {
            engine.open_position(Side::Buy, 50000.0, i * 60_000);
            engine.close_position(49500.0, (i + 1) * 60_000, "test_loss");
        }

        // Each trade loses (50000 - 49500) * quantity
        // quantity = 10000 * (0.5/100) / (0.15/100) = 33333.33 base units (clamped to 10000/50000 = 0.2)
        // Actually: quantity_usd = 10000 * 0.005 / 0.0015 = 33333, clamped to 10000
        // quantity = 10000 / 50000 = 0.2
        // Each loss = 500 * 0.2 = 100
        // After 5 trades: daily_pnl = -500, which is -5% of 10000

        // Trigger circuit breaker check via on_bar (check_circuit_breakers is private)
        let bar = make_bullish_bar(400_000);
        let signal = engine.on_bar(&bar);

        assert!(!engine.is_session_active(), "Session should be inactive after exceeding daily loss limit");
        assert_eq!(signal, Signal::Hold, "Should not enter when circuit breaker is active");
    }

    #[test]
    fn test_performance_report() {
        let trades = vec![
            TradeRecord {
                id: 1,
                symbol: "BTCUSDT".to_string(),
                side: Side::Buy,
                entry_time_ms: 0,
                exit_time_ms: 60_000,
                entry_price: 50000.0,
                exit_price: 50500.0,
                quantity: 0.1,
                pnl: 50.0,
                pnl_pct: 1.0,
                hold_duration_ms: 60_000,
                exit_reason: "take_profit".to_string(),
            },
            TradeRecord {
                id: 2,
                symbol: "BTCUSDT".to_string(),
                side: Side::Buy,
                entry_time_ms: 120_000,
                exit_time_ms: 180_000,
                entry_price: 50000.0,
                exit_price: 49750.0,
                quantity: 0.1,
                pnl: -25.0,
                pnl_pct: -0.5,
                hold_duration_ms: 60_000,
                exit_reason: "stop_loss".to_string(),
            },
            TradeRecord {
                id: 3,
                symbol: "BTCUSDT".to_string(),
                side: Side::Sell,
                entry_time_ms: 240_000,
                exit_time_ms: 300_000,
                entry_price: 50000.0,
                exit_price: 49500.0,
                quantity: 0.1,
                pnl: 50.0,
                pnl_pct: 1.0,
                hold_duration_ms: 60_000,
                exit_reason: "take_profit".to_string(),
            },
        ];

        let report = PerformanceReport::from_trades(&trades, 10_000.0);

        assert_eq!(report.total_trades, 3);
        assert_eq!(report.winning_trades, 2);
        assert_eq!(report.losing_trades, 1);
        assert!((report.win_rate - 66.666).abs() < 0.1);
        assert!((report.total_pnl - 75.0).abs() < 0.01);
        assert!((report.total_pnl_pct - 0.75).abs() < 0.01);
        assert!((report.avg_win - 50.0).abs() < 0.01);
        assert!((report.avg_loss - (-25.0)).abs() < 0.01);
        assert!((report.profit_factor - 4.0).abs() < 0.01);
        assert_eq!(report.best_trade_pnl, 50.0);
        assert_eq!(report.worst_trade_pnl, -25.0);
        assert_eq!(report.longest_win_streak, 1); // Win, Loss, Win
        assert_eq!(report.longest_loss_streak, 1);
        assert_eq!(report.avg_hold_duration_ms, 60_000);
    }

    #[test]
    fn test_linear_slope() {
        // Test with constant values (slope should be 0)
        let constant = vec![5.0, 5.0, 5.0, 5.0, 5.0];
        let slope = SignalBarAggregator::linear_slope(&constant);
        assert!(slope.abs() < 1e-10, "Constant values should have zero slope");

        // Test with linearly increasing values (slope should be 1)
        let increasing: Vec<f64> = (0..5).map(|i| i as f64).collect();
        let slope = SignalBarAggregator::linear_slope(&increasing);
        assert!((slope - 1.0).abs() < 1e-10, "Linear increase should have slope 1");

        // Test with linearly decreasing values (slope should be -1)
        let decreasing: Vec<f64> = (0..5).map(|i| (4 - i) as f64).collect();
        let slope = SignalBarAggregator::linear_slope(&decreasing);
        assert!((slope - (-1.0)).abs() < 1e-10, "Linear decrease should have slope -1");

        // Test with steeper slope (2x)
        let steep: Vec<f64> = (0..5).map(|i| 2.0 * i as f64).collect();
        let slope = SignalBarAggregator::linear_slope(&steep);
        assert!((slope - 2.0).abs() < 1e-10, "2x linear should have slope 2");

        // Test with single value (should return 0)
        let single = vec![42.0];
        let slope = SignalBarAggregator::linear_slope(&single);
        assert_eq!(slope, 0.0, "Single value should have zero slope");

        // Test with empty vec (should return 0)
        let empty: Vec<f64> = vec![];
        let slope = SignalBarAggregator::linear_slope(&empty);
        assert_eq!(slope, 0.0, "Empty vec should have zero slope");
    }

    #[test]
    fn test_on_bar_opens_position_and_records_trade() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);

        // Entry: bullish bar should open a long position
        let entry_bar = make_bullish_bar(60_000);
        let signal = engine.on_bar(&entry_bar);
        assert_eq!(signal, Signal::GoLong);
        assert!(!engine.position().is_flat(), "Position should be open after GoLong");
        assert_eq!(engine.position().state, PositionState::Long);
        assert_eq!(engine.position().entry_price, entry_bar.close_price);
        assert_eq!(engine.trade_history().len(), 0, "No trade recorded until exit");

        // Exit: bearish bar should close the long (score < long_exit_threshold = -0.2)
        let exit_bar = make_bearish_bar(90_000);
        let exit_signal = engine.on_bar(&exit_bar);
        assert_eq!(exit_signal, Signal::ExitLong);
        assert!(engine.position().is_flat(), "Position should be flat after exit");
        assert_eq!(engine.trade_history().len(), 1, "One trade should be recorded");

        let trade = &engine.trade_history()[0];
        assert_eq!(trade.side, Side::Buy);
        assert_eq!(trade.entry_price, entry_bar.close_price);
        assert_eq!(trade.exit_price, exit_bar.close_price);
        assert!(!trade.exit_reason.is_empty());
    }

    #[test]
    fn test_on_bar_short_entry_and_exit() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);

        // Entry: bearish bar should open a short position
        let entry_bar = make_bearish_bar(60_000);
        let signal = engine.on_bar(&entry_bar);
        assert_eq!(signal, Signal::GoShort);
        assert_eq!(engine.position().state, PositionState::Short);

        // Exit: bullish bar should close the short (score > short_exit_threshold = 0.2)
        let exit_bar = make_bullish_bar(90_000);
        let exit_signal = engine.on_bar(&exit_bar);
        assert_eq!(exit_signal, Signal::ExitShort);
        assert!(engine.position().is_flat());
        assert_eq!(engine.trade_history().len(), 1);
        assert_eq!(engine.trade_history()[0].side, Side::Sell);
    }

    // =========================================================================
    // Tests for configurable thresholds
    // =========================================================================

    #[test]
    fn test_timeframe_presets_have_correct_bar_duration() {
        let config_15s = StrategyConfig::for_15s();
        let config_30s = StrategyConfig::for_30s();
        let config_1m = StrategyConfig::for_1m();

        assert_eq!(config_15s.bar_duration_ms, 15_000);
        assert_eq!(config_30s.bar_duration_ms, 30_000);
        assert_eq!(config_1m.bar_duration_ms, 60_000);
    }

    #[test]
    fn test_timeframe_presets_weights_sum_to_one() {
        let config_15s = StrategyConfig::for_15s();
        let config_30s = StrategyConfig::for_30s();
        let config_1m = StrategyConfig::for_1m();

        let sum_15s = config_15s.weight_fk_direction + config_15s.weight_aggression_shift
            + config_15s.weight_ob_imbalance + config_15s.weight_absorption
            + config_15s.weight_buy_volume_pct;
        let sum_30s = config_30s.weight_fk_direction + config_30s.weight_aggression_shift
            + config_30s.weight_ob_imbalance + config_30s.weight_absorption
            + config_30s.weight_buy_volume_pct;
        let sum_1m = config_1m.weight_fk_direction + config_1m.weight_aggression_shift
            + config_1m.weight_ob_imbalance + config_1m.weight_absorption
            + config_1m.weight_buy_volume_pct;

        assert!((sum_15s - 1.0).abs() < 1e-10, "15s weights should sum to 1.0, got {}", sum_15s);
        assert!((sum_30s - 1.0).abs() < 1e-10, "30s weights should sum to 1.0, got {}", sum_30s);
        assert!((sum_1m - 1.0).abs() < 1e-10, "1m weights should sum to 1.0, got {}", sum_1m);
    }

    #[test]
    fn test_timeframe_presets_progressive_thresholds() {
        let config_15s = StrategyConfig::for_15s();
        let config_30s = StrategyConfig::for_30s();
        let config_1m = StrategyConfig::for_1m();

        // 15s should have widest entry thresholds (most noise filtering)
        assert!(config_15s.long_entry_threshold > config_30s.long_entry_threshold,
            "15s entry threshold ({}) should be > 30s ({})",
            config_15s.long_entry_threshold, config_30s.long_entry_threshold);
        assert!(config_30s.long_entry_threshold > config_1m.long_entry_threshold,
            "30s entry threshold ({}) should be > 1m ({})",
            config_30s.long_entry_threshold, config_1m.long_entry_threshold);

        // 15s should have tightest exit thresholds
        assert!(config_15s.long_exit_threshold.abs() < config_30s.long_exit_threshold.abs(),
            "15s exit threshold ({}) should be tighter than 30s ({})",
            config_15s.long_exit_threshold, config_30s.long_exit_threshold);

        // 15s should have highest void level requirement
        assert!(config_15s.min_void_levels_for_reversion > config_30s.min_void_levels_for_reversion,
            "15s void levels ({}) should be > 30s ({})",
            config_15s.min_void_levels_for_reversion, config_30s.min_void_levels_for_reversion);
        assert!(config_30s.min_void_levels_for_reversion > config_1m.min_void_levels_for_reversion,
            "30s void levels ({}) should be > 1m ({})",
            config_30s.min_void_levels_for_reversion, config_1m.min_void_levels_for_reversion);

        // 1m should have widest stop loss
        assert!(config_1m.stop_loss_pct > config_30s.stop_loss_pct,
            "1m stop loss ({}) should be > 30s ({})",
            config_1m.stop_loss_pct, config_30s.stop_loss_pct);
        assert!(config_30s.stop_loss_pct > config_15s.stop_loss_pct,
            "30s stop loss ({}) should be > 15s ({})",
            config_30s.stop_loss_pct, config_15s.stop_loss_pct);
    }

    #[test]
    fn test_for_bar_duration_selects_correct_preset() {
        let config_10s = StrategyConfig::for_bar_duration(10_000);
        assert_eq!(config_10s.bar_duration_ms, 15_000, "10s should map to 15s preset");

        let config_15s = StrategyConfig::for_bar_duration(15_000);
        assert_eq!(config_15s.bar_duration_ms, 15_000);

        let config_20s = StrategyConfig::for_bar_duration(20_000);
        assert_eq!(config_20s.bar_duration_ms, 15_000, "20s should map to 15s preset");

        let config_30s = StrategyConfig::for_bar_duration(30_000);
        assert_eq!(config_30s.bar_duration_ms, 30_000);

        let config_45s = StrategyConfig::for_bar_duration(45_000);
        assert_eq!(config_45s.bar_duration_ms, 30_000, "45s should map to 30s preset");

        let config_60s = StrategyConfig::for_bar_duration(60_000);
        assert_eq!(config_60s.bar_duration_ms, 60_000);

        let config_120s = StrategyConfig::for_bar_duration(120_000);
        assert_eq!(config_120s.bar_duration_ms, 60_000, "120s should map to 1m preset");
    }

    #[test]
    fn test_void_confirmation_requires_all_conditions() {
        // Create config with strict void confirmation thresholds
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            min_void_levels_for_reversion: 25,
            void_price_recovery_pct: 1.001, // 0.1% recovery required
            void_ob_imbalance_threshold: 0.1,
            void_spread_tightness_pct: 0.5, // Very tight spread required
            max_spread_bps: 10.0,
            ..Default::default()
        };
        let engine = StrategyEngine::new(config, 10_000.0);

        // Bar with void but NO price recovery, NO OB imbalance, NO tight spread
        let void_bar_no_confirm = AggregatedBar {
            bar_start_ms: 0,
            bar_end_ms: 30_000,
            sample_count: 30,
            open_price: 50000.0,
            close_price: 49900.0, // BELOW low * 1.001 - no recovery
            high_price: 50100.0,
            low_price: 49800.0, // close = 49900, low = 49800, 49800 * 1.001 = 49849.8, 49900 > 49849.8 - actually recovers!
            avg_mid_price: 49950.0,
            ob_imbalance_mean: 0.0, // No OB imbalance
            ob_imbalance_max: 0.0,
            ob_imbalance_min: 0.0,
            ob_imbalance_trend: 0.0,
            avg_spread_bps: 8.0, // 8.0 < 10.0 * 0.5 = 5.0? No, 8.0 > 5.0 - spread not tight
            cum_net_qty_final: 0.0,
            cum_ratio_final: 1.0,
            fk_net_direction_mean: 0.0,
            fk_net_direction_max: 0.0,
            fk_net_direction_min: 0.0,
            fk_net_direction_final: 0.0,
            fk_buy_fill_total: 0.0,
            fk_sell_fill_total: 100.0,
            fk_buy_kill_total: 0.0,
            fk_sell_kill_total: 50.0,
            aggression_shift_mean: 0.0,
            aggression_shift_max: 0.0,
            aggression_shift_min: 0.0,
            aggression_shift_final: 0.0,
            absorption_score_max: 0.0,
            absorption_score_mean: 0.0,
            absorption_score_final: 0.0,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 30, // Void exists!
            avg_impact_buy_slippage_bps: 5.0,
            avg_impact_sell_slippage_bps: 5.0,
            total_trade_volume: 1000.0,
            avg_buy_volume_pct: 30.0,
            avg_trade_count_per_sec: 2.0,
        };

        let signal = engine.check_entry(0.0, &void_bar_no_confirm);
        assert_eq!(signal, Signal::Hold, "Should not enter void without all confirmations");
    }

    #[test]
    fn test_void_confirmation_with_all_conditions_met() {
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            min_void_levels_for_reversion: 25,
            void_price_recovery_pct: 1.0005,
            void_ob_imbalance_threshold: 0.05,
            void_spread_tightness_pct: 0.7,
            max_spread_bps: 10.0,
            ..Default::default()
        };
        let engine = StrategyEngine::new(config, 10_000.0);

        // Bar with void AND all confirmations
        let void_bar_with_confirm = AggregatedBar {
            bar_start_ms: 0,
            bar_end_ms: 30_000,
            sample_count: 30,
            open_price: 50000.0,
            close_price: 49950.0, // Must be > low * 1.0005
            high_price: 50100.0,
            low_price: 49900.0, // 49900 * 1.0005 = 49924.95, close 49950 > 49924.95 ✓
            avg_mid_price: 49975.0,
            ob_imbalance_mean: 0.1, // > 0.05 ✓
            ob_imbalance_max: 0.15,
            ob_imbalance_min: 0.05,
            ob_imbalance_trend: 0.0,
            avg_spread_bps: 5.0, // 5.0 < 10.0 * 0.7 = 7.0 ✓
            cum_net_qty_final: 0.0,
            cum_ratio_final: 1.0,
            fk_net_direction_mean: 0.0,
            fk_net_direction_max: 0.0,
            fk_net_direction_min: 0.0,
            fk_net_direction_final: 0.0,
            fk_buy_fill_total: 0.0,
            fk_sell_fill_total: 100.0,
            fk_buy_kill_total: 0.0,
            fk_sell_kill_total: 50.0,
            aggression_shift_mean: 0.0,
            aggression_shift_max: 0.0,
            aggression_shift_min: 0.0,
            aggression_shift_final: 0.0,
            absorption_score_max: 0.0,
            absorption_score_mean: 0.0,
            absorption_score_final: 0.0,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 30, // Void exists ✓
            avg_impact_buy_slippage_bps: 5.0,
            avg_impact_sell_slippage_bps: 5.0,
            total_trade_volume: 1000.0,
            avg_buy_volume_pct: 30.0,
            avg_trade_count_per_sec: 2.0,
        };

        let signal = engine.check_entry(0.0, &void_bar_with_confirm);
        assert_eq!(signal, Signal::GoLong, "Should enter void long with all confirmations");
    }

    #[test]
    fn test_stale_exit_respects_min_bars() {
        // Config with 2 bar minimum for stale exit, 30s bars
        let config = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            bar_duration_ms: 30_000,
            time_stop_ms: 60_000, // 2 bars
            stale_exit_min_bars: 2, // Require 2 bars before stale exit
            stop_loss_pct: 1.0, // High to not trigger
            take_profit_pct: 1.0, // High to not trigger
            ..Default::default()
        };
        let mut engine = StrategyEngine::new(config, 10_000.0);
        engine.open_position(Side::Buy, 50000.0, 0);

        // Update position to show no favorable movement
        engine.position.update_market(49900.0); // Moved against us

        // Bar at 20s (less than 2 bars = 60s) - should NOT trigger stale exit
        let bar_early = AggregatedBar {
            bar_start_ms: 0,
            bar_end_ms: 20_000, // Only 20s, less than 2 bars
            sample_count: 20,
            open_price: 50000.0,
            close_price: 49900.0,
            high_price: 50000.0,
            low_price: 49900.0,
            avg_mid_price: 49950.0,
            ob_imbalance_mean: 0.0,
            ob_imbalance_max: 0.0,
            ob_imbalance_min: 0.0,
            ob_imbalance_trend: 0.0,
            avg_spread_bps: 5.0,
            cum_net_qty_final: 0.0,
            cum_ratio_final: 1.0,
            fk_net_direction_mean: 0.0,
            fk_net_direction_max: 0.0,
            fk_net_direction_min: 0.0,
            fk_net_direction_final: 0.0,
            fk_buy_fill_total: 0.0,
            fk_sell_fill_total: 0.0,
            fk_buy_kill_total: 0.0,
            fk_sell_kill_total: 0.0,
            aggression_shift_mean: 0.0,
            aggression_shift_max: 0.0,
            aggression_shift_min: 0.0,
            aggression_shift_final: 0.0,
            absorption_score_max: 0.0,
            absorption_score_mean: 0.0,
            absorption_score_final: 0.0,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 0,
            avg_impact_buy_slippage_bps: 5.0,
            avg_impact_sell_slippage_bps: 5.0,
            total_trade_volume: 0.0,
            avg_buy_volume_pct: 50.0,
            avg_trade_count_per_sec: 1.0,
        };

        let signal_early = engine.check_exit(&bar_early);
        assert_ne!(signal_early, Signal::ExitLong,
            "Should NOT exit stale before min_bars (20s < 60s)");

        // Bar at 70s (more than 2 bars = 60s) - SHOULD trigger stale exit
        let bar_late = AggregatedBar {
            bar_start_ms: 0,
            bar_end_ms: 70_000, // 70s, more than 2 bars (60s)
            sample_count: 70,
            open_price: 50000.0,
            close_price: 49900.0,
            high_price: 50000.0,
            low_price: 49900.0,
            avg_mid_price: 49950.0,
            ob_imbalance_mean: 0.0,
            ob_imbalance_max: 0.0,
            ob_imbalance_min: 0.0,
            ob_imbalance_trend: 0.0,
            avg_spread_bps: 5.0,
            cum_net_qty_final: 0.0,
            cum_ratio_final: 1.0,
            fk_net_direction_mean: 0.0,
            fk_net_direction_max: 0.0,
            fk_net_direction_min: 0.0,
            fk_net_direction_final: 0.0,
            fk_buy_fill_total: 0.0,
            fk_sell_fill_total: 0.0,
            fk_buy_kill_total: 0.0,
            fk_sell_kill_total: 0.0,
            aggression_shift_mean: 0.0,
            aggression_shift_max: 0.0,
            aggression_shift_min: 0.0,
            aggression_shift_final: 0.0,
            absorption_score_max: 0.0,
            absorption_score_mean: 0.0,
            absorption_score_final: 0.0,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 0,
            avg_impact_buy_slippage_bps: 5.0,
            avg_impact_sell_slippage_bps: 5.0,
            total_trade_volume: 0.0,
            avg_buy_volume_pct: 50.0,
            avg_trade_count_per_sec: 1.0,
        };

        let signal_late = engine.check_exit(&bar_late);
        assert_eq!(signal_late, Signal::ExitLong,
            "SHOULD exit stale after min_bars (70s > 60s)");
    }

    #[test]
    fn test_momentum_threshold_configurable() {
        // Test with high momentum threshold - should NOT enter
        let config_high = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            long_entry_threshold: 0.3,
            min_momentum_threshold: 0.5, // Very high
            min_absorption_for_reversal: 0.9, // Very high (effectively disabled)
            ..Default::default()
        };
        let engine_high = StrategyEngine::new(config_high, 10_000.0);

        let bar_moderate_momentum = AggregatedBar {
            bar_start_ms: 0,
            bar_end_ms: 30_000,
            sample_count: 30,
            open_price: 50000.0,
            close_price: 50100.0,
            high_price: 50100.0,
            low_price: 50000.0,
            avg_mid_price: 50050.0,
            ob_imbalance_mean: 0.2,
            ob_imbalance_max: 0.3,
            ob_imbalance_min: 0.1,
            ob_imbalance_trend: 0.0,
            avg_spread_bps: 5.0,
            cum_net_qty_final: 100.0,
            cum_ratio_final: 1.5,
            fk_net_direction_mean: 0.4, // Score = 0.4 * 0.3 = 0.12... but we pass score directly
            fk_net_direction_max: 0.5,
            fk_net_direction_min: 0.3,
            fk_net_direction_final: 0.4,
            fk_buy_fill_total: 100.0,
            fk_sell_fill_total: 20.0,
            fk_buy_kill_total: 10.0,
            fk_sell_kill_total: 5.0,
            aggression_shift_mean: 0.3,
            aggression_shift_max: 0.4,
            aggression_shift_min: 0.2,
            aggression_shift_final: 0.3,
            absorption_score_max: 0.5, // Below 0.9 threshold
            absorption_score_mean: 0.4,
            absorption_score_final: 0.5,
            vd_buy_levels_max: 0,
            vd_sell_levels_max: 0,
            avg_impact_buy_slippage_bps: 5.0,
            avg_impact_sell_slippage_bps: 8.0, // skew = 8/5 = 1.6 > 1.2
            total_trade_volume: 1000.0,
            avg_buy_volume_pct: 80.0,
            avg_trade_count_per_sec: 2.0,
        };

        // Score above entry threshold, but momentum below min_momentum_threshold
        let signal = engine_high.check_entry(0.35, &bar_moderate_momentum);
        assert_eq!(signal, Signal::Hold,
            "Should NOT enter when momentum ({}) < min_momentum_threshold (0.5)",
            bar_moderate_momentum.fk_net_direction_mean);

        // Test with low momentum threshold - SHOULD enter
        let config_low = StrategyConfig {
            symbol: "BTCUSDT".to_string(),
            long_entry_threshold: 0.3,
            min_momentum_threshold: 0.1, // Very low
            min_absorption_for_reversal: 0.9,
            ..Default::default()
        };
        let engine_low = StrategyEngine::new(config_low, 10_000.0);

        let signal = engine_low.check_entry(0.35, &bar_moderate_momentum);
        assert_eq!(signal, Signal::GoLong,
            "SHOULD enter when momentum ({}) > min_momentum_threshold (0.1)",
            bar_moderate_momentum.fk_net_direction_mean);
    }

    #[test]
    fn test_default_config_is_30s_preset() {
        let default_config = StrategyConfig::default();
        let preset_30s = StrategyConfig::for_30s();

        assert_eq!(default_config.bar_duration_ms, preset_30s.bar_duration_ms);
        assert_eq!(default_config.long_entry_threshold, preset_30s.long_entry_threshold);
        assert_eq!(default_config.short_entry_threshold, preset_30s.short_entry_threshold);
        assert_eq!(default_config.stop_loss_pct, preset_30s.stop_loss_pct);
        assert_eq!(default_config.min_void_levels_for_reversion, preset_30s.min_void_levels_for_reversion);
    }
}