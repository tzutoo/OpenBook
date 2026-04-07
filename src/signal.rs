//! Non-intrusive trading signal extraction from [`SharedState`].
//!
//! This module provides [`SignalExtractor`], which receives a clone of the app's
//! `Arc<Mutex<SharedState>>` and periodically samples it to compute derived
//! trading signals. It does **not** modify any existing types or state.
//!
//! # Example
//!
//! ```rust,ignore
//! // In main.rs or a separate binary, after the app creates `shared`:
//! let signal_config = signal::SignalConfig::default();
//! let mut extractor = signal::SignalExtractor::new(
//!     Arc::clone(&shared),
//!     signal_config,
//! );
//! // In a loop or timer:
//! let sample = extractor.sample();
//! println!("{:?}", sample);
//! ```

use crate::micro::{
    BurstDirection, MicroMetrics, ROLLING_WINDOW_MS, RatioValue,
};
use crate::models::{OrderBook, SharedState};
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::Write;
use std::sync::{Arc, Mutex};

// ---------------------------------------------------------------------------
// SignalSample
// ---------------------------------------------------------------------------

/// A single point-in-time snapshot of all computed trading signals.
///
/// Designed to be cheaply [`Clone`]able for buffering and [`Serialize`]able
/// for future CSV / JSON export.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignalSample {
    pub timestamp_ms: u64,
    pub symbol: String,

    // -- Price ---------------------------------------------------------------
    pub mid_price: f64,
    pub spread_bps: f64,

    // -- Order book imbalance (within N ticks of mid) ------------------------
    pub ob_imbalance: f64,      // range [-1, 1], positive = bid-heavy
    pub ob_bid_depth_near: f64, // total bid qty within N ticks
    pub ob_ask_depth_near: f64, // total ask qty within N ticks

    // -- Fill:Kill (from rolling window) -------------------------------------
    pub fk_buy_fill_qty: f64,
    pub fk_buy_kill_qty: f64,
    pub fk_sell_fill_qty: f64,
    pub fk_sell_kill_qty: f64,
    pub fk_net_direction: f64, // positive = buy-aggressive, negative = sell-aggressive
    pub fk_buy_signed_log_ratio: Option<f64>,
    pub fk_sell_signed_log_ratio: Option<f64>,

    // -- Cumulative fill:kill KPIs -------------------------------------------
    pub cum_fill_qty: f64,
    pub cum_kill_qty: f64,
    pub cum_net_qty: f64,
    pub cum_ratio: f64, // Na → 0.0

    // -- Market impact (for a standard notional, e.g. $10k) ------------------
    pub impact_buy_slippage_bps: f64,
    pub impact_sell_slippage_bps: f64,
    pub impact_buy_levels_consumed: usize,
    pub impact_sell_levels_consumed: usize,

    // -- Trade flow (last N seconds) -----------------------------------------
    pub trade_count_1s: u64,
    pub trade_volume_1s: f64,   // total notional in last 1s
    pub buy_volume_pct_1s: f64, // % of volume that was buy-aggressive

    // -- Derived signals -----------------------------------------------------
    pub absorption_score: f64,  // high when large fills don't move price
    pub aggression_shift: f64,  // change in fill:kill direction over short window
}

// ---------------------------------------------------------------------------
// SignalConfig
// ---------------------------------------------------------------------------

/// Configuration for the [`SignalExtractor`].
#[derive(Debug, Clone)]
pub struct SignalConfig {
    /// Sampling interval in milliseconds. Default: 1000 (1 second).
    pub sample_interval_ms: u64,

    /// Number of price ticks from mid-price to include in the order-book
    /// imbalance calculation. Default: 10.
    pub ob_near_ticks: usize,

    /// Notional value (USD) used when estimating market impact. Default: 10_000.0.
    pub impact_notional_usd: f64,

    /// Time window (ms) for trade-flow statistics. Default: 1_000.
    pub trade_window_ms: u64,

    /// Lookback window (ms) for absorption detection. Default: 5_000.
    pub absorption_lookback_ms: u64,

    /// Window (ms) over which aggression shift is measured. Default: 30_000.
    pub aggression_shift_window_ms: u64,

    /// Trading symbol, e.g. `"btcusdt"`. Used for labelling samples.
    pub symbol: String,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            sample_interval_ms: 1_000,
            ob_near_ticks: 10,
            impact_notional_usd: 10_000.0,
            trade_window_ms: 1_000,
            absorption_lookback_ms: 5_000,
            aggression_shift_window_ms: 30_000,
            symbol: String::from("btcusdt"),
        }
    }
}

// ---------------------------------------------------------------------------
// SignalExtractor
// ---------------------------------------------------------------------------

/// Maximum number of [`SignalSample`]s retained in the internal ring buffer
/// (~3 minutes at 1 s cadence).
const BUFFER_CAPACITY: usize = 180;

/// Non-intrusive signal extractor.
///
/// Holds a clone of the application's `Arc<Mutex<SharedState>>` and computes
/// derived signals on each call to [`sample`](Self::sample).
pub struct SignalExtractor {
    shared: Arc<Mutex<SharedState>>,
    config: SignalConfig,
    buffer: VecDeque<SignalSample>,
}

impl SignalExtractor {
    /// Create a new extractor that reads from `shared`.
    pub fn new(shared: Arc<Mutex<SharedState>>, config: SignalConfig) -> Self {
        Self {
            shared,
            config,
            buffer: VecDeque::with_capacity(BUFFER_CAPACITY),
        }
    }

    /// Lock [`SharedState`], compute all signals, push to the internal buffer,
    /// and return the sample.
    pub fn sample(&mut self) -> SignalSample {
        let guard = self.shared.lock().expect("SharedState lock poisoned");
        let now_ms = guard.order_book.last_event_time;
        let sample = self.compute_sample(&guard, now_ms);
        drop(guard);

        // Maintain ring-buffer size.
        if self.buffer.len() >= BUFFER_CAPACITY {
            self.buffer.pop_front();
        }
        self.buffer.push_back(sample.clone());
        sample
    }

    /// Return the last `count` samples (or fewer if not enough have been
    /// collected yet).
    pub fn recent_samples(&mut self, count: usize) -> &[SignalSample] {
        let start = self.buffer.len().saturating_sub(count);
        let contig = self.buffer.make_contiguous();
        &contig[start..]
    }

    /// Return the most recent sample, if any.
    pub fn latest(&self) -> Option<&SignalSample> {
        self.buffer.back()
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    fn compute_sample(&self, state: &SharedState, now_ms: u64) -> SignalSample {
        let mid_price = Self::mid_price(&state.order_book);
        let spread_bps = Self::spread_bps(&state.order_book, mid_price);
        let tick_size = state.tick_size;

        let (ob_imbalance, ob_bid_depth_near, ob_ask_depth_near) =
            Self::order_book_imbalance(&state.order_book, mid_price, tick_size, self.config.ob_near_ticks);

        let (fk_buy_fill_qty, fk_buy_kill_qty, fk_sell_fill_qty, fk_sell_kill_qty,
             fk_net_direction, fk_buy_signed_log_ratio, fk_sell_signed_log_ratio) =
            Self::fill_kill_aggregation(&state.micro_metrics, now_ms);

        let (cum_fill_qty, cum_kill_qty, cum_net_qty, cum_ratio) =
            Self::cumulative_kpis(&state.micro_metrics);

        let (impact_buy_slippage_bps, impact_sell_slippage_bps,
             impact_buy_levels_consumed, impact_sell_levels_consumed) =
            Self::market_impact(&state.order_book, mid_price, self.config.impact_notional_usd);

        let (trade_count_1s, trade_volume_1s, buy_volume_pct_1s) =
            Self::trade_flow(&state.trade_history, now_ms, self.config.trade_window_ms);

        let absorption_score = Self::absorption_score(
            &state.trade_history,
            now_ms,
            self.config.absorption_lookback_ms,
            mid_price,
        );

        let aggression_shift = Self::aggression_shift(
            &state.micro_metrics,
            now_ms,
            self.config.aggression_shift_window_ms,
        );

        SignalSample {
            timestamp_ms: now_ms,
            symbol: self.config.symbol.clone(),
            mid_price,
            spread_bps,
            ob_imbalance,
            ob_bid_depth_near,
            ob_ask_depth_near,
            fk_buy_fill_qty,
            fk_buy_kill_qty,
            fk_sell_fill_qty,
            fk_sell_kill_qty,
            fk_net_direction,
            fk_buy_signed_log_ratio,
            fk_sell_signed_log_ratio,
            cum_fill_qty,
            cum_kill_qty,
            cum_net_qty,
            cum_ratio,
            impact_buy_slippage_bps,
            impact_sell_slippage_bps,
            impact_buy_levels_consumed,
            impact_sell_levels_consumed,
            trade_count_1s,
            trade_volume_1s,
            buy_volume_pct_1s,
            absorption_score,
            aggression_shift,
        }
    }

    // -- Price helpers -------------------------------------------------------

    fn mid_price(book: &OrderBook) -> f64 {
        match (book.best_bid(), book.best_ask()) {
            (Some((bid, _)), Some((ask, _))) => (bid + ask) / 2.0,
            _ => 0.0,
        }
    }

    fn spread_bps(book: &OrderBook, mid_price: f64) -> f64 {
        if mid_price <= 0.0 {
            return 0.0;
        }
        match book.spread() {
            Some(spread) => (spread / mid_price) * 10_000.0,
            None => 0.0,
        }
    }

    // -- Order book imbalance ------------------------------------------------

    /// Compute order-book imbalance within `near_ticks` ticks of mid-price.
    ///
    /// Returns `(imbalance, bid_depth, ask_depth)` where `imbalance` is in
    /// `[-1, 1]` (positive = bid-heavy).
    fn order_book_imbalance(
        book: &OrderBook,
        mid_price: f64,
        tick_size: f64,
        near_ticks: usize,
    ) -> (f64, f64, f64) {
        if mid_price <= 0.0 || tick_size <= 0.0 {
            return (0.0, 0.0, 0.0);
        }

        let upper_bound = mid_price + (near_ticks as f64) * tick_size;
        let lower_bound = mid_price - (near_ticks as f64) * tick_size;

        let bid_depth: f64 = book
            .bids
            .range(OrderedFloat(lower_bound)..=OrderedFloat(mid_price))
            .map(|(_, &qty)| qty)
            .sum();

        let ask_depth: f64 = book
            .asks
            .range(OrderedFloat(mid_price)..=OrderedFloat(upper_bound))
            .map(|(_, &qty)| qty)
            .sum();

        let total = bid_depth + ask_depth;
        if total < 1e-12 {
            return (0.0, 0.0, 0.0);
        }

        let imbalance = (bid_depth - ask_depth) / total;
        (imbalance, bid_depth, ask_depth)
    }

    // -- Fill:Kill aggregation -----------------------------------------------

    /// Aggregate fill:kill samples from the rolling window, split by direction.
    ///
    /// Returns `(buy_fill, buy_kill, sell_fill, sell_kill, net_direction,
    /// buy_signed_log_ratio, sell_signed_log_ratio)`.
    fn fill_kill_aggregation(
        micro: &MicroMetrics,
        now_ms: u64,
    ) -> (f64, f64, f64, f64, f64, Option<f64>, Option<f64>) {
        let cutoff = now_ms.saturating_sub(ROLLING_WINDOW_MS);

        let mut buy_fill = 0.0_f64;
        let mut buy_kill = 0.0_f64;
        let mut sell_fill = 0.0_f64;
        let mut sell_kill = 0.0_f64;
        let mut buy_slr_vals: Vec<f64> = Vec::new();
        let mut sell_slr_vals: Vec<f64> = Vec::new();

        for sample in &micro.fill_kill_history.samples {
            if sample.timestamp_ms < cutoff {
                continue;
            }
            match sample.direction {
                BurstDirection::Buy => {
                    buy_fill += sample.fill_qty;
                    buy_kill += sample.kill_qty;
                    if let Some(slr) = sample.signed_log_ratio {
                        buy_slr_vals.push(slr);
                    }
                }
                BurstDirection::Sell => {
                    sell_fill += sample.fill_qty;
                    sell_kill += sample.kill_qty;
                    if let Some(slr) = sample.signed_log_ratio {
                        sell_slr_vals.push(slr);
                    }
                }
            }
        }

        let total_fill = buy_fill + sell_fill;
        let eps = 1e-12;
        let net_direction = if total_fill > eps {
            (buy_fill - sell_fill) / total_fill
        } else {
            0.0
        };

        let buy_signed_log_ratio = Self::average_option_f64(&buy_slr_vals);
        let sell_signed_log_ratio = Self::average_option_f64(&sell_slr_vals);

        (
            buy_fill,
            buy_kill,
            sell_fill,
            sell_kill,
            net_direction,
            buy_signed_log_ratio,
            sell_signed_log_ratio,
        )
    }

    fn average_option_f64(vals: &[f64]) -> Option<f64> {
        if vals.is_empty() {
            return None;
        }
        Some(vals.iter().sum::<f64>() / vals.len() as f64)
    }

    // -- Cumulative KPIs -----------------------------------------------------

    /// Read cumulative fill:kill KPIs directly from [`MicroMetrics`].
    ///
    /// Returns `(cum_fill_qty, cum_kill_qty, cum_net_qty, cum_ratio)`.
    fn cumulative_kpis(micro: &MicroMetrics) -> (f64, f64, f64, f64) {
        let cum_fill_qty = micro.cum_fill_qty;
        let cum_kill_qty = micro.cum_kill_qty;
        let cum_net_qty = cum_fill_qty - cum_kill_qty;

        // Use the latest cumulative sample for the ratio.
        let cum_ratio = micro
            .cumulative_history
            .samples
            .back()
            .map(|s| match s.cum_ratio {
                RatioValue::Finite(v) => v,
                RatioValue::Infinite => f64::INFINITY,
                RatioValue::Na => 0.0,
            })
            .unwrap_or(0.0);

        (cum_fill_qty, cum_kill_qty, cum_net_qty, cum_ratio)
    }

    // -- Market impact -------------------------------------------------------

    /// Estimate market impact for a standard notional in both directions.
    ///
    /// Returns `(buy_slippage_bps, sell_slippage_bps,
    /// buy_levels_consumed, sell_levels_consumed)`.
    fn market_impact(
        book: &OrderBook,
        mid_price: f64,
        notional_usd: f64,
    ) -> (f64, f64, usize, usize) {
        let buy_impact = book.estimate_market_impact(notional_usd, true, mid_price);
        let sell_impact = book.estimate_market_impact(notional_usd, false, mid_price);

        (
            buy_impact.slippage_bps,
            sell_impact.slippage_bps,
            buy_impact.levels_consumed,
            sell_impact.levels_consumed,
        )
    }

    // -- Trade flow ----------------------------------------------------------

    /// Compute trade-flow statistics over `window_ms`.
    ///
    /// Returns `(trade_count, total_volume, buy_volume_pct)`.
    fn trade_flow(
        trade_history: &crate::models::TradeHistory,
        now_ms: u64,
        window_ms: u64,
    ) -> (u64, f64, f64) {
        let cutoff = now_ms.saturating_sub(window_ms);
        let mut count = 0u64;
        let mut total_volume = 0.0_f64;
        let mut buy_volume = 0.0_f64;

        for trade in &trade_history.trades {
            // TradeHistory uses received_at_ms for pruning, but we compare
            // against timestamp_ms for exchange-time alignment.
            if trade.received_at_ms < cutoff {
                continue;
            }
            count += 1;
            let notional = trade.price * trade.quantity;
            total_volume += notional;
            if trade.is_buy {
                buy_volume += notional;
            }
        }

        let buy_volume_pct = if total_volume > 1e-12 {
            (buy_volume / total_volume) * 100.0
        } else {
            50.0 // neutral when no data
        };

        (count, total_volume, buy_volume_pct)
    }

    // -- Absorption score ----------------------------------------------------

    /// Compare trade volume in the lookback window to the resulting price
    /// movement. Large volume with small price change → high absorption.
    ///
    /// Returns a value in `[0, 1]`.
    fn absorption_score(
        trade_history: &crate::models::TradeHistory,
        now_ms: u64,
        lookback_ms: u64,
        _current_mid_price: f64,
    ) -> f64 {
        let cutoff = now_ms.saturating_sub(lookback_ms);

        let mut total_volume = 0.0_f64;
        let mut first_price: Option<f64> = None;
        let mut last_price: Option<f64> = None;

        for trade in &trade_history.trades {
            if trade.received_at_ms < cutoff {
                continue;
            }
            total_volume += trade.price * trade.quantity;
            first_price = Some(first_price.unwrap_or(trade.price));
            last_price = Some(trade.price);
        }

        if total_volume < 1e-12 {
            return 0.0;
        }

        let price_change_pct = match (first_price, last_price) {
            (Some(fp), Some(lp)) if fp > 0.0 => ((lp - fp) / fp).abs() * 100.0,
            _ => return 0.0,
        };

        if price_change_pct < 1e-6 {
            // Effectively zero price change with non-zero volume → maximum absorption.
            return 1.0;
        }

        // Raw ratio: volume / price_change_pct. Higher = more absorption.
        // Use log scale to compress the wide dynamic range, then clamp.
        let raw = total_volume / price_change_pct;
        let log_val = raw.ln(); // natural log
        // Clamp: ln(100) ≈ 4.6 as "full absorption", ln(0.01) ≈ -4.6 as "none".
        let normalized = (log_val + 5.0) / 10.0; // maps [-5, 5] → [0, 1]
        normalized.clamp(0.0, 1.0)
    }

    // -- Aggression shift ----------------------------------------------------

    /// Compare fill:kill net direction in the most recent half of the window
    /// vs the earlier half. Positive = shift toward buying, negative = shift
    /// toward selling.
    fn aggression_shift(
        micro: &MicroMetrics,
        now_ms: u64,
        window_ms: u64,
    ) -> f64 {
        let window_start = now_ms.saturating_sub(window_ms);
        let half_window = window_ms / 2;
        let midpoint = window_start.saturating_add(half_window);

        let mut early_buy_fill = 0.0_f64;
        let mut early_sell_fill = 0.0_f64;
        let mut late_buy_fill = 0.0_f64;
        let mut late_sell_fill = 0.0_f64;

        for sample in &micro.fill_kill_history.samples {
            if sample.timestamp_ms < window_start {
                continue;
            }
            match sample.direction {
                BurstDirection::Buy => {
                    if sample.timestamp_ms < midpoint {
                        early_buy_fill += sample.fill_qty;
                    } else {
                        late_buy_fill += sample.fill_qty;
                    }
                }
                BurstDirection::Sell => {
                    if sample.timestamp_ms < midpoint {
                        early_sell_fill += sample.fill_qty;
                    } else {
                        late_sell_fill += sample.fill_qty;
                    }
                }
            }
        }

        let eps = 1e-12;
        let early_total = early_buy_fill + early_sell_fill;
        let late_total = late_buy_fill + late_sell_fill;

        // Compute net direction for each half.
        let early_dir = if early_total > eps {
            (early_buy_fill - early_sell_fill) / early_total
        } else {
            0.0
        };

        let late_dir = if late_total > eps {
            (late_buy_fill - late_sell_fill) / late_total
        } else {
            0.0
        };

        // Shift = late direction minus early direction.
        // Positive means aggression shifted toward buying.
        late_dir - early_dir
    }
}

// ---------------------------------------------------------------------------
// SignalLogger
// ---------------------------------------------------------------------------

/// Simple CSV logger for [`SignalSample`]s.
///
/// Uses only `std::fs` / `std::io` — no external CSV crate required.
pub struct SignalLogger {
    writer: Option<File>,
}

impl SignalLogger {
    /// Open (or create) a CSV file at `path` and write the header row.
    pub fn open(path: &str) -> std::io::Result<Self> {
        let mut file = fs::File::create(path)?;
        writeln!(
            file,
            "timestamp_ms,symbol,mid_price,spread_bps,\
             ob_imbalance,ob_bid_depth_near,ob_ask_depth_near,\
             fk_buy_fill_qty,fk_buy_kill_qty,fk_sell_fill_qty,fk_sell_kill_qty,\
             fk_net_direction,fk_buy_signed_log_ratio,fk_sell_signed_log_ratio,\
             cum_fill_qty,cum_kill_qty,cum_net_qty,cum_ratio,\
             impact_buy_slippage_bps,impact_sell_slippage_bps,\
             impact_buy_levels_consumed,impact_sell_levels_consumed,\
             trade_count_1s,trade_volume_1s,buy_volume_pct_1s,\
             absorption_score,aggression_shift"
        )?;
        Ok(Self {
            writer: Some(file),
        })
    }

    /// Append a single [`SignalSample`] as a CSV row.
    pub fn write_sample(&mut self, sample: &SignalSample) -> std::io::Result<()> {
        let f = match self.writer.as_mut() {
            Some(w) => w,
            None => return Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "logger file is closed",
            )),
        };

        // Helper to format Option<f64>: empty string for None.
        let opt_f = |v: Option<f64>| match v {
            Some(x) => format!("{x}"),
            None => String::new(),
        };

        writeln!(
            f,
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            sample.timestamp_ms,
            sample.symbol,
            sample.mid_price,
            sample.spread_bps,
            sample.ob_imbalance,
            sample.ob_bid_depth_near,
            sample.ob_ask_depth_near,
            sample.fk_buy_fill_qty,
            sample.fk_buy_kill_qty,
            sample.fk_sell_fill_qty,
            sample.fk_sell_kill_qty,
            sample.fk_net_direction,
            opt_f(sample.fk_buy_signed_log_ratio),
            opt_f(sample.fk_sell_signed_log_ratio),
            sample.cum_fill_qty,
            sample.cum_kill_qty,
            sample.cum_net_qty,
            sample.cum_ratio,
            sample.impact_buy_slippage_bps,
            sample.impact_sell_slippage_bps,
            sample.impact_buy_levels_consumed,
            sample.impact_sell_levels_consumed,
            sample.trade_count_1s,
            sample.trade_volume_1s,
            sample.buy_volume_pct_1s,
            sample.absorption_score,
            sample.aggression_shift,
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signal_config_defaults() {
        let cfg = SignalConfig::default();
        assert_eq!(cfg.sample_interval_ms, 1_000);
        assert_eq!(cfg.ob_near_ticks, 10);
        assert!((cfg.impact_notional_usd - 10_000.0).abs() < f64::EPSILON);
        assert_eq!(cfg.trade_window_ms, 1_000);
        assert_eq!(cfg.absorption_lookback_ms, 5_000);
        assert_eq!(cfg.aggression_shift_window_ms, 30_000);
        assert_eq!(cfg.symbol, "btcusdt");
    }

    #[test]
    fn signal_sample_is_send_sync() {
        // SignalSample must be Send + Sync so it can cross thread boundaries
        // safely (e.g. when passed from a sampling thread to UI).
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SignalSample>();
    }

    #[test]
    fn signal_sample_serialize_roundtrip() {
        let sample = SignalSample {
            timestamp_ms: 1_700_000_000_000,
            symbol: "btcusdt".into(),
            mid_price: 42_000.0,
            spread_bps: 1.2,
            ob_imbalance: 0.15,
            ob_bid_depth_near: 100.0,
            ob_ask_depth_near: 75.0,
            fk_buy_fill_qty: 50.0,
            fk_buy_kill_qty: 30.0,
            fk_sell_fill_qty: 40.0,
            fk_sell_kill_qty: 35.0,
            fk_net_direction: 0.111,
            fk_buy_signed_log_ratio: Some(0.5),
            fk_sell_signed_log_ratio: None,
            cum_fill_qty: 500.0,
            cum_kill_qty: 300.0,
            cum_net_qty: 200.0,
            cum_ratio: 1.667,
            impact_buy_slippage_bps: 2.5,
            impact_sell_slippage_bps: 2.3,
            impact_buy_levels_consumed: 5,
            impact_sell_levels_consumed: 4,
            trade_count_1s: 120,
            trade_volume_1s: 1_500_000.0,
            buy_volume_pct_1s: 55.0,
            absorption_score: 0.7,
            aggression_shift: 0.2,
        };

        let json = serde_json::to_string(&sample).expect("serialize");
        let deserialized: SignalSample =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized.timestamp_ms, sample.timestamp_ms);
        assert_eq!(deserialized.symbol, sample.symbol);
        assert_eq!(deserialized.mid_price, sample.mid_price);
        assert_eq!(deserialized.fk_buy_signed_log_ratio, sample.fk_buy_signed_log_ratio);
        assert_eq!(deserialized.fk_sell_signed_log_ratio, sample.fk_sell_signed_log_ratio);
    }

    #[test]
    fn signal_extractor_empty_book() {
        let shared = Arc::new(Mutex::new(SharedState::new()));
        let config = SignalConfig::default();
        let mut extractor = SignalExtractor::new(shared, config);

        let sample = extractor.sample();
        assert_eq!(sample.mid_price, 0.0);
        assert_eq!(sample.spread_bps, 0.0);
        assert_eq!(sample.ob_imbalance, 0.0);
        assert!(extractor.latest().is_some());
        assert_eq!(extractor.recent_samples(5).len(), 1);
    }

    #[test]
    fn average_option_f64_empty() {
        assert_eq!(SignalExtractor::average_option_f64(&[]), None);
    }

    #[test]
    fn average_option_f64_single() {
        assert_eq!(SignalExtractor::average_option_f64(&[3.0]), Some(3.0));
    }

    #[test]
    fn average_option_f64_multiple() {
        let vals = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(SignalExtractor::average_option_f64(&vals), Some(3.0));
    }

    #[test]
    fn signal_logger_writes_header_and_row() {
        let dir = std::env::temp_dir();
        let path = dir.join("signal_test.csv");
        let path_str = path.to_str().unwrap();

        {
            let mut logger = SignalLogger::open(path_str).expect("open");
            let sample = SignalSample {
                timestamp_ms: 1000,
                symbol: "ethusdt".into(),
                mid_price: 3000.0,
                spread_bps: 1.0,
                ob_imbalance: 0.0,
                ob_bid_depth_near: 0.0,
                ob_ask_depth_near: 0.0,
                fk_buy_fill_qty: 0.0,
                fk_buy_kill_qty: 0.0,
                fk_sell_fill_qty: 0.0,
                fk_sell_kill_qty: 0.0,
                fk_net_direction: 0.0,
                fk_buy_signed_log_ratio: None,
                fk_sell_signed_log_ratio: Some(-0.5),
                cum_fill_qty: 0.0,
                cum_kill_qty: 0.0,
                cum_net_qty: 0.0,
                cum_ratio: 0.0,
                impact_buy_slippage_bps: 0.0,
                impact_sell_slippage_bps: 0.0,
                impact_buy_levels_consumed: 0,
                impact_sell_levels_consumed: 0,
                trade_count_1s: 0,
                trade_volume_1s: 0.0,
                buy_volume_pct_1s: 50.0,
                absorption_score: 0.0,
                aggression_shift: 0.0,
            };
            logger.write_sample(&sample).expect("write");
        }

        let contents = std::fs::read_to_string(path_str).expect("read");
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2); // header + 1 data row
        assert!(lines[0].starts_with("timestamp_ms,"));
        assert!(lines[1].starts_with("1000,ethusdt,"));
        // Clean up.
        let _ = std::fs::remove_file(path);
    }
}