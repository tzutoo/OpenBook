# Signal Extraction Module (`signal.rs`)

## Overview

The `signal` module provides **non-intrusive real-time trading signal extraction** from OpenBook's existing `SharedState`. It reads the same order book, trade, and microstructure data that powers the GUI, and computes a structured set of signals suitable for algorithmic trading strategies — all without modifying any existing application code.

## Design Principles

- **Non-intrusive**: Receives a clone of `Arc<Mutex<SharedState>>`. Never writes to or mutates shared state.
- **Read-only observer**: Locks the mutex briefly during sampling, computes everything from immutable references, then releases.
- **Decoupled sampling rate**: Independent of the GUI frame rate or WebSocket update frequency. Call `sample()` at whatever cadence your strategy needs.
- **Zero new dependencies**: Uses only `serde`, `serde_json`, and `ordered_float` — already in the project's `Cargo.toml`.

## Architecture

```
┌─────────────────┐     Arc<Mutex<SharedState>>     ┌──────────────────┐
│  WebSocket Task │ ──────────────────────────────▶ │   SharedState    │
│  (existing)     │                                 │  (existing)      │
└─────────────────┘                                 └────────┬─────────┘
                                                            │
                                                   clone Arc
                                                            │
                                                            ▼
                                                   ┌─────────────────┐
                                                   │ SignalExtractor │  ← NEW
                                                   │   .sample()     │
                                                   └────────┬────────┘
                                                            │
                                              ┌─────────────┼─────────────┐
                                              ▼             ▼             ▼
                                        SignalSample   Ring Buffer   SignalLogger
                                        (returned)     (180 slots)    (CSV file)
```

## Public Types

### `SignalSample`

A point-in-time snapshot of all computed signals. Derives `Clone`, `Debug`, `Serialize`, `Deserialize`.

#### Price

| Field | Type | Description |
|---|---|---|
| `timestamp_ms` | `u64` | Exchange timestamp of the sample (from `order_book.last_event_time`) |
| `symbol` | `String` | Label from config, e.g. `"btcusdt"` |
| `mid_price` | `f64` | `(best_bid + best_ask) / 2`. `0.0` if book is empty. |
| `spread_bps` | `f64` | Spread in basis points: `(ask - bid) / mid * 10000` |

#### Order Book Imbalance

Computed by summing bid and ask quantities within `ob_near_ticks` price levels of mid-price.

| Field | Type | Description |
|---|---|---|
| `ob_imbalance` | `f64` | `(bid_depth - ask_depth) / (bid_depth + ask_depth)`. Range `[-1, +1]`. Positive = bid-heavy (more liquidity on buy side). `0.0` if book is empty or no depth in range. |
| `ob_bid_depth_near` | `f64` | Total bid quantity within N ticks of mid |
| `ob_ask_depth_near` | `f64` | Total ask quantity within N ticks of mid |

#### Fill:Kill Aggregation

Aggregated from `MicroMetrics.fill_kill_history.samples` within the rolling window (180 seconds, per `ROLLING_WINDOW_MS`). Split by `BurstDirection`.

| Field | Type | Description |
|---|---|---|
| `fk_buy_fill_qty` | `f64` | Total quantity filled on buy-side bursts in window |
| `fk_buy_kill_qty` | `f64` | Total quantity killed (canceled) on buy-side bursts in window |
| `fk_sell_fill_qty` | `f64` | Total quantity filled on sell-side bursts in window |
| `fk_sell_kill_qty` | `f64` | Total quantity killed on sell-side bursts in window |
| `fk_net_direction` | `f64` | `(buy_fill - sell_fill) / (buy_fill + sell_fill)`. Range `[-1, +1]`. Positive = buy-aggressive market. `0.0` if no fills. |
| `fk_buy_signed_log_ratio` | `Option<f64>` | Average `signed_log_ratio` across buy-side bursts. `None` if no valid samples. Clamped to `[-3, +3]` in source. |
| `fk_sell_signed_log_ratio` | `Option<f64>` | Average `signed_log_ratio` across sell-side bursts. `None` if no valid samples. |

#### Cumulative KPIs

Session-level running totals from `MicroMetrics`.

| Field | Type | Description |
|---|---|---|
| `cum_fill_qty` | `f64` | Total filled quantity since session start or last reset |
| `cum_kill_qty` | `f64` | Total killed quantity since session start or last reset |
| `cum_net_qty` | `f64` | `cum_fill_qty - cum_kill_qty` |
| `cum_ratio` | `f64` | `cum_fill_qty / cum_kill_qty`. `0.0` if `Na`, `f64::INFINITY` if `Infinite` |

#### Market Impact

Estimated by calling `OrderBook.estimate_market_impact()` with `impact_notional_usd` for both buy and sell directions.

| Field | Type | Description |
|---|---|---|
| `impact_buy_slippage_bps` | `f64` | Slippage in basis points to buy `notional_usd` worth |
| `impact_sell_slippage_bps` | `f64` | Slippage in basis points to sell `notional_usd` worth |
| `impact_buy_levels_consumed` | `usize` | Number of ask levels consumed by a buy sweep |
| `impact_sell_levels_consumed` | `usize` | Number of bid levels consumed by a sell sweep |

#### Trade Flow

Statistics over the configured `trade_window_ms` (default: 1 second).

| Field | Type | Description |
|---|---|---|
| `trade_count_1s` | `u64` | Number of trades in the window |
| `trade_volume_1s` | `f64` | Total notional volume (`price * quantity`) in the window |
| `buy_volume_pct_1s` | `f64` | Percentage of volume that was buy-aggressive (`is_buy == true`). Returns `50.0` (neutral) when no data. |

#### Derived Signals

Composite signals built from the raw metrics above.

| Field | Type | Description |
|---|---|---|
| `absorption_score` | `f64` | Measures how much volume was absorbed without moving price. Range `[0, 1]`. `1.0` = maximum absorption (high volume, near-zero price change). `0.0` = no absorption or no data. Uses log-scaled normalization. |
| `aggression_shift` | `f64` | Change in fill:kill net direction between the first half and second half of `aggression_shift_window_ms`. Range approximately `[-2, +2]`. Positive = aggression shifted toward buying. `0.0` = no change. |

---

### `SignalConfig`

Configuration for the extractor. All fields have sensible defaults.

| Field | Default | Description |
|---|---|---|
| `sample_interval_ms` | `1000` | Suggested sampling interval. Not enforced by the extractor itself — the caller controls timing. |
| `ob_near_ticks` | `10` | Number of price ticks from mid to include in order book imbalance calculation. |
| `impact_notional_usd` | `10000.0` | Notional value (USD) for market impact estimation. |
| `trade_window_ms` | `1000` | Window for trade flow statistics (trade count, volume, buy %). |
| `absorption_lookback_ms` | `5000` | Lookback window for absorption score calculation. |
| `aggression_shift_window_ms` | `30000` | Window for aggression shift calculation (split into two halves for comparison). |
| `symbol` | `"btcusdt"` | Label string included in every `SignalSample`. |

---

### `SignalExtractor`

The core signal computation engine.

```rust
let mut extractor = SignalExtractor::new(
    Arc::clone(&shared),  // clone of the app's Arc<Mutex<SharedState>>
    SignalConfig::default(),
);
```

#### Methods

| Method | Signature | Description |
|---|---|---|
| `new` | `(Arc<Mutex<SharedState>>, SignalConfig) -> Self` | Create extractor. Does not start any background tasks. |
| `sample` | `(&mut self) -> SignalSample` | Lock shared state, compute all 27 signal fields, push to ring buffer, return sample. |
| `recent_samples` | `(&mut self, count: usize) -> &[SignalSample]` | Return last N samples from the ring buffer. Requires `&mut self` because it calls `make_contiguous()`. |
| `latest` | `(&self) -> Option<&SignalSample>` | Return the most recent sample without mutation. |

#### Internal Buffer

Maintains a ring buffer of up to **180 samples** (~3 minutes at 1-second cadence). Older samples are automatically evicted. Access the buffer via `recent_samples()` or `latest()`.

---

### `SignalLogger`

Minimal CSV writer for recording `SignalSample` data to disk. Uses only `std::fs` and `std::io` — no external CSV crate.

```rust
let mut logger = SignalLogger::open("signals_btcusdt.csv")?;
logger.write_sample(&sample)?;
```

#### Methods

| Method | Signature | Description |
|---|---|---|
| `open` | `(path: &str) -> io::Result<Self>` | Create (or truncate) file and write CSV header row. |
| `write_sample` | `(&mut self, &SignalSample) -> io::Result<()>` | Append one sample as a CSV row. `Option<f64>` fields are written as empty strings when `None`. |

---

## Signal Computation Details

### Order Book Imbalance

```
upper_bound = mid_price + (ob_near_ticks * tick_size)
lower_bound = mid_price - (ob_near_ticks * tick_size)

bid_depth  = Σ qty for bids in [lower_bound, mid_price]
ask_depth  = Σ qty for asks in [mid_price, upper_bound]

imbalance  = (bid_depth - ask_depth) / (bid_depth + ask_depth)
```

Uses `BTreeMap::range()` for O(log N + K) lookup where K is the number of levels in range.

### Fill:Kill Net Direction

```
For each FillKillSample in rolling window (180s):
  if direction == Buy:  buy_fill += fill_qty
  if direction == Sell: sell_fill += fill_qty

net_direction = (buy_fill - sell_fill) / (buy_fill + sell_fill + ε)
```

### Absorption Score

```
In the lookback window:
  total_volume = Σ (price * quantity) for each trade
  first_price  = price of the earliest trade
  last_price   = price of the latest trade
  price_change_pct = |last_price - first_price| / first_price * 100

if price_change_pct ≈ 0:  return 1.0  (max absorption)
else:
  raw = total_volume / price_change_pct
  normalized = (ln(raw) + 5.0) / 10.0
  return clamp(normalized, 0.0, 1.0)
```

The log scale compresses the wide dynamic range. A value of 1.0 means heavy volume with no price movement — typical of a strong support/resistance level absorbing market orders.

### Aggression Shift

```
Split the window into two halves: [start, midpoint) and [midpoint, now]

For each half, compute net direction:
  half_dir = (buy_fill - sell_fill) / (buy_fill + sell_fill + ε)

aggression_shift = late_dir - early_dir
```

Positive values indicate aggression is shifting toward buying (bullish momentum). Negative values indicate shift toward selling.

---

## CSV Format Reference

Output by `SignalLogger::open()` header:

```
timestamp_ms,symbol,mid_price,spread_bps,ob_imbalance,ob_bid_depth_near,ob_ask_depth_near,
fk_buy_fill_qty,fk_buy_kill_qty,fk_sell_fill_qty,fk_sell_kill_qty,fk_net_direction,
fk_buy_signed_log_ratio,fk_sell_signed_log_ratio,cum_fill_qty,cum_kill_qty,cum_net_qty,
cum_ratio,impact_buy_slippage_bps,impact_sell_slippage_bps,impact_buy_levels_consumed,
impact_sell_levels_consumed,trade_count_1s,trade_volume_1s,buy_volume_pct_1s,
absorption_score,aggression_shift
```

27 columns. `fk_buy_signed_log_ratio` and `fk_sell_signed_log_ratio` may be empty when no valid samples exist.

---

## Usage Examples

### Basic Sampling Loop

```rust
use std::sync::{Arc, Mutex};
use std::time::Duration;

let mut extractor = SignalExtractor::new(
    Arc::clone(&shared),
    SignalConfig::default(),
);

loop {
    let sample = extractor.sample();
    println!(
        "mid={:.1} imbalance={:.3} aggression_shift={:.3}",
        sample.mid_price, sample.ob_imbalance, sample.aggression_shift
    );
    std::thread::sleep(Duration::from_millis(1000));
}
```

### Logging to CSV for Backtesting

```rust
let config = SignalConfig {
    symbol: "ethusdt".to_string(),
    ..SignalConfig::default()
};

let mut extractor = SignalExtractor::new(Arc::clone(&shared), config.clone());
let mut logger = SignalLogger::open("ethusdt_signals.csv")?;

loop {
    let sample = extractor.sample();
    logger.write_sample(&sample)?;
    std::thread::sleep(Duration::from_millis(config.sample_interval_ms));
}
```

### Accessing Recent History

```rust
let sample = extractor.sample();

// Get last 30 samples for trend analysis
let recent = extractor.recent_samples(30);
let avg_imbalance: f64 = recent.iter().map(|s| s.ob_imbalance).sum::<f64>()
    / recent.len() as f64;

println!("30s average imbalance: {:.3}", avg_imbalance);
```

### Custom Configuration for Smaller Tickers

```rust
let config = SignalConfig {
    ob_near_ticks: 20,              // wider range for thinner books
    impact_notional_usd: 1_000.0,   // smaller notional for altcoins
    absorption_lookback_ms: 10_000, // longer lookback for slower markets
    aggression_shift_window_ms: 60_000,
    symbol: "solusdt".to_string(),
    ..SignalConfig::default()
};
```

---

## Integration with Strategy Engine

The `SignalSample` is designed to be fed into a strategy engine that runs on 1-minute candles:

```
Every second:
  sample = extractor.sample()
  push to 60-second aggregation buffer

Every minute (on candle close):
  aggregate = compute_1min_stats(buffer)  // mean, min, max, std of each field
  score = strategy_model.score(aggregate)
  if score > ENTRY_THRESHOLD:  enter position
  if position_open && score < EXIT_THRESHOLD:  exit position
```

Key fields for strategy scoring:
- `fk_net_direction` + `aggression_shift` → momentum confirmation
- `absorption_score` → reversal detection
- `ob_imbalance` → liquidity context filter
- `impact_buy/sell_slippage_bps` → position sizing input

---

## Testing

The module includes 8 unit tests:

| Test | What It Validates |
|---|---|
| `signal_config_defaults` | All default values match documentation |
| `signal_sample_is_send_sync` | `SignalSample` is `Send + Sync` (safe across threads) |
| `signal_sample_serialize_roundtrip` | JSON serialize → deserialize produces equal value |
| `signal_extractor_empty_book` | Returns zeroed signals when order book is empty |
| `average_option_f64_empty` | Returns `None` for empty slice |
| `average_option_f64_single` | Returns the single value |
| `average_option_f64_multiple` | Returns correct arithmetic mean |
| `signal_logger_writes_header_and_row` | CSV file contains header + one data row |

Run with: `cargo test --release`

---

## Limitations

- **Timestamp source**: Uses `order_book.last_event_time` (exchange time). If the WebSocket is disconnected, this timestamp goes stale. Check `SharedState.connected` separately if needed.
- **Trade history pruning**: `TradeHistory` prunes by `received_at_ms`, but signal computation filters by `received_at_ms` as well. There may be slight misalignment with exchange-time-based metrics.
- **No internal timer**: The extractor does not spawn threads or timers. The caller is responsible for calling `sample()` at the desired cadence.
- **Single symbol**: Each `SignalExtractor` instance is bound to one symbol via `SignalConfig.symbol`. For multi-symbol strategies, create multiple extractors (each with its own `SharedState`).

---

# Strategy Module (`strategy.rs`)

## Overview

The `strategy` module consumes `SignalSample`s from the signal module, aggregates them into 1-minute bars, and produces trading decisions (`GoLong`, `GoShort`, `ExitLong`, `ExitShort`, `Hold`). It includes position management, risk controls, circuit breakers, and performance reporting — all without touching existing application code.

## Architecture

```
SignalExtractor.sample()  →  SignalSample (every 1s)
         │
         ▼
SignalBarAggregator.push()  →  AggregatedBar (every 60s)
         │
         ▼
StrategyEngine.on_bar()  →  Signal (GoLong/GoShort/Exit/Hold)
         │
    ┌────┼────┐
    ▼    ▼    ▼
Position  TradeLogger  PerformanceReport
```

## Public Types

### `AggregatedBar`

Rolls up per-second `SignalSample`s into a single 1-minute bar with statistics.

| Field Group | Fields | Description |
|---|---|---|
| **Time** | `bar_start_ms`, `bar_end_ms`, `sample_count` | Bar boundaries and sample count |
| **Price** | `open_price`, `close_price`, `high_price`, `low_price`, `avg_mid_price` | OHLCV from mid-price series |
| **Order Book** | `ob_imbalance_mean/max/min`, `ob_imbalance_trend` | Imbalance stats + linear regression slope |
| **Fill:Kill** | `fk_net_direction_mean/max/min/final`, `fk_buy/sell_fill/kill_total` | Direction aggregates + raw totals |
| **Aggression** | `aggression_shift_mean/max/min/final` | Shift statistics |
| **Absorption** | `absorption_score_max/mean/final` | Absorption statistics |
| **Impact** | `avg_impact_buy/sell_slippage_bps` | Average slippage for standard notional |
| **Trade Flow** | `total_trade_volume`, `avg_buy_volume_pct`, `avg_trade_count_per_sec` | Activity metrics |

### `SignalBarAggregator`

```rust
let mut aggregator = SignalBarAggregator::new(60_000); // 1-minute bars

// Call every second:
if let Some(bar) = aggregator.push(sample) {
    // A bar completed — feed it to the strategy engine
    let signal = engine.on_bar(&bar);
}
```

| Method | Description |
|---|---|
| `new(bar_duration_ms)` | Create aggregator. Bar boundaries are aligned to wall-clock multiples of `bar_duration_ms`. |
| `push(sample)` | Add a sample. Returns `Some(AggregatedBar)` when a bar closes. |
| `flush()` | Force-close the current partial bar (call at shutdown). |

### `Signal` enum

```rust
pub enum Signal {
    GoLong,      // Open a long position
    GoShort,     // Open a short position
    ExitLong,    // Close long position
    ExitShort,   // Close short position
    Hold,        // Do nothing
}
```

### `StrategyConfig`

All tunable parameters with sensible defaults.

| Field | Default | Description |
|---|---|---|
| `bar_duration_ms` | `60_000` | Bar aggregation window |
| `long_entry_threshold` | `0.5` | Minimum composite score for long entry |
| `short_entry_threshold` | `-0.5` | Maximum composite score for short entry |
| `min_absorption_for_reversal` | `0.7` | Absorption must exceed this for reversal entries |
| `long_exit_threshold` | `-0.2` | Score below this exits long |
| `short_exit_threshold` | `0.2` | Score above this exits short |
| `weight_fk_direction` | `0.30` | Weight for fill:kill net direction |
| `weight_aggression_shift` | `0.25` | Weight for aggression shift |
| `weight_ob_imbalance` | `0.15` | Weight for order book imbalance |
| `weight_absorption` | `0.15` | Weight for absorption signal |
| `weight_buy_volume_pct` | `0.15` | Weight for buy volume deviation |
| `risk_per_trade_pct` | `0.5` | Max equity % risked per trade |
| `max_position_size_usd` | `10_000.0` | Maximum position notional |
| `stop_loss_pct` | `0.15` | Stop loss as % of position value |
| `take_profit_pct` | `0.30` | Take profit as % of position value (~2:1 R:R) |
| `trailing_stop_activation_pct` | `0.15` | Activate trailing after this profit % |
| `trailing_stop_distance_pct` | `0.08` | Trailing stop distance from peak |
| `time_stop_ms` | `180_000` | Exit if no profit after 3 minutes |
| `max_open_positions` | `1` | Max concurrent positions |
| `daily_loss_limit_pct` | `3.0` | Halt trading if daily loss exceeds this % |
| `max_consecutive_losses` | `5` | Halt after this many consecutive losses |
| `cooldown_after_loss_ms` | `30_000` | Wait before re-entering after a loss |

### `StrategyEngine`

The core decision engine.

```rust
let mut engine = StrategyEngine::new(
    StrategyConfig::default(),
    10_000.0, // starting equity in USD
);

// On each completed bar:
let signal = engine.on_bar(&bar);
match signal {
    Signal::GoLong  => { /* execute buy */ }
    Signal::GoShort => { /* execute sell */ }
    Signal::ExitLong | Signal::ExitShort => { /* close position */ }
    Signal::Hold    => {}
}
```

#### Composite Score Formula

```
absorption_direction =
  if absorption_score_max > 0.7 AND fk_net_direction_mean < 0:  +absorption_score_max  (buy reversal)
  elif absorption_score_max > 0.7 AND fk_net_direction_mean > 0: -absorption_score_max  (sell reversal)
  else: 0.0

buy_volume_normalized = (avg_buy_volume_pct - 50.0) / 50.0

score = weight_fk_direction     * fk_net_direction_mean
      + weight_aggression_shift * aggression_shift_mean
      + weight_ob_imbalance     * ob_imbalance_mean
      + weight_absorption       * absorption_direction
      + weight_buy_volume_pct   * buy_volume_normalized
```

#### Entry Logic

1. Session must be active (circuit breakers not triggered)
2. Cooldown period must have elapsed since last exit
3. Score must cross threshold (`> 0.5` for long, `< -0.5` for short)
4. **Confirmation required**: either `absorption_score_max >= 0.7` (reversal) OR `fk_net_direction_mean > 0.2` / `< -0.2` (momentum)

#### Exit Logic (priority order)

1. **Stop loss**: unrealized loss >= `stop_loss_pct`
2. **Take profit**: unrealized profit >= `take_profit_pct`
3. **Trailing stop**: after `trailing_stop_activation_pct` profit, trail at `trailing_stop_distance_pct` from peak
4. **Time stop**: hold duration > `time_stop_ms` with no profit
5. **Signal reversal**: score crosses exit threshold

#### Position Sizing

```
quantity_usd = equity * (risk_per_trade_pct / 100) / (stop_loss_pct / 100)
quantity_usd = min(quantity_usd, max_position_size_usd)
quantity = quantity_usd / entry_price
```

#### Circuit Breakers

Trading halts when:
- Daily P&L drops below `-daily_loss_limit_pct` of equity
- Consecutive losses reach `max_consecutive_losses`

Reset with `engine.reset_daily(current_equity)` at session start.

#### Performance Accessors

| Method | Returns |
|---|---|
| `position()` | Current `&Position` |
| `trade_history()` | `&[TradeRecord]` |
| `equity()` | Current equity |
| `daily_pnl()` | Session P&L |
| `is_session_active()` | Whether circuit breakers allow trading |
| `win_rate()` | Win rate from trade history |
| `profit_factor()` | Gross profit / gross loss |
| `total_pnl()` | Sum of all trade P&Ls |
| `max_drawdown_pct()` | Maximum drawdown from equity curve |

### `Position`

| Field | Description |
|---|---|
| `state` | `Flat`, `Long`, or `Short` |
| `entry_price` | Fill price |
| `entry_time_ms` | Entry timestamp |
| `quantity` | Position size in base units |
| `unrealized_pnl` | Current floating P&L |
| `max_favorable` | Best unrealized profit since entry (for trailing stop) |
| `max_adverse` | Worst unrealized loss since entry |

### `TradeRecord`

Complete record of a closed trade: `id`, `symbol`, `side`, entry/exit times and prices, `quantity`, `pnl`, `pnl_pct`, `hold_duration_ms`, `exit_reason`.

### `TradeLogger`

```rust
let mut logger = TradeLogger::open("trades.csv")?;
logger.write_trade(&trade_record)?;
```

CSV columns: `id,symbol,side,entry_time_ms,exit_time_ms,entry_price,exit_price,quantity,pnl,pnl_pct,hold_duration_ms,exit_reason`

### `PerformanceReport`

Comprehensive analytics generated from trade history:

```rust
let report = PerformanceReport::from_trades(&engine.trade_history(), starting_equity);
println!("Win rate: {:.1}%", report.win_rate);
println!("Profit factor: {:.2}", report.profit_factor);
println!("Max drawdown: {:.2}%", report.max_drawdown_pct);
println!("Sharpe ratio: {:.2}", report.sharpe_ratio);
```

| Field | Description |
|---|---|
| `total_trades` | Total closed trades |
| `win_rate` | Winning trades / total trades |
| `total_pnl` / `total_pnl_pct` | Absolute and percentage P&L |
| `avg_win` / `avg_loss` | Average winning/losing trade P&L |
| `profit_factor` | Gross profit / gross loss |
| `max_drawdown_pct` | Maximum peak-to-trough drawdown |
| `sharpe_ratio` | Simplified Sharpe (risk-free = 0) |
| `avg_hold_duration_ms` | Average time in position |
| `best_trade_pnl` / `worst_trade_pnl` | Extremes |
| `longest_win_streak` / `longest_loss_streak` | Consecutive streaks |

---

## Full Pipeline Example

```rust
use crate::signal::{SignalExtractor, SignalConfig, SignalLogger};
use crate::strategy::{
    SignalBarAggregator, StrategyEngine, StrategyConfig, TradeLogger, PerformanceReport,
};

// Setup
let signal_config = SignalConfig { symbol: "btcusdt".into(), ..Default::default() };
let mut extractor = SignalExtractor::new(Arc::clone(&shared), signal_config);
let mut aggregator = SignalBarAggregator::new(60_000);
let mut engine = StrategyEngine::new(StrategyConfig::default(), 10_000.0);
let mut signal_logger = SignalLogger::open("btcusdt_signals.csv")?;
let mut trade_logger = TradeLogger::open("btcusdt_trades.csv")?;

// Main loop (every second)
loop {
    let sample = extractor.sample();
    signal_logger.write_sample(&sample)?;

    if let Some(bar) = aggregator.push(sample) {
        let signal = engine.on_bar(&bar);

        match signal {
            Signal::GoLong => execute_market_buy(engine.calculate_position_size(bar.close_price)),
            Signal::GoShort => execute_market_sell(engine.calculate_position_size(bar.close_price)),
            Signal::ExitLong | Signal::ExitShort => close_position(),
            Signal::Hold => {}
        }

        // Log completed trades
        for trade in &engine.trade_history()[prev_count..] {
            trade_logger.write_trade(trade)?;
        }
    }

    std::thread::sleep(Duration::from_secs(1));
}

// At end of session:
if let Some(partial) = aggregator.flush() {
    engine.on_bar(&partial);
}
let report = PerformanceReport::from_trades(engine.trade_history(), 10_000.0);
println!("{:#?}", report);
```

## Strategy Module Tests

11 unit tests covering:

| Test | What It Validates |
|---|---|
| `test_bar_aggregation` | 60 samples produce correct AggregatedBar fields |
| `test_bar_flush` | Partial bar returned on flush |
| `test_linear_slope` | Trend calculation for constant, increasing, decreasing series |
| `test_position_flat` | Initial position state |
| `test_position_update_market` | Unrealized P&L for long and short positions |
| `test_strategy_long_entry` | Bullish bar triggers GoLong |
| `test_strategy_short_entry` | Bearish bar triggers GoShort |
| `test_strategy_stop_loss` | Losing position triggers exit |
| `test_strategy_take_profit` | Winning position triggers exit at target |
| `test_strategy_circuit_breaker` | Trading halts after daily loss limit |
| `test_performance_report` | Report metrics calculated correctly from known trades |

Run with: `cargo test --release`