//! Binance Futures Demo execution client module.
//!
//! Provides a blocking HTTP client for interacting with the Binance Futures
//! Demo API (https://demo-fapi.binance.com), including account queries,
//! order placement, and trade history.

use std::sync::{Arc, Mutex};

use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

/// Characters to encode: everything except unreserved URI chars (A-Z, a-z, 0-9, -, _, ., ~)
const URLENCODE_SET: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');
use reqwest::blocking::Client;
use sha2::Sha256;

// =============================================================================
// Data Structures
// =============================================================================

/// A single API error log entry.
#[derive(Clone, Debug)]
pub struct ApiLogEntry {
    /// Timestamp in epoch milliseconds.
    pub timestamp_ms: u64,
    /// HTTP method used ("GET", "POST", "DELETE").
    pub method: String,
    /// API endpoint path (e.g., "/fapi/v1/order").
    pub endpoint: String,
    /// HTTP status code received (None if network error).
    pub status_code: Option<u16>,
    /// Response body or error message.
    pub error_body: String,
    /// true = error, false = info/success.
    pub is_error: bool,
}

/// Account information from Binance Futures.
#[derive(Debug, Clone)]
pub struct AccountInfo {
    /// Total wallet balance across all assets (in USDT value).
    pub total_wallet_balance: f64,
    /// Total margin balance (wallet + unrealized PnL) — matches Binance UI "Margin Balance".
    pub total_margin_balance: f64,
    /// Available balance for trading.
    pub available_balance: f64,
    /// Total unrealized profit/loss.
    pub unrealized_pnl: f64,
    /// Per-asset balance breakdown.
    pub assets: Vec<AssetBalance>,
}

/// Per-asset balance information.
#[derive(Debug, Clone)]
pub struct AssetBalance {
    /// Asset symbol (e.g., "USDT").
    pub asset: String,
    /// Wallet balance for this asset.
    pub wallet_balance: f64,
    /// Unrealized profit/loss for this asset.
    pub unrealized_profit: f64,
}

/// Position information for a single symbol.
#[derive(Debug, Clone)]
pub struct PositionInfo {
    /// Trading symbol (e.g., "BTCUSDT").
    pub symbol: String,
    /// Position amount (positive = long, negative = short).
    pub position_amt: f64,
    /// Average entry price.
    pub entry_price: f64,
    /// Unrealized profit/loss.
    pub unrealized_pnl: f64,
    /// Current leverage.
    pub leverage: u32,
    /// Margin type ("isolated" or "cross").
    pub margin_type: String,
}

/// Result from placing an order.
#[derive(Debug, Clone)]
pub struct OrderResult {
    /// Order ID assigned by Binance.
    pub order_id: u64,
    /// Trading symbol.
    pub symbol: String,
    /// Order side ("BUY" or "SELL").
    pub side: String,
    /// Fill price (0 for market orders until filled).
    pub price: f64,
    /// Original order quantity.
    pub orig_qty: f64,
    /// Executed quantity.
    pub executed_qty: f64,
    /// Order status ("NEW", "PARTIALLY_FILLED", "FILLED", etc.).
    pub status: String,
    /// Order type ("MARKET", "LIMIT", etc.).
    #[allow(clippy::renamed_and_removed_lints)]
    #[allow(non_snake_case)]
    pub r#type: String,
}

/// A single user trade from trade history.
#[derive(Debug, Clone)]
pub struct UserTrade {
    /// Trade ID.
    pub id: u64,
    /// Trading symbol.
    pub symbol: String,
    /// Trade side ("BUY" or "SELL").
    pub side: String,
    /// Trade price.
    pub price: f64,
    /// Trade quantity.
    pub qty: f64,
    /// Realized profit/loss.
    pub realized_pnl: f64,
    /// Commission amount.
    pub commission: f64,
    /// Commission asset (e.g., "USDT").
    pub commission_asset: String,
    /// Trade timestamp in epoch milliseconds.
    pub time: u64,
    /// Position side ("BOTH", "LONG", "SHORT").
    pub position_side: String,
    /// Whether this user was the buyer.
    pub buyer: bool,
    /// Whether this user was the maker.
    pub maker: bool,
}

/// A completed round-trip trade, reconstructed from individual Binance fills.
///
/// Pairs consecutive fills into entry+exit. For example, BUY then SELL
/// becomes one LONG trade with entry price, exit price, and net PnL.
#[derive(Debug, Clone)]
pub struct RoundTripTrade {
    /// Sequential trade number (1-based).
    pub id: u64,
    /// Trade direction: "LONG" or "SHORT".
    pub side: String,
    /// Timestamp of the entry fill (epoch ms).
    pub entry_time_ms: u64,
    /// Timestamp of the exit fill (epoch ms).
    pub exit_time_ms: u64,
    /// Volume-weighted average entry price.
    pub entry_price: f64,
    /// Volume-weighted average exit price.
    pub exit_price: f64,
    /// Total quantity of the round-trip.
    pub quantity: f64,
    /// Sum of realizedPnl from all fills in this round-trip.
    pub realized_pnl: f64,
    /// Sum of commission from all fills in this round-trip (negative value from Binance).
    pub commission: f64,
    /// Net PnL = realized_pnl + commission.
    pub net_pnl: f64,
}

/// Pair individual Binance fills into round-trip trades.
///
/// Walks through fills chronologically and detects when a position is fully closed.
/// Handles partial fills, multiple entry fills at different prices, and partial closes.
///
/// # Algorithm
/// - Track cumulative position quantity (positive = long, negative = short)
/// - Track total cost basis for weighted average entry price
/// - When position returns to zero, record a completed round-trip
/// - Accumulate commission and realized_pnl across all fills in the trade
pub fn pair_round_trips(fills: &[UserTrade]) -> Vec<RoundTripTrade> {
    let mut round_trips = Vec::new();
    let mut position_qty: f64 = 0.0;    // positive = long, negative = short
    let mut position_cost: f64 = 0.0;   // total notional of current position
    let mut entry_time_ms: u64 = 0;
    let mut entry_side: &str = "";
    let mut trade_commission: f64 = 0.0;
    let mut trade_realized_pnl: f64 = 0.0;
    let mut next_id: u64 = 1;

    for fill in fills {
        let fill_qty = if fill.side == "BUY" {
            fill.qty
        } else {
            -fill.qty
        };
        let fill_notional = fill.price * fill.qty;

        let prev_qty = position_qty;
        position_qty += fill_qty;

        if prev_qty.abs() < 1e-15 {
            // Was flat — opening a new position
            entry_time_ms = fill.time;
            entry_side = if fill_qty > 0.0 { "LONG" } else { "SHORT" };
            position_cost = fill_notional;
            trade_commission = fill.commission;
            trade_realized_pnl = fill.realized_pnl;
        } else if prev_qty * position_qty > 0.0 {
            // Same direction — adding to position
            position_cost += fill_notional;
            trade_commission += fill.commission;
            trade_realized_pnl += fill.realized_pnl;
        } else {
            // Opposite direction — closing (partial or full)
            trade_commission += fill.commission;
            trade_realized_pnl += fill.realized_pnl;

            let closed_qty = fill_qty.abs().min(prev_qty.abs());
            let remaining_qty = prev_qty.abs() - closed_qty;

            if remaining_qty.abs() < 1e-15 {
                // Fully closed
                let entry_price = if prev_qty.abs() > 1e-15 {
                    position_cost / prev_qty.abs()
                } else {
                    fill.price
                };
                let exit_price = fill.price;
                let quantity = prev_qty.abs();

                // Commission is always a cost: testnet returns positive, mainnet returns negative.
                // Use abs() to correctly subtract regardless of sign.
                let net_pnl = trade_realized_pnl - trade_commission.abs();
                round_trips.push(RoundTripTrade {
                    id: next_id,
                    side: entry_side.to_string(),
                    entry_time_ms,
                    exit_time_ms: fill.time,
                    entry_price,
                    exit_price,
                    quantity,
                    realized_pnl: trade_realized_pnl,
                    commission: trade_commission,
                    net_pnl,
                });
                next_id += 1;

                // Reset tracking
                position_cost = 0.0;
                trade_commission = 0.0;
                trade_realized_pnl = 0.0;

                // Handle position flip (e.g., close long + open short in one fill)
                let flip_qty = fill_qty.abs() - closed_qty;
                if flip_qty.abs() > 1e-15 {
                    entry_time_ms = fill.time;
                    entry_side = if fill_qty > 0.0 { "LONG" } else { "SHORT" };
                    position_cost = fill.price * flip_qty;
                    // commission and realized_pnl already accumulated above for the
                    // closing portion; the flip portion's realized_pnl starts at 0
                    trade_realized_pnl = 0.0;
                }
            } else {
                // Partially closed — adjust cost basis proportionally
                let remaining_ratio = remaining_qty / prev_qty.abs();
                position_cost *= remaining_ratio;
                // position_qty already updated above
            }
        }
    }

    round_trips
}

/// Binance Futures Testnet HTTP client.
#[derive(Clone)]
pub struct BinanceTestnetClient {
    api_key: String,
    secret_key: String,
    base_url: String,
    http: Client,
    recv_window: u64,
    error_log: Arc<Mutex<Vec<ApiLogEntry>>>,
    step_size: f64,
    /// Step size for market orders (from MARKET_LOT_SIZE filter). Falls back to LOT_SIZE.
    market_step_size: f64,
    /// Minimum quantity for orders (from MARKET_LOT_SIZE or LOT_SIZE filter).
    min_qty: f64,
}

type HmacSha256 = Hmac<Sha256>;

// =============================================================================
// Helper Functions
// =============================================================================

/// Extract a numeric value from JSON, handling both Number and String types.
fn extract_f64(value: &serde_json::Value, field: &str) -> Result<f64, String> {
    match value.get(field) {
        Some(serde_json::Value::Number(n)) => n
            .as_f64()
            .ok_or_else(|| format!("Field '{}' is not a valid f64 number", field)),
        Some(serde_json::Value::String(s)) => s
            .parse::<f64>()
            .map_err(|e| format!("Field '{}' string '{}' is not a valid f64: {}", field, s, e)),
        Some(other) => Err(format!(
            "Field '{}' has unexpected type: {}",
            field,
            other
        )),
        None => Err(format!("Field '{}' not found in response", field)),
    }
}

/// Extract a numeric value as u64 from JSON, handling both Number and String types.
fn extract_u64(value: &serde_json::Value, field: &str) -> Result<u64, String> {
    match value.get(field) {
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| format!("Field '{}' is not a valid u64 number", field)),
        Some(serde_json::Value::String(s)) => s
            .parse::<u64>()
            .map_err(|e| format!("Field '{}' string '{}' is not a valid u64: {}", field, s, e)),
        Some(other) => Err(format!(
            "Field '{}' has unexpected type: {}",
            field,
            other
        )),
        None => Err(format!("Field '{}' not found in response", field)),
    }
}

/// Extract a numeric value as u32 from JSON, handling both Number and String types.
fn extract_u32(value: &serde_json::Value, field: &str) -> Result<u32, String> {
    match value.get(field) {
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .and_then(|v| v.try_into().ok())
            .ok_or_else(|| format!("Field '{}' is not a valid u32 number", field)),
        Some(serde_json::Value::String(s)) => s
            .parse::<u32>()
            .map_err(|e| format!("Field '{}' string '{}' is not a valid u32: {}", field, s, e)),
        Some(other) => Err(format!(
            "Field '{}' has unexpected type: {}",
            field,
            other
        )),
        None => Err(format!("Field '{}' not found in response", field)),
    }
}

/// Extract a string field from JSON.
fn extract_string(value: &serde_json::Value, field: &str) -> Result<String, String> {
    match value.get(field) {
        Some(serde_json::Value::String(s)) => Ok(s.clone()),
        Some(other) => Err(format!(
            "Field '{}' expected string, got: {}",
            field, other
        )),
        None => Err(format!("Field '{}' not found in response", field)),
    }
}

/// Extract a boolean field from JSON.
fn extract_bool(value: &serde_json::Value, field: &str) -> Result<bool, String> {
    match value.get(field) {
        Some(serde_json::Value::Bool(b)) => Ok(*b),
        Some(serde_json::Value::String(s)) => s
            .parse::<bool>()
            .map_err(|e| format!("Field '{}' string '{}' is not a valid bool: {}", field, s, e)),
        Some(other) => Err(format!(
            "Field '{}' expected bool, got: {}",
            field, other
        )),
        None => Err(format!("Field '{}' not found in response", field)),
    }
}

/// Get current timestamp in epoch milliseconds.
fn current_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// URL-encode a string for query parameters.
/// Encodes all characters except unreserved ones (A-Z, a-z, 0-9, -, _, ., ~).
fn url_encode(s: &str) -> String {
    utf8_percent_encode(s, URLENCODE_SET).to_string()
}

// =============================================================================
// BinanceTestnetClient Implementation
// =============================================================================

impl BinanceTestnetClient {
    /// Create a new Binance Futures Testnet client.
    ///
    /// # Arguments
    /// * `api_key` - Binance API key
    /// * `secret_key` - Binance secret key for signing requests
    pub fn new(api_key: String, secret_key: String) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("Failed to create HTTP client");

        Self {
            api_key,
            secret_key,
            base_url: "https://demo-fapi.binance.com".to_string(),
            http,
            recv_window: 5000,
            error_log: Arc::new(Mutex::new(Vec::new())),
            step_size: 0.001,
            market_step_size: 0.001,
            min_qty: 0.001,
        }
    }

    /// Sign request parameters with HMAC-SHA256.
    ///
    /// Adds timestamp and recvWindow, builds query string, signs it, and appends signature.
    fn sign(&self, params: &mut Vec<(String, String)>) -> String {
        // Add timestamp and recvWindow
        params.push(("timestamp".to_string(), current_timestamp_ms().to_string()));
        params.push(("recvWindow".to_string(), self.recv_window.to_string()));

        // Sort params alphabetically by key for consistent signing
        params.sort_by(|a, b| a.0.cmp(&b.0));

        // Build query string with URL encoding
        let query_string: String = params
            .iter()
            .map(|(k, v)| format!("{}={}", url_encode(k), url_encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        // HMAC-SHA256 sign
        let mut mac =
            HmacSha256::new_from_slice(self.secret_key.as_bytes()).expect("HMAC can take key of any size");
        mac.update(query_string.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        // Append signature to params
        params.push(("signature".to_string(), signature.clone()));

        // Return full query string including signature
        format!("{}&signature={}", query_string, signature)
    }

    /// Log an API call entry.
    fn log(
        &self,
        method: &str,
        endpoint: &str,
        status_code: Option<u16>,
        error_body: &str,
        is_error: bool,
    ) {
        let entry = ApiLogEntry {
            timestamp_ms: current_timestamp_ms(),
            method: method.to_string(),
            endpoint: endpoint.to_string(),
            status_code,
            error_body: error_body.to_string(),
            is_error,
        };

        if let Ok(mut log) = self.error_log.lock() {
            log.push(entry);
            // Keep max 500 entries
            if log.len() > 500 {
                let drain_count = log.len() - 500;
                log.drain(..drain_count);
            }
        }
    }

    /// Execute a signed GET request.
    fn signed_get(
        &self,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<serde_json::Value, String> {
        let mut params = params;
        let query_string = self.sign(&mut params);
        let url = format!("{}{}?{}", self.base_url, path, query_string);

        let result = self
            .http
            .get(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send();

        match result {
            Ok(resp) => {
                let status = resp.status();
                let status_code = status.as_u16();
                match resp.text() {
                    Ok(body) => {
                        if status.is_success() {
                            self.log("GET", path, Some(status_code), &body, false);
                            serde_json::from_str(&body).map_err(|e| {
                                let err_msg = format!("Failed to parse GET {} response: {}", path, e);
                                self.log("GET", path, Some(status_code), &err_msg, true);
                                err_msg
                            })
                        } else {
                            let err_msg = format!("GET {} failed ({}): {}", path, status_code, body);
                            self.log("GET", path, Some(status_code), &err_msg, true);
                            Err(err_msg)
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("GET {} failed to read response: {}", path, e);
                        self.log("GET", path, Some(status_code), &err_msg, true);
                        Err(err_msg)
                    }
                }
            }
            Err(e) => {
                let err_msg = format!("GET {} request failed: {}", path, e);
                self.log("GET", path, None, &err_msg, true);
                Err(err_msg)
            }
        }
    }

    /// Execute a signed POST request.
    fn signed_post(
        &self,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<serde_json::Value, String> {
        let mut params = params;
        let query_string = self.sign(&mut params);
        let url = format!("{}{}?{}", self.base_url, path, query_string);

        let result = self
            .http
            .post(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send();

        match result {
            Ok(resp) => {
                let status = resp.status();
                let status_code = status.as_u16();
                match resp.text() {
                    Ok(body) => {
                        if status.is_success() {
                            self.log("POST", path, Some(status_code), &body, false);
                            serde_json::from_str(&body).map_err(|e| {
                                let err_msg = format!("Failed to parse POST {} response: {}", path, e);
                                self.log("POST", path, Some(status_code), &err_msg, true);
                                err_msg
                            })
                        } else {
                            let err_msg = format!("POST {} failed ({}): {}", path, status_code, body);
                            self.log("POST", path, Some(status_code), &err_msg, true);
                            Err(err_msg)
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("POST {} failed to read response: {}", path, e);
                        self.log("POST", path, Some(status_code), &err_msg, true);
                        Err(err_msg)
                    }
                }
            }
            Err(e) => {
                let err_msg = format!("POST {} request failed: {}", path, e);
                self.log("POST", path, None, &err_msg, true);
                Err(err_msg)
            }
        }
    }

    /// Execute a signed DELETE request.
    fn signed_delete(
        &self,
        path: &str,
        params: Vec<(String, String)>,
    ) -> Result<serde_json::Value, String> {
        let mut params = params;
        let query_string = self.sign(&mut params);
        let url = format!("{}{}?{}", self.base_url, path, query_string);

        let result = self
            .http
            .delete(&url)
            .header("X-MBX-APIKEY", &self.api_key)
            .send();

        match result {
            Ok(resp) => {
                let status = resp.status();
                let status_code = status.as_u16();
                match resp.text() {
                    Ok(body) => {
                        if status.is_success() {
                            self.log("DELETE", path, Some(status_code), &body, false);
                            serde_json::from_str(&body).map_err(|e| {
                                let err_msg =
                                    format!("Failed to parse DELETE {} response: {}", path, e);
                                self.log("DELETE", path, Some(status_code), &err_msg, true);
                                err_msg
                            })
                        } else {
                            let err_msg =
                                format!("DELETE {} failed ({}): {}", path, status_code, body);
                            self.log("DELETE", path, Some(status_code), &err_msg, true);
                            Err(err_msg)
                        }
                    }
                    Err(e) => {
                        let err_msg = format!("DELETE {} failed to read response: {}", path, e);
                        self.log("DELETE", path, Some(status_code), &err_msg, true);
                        Err(err_msg)
                    }
                }
            }
            Err(e) => {
                let err_msg = format!("DELETE {} request failed: {}", path, e);
                self.log("DELETE", path, None, &err_msg, true);
                Err(err_msg)
            }
        }
    }

    // =========================================================================
    // Public API Methods
    // =========================================================================

    /// Test API connection and validate API keys.
    ///
    /// Calls GET /fapi/v2/account to verify the keys are valid.
    /// Returns Ok(()) if successful, Err with description if not.
    pub fn test_connection(&self) -> Result<(), String> {
        match self.signed_get("/fapi/v2/account", vec![]) {
            Ok(_) => Ok(()),
            Err(e) => {
                // Check for authentication errors
                if e.contains("401") || e.contains("403") || e.contains("Invalid API-key") {
                    Err(format!("API keys are invalid: {}", e))
                } else if e.contains("Signature") {
                    Err(format!("Signature verification failed: {}", e))
                } else {
                    Err(format!("Connection test failed: {}", e))
                }
            }
        }
    }

    /// Get account information including balances.
    ///
    /// Returns account summary with total balances and per-asset breakdown.
    pub fn get_account(&self) -> Result<AccountInfo, String> {
        let response = self.signed_get("/fapi/v2/account", vec![])?;

        let total_wallet_balance = extract_f64(&response, "totalWalletBalance")?;
        let total_margin_balance = extract_f64(&response, "totalMarginBalance")?;
        let available_balance = extract_f64(&response, "availableBalance")?;
        let unrealized_pnl = extract_f64(&response, "totalUnrealizedProfit")?;

        let assets = match response.get("assets") {
            Some(serde_json::Value::Array(arr)) => arr
                .iter()
                .filter_map(|asset| {
                    let asset_symbol = extract_string(asset, "asset").ok()?;
                    let wallet_balance = extract_f64(asset, "walletBalance").ok()?;
                    let unrealized_profit = extract_f64(asset, "unrealizedProfit").ok()?;
                    Some(AssetBalance {
                        asset: asset_symbol,
                        wallet_balance,
                        unrealized_profit,
                    })
                })
                .collect(),
            _ => Vec::new(),
        };

        Ok(AccountInfo {
            total_wallet_balance,
            total_margin_balance,
            available_balance,
            unrealized_pnl,
            assets,
        })
    }

    /// Get position information for a specific symbol.
    ///
    /// Returns None if no position is held (positionAmt == 0).
    pub fn get_position(&self, symbol: &str) -> Result<Option<PositionInfo>, String> {
        let params = vec![("symbol".to_string(), symbol.to_string())];
        let response = self.signed_get("/fapi/v2/positionRisk", params)?;

        let positions = match response {
            serde_json::Value::Array(arr) => arr,
            _ => return Err("Expected array response from positionRisk".to_string()),
        };

        // Find the position matching the symbol
        for pos in positions {
            let pos_symbol = match extract_string(&pos, "symbol") {
                Ok(s) => s,
                Err(_) => continue,
            };

            if pos_symbol != symbol {
                continue;
            }

            let position_amt = extract_f64(&pos, "positionAmt")?;

            // Return None if no position
            if position_amt == 0.0 {
                return Ok(None);
            }

            let entry_price = extract_f64(&pos, "entryPrice")?;
            let unrealized_pnl = extract_f64(&pos, "unRealizedProfit")?;
            let leverage = extract_u32(&pos, "leverage")?;
            let margin_type = extract_string(&pos, "marginType")?;

            return Ok(Some(PositionInfo {
                symbol: pos_symbol,
                position_amt,
                entry_price,
                unrealized_pnl,
                leverage,
                margin_type,
            }));
        }

        Ok(None)
    }

    /// Place a market order.
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "BTCUSDT")
    /// * `side` - Order side ("BUY" or "SELL")
    /// * `quantity` - Order quantity in base asset
    pub fn place_market_order(
        &self,
        symbol: &str,
        side: &str,
        quantity: f64,
    ) -> Result<OrderResult, String> {
        // Enforce minimum quantity from MARKET_LOT_SIZE / LOT_SIZE filter
        if quantity < self.min_qty {
            return Err(format!(
                "Quantity {} is below minimum {} for {}",
                quantity, self.min_qty, symbol
            ));
        }

        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_uppercase()),
            ("type".to_string(), "MARKET".to_string()),
            ("quantity".to_string(), self.format_quantity(quantity)),
        ];

        let response = self.signed_post("/fapi/v1/order", params)?;

        let order_id = extract_u64(&response, "orderId")?;
        let order_symbol = extract_string(&response, "symbol")?;
        let order_side = extract_string(&response, "side")?;
        let price = extract_f64(&response, "price")?;
        let orig_qty = extract_f64(&response, "origQty")?;
        let executed_qty = extract_f64(&response, "executedQty")?;
        let status = extract_string(&response, "status")?;
        let order_type = extract_string(&response, "type")?;

        Ok(OrderResult {
            order_id,
            symbol: order_symbol,
            side: order_side,
            price,
            orig_qty,
            executed_qty,
            status,
            r#type: order_type,
        })
    }

    /// Fetch symbol info from exchange and update quantity precision fields.
    ///
    /// Extracts `MARKET_LOT_SIZE` (preferred for market orders) and `LOT_SIZE`
    /// (fallback) filters to set `market_step_size`, `step_size`, and `min_qty`.
    ///
    /// This should be called after login and when changing symbols.
    pub fn fetch_symbol_info(&mut self, symbol: &str) {
        let url = format!(
            "{}{}?symbol={}",
            self.base_url, "/fapi/v1/exchangeInfo", symbol.to_uppercase()
        );
        
        let result = self.http.get(&url).send();
        match result {
            Ok(resp) => {
                let status = resp.status().as_u16();
                match resp.text() {
                    Ok(body) => {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&body) {
                            // Navigate to filters
                            if let Some(symbols) = json.get("symbols").and_then(|s| s.as_array()) {
                                for sym in symbols {
                                    if sym.get("symbol").and_then(|v| v.as_str()) == Some(&symbol.to_uppercase()) {
                                        if let Some(filters) = sym.get("filters").and_then(|f| f.as_array()) {
                                            // Scan all filters to extract both MARKET_LOT_SIZE and LOT_SIZE
                                            let mut lot_step: Option<f64> = None;
                                            let mut lot_min: Option<f64> = None;
                                            let mut market_step: Option<f64> = None;
                                            let mut market_min: Option<f64> = None;

                                            for filter in filters {
                                                let filter_type = filter.get("filterType")
                                                    .and_then(|v| v.as_str())
                                                    .unwrap_or("");

                                                match filter_type {
                                                    "LOT_SIZE" => {
                                                        lot_step = filter.get("stepSize")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok())
                                                            .filter(|&v| v > 0.0);
                                                        lot_min = filter.get("minQty")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok())
                                                            .filter(|&v| v > 0.0);
                                                    }
                                                    "MARKET_LOT_SIZE" => {
                                                        market_step = filter.get("stepSize")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok())
                                                            .filter(|&v| v > 0.0);
                                                        market_min = filter.get("minQty")
                                                            .and_then(|v| v.as_str())
                                                            .and_then(|s| s.parse::<f64>().ok())
                                                            .filter(|&v| v > 0.0);
                                                    }
                                                    _ => {}
                                                }
                                            }

                                            // Apply: prefer MARKET_LOT_SIZE, fall back to LOT_SIZE
                                            if let Some(step) = market_step {
                                                self.market_step_size = step;
                                            } else if let Some(step) = lot_step {
                                                self.market_step_size = step;
                                            }
                                            if let Some(step) = lot_step {
                                                self.step_size = step;
                                            }
                                            // min_qty: prefer MARKET_LOT_SIZE, fall back to LOT_SIZE
                                            if let Some(min) = market_min {
                                                self.min_qty = min;
                                            } else if let Some(min) = lot_min {
                                                self.min_qty = min;
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                            self.log("GET", "/fapi/v1/exchangeInfo", Some(status), "Symbol info fetched", false);
                        }
                    }
                    Err(e) => {
                        self.log("GET", "/fapi/v1/exchangeInfo", Some(status), &format!("Failed to read response: {}", e), true);
                    }
                }
            }
            Err(e) => {
                self.log("GET", "/fapi/v1/exchangeInfo", None, &format!("Request failed: {}", e), true);
            }
        }
    }

    /// Round quantity to the symbol's market_step_size and format without trailing zeros.
    ///
    /// Uses MARKET_LOT_SIZE stepSize (preferred) or LOT_SIZE stepSize (fallback).
    ///
    /// For example, with market_step_size=0.001:
    ///   0.0049999999 -> "0.005"
    ///   1.23456 -> "1.234"
    fn format_quantity(&self, quantity: f64) -> String {
        if self.market_step_size <= 0.0 {
            return format!("{}", quantity);
        }
        
        // Calculate decimal places from market_step_size
        let decimals = if self.market_step_size >= 1.0 {
            0
        } else {
            let s = format!("{}", self.market_step_size);
            // Find decimal places from string representation
            if let Some(dot_pos) = s.find('.') {
                let fractional = s[dot_pos + 1..].trim_end_matches('0');
                fractional.len()
            } else {
                0
            }
        };
        
        // Round to market_step_size
        let rounded = (quantity / self.market_step_size).round() * self.market_step_size;
        
        // Format with the correct number of decimal places, then strip trailing zeros
        let formatted = format!("{:.prec$}", rounded, prec = decimals);
        // Strip trailing zeros after decimal point, but keep at least one decimal if decimals > 0
        if decimals > 0 {
            let trimmed = formatted.trim_end_matches('0').trim_end_matches('.');
            // If we stripped everything after the dot, add back a single "0" for the required precision
            if !trimmed.contains('.') && decimals > 0 {
                // This means the value is an integer but step_size requires decimals
                format!("{}.", trimmed)
            } else {
                trimmed.to_string()
            }
        } else {
            format!("{:.0}", rounded)
        }
    }

    /// Get the current step_size for quantity precision.
    pub fn step_size(&self) -> f64 {
        self.step_size
    }

    /// Get user trade history for a symbol.
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol
    /// * `from_id` - Optional trade ID to start from (inclusive)
    /// * `limit` - Number of trades to return (default 50, max 1000)
    pub fn get_user_trades(
        &self,
        symbol: &str,
        from_id: Option<u64>,
        limit: u32,
    ) -> Result<Vec<UserTrade>, String> {
        let mut params: Vec<(String, String)> = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("limit".to_string(), limit.min(1000).to_string()),
        ];

        if let Some(id) = from_id {
            params.push(("fromId".to_string(), id.to_string()));
        }

        let response = self.signed_get("/fapi/v1/userTrades", params)?;

        let trades = match response {
            serde_json::Value::Array(arr) => arr,
            _ => return Err("Expected array response from userTrades".to_string()),
        };

        let mut result = Vec::with_capacity(trades.len());

        for trade in trades {
            let id = extract_u64(&trade, "id")?;
            let trade_symbol = extract_string(&trade, "symbol")?;
            let side = extract_string(&trade, "side")?;
            let price = extract_f64(&trade, "price")?;
            let qty = extract_f64(&trade, "qty")?;
            let realized_pnl = extract_f64(&trade, "realizedPnl")?;
            let commission = extract_f64(&trade, "commission")?;
            let commission_asset = extract_string(&trade, "commissionAsset")?;
            let time = extract_u64(&trade, "time")?;
            let position_side = extract_string(&trade, "positionSide")?;
            let buyer = extract_bool(&trade, "buyer")?;
            let maker = extract_bool(&trade, "maker")?;

            result.push(UserTrade {
                id,
                symbol: trade_symbol,
                side,
                price,
                qty,
                realized_pnl,
                commission,
                commission_asset,
                time,
                position_side,
                buyer,
                maker,
            });
        }

        Ok(result)
    }

    /// Fetch all user trades for a symbol, paginating through the full history.
    ///
    /// Returns trades oldest-first. The Binance API returns max 1000 per request,
    /// so this method pages through using `fromId` until fewer than 1000 are returned.
    pub fn fetch_all_user_trades(
        &self,
        symbol: &str,
    ) -> Result<Vec<UserTrade>, String> {
        let mut all_trades = Vec::new();
        // Start from the oldest available trades (id=1) and page forward.
        // Without fromId the API returns the most recent 7 days only.
        let mut from_id: Option<u64> = Some(1);
        let page_limit = 1000u32;

        loop {
            let mut trades = self.get_user_trades(symbol, from_id, page_limit)?;
            let count = trades.len();

            if let Some(last) = trades.last() {
                from_id = Some(last.id + 1);
            }

            all_trades.append(&mut trades);

            if count < page_limit as usize {
                break;
            }
        }

        Ok(all_trades)
    }

    /// Get available USDT balance.
    ///
    /// Convenience method that calls get_account and returns the USDT available balance.
    pub fn get_balance(&self) -> Result<f64, String> {
        let account = self.get_account()?;
        // Return totalMarginBalance — matches "Margin Balance" in Binance UI
        Ok(account.total_margin_balance)
    }

    /// Get a copy of the current error log entries.
    pub fn error_log(&self) -> Vec<ApiLogEntry> {
        self.error_log
            .lock()
            .map(|log| log.clone())
            .unwrap_or_default()
    }

    /// Clear all error log entries.
    pub fn clear_error_log(&self) {
        if let Ok(mut log) = self.error_log.lock() {
            log.clear();
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_f64_from_number() {
        let json: serde_json::Value = serde_json::json!({"value": 123.45});
        assert_eq!(extract_f64(&json, "value").unwrap(), 123.45);
    }

    #[test]
    fn test_extract_f64_from_string() {
        let json: serde_json::Value = serde_json::json!({"value": "123.45"});
        assert_eq!(extract_f64(&json, "value").unwrap(), 123.45);
    }

    #[test]
    fn test_extract_f64_missing() {
        let json: serde_json::Value = serde_json::json!({"other": 123.45});
        assert!(extract_f64(&json, "value").is_err());
    }

    #[test]
    fn test_extract_u64_from_number() {
        let json: serde_json::Value = serde_json::json!({"value": 12345});
        assert_eq!(extract_u64(&json, "value").unwrap(), 12345);
    }

    #[test]
    fn test_extract_u64_from_string() {
        let json: serde_json::Value = serde_json::json!({"value": "12345"});
        assert_eq!(extract_u64(&json, "value").unwrap(), 12345);
    }

    #[test]
    fn test_extract_string() {
        let json: serde_json::Value = serde_json::json!({"name": "BTCUSDT"});
        assert_eq!(extract_string(&json, "name").unwrap(), "BTCUSDT");
    }

    #[test]
    fn test_extract_bool() {
        let json: serde_json::Value = serde_json::json!({"buyer": true});
        assert!(extract_bool(&json, "buyer").unwrap());
    }

    #[test]
    fn test_url_encode() {
        // Simple alphanumeric should pass through
        assert_eq!(url_encode("BTCUSDT"), "BTCUSDT");
        assert_eq!(url_encode("12345"), "12345");
        
        // Special characters should be encoded
        assert_eq!(url_encode(" "), "%20");
        assert_eq!(url_encode("&"), "%26");
        assert_eq!(url_encode("="), "%3D");
        
        // Unreserved characters should pass through
        assert_eq!(url_encode("-"), "-");
        assert_eq!(url_encode("_"), "_");
        assert_eq!(url_encode("."), ".");
        assert_eq!(url_encode("~"), "~");
    }

    #[test]
    fn test_sign_produces_valid_signature() {
        // Create a client with known keys
        let client = BinanceTestnetClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
        );

        let mut params = vec![
            ("symbol".to_string(), "BTCUSDT".to_string()),
        ];

        let query_string = client.sign(&mut params);

        // Should contain the original params
        assert!(query_string.contains("symbol=BTCUSDT"));
        // Should contain timestamp and recvWindow
        assert!(query_string.contains("timestamp="));
        assert!(query_string.contains("recvWindow=5000"));
        // Should contain signature
        assert!(query_string.contains("signature="));

        // Signature should be a hex string
        let sig_part: Vec<&str> = query_string.split("signature=").collect();
        assert_eq!(sig_part.len(), 2);
        let sig = sig_part[1];
        assert!(sig.len() == 64); // SHA256 hex is 64 chars
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_log_entry_creation() {
        let client = BinanceTestnetClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
        );

        client.log("GET", "/fapi/v1/test", Some(200), "OK", false);

        let log = client.error_log();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0].method, "GET");
        assert_eq!(log[0].endpoint, "/fapi/v1/test");
        assert_eq!(log[0].status_code, Some(200));
        assert!(!log[0].is_error);
    }

    #[test]
    fn test_log_max_entries() {
        let client = BinanceTestnetClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
        );

        // Add more than 500 entries
        for i in 0..600 {
            client.log("GET", &format!("/test/{}", i), Some(200), "OK", false);
        }

        let log = client.error_log();
        assert_eq!(log.len(), 500);
        // Oldest entries should be drained
        assert_eq!(log[0].endpoint, "/test/100");
    }

    #[test]
    fn test_clear_error_log() {
        let client = BinanceTestnetClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
        );

        client.log("GET", "/test1", Some(200), "OK", false);
        client.log("GET", "/test2", Some(500), "Error", true);
        assert_eq!(client.error_log().len(), 2);

        client.clear_error_log();
        assert_eq!(client.error_log().len(), 0);
    }

    #[test]
    fn test_params_sorted_before_signing() {
        let client = BinanceTestnetClient::new(
            "test_api_key".to_string(),
            "test_secret_key".to_string(),
        );

        let mut params = vec![
            ("z".to_string(), "last".to_string()),
            ("a".to_string(), "first".to_string()),
            ("m".to_string(), "middle".to_string()),
        ];

        let query_string = client.sign(&mut params);

        // Check that params are sorted (after removing signature part)
        let without_sig = query_string.split("&signature=").next().unwrap();
        let parts: Vec<&str> = without_sig.split('&').collect();

        // Find the non-timestamp/recvWindow params
        let param_parts: Vec<&str> = parts
            .iter()
            .filter(|p| !p.starts_with("timestamp=") && !p.starts_with("recvWindow="))
            .copied()
            .collect();

        assert_eq!(param_parts[0], "a=first");
        assert_eq!(param_parts[1], "m=middle");
        assert_eq!(param_parts[2], "z=last");
    }
}