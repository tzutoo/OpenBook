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

/// Binance Futures Testnet HTTP client.
pub struct BinanceTestnetClient {
    api_key: String,
    secret_key: String,
    base_url: String,
    http: Client,
    recv_window: u64,
    error_log: Arc<Mutex<Vec<ApiLogEntry>>>,
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
        let params = vec![
            ("symbol".to_string(), symbol.to_string()),
            ("side".to_string(), side.to_uppercase()),
            ("type".to_string(), "MARKET".to_string()),
            ("quantity".to_string(), quantity.to_string()),
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

    /// Get available USDT balance.
    ///
    /// Convenience method that calls get_account and returns the USDT available balance.
    pub fn get_balance(&self) -> Result<f64, String> {
        let account = self.get_account()?;

        // Find USDT in assets or use availableBalance from top level
        for asset in &account.assets {
            if asset.asset == "USDT" {
                return Ok(asset.wallet_balance);
            }
        }

        // Fallback to top-level available balance (already in USDT terms)
        Ok(account.available_balance)
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