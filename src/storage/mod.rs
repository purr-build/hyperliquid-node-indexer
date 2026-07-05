use chrono::Utc;
use clickhouse::{Client, Row, error::Result, inserter::Inserter};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use std::time::Duration;

pub type Address = [u8; 20];
pub type Hash = [u8; 32];
pub type Cloid = [u8; 16];
pub type Decimal = i128;

const DECIMAL_SCALE: u32 = 18;
pub const DECIMAL_MULTIPLIER: i128 = 1_000_000_000_000_000_000;

pub fn hash(message: &[u8]) -> Hash {
    Keccak256::digest(message).into()
}

fn bytes_from_hex<const N: usize>(s: &str) -> Result<[u8; N], &'static str> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() != N * 2 {
        return Err("unexpected hex length");
    }
    let mut out = [0u8; N];
    for (i, chunk) in s.as_bytes().chunks(2).enumerate() {
        let hi = (chunk[0] as char).to_digit(16).ok_or("invalid hex")?;
        let lo = (chunk[1] as char).to_digit(16).ok_or("invalid hex")?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Ok(out)
}

pub fn address_from_hex(s: &str) -> Result<Address, &'static str> {
    bytes_from_hex(s)
}

pub fn hash_from_hex(s: &str) -> Result<Hash, &'static str> {
    bytes_from_hex(s)
}

pub fn cloid_from_hex(s: &str) -> Result<Cloid, &'static str> {
    bytes_from_hex(s)
}

/// Parse a big-endian integer hex string (e.g. an ECDSA signature `r`/`s`)
/// into a fixed-size buffer, right-aligned. Hyperliquid emits these as minimal
/// hex with leading zero bytes (and a leading zero nibble) stripped, so the
/// string may be shorter than `N * 2` chars or have odd length.
fn uint_from_hex<const N: usize>(s: &str) -> Result<[u8; N], &'static str> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() > N * 2 {
        return Err("unexpected hex length");
    }
    let mut out = [0u8; N];
    // Walk from the least-significant nibble so an odd-length string pads the
    // top nibble of the most-significant byte with zero.
    let bytes = s.as_bytes();
    let mut byte_idx = N;
    let mut i = bytes.len();
    while i > 0 {
        byte_idx -= 1;
        let lo = (bytes[i - 1] as char).to_digit(16).ok_or("invalid hex")?;
        let hi = if i >= 2 {
            (bytes[i - 2] as char).to_digit(16).ok_or("invalid hex")?
        } else {
            0
        };
        out[byte_idx] = ((hi << 4) | lo) as u8;
        i = i.saturating_sub(2);
    }
    Ok(out)
}

pub fn sig_from_hex(s: &str) -> Result<Hash, &'static str> {
    uint_from_hex(s)
}

pub fn decimal_from_str(s: &str) -> Result<Decimal, &'static str> {
    let (negative, unsigned) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };

    if unsigned.is_empty() {
        return Err("empty decimal");
    }

    let mut parts = unsigned.split('.');
    let whole = parts.next().unwrap_or_default();
    let frac = parts.next().unwrap_or_default();
    if parts.next().is_some() {
        return Err("invalid decimal");
    }
    if whole.is_empty() && frac.is_empty() {
        return Err("invalid decimal");
    }
    if frac.len() > DECIMAL_SCALE as usize {
        return Err("decimal scale exceeds supported precision");
    }

    let whole = if whole.is_empty() { "0" } else { whole };
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return Err("invalid decimal");
    }

    let whole = whole.parse::<i128>().map_err(|_| "invalid decimal")?;
    let mut value = whole
        .checked_mul(DECIMAL_MULTIPLIER)
        .ok_or("decimal overflow")?;

    if !frac.is_empty() {
        let padding = DECIMAL_SCALE - frac.len() as u32;
        let frac = frac.parse::<i128>().map_err(|_| "invalid decimal")?;
        let frac = frac
            .checked_mul(10_i128.pow(padding))
            .ok_or("decimal overflow")?;
        value = value.checked_add(frac).ok_or("decimal overflow")?;
    }

    Ok(if negative { -value } else { value })
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct BlockRow {
    pub number: u64,
    pub hash: Hash,
    pub proposer: Address,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub time: chrono::DateTime<Utc>,
    pub round: u64,
    pub parent_round: u64,
    pub hardfork_version: Option<u64>,
    pub hardfork_round: Option<u64>,
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct SignedActionBundleRow {
    pub block_number: u64,
    pub hash: Hash,
    pub broadcaster: Address,
    pub broadcaster_nonce: u64,
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct ActionRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub round: u64,
    pub proposer: Address,
    pub bundle_hash: Hash,
    pub broadcaster: Address,
    pub broadcaster_nonce: u64,
    pub nonce: u64,
    pub vault_address: Option<Address>,
    pub expires_after: Option<u64>,
    pub sig_r: Hash,
    pub sig_s: Hash,
    pub sig_v: u8,
    pub action_type: String,
    pub status: String,
    pub user: Option<Address>,
    pub response_type: String,
    pub payload: String,
}

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct NodeFillRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub local_time: chrono::DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub user: Address,
    pub coin: String,
    pub px: Decimal,
    pub sz: Decimal,
    pub side: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub time: chrono::DateTime<Utc>,
    pub start_position: Decimal,
    pub dir: String,
    pub closed_pnl: Decimal,
    pub hash: Hash,
    pub oid: u64,
    pub crossed: bool,
    pub liquidation_liquidated_user: Option<Address>,
    pub liquidation_mark_px: Option<Decimal>,
    pub liquidation_method: Option<String>,
    pub fee: Decimal,
    pub builder_fee: Option<Decimal>,
    pub tid: u64,
    pub cloid: Option<Cloid>,
    pub fee_token: String,
    pub builder: Option<Address>,
    pub twap_id: Option<u64>,
    pub deployer_fee: Option<Decimal>,
    pub priority_gas: Option<Decimal>,
}

/// Soft batch limits per inserter. Flushing on size (rows/bytes) keeps batches
/// large enough to avoid ClickHouse "too many parts" throttling; the period
/// bounds how long rows wait before being inserted during low-volume periods.
const INSERT_MAX_ROWS: u64 = 500_000;
const INSERT_MAX_BYTES: u64 = 256 * 1024 * 1024;
const INSERT_PERIOD: Duration = Duration::from_secs(10);

pub fn new_inserter<T: Row>(client: &Client, table: &str) -> Inserter<T> {
    client
        .inserter::<T>(table)
        .with_max_rows(INSERT_MAX_ROWS)
        .with_max_bytes(INSERT_MAX_BYTES)
        .with_period(Some(INSERT_PERIOD))
        .with_period_bias(0.1)
}
