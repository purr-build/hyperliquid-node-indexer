use clickhouse::{Client, Row, error::Result, inserter::Inserter};
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

fn uint_from_hex<const N: usize>(s: &str) -> Result<[u8; N], &'static str> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.len() > N * 2 {
        return Err("unexpected hex length");
    }
    let mut out = [0u8; N];

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
