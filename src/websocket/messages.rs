//! Outgoing websocket message shapes and JSON formatting helpers.

use chrono::{DateTime, Utc};
use serde::Serialize;
use tokio_tungstenite::tungstenite::Utf8Bytes;

use crate::{
    storage::{DECIMAL_MULTIPLIER, Decimal},
    streams::{
        hip3_oracle_updates::Hip3OracleUpdateRow, node_fills::NodeFillRow, replica_cmds::BlockRow,
    },
};

pub fn channel_msg<T: Serialize>(channel: &str, data: &T) -> Utf8Bytes {
    let msg = serde_json::json!({ "channel": channel, "data": data });
    Utf8Bytes::from(msg.to_string())
}

/// Displays a byte slice as 0x-prefixed lowercase hex.
struct Hex<'a>(&'a [u8]);

impl std::fmt::Display for Hex<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

fn decimal_string(v: Decimal) -> String {
    let negative = v < 0;
    let v = v.unsigned_abs();
    let multiplier = DECIMAL_MULTIPLIER as u128;
    let whole = v / multiplier;
    let frac = v % multiplier;

    let sign = if negative { "-" } else { "" };
    if frac == 0 {
        return format!("{sign}{whole}");
    }

    let frac = format!("{frac:018}");
    let frac = frac.trim_end_matches('0');
    format!("{sign}{whole}.{frac}")
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BlockMsg {
    number: u64,
    hash: String,
    proposer: String,
    time: DateTime<Utc>,
    round: u64,
    parent_round: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    hardfork_version: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hardfork_round: Option<u64>,
}

impl From<&BlockRow> for BlockMsg {
    fn from(row: &BlockRow) -> Self {
        Self {
            number: row.number,
            hash: Hex(&row.hash).to_string(),
            proposer: Hex(&row.proposer).to_string(),
            time: row.time,
            round: row.round,
            parent_round: row.parent_round,
            hardfork_version: row.hardfork_version,
            hardfork_round: row.hardfork_round,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LiquidationMsg {
    liquidated_user: String,
    mark_px: String,
    method: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeFillMsg {
    local_time: DateTime<Utc>,
    block_time: DateTime<Utc>,
    block_number: u64,
    user: String,
    coin: String,
    px: String,
    sz: String,
    side: String,
    time: i64,
    start_position: String,
    dir: String,
    closed_pnl: String,
    hash: String,
    oid: u64,
    crossed: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    liquidation: Option<LiquidationMsg>,
    fee: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    builder_fee: Option<String>,
    tid: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    cloid: Option<String>,
    fee_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    builder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    twap_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    deployer_fee: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority_gas: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Hip3OracleUpdateMsg {
    local_time: DateTime<Utc>,
    block_time: DateTime<Utc>,
    block_number: u64,
    coin: String,
    update_class: String,
    oracle_px: String,
    oracle_last_update_time: DateTime<Utc>,
    oracle_daily_px: String,
    mark_px: String,
    mark_last_update_time: DateTime<Utc>,
    mark_daily_px: String,
    external_px: String,
    external_last_update_time: DateTime<Utc>,
    external_daily_px: String,
    spot_px: String,
}

impl From<&Hip3OracleUpdateRow> for Hip3OracleUpdateMsg {
    fn from(row: &Hip3OracleUpdateRow) -> Self {
        Self {
            local_time: row.local_time,
            block_time: row.block_time,
            block_number: row.block_number,
            coin: row.coin.clone(),
            update_class: row.update_class.clone(),
            oracle_px: decimal_string(row.oracle_px),
            oracle_last_update_time: row.oracle_last_update_time,
            oracle_daily_px: decimal_string(row.oracle_daily_px),
            mark_px: decimal_string(row.mark_px),
            mark_last_update_time: row.mark_last_update_time,
            mark_daily_px: decimal_string(row.mark_daily_px),
            external_px: decimal_string(row.external_px),
            external_last_update_time: row.external_last_update_time,
            external_daily_px: decimal_string(row.external_daily_px),
            spot_px: decimal_string(row.spot_px),
        }
    }
}

impl From<&NodeFillRow> for NodeFillMsg {
    fn from(row: &NodeFillRow) -> Self {
        let liquidation = match (
            &row.liquidation_liquidated_user,
            row.liquidation_mark_px,
            &row.liquidation_method,
        ) {
            (Some(user), Some(mark_px), Some(method)) => Some(LiquidationMsg {
                liquidated_user: Hex(user).to_string(),
                mark_px: decimal_string(mark_px),
                method: method.clone(),
            }),
            _ => None,
        };

        Self {
            local_time: row.local_time,
            block_time: row.block_time,
            block_number: row.block_number,
            user: Hex(&row.user).to_string(),
            coin: row.coin.clone(),
            px: decimal_string(row.px),
            sz: decimal_string(row.sz),
            side: row.side.clone(),
            time: row.time.timestamp_millis(),
            start_position: decimal_string(row.start_position),
            dir: row.dir.clone(),
            closed_pnl: decimal_string(row.closed_pnl),
            hash: Hex(&row.hash).to_string(),
            oid: row.oid,
            crossed: row.crossed,
            liquidation,
            fee: decimal_string(row.fee),
            builder_fee: row.builder_fee.map(decimal_string),
            tid: row.tid,
            cloid: row.cloid.as_ref().map(|c| Hex(c).to_string()),
            fee_token: row.fee_token.clone(),
            builder: row.builder.as_ref().map(|b| Hex(b).to_string()),
            twap_id: row.twap_id,
            deployer_fee: row.deployer_fee.map(decimal_string),
            priority_gas: row.priority_gas.map(decimal_string),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decimal_string_formats() {
        assert_eq!(decimal_string(0), "0");
        assert_eq!(decimal_string(DECIMAL_MULTIPLIER), "1");
        assert_eq!(decimal_string(DECIMAL_MULTIPLIER / 2), "0.5");
        assert_eq!(decimal_string(-3 * DECIMAL_MULTIPLIER / 2), "-1.5");
        assert_eq!(decimal_string(1), "0.000000000000000001");
        assert_eq!(
            decimal_string(123 * DECIMAL_MULTIPLIER + DECIMAL_MULTIPLIER / 4),
            "123.25"
        );
    }

    #[test]
    fn hex_formats() {
        assert_eq!(Hex(&[0x00, 0xff, 0x1a]).to_string(), "0x00ff1a");
    }
}
