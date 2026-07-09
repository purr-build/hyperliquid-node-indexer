use chrono::Utc;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use tailer::Event;
use types::SystemAndCoreWriterActionsData;

use crate::{
    metrics::Metrics,
    storage::{Address, Hash, address_from_hex, hash_from_hex, new_inserter},
    streams::{Stream, commit_metered, parse_datetime_nanos},
};

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct SystemAndCoreWriterActionRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub local_time: chrono::DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub event_index: u32,
    pub user: Address,
    pub nonce: u64,
    pub evm_tx_hash: Hash,
    pub action_type: String,
    pub payload: String,
}

pub fn parse(mut line: Vec<u8>) -> anyhow::Result<Vec<SystemAndCoreWriterActionRow>> {
    let data: SystemAndCoreWriterActionsData = simd_json::serde::from_slice(&mut line)?;
    let local_time = parse_datetime_nanos(&data.local_time)?;
    let block_time = parse_datetime_nanos(&data.block_time)?;
    let block_number = data.block_number;
    let mut rows = Vec::with_capacity(data.events.len());

    for (event_index, event) in data.events.into_iter().enumerate() {
        let event_index = u32::try_from(event_index)
            .map_err(|_| anyhow::anyhow!("system action event index overflow"))?;
        let user = address_from_hex(&event.user)
            .map_err(|e| anyhow::anyhow!(format!("parse system action user: {e}")))?;
        let evm_tx_hash = hash_from_hex(&event.evm_tx_hash)
            .map_err(|e| anyhow::anyhow!(format!("parse EVM transaction hash: {e}")))?;
        let mut action = event.action;
        let action_type = match action.remove("type") {
            Some(serde_json::Value::String(action_type)) => action_type,
            Some(_) => return Err(anyhow::anyhow!("system action type must be a string")),
            None => return Err(anyhow::anyhow!("system action type is missing")),
        };
        let payload = serde_json::to_string(&action)?;

        rows.push(SystemAndCoreWriterActionRow {
            local_time,
            block_time,
            block_number,
            event_index,
            user,
            nonce: event.nonce,
            evm_tx_hash,
            action_type,
            payload,
        });
    }

    Ok(rows)
}

pub struct SystemAndCoreWriterActions {
    inserter: Inserter<SystemAndCoreWriterActionRow>,
}

impl SystemAndCoreWriterActions {
    pub fn new(ch: &Client) -> Self {
        Self {
            inserter: new_inserter(ch, "system_and_core_writer_actions"),
        }
    }
}

impl Stream for SystemAndCoreWriterActions {
    const NAME: &'static str = "system_and_core_writer_actions";
    const SOURCE_DIR: &'static str = "system_and_core_writer_actions_streaming";

    type Rows = Vec<SystemAndCoreWriterActionRow>;

    fn parse(event: Event) -> anyhow::Result<Self::Rows> {
        parse(event.line)
    }

    async fn write(&mut self, rows: &Self::Rows, metrics: &Metrics) -> anyhow::Result<()> {
        for row in rows {
            let lag = (Utc::now() - row.block_time).as_seconds_f64();
            metrics.record_ingested(Self::NAME, "action");
            metrics.record_lag(Self::NAME, lag);
            self.inserter.write(row).await?;
        }

        Ok(())
    }

    async fn commit(&mut self, metrics: &Metrics, force: bool) -> anyhow::Result<()> {
        commit_metered(
            &mut self.inserter,
            "system_and_core_writer_actions",
            metrics,
            force,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINE: &[u8] = br#"{"local_time":"2026-07-09T19:00:49.731132384","block_time":"2026-07-09T19:00:49.386674453","block_number":1066583157,"events":[{"user":"0x8036620a20bedd7fc0a7c4ad2a356f5d5fab6b10","nonce":2153263,"evm_tx_hash":"0xb21121a74ebae96056849e7f39b4b200eda3383c44b1988f4e458bdd1064915c","action":{"type":"SystemSendAssetAction","destination":"0x6043daf4fbd6bd60601d277312a0664551302e70","fromSubAccount":null,"sourceDexOrSpot":0,"destinationDexOrSpot":0,"token":0,"wei":795700}},{"user":"0xb7b66a2fa21875cb62bb474a9d41808bdc77107b","nonce":2153272,"evm_tx_hash":"0xb7b417c43f774f0ac6e34ccbc4be3e48805d07279c37f570c6e0551b490ec557","action":{"type":"order","orders":[{"a":10107,"b":true,"p":"67.83","s":"1.8","r":false,"t":{"limit":{"tif":"Ioc"}}}],"grouping":"na"}}]}"#;

    #[test]
    fn parses_system_and_order_actions() {
        let rows = parse(LINE.to_vec()).expect("system actions should parse");

        assert_eq!(rows.len(), 2);
        let transfer = &rows[0];
        assert_eq!(transfer.block_number, 1066583157);
        assert_eq!(transfer.event_index, 0);
        assert_eq!(transfer.nonce, 2153263);
        assert_eq!(transfer.action_type, "SystemSendAssetAction");
        let payload: serde_json::Value = serde_json::from_str(&transfer.payload).unwrap();
        assert_eq!(payload["wei"], 795700);
        assert_eq!(payload["fromSubAccount"], serde_json::Value::Null);
        assert!(payload.get("type").is_none());

        let order = &rows[1];
        assert_eq!(order.event_index, 1);
        assert_eq!(order.action_type, "order");
        let payload: serde_json::Value = serde_json::from_str(&order.payload).unwrap();
        assert_eq!(payload["orders"][0]["a"], 10107);
        assert_eq!(payload["orders"][0]["p"], "67.83");
    }

    #[test]
    fn rejects_an_action_without_a_type() {
        let line = br#"{"local_time":"2026-07-09T19:00:49.731132384","block_time":"2026-07-09T19:00:49.386674453","block_number":1,"events":[{"user":"0xb7b66a2fa21875cb62bb474a9d41808bdc77107b","nonce":1,"evm_tx_hash":"0xb7b417c43f774f0ac6e34ccbc4be3e48805d07279c37f570c6e0551b490ec557","action":{"wei":1}}]}"#;

        let error = parse(line.to_vec()).expect_err("missing type should fail");
        assert!(error.to_string().contains("type is missing"));
    }
}
