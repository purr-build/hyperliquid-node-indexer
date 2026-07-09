use chrono::Utc;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use tailer::Event;
use types::NodeTwapStatusesData;

use crate::{
    metrics::Metrics,
    storage::{Address, Decimal, address_from_hex, new_inserter},
    streams::{Stream, commit_metered, parse_datetime_nanos, parse_decimal_field, parse_millis},
};

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct NodeTwapStatusRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub local_time: chrono::DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub event_index: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub time: chrono::DateTime<Utc>,
    pub twap_id: u64,
    pub coin: String,
    pub user: Address,
    pub side: String,
    pub sz: Decimal,
    pub executed_sz: Decimal,
    pub executed_ntl: Decimal,
    pub minutes: u64,
    pub reduce_only: bool,
    pub randomize: bool,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: chrono::DateTime<Utc>,
    pub status: String,
    pub error: Option<String>,
}

pub fn parse(mut line: Vec<u8>) -> anyhow::Result<Vec<NodeTwapStatusRow>> {
    let data: NodeTwapStatusesData = simd_json::serde::from_slice(&mut line)?;
    let local_time = parse_datetime_nanos(&data.local_time)?;
    let block_time = parse_datetime_nanos(&data.block_time)?;
    let block_number = data.block_number;
    let mut rows = Vec::with_capacity(data.events.len());

    for (event_index, event) in data.events.into_iter().enumerate() {
        let event_index = u32::try_from(event_index)
            .map_err(|_| anyhow::anyhow!("node TWAP status event index overflow"))?;
        let time = parse_datetime_nanos(&event.time)?;
        let state = event.state;
        let user = address_from_hex(&state.user)
            .map_err(|e| anyhow::anyhow!(format!("parse TWAP user: {e}")))?;
        let timestamp = parse_millis(state.timestamp)?;
        let (status, error) = event.status.into_parts();

        rows.push(NodeTwapStatusRow {
            local_time,
            block_time,
            block_number,
            event_index,
            time,
            twap_id: event.twap_id,
            coin: state.coin,
            user,
            side: state.side,
            sz: parse_decimal_field(&state.sz, "sz")?,
            executed_sz: parse_decimal_field(&state.executed_sz, "executedSz")?,
            executed_ntl: parse_decimal_field(&state.executed_ntl, "executedNtl")?,
            minutes: state.minutes,
            reduce_only: state.reduce_only,
            randomize: state.randomize,
            timestamp,
            status,
            error,
        });
    }

    Ok(rows)
}

pub struct NodeTwapStatuses {
    inserter: Inserter<NodeTwapStatusRow>,
}

impl NodeTwapStatuses {
    pub fn new(ch: &Client) -> Self {
        Self {
            inserter: new_inserter(ch, "node_twap_statuses"),
        }
    }
}

impl Stream for NodeTwapStatuses {
    const NAME: &'static str = "node_twap_statuses";
    const SOURCE_DIR: &'static str = "node_twap_statuses_streaming";

    type Rows = Vec<NodeTwapStatusRow>;

    fn parse(event: Event) -> anyhow::Result<Self::Rows> {
        parse(event.line)
    }

    async fn write(&mut self, rows: &Self::Rows, metrics: &Metrics) -> anyhow::Result<()> {
        for row in rows {
            let lag = (Utc::now() - row.block_time).as_seconds_f64();
            metrics.record_ingested(Self::NAME, "status");
            metrics.record_lag(Self::NAME, lag);
            self.inserter.write(row).await?;
        }

        Ok(())
    }

    async fn commit(&mut self, metrics: &Metrics, force: bool) -> anyhow::Result<()> {
        commit_metered(&mut self.inserter, "node_twap_statuses", metrics, force).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::decimal_from_str;

    const LINE: &[u8] = br#"{"local_time":"2026-07-09T19:00:44.330633383","block_time":"2026-07-09T19:00:44.010972875","block_number":1066583083,"events":[{"time":"2026-07-09T19:00:44.010972875","twap_id":2018017,"state":{"coin":"xyz:LITE","user":"0xa6c9c1886ebfc76a4f409cb325e85916c0c4632c","side":"B","sz":"0.124","executedSz":"0.124","executedNtl":"99.4204","minutes":5,"reduceOnly":false,"randomize":false,"timestamp":1783623366714},"status":"finished"},{"time":"2026-07-09T19:00:44.010972875","twap_id":2017981,"state":{"coin":"ZEC","user":"0x63d6cbd12a2984c211ff623074e9362e4520e902","side":"B","sz":"3.55","executedSz":"2.29","executedNtl":"1132.9198","minutes":25,"reduceOnly":false,"randomize":true,"timestamp":1783622652255},"status":{"error":"Insufficient margin to place order."}}]}"#;

    #[test]
    fn parses_named_and_error_twap_statuses() {
        let rows = parse(LINE.to_vec()).expect("TWAP statuses should parse");

        assert_eq!(rows.len(), 2);
        let finished = &rows[0];
        assert_eq!(finished.block_number, 1066583083);
        assert_eq!(finished.event_index, 0);
        assert_eq!(finished.twap_id, 2018017);
        assert_eq!(finished.coin, "xyz:LITE");
        assert_eq!(finished.sz, decimal_from_str("0.124").unwrap());
        assert_eq!(finished.executed_ntl, decimal_from_str("99.4204").unwrap());
        assert_eq!(finished.timestamp.timestamp_millis(), 1783623366714);
        assert_eq!(finished.status, "finished");
        assert_eq!(finished.error, None);

        let error = &rows[1];
        assert_eq!(error.event_index, 1);
        assert_eq!(error.status, "error");
        assert_eq!(
            error.error.as_deref(),
            Some("Insufficient margin to place order.")
        );
        assert!(error.randomize);
    }
}
