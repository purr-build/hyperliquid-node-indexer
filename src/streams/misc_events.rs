use chrono::Utc;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use tailer::Event;
use types::MiscEventsData;

use crate::{
    metrics::Metrics,
    storage::{Hash, hash, hash_from_hex, new_inserter},
    streams::{Stream, commit_metered, parse_datetime_nanos},
};

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct MiscEventRow {
    pub id: Hash,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub local_time: chrono::DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub event_index: u32,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub time: chrono::DateTime<Utc>,
    pub hash: Hash,
    pub event_type: String,
    pub payload: String,
}

pub fn parse(mut line: Vec<u8>) -> anyhow::Result<Vec<MiscEventRow>> {
    let data: MiscEventsData = simd_json::serde::from_slice(&mut line)?;
    let local_time = parse_datetime_nanos(&data.local_time)?;
    let block_time = parse_datetime_nanos(&data.block_time)?;
    let block_number = data.block_number;
    let mut rows = Vec::with_capacity(data.events.len());

    for (event_index, event) in data.events.into_iter().enumerate() {
        let event_index =
            u32::try_from(event_index).map_err(|_| anyhow::anyhow!("misc event index overflow"))?;
        let event_time = parse_datetime_nanos(&event.time)?;
        let event_hash = hash_from_hex(&event.hash)
            .map_err(|e| anyhow::anyhow!(format!("parse misc event hash: {e}")))?;
        let (event_type, payload) = event.kind_and_payload().map_err(|e| anyhow::anyhow!(e))?;
        let event_type = event_type.to_string();
        let payload = serde_json::to_string(&payload)?;
        let id = event_id(
            block_number,
            &data.local_time,
            event_index,
            &event.time,
            &event_type,
            &payload,
        );

        rows.push(MiscEventRow {
            id,
            local_time,
            block_time,
            block_number,
            event_index,
            time: event_time,
            hash: event_hash,
            event_type,
            payload,
        });
    }

    Ok(rows)
}

fn event_id(
    block_number: u64,
    local_time: &str,
    event_index: u32,
    time: &str,
    event_type: &str,
    payload: &str,
) -> Hash {
    let mut identity = Vec::with_capacity(
        12 + local_time.len() + time.len() + event_type.len() + payload.len() + 3,
    );
    identity.extend_from_slice(&block_number.to_be_bytes());
    identity.extend_from_slice(local_time.as_bytes());
    identity.push(0);
    identity.extend_from_slice(&event_index.to_be_bytes());
    identity.extend_from_slice(time.as_bytes());
    identity.push(0);
    identity.extend_from_slice(event_type.as_bytes());
    identity.push(0);
    identity.extend_from_slice(payload.as_bytes());
    hash(&identity)
}

pub struct MiscEvents {
    inserter: Inserter<MiscEventRow>,
}

impl MiscEvents {
    pub fn new(ch: &Client) -> Self {
        Self {
            inserter: new_inserter(ch, "misc_events"),
        }
    }
}

impl Stream for MiscEvents {
    const NAME: &'static str = "misc_events";
    const SOURCE_DIR: &'static str = "misc_events_streaming";

    type Rows = Vec<MiscEventRow>;

    fn parse(event: Event) -> anyhow::Result<Self::Rows> {
        parse(event.line)
    }

    async fn write(&mut self, rows: &Self::Rows, metrics: &Metrics) -> anyhow::Result<()> {
        for row in rows {
            let lag = (Utc::now() - row.block_time).as_seconds_f64();
            metrics.record_ingested(Self::NAME, "event");
            metrics.record_lag(Self::NAME, lag);
            self.inserter.write(row).await?;
        }

        Ok(())
    }

    async fn commit(&mut self, metrics: &Metrics, force: bool) -> anyhow::Result<()> {
        commit_metered(&mut self.inserter, "misc_events", metrics, force).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINE: &[u8] = br#"{"local_time":"2026-07-09T19:03:32.961209966","block_time":"2026-07-09T19:03:32.561209966","block_number":1066589454,"events":[{"time":"2026-07-09T19:03:32.561209966","hash":"0xc969ce4819514a6dcae3043f92cd6b01de00e62db454693f6d32799ad8552458","inner":{"CDeposit":{"user":"0x821717910ddb86892130f68465757d5f9bbb824d","amount":"10.0"}}},{"time":"2026-07-09T19:03:32.561209966","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","inner":{"FutureEvent":{"enabled":true,"values":["1.0","2.0"]}}}]}"#;

    #[test]
    fn parses_known_and_unknown_misc_events() {
        let rows = parse(LINE.to_vec()).expect("misc events should parse");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].block_number, 1066589454);
        assert_eq!(rows[0].event_index, 0);
        assert_eq!(rows[0].event_type, "CDeposit");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&rows[0].payload).unwrap(),
            serde_json::json!({
                "user": "0x821717910ddb86892130f68465757d5f9bbb824d",
                "amount": "10.0"
            })
        );

        assert_eq!(rows[1].event_index, 1);
        assert_eq!(rows[1].event_type, "FutureEvent");
        assert_ne!(rows[0].id, rows[1].id);
    }

    #[test]
    fn event_ids_are_deterministic() {
        let first = parse(LINE.to_vec()).unwrap();
        let second = parse(LINE.to_vec()).unwrap();

        assert_eq!(first[0].id, second[0].id);
        assert_eq!(first[1].id, second[1].id);
    }

    #[test]
    fn event_index_distinguishes_identical_events_in_one_record() {
        let first = event_id(1, "local", 0, "time", "Funding", r#"{"deltas":[]}"#);
        let second = event_id(1, "local", 1, "time", "Funding", r#"{"deltas":[]}"#);

        assert_ne!(first, second);
    }

    #[test]
    fn rejects_an_inner_object_with_multiple_event_kinds() {
        let line = br#"{"local_time":"2026-07-09T19:03:32.961209966","block_time":"2026-07-09T19:03:32.561209966","block_number":1,"events":[{"time":"2026-07-09T19:03:32.561209966","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","inner":{"One":{},"Two":{}}}]}"#;

        let error = parse(line.to_vec()).expect_err("ambiguous event should fail");
        assert!(error.to_string().contains("exactly one event kind"));
    }
}
