use std::collections::HashMap;

use chrono::Utc;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use tailer::Event;
use types::{Hip3OraclePx, Hip3OracleUpdatesData};

use crate::{
    metrics::Metrics,
    storage::{Decimal, new_inserter},
    streams::{Stream, commit_metered, parse_datetime_nanos, parse_decimal_field},
    websocket::{WsData, WsServer},
};

#[derive(Deserialize, Row, Serialize, Debug)]
pub struct Hip3OracleUpdateRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub local_time: chrono::DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub block_time: chrono::DateTime<Utc>,
    pub block_number: u64,
    pub coin: String,
    pub update_class: String,

    pub oracle_px: Decimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub oracle_last_update_time: chrono::DateTime<Utc>,
    pub oracle_daily_px: Decimal,

    pub mark_px: Decimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub mark_last_update_time: chrono::DateTime<Utc>,
    pub mark_daily_px: Decimal,

    pub external_px: Decimal,
    #[serde(with = "clickhouse::serde::chrono::datetime64::nanos")]
    pub external_last_update_time: chrono::DateTime<Utc>,
    pub external_daily_px: Decimal,

    pub spot_px: Decimal,
}

pub fn parse(mut line: Vec<u8>) -> anyhow::Result<Vec<Hip3OracleUpdateRow>> {
    let data: Hip3OracleUpdatesData = simd_json::serde::from_slice(&mut line)?;
    let local_time = parse_datetime_nanos(&data.local_time)?;
    let block_time = parse_datetime_nanos(&data.block_time)?;
    let block_number = data.block_number;
    let capacity = data
        .events
        .iter()
        .map(|event| event.oracle_pxs.coin_to_oracle_px.len())
        .sum();
    let mut rows = Vec::with_capacity(capacity);

    for event in data.events {
        let spot_inputs: HashMap<_, _> = event
            .spot_px_inputs
            .iter()
            .map(|input| (input.coin(), input.px()))
            .collect();
        let mark_pxs: HashMap<_, _> = event
            .oracle_pxs
            .coin_to_mark_px
            .iter()
            .map(|entry| (entry.coin(), entry.px()))
            .collect();
        let external_pxs: HashMap<_, _> = event
            .oracle_pxs
            .coin_to_external_perp_px
            .iter()
            .map(|entry| (entry.coin(), entry.px()))
            .collect();

        for oracle_entry in &event.oracle_pxs.coin_to_oracle_px {
            let coin = oracle_entry.coin();
            let oracle = oracle_entry.px();
            let mark = required_px(&mark_pxs, coin, "coin_to_mark_px")?;
            let external = required_px(&external_pxs, coin, "coin_to_external_perp_px")?;
            let spot_px = spot_inputs.get(coin).copied().unwrap_or(&oracle.px);

            rows.push(Hip3OracleUpdateRow {
                local_time,
                block_time,
                block_number,
                coin: coin.to_string(),
                update_class: event.update_class.clone(),

                oracle_px: parse_decimal_field(&oracle.px, "oracle_px")?,
                oracle_last_update_time: parse_datetime_nanos(&oracle.last_update_time)?,
                oracle_daily_px: parse_decimal_field(&oracle.daily_px, "oracle_daily_px")?,

                mark_px: parse_decimal_field(&mark.px, "mark_px")?,
                mark_last_update_time: parse_datetime_nanos(&mark.last_update_time)?,
                mark_daily_px: parse_decimal_field(&mark.daily_px, "mark_daily_px")?,

                external_px: parse_decimal_field(&external.px, "external_px")?,
                external_last_update_time: parse_datetime_nanos(&external.last_update_time)?,
                external_daily_px: parse_decimal_field(&external.daily_px, "external_daily_px")?,

                spot_px: parse_decimal_field(spot_px, "spot_px")?,
            });
        }
    }

    Ok(rows)
}

fn required_px<'a>(
    pxs: &HashMap<&str, &'a Hip3OraclePx>,
    coin: &str,
    field: &str,
) -> anyhow::Result<&'a Hip3OraclePx> {
    pxs.get(coin)
        .copied()
        .ok_or_else(|| anyhow::anyhow!("missing {field} for {coin}"))
}

pub struct Hip3OracleUpdates;

pub struct Hip3OracleUpdatesSinks {
    hip3_oracle_updates: Inserter<Hip3OracleUpdateRow>,
    ws: Option<WsServer>,
}

impl Hip3OracleUpdatesSinks {
    pub fn new(ch: &Client, ws: Option<WsServer>) -> Self {
        Self {
            hip3_oracle_updates: new_inserter(ch, "hip3_oracle_updates"),
            ws,
        }
    }
}

impl Stream for Hip3OracleUpdates {
    const NAME: &'static str = "hip3_oracle_updates";
    const SOURCE_DIR: &'static str = "hip3_oracle_updates_streaming";

    type Rows = Vec<Hip3OracleUpdateRow>;
    type Sinks = Hip3OracleUpdatesSinks;

    fn parse(event: Event) -> anyhow::Result<Self::Rows> {
        parse(event.line)
    }

    async fn write(
        sinks: &mut Self::Sinks,
        rows: &Self::Rows,
        metrics: &Metrics,
    ) -> anyhow::Result<()> {
        for row in rows {
            let lag = (Utc::now() - row.block_time).as_seconds_f64();
            metrics.record_ingested(Self::NAME, "hip3_oracle_update");
            metrics.record_lag(Self::NAME, lag);
            sinks.hip3_oracle_updates.write(row).await?;
        }

        if let Some(ws) = &sinks.ws {
            ws.send(WsData::Hip3OracleUpdates(rows));
        }

        Ok(())
    }

    async fn commit(sinks: &mut Self::Sinks, metrics: &Metrics, force: bool) -> anyhow::Result<()> {
        commit_metered(
            &mut sinks.hip3_oracle_updates,
            "hip3_oracle_updates",
            metrics,
            force,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::decimal_from_str;

    #[test]
    fn parses_hip3_oracle_updates() {
        let rows = parse(include_bytes!("../../hl-data/hip3_oracle_updates_streaming").to_vec())
            .expect("hip3 oracle updates should parse");

        assert_eq!(rows.len(), 5);

        let avgo = &rows[0];
        assert_eq!(avgo.block_number, 1061545103);
        assert_eq!(avgo.coin, "para:AVGO");
        assert_eq!(avgo.update_class, "Deployer");
        assert_eq!(avgo.oracle_px, decimal_from_str("368.11").unwrap());
        assert_eq!(avgo.mark_px, decimal_from_str("368.41").unwrap());
        assert_eq!(avgo.external_px, decimal_from_str("361.75").unwrap());
        assert_eq!(avgo.spot_px, decimal_from_str("368.11").unwrap());

        let h100 = rows
            .iter()
            .find(|row| row.coin == "para:H100")
            .expect("H100 row should be present");
        assert_eq!(h100.spot_px, decimal_from_str("2.56").unwrap());
    }
}
