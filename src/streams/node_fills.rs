use chrono::Utc;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use tailer::Event;
use types::{NodeFillEvent, NodeFillsData};

use crate::{
    metrics::Metrics,
    storage::{
        Address, Cloid, Decimal, Hash, address_from_hex, cloid_from_hex, hash_from_hex,
        new_inserter,
    },
    streams::{
        Stream, commit_metered, parse_datetime_nanos, parse_decimal_field, parse_millis,
        parse_optional_decimal_field,
    },
    websocket::{WsData, WsServer},
};

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

pub fn parse(mut line: Vec<u8>) -> anyhow::Result<Vec<NodeFillRow>> {
    let data: NodeFillsData = simd_json::serde::from_slice(&mut line)?;
    let local_time = parse_datetime_nanos(&data.local_time)?;
    let block_time = parse_datetime_nanos(&data.block_time)?;
    let block_number = data.block_number;
    let mut rows = Vec::with_capacity(data.events.len());

    for NodeFillEvent(user, fill) in data.events {
        let user = address_from_hex(&user)
            .map_err(|e| anyhow::anyhow!(format!("parse fill user: {e}")))?;
        let hash =
            hash_from_hex(&fill.hash).map_err(|e| anyhow::anyhow!(format!("parse hash: {e}")))?;
        let time = parse_millis(fill.time)?;
        let cloid = fill
            .cloid
            .as_deref()
            .map(cloid_from_hex)
            .transpose()
            .map_err(|e| anyhow::anyhow!(format!("parse cloid: {e}")))?;
        let builder = fill
            .builder
            .as_deref()
            .map(address_from_hex)
            .transpose()
            .map_err(|e| anyhow::anyhow!(format!("parse builder: {e}")))?;

        let (liquidation_liquidated_user, liquidation_mark_px, liquidation_method) =
            match fill.liquidation {
                Some(liquidation) => (
                    Some(address_from_hex(&liquidation.liquidated_user).map_err(|e| {
                        anyhow::anyhow!(format!("parse liquidation liquidated_user: {e}"))
                    })?),
                    Some(parse_decimal_field(
                        &liquidation.mark_px,
                        "liquidation.mark_px",
                    )?),
                    Some(liquidation.method),
                ),
                None => (None, None, None),
            };

        rows.push(NodeFillRow {
            local_time,
            block_time,
            block_number,
            user,
            coin: fill.coin,
            px: parse_decimal_field(&fill.px, "px")?,
            sz: parse_decimal_field(&fill.sz, "sz")?,
            side: fill.side,
            time,
            start_position: parse_decimal_field(&fill.start_position, "startPosition")?,
            dir: fill.dir,
            closed_pnl: parse_decimal_field(&fill.closed_pnl, "closedPnl")?,
            hash,
            oid: fill.oid,
            crossed: fill.crossed,
            liquidation_liquidated_user,
            liquidation_mark_px,
            liquidation_method,
            fee: parse_decimal_field(&fill.fee, "fee")?,
            builder_fee: parse_optional_decimal_field(fill.builder_fee.as_deref(), "builderFee")?,
            tid: fill.tid,
            cloid,
            fee_token: fill.fee_token,
            builder,
            twap_id: fill.twap_id,
            deployer_fee: parse_optional_decimal_field(
                fill.deployer_fee.as_deref(),
                "deployerFee",
            )?,
            priority_gas: parse_optional_decimal_field(
                fill.priority_gas.as_deref(),
                "priorityGas",
            )?,
        });
    }

    Ok(rows)
}

pub struct NodeFills {
    inserter: Inserter<NodeFillRow>,
    websocket: Option<WsServer>,
}

impl NodeFills {
    pub fn new(ch: &Client, websocket: Option<WsServer>) -> Self {
        Self {
            inserter: new_inserter(ch, "node_fills"),
            websocket,
        }
    }
}

impl Stream for NodeFills {
    const NAME: &'static str = "node_fills";
    const SOURCE_DIR: &'static str = "node_fills_streaming";

    type Rows = Vec<NodeFillRow>;

    fn parse(event: Event) -> anyhow::Result<Self::Rows> {
        parse(event.line)
    }

    async fn write(&mut self, rows: &Self::Rows, metrics: &Metrics) -> anyhow::Result<()> {
        for row in rows {
            let lag = (Utc::now() - row.block_time).as_seconds_f64();
            metrics.record_ingested(Self::NAME, "fill");
            metrics.record_lag(Self::NAME, lag);
            self.inserter.write(row).await?;
        }

        if let Some(websocket) = &self.websocket {
            websocket.send(WsData::NodeFills(rows));
        }

        Ok(())
    }

    async fn commit(&mut self, metrics: &Metrics, force: bool) -> anyhow::Result<()> {
        commit_metered(&mut self.inserter, "node_fills", metrics, force).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::decimal_from_str;

    #[test]
    fn parses_node_fill_with_liquidation() {
        let line = br#"{"local_time":"2026-06-28T08:43:53.687645997","block_time":"2026-06-28T08:43:53.257028820","block_number":1052527672,"events":[["0xa775d1bd91ee2cc4cae6113e5fd9c0c17b355277",{"coin":"SOL","px":"72.153","sz":"0.58","side":"A","time":1782636233257,"startPosition":"-162.6","dir":"Open Short","closedPnl":"0.0","hash":"0xdc25d330b40799afdd9f043ebc4c38000060eb164f0ab8817fee7e83730b739a","oid":481702553903,"crossed":false,"fee":"-0.001255","tid":340554939716632,"cloid":"0x00000000000000000000019f08a8bdd8","liquidation":{"liquidatedUser":"0x5ef843ccf26810073ee689a954f6d05f5b48fccb","markPx":"72.149","method":"market"},"feeToken":"USDC","twapId":null}]]}"#;

        let rows = parse(line.to_vec()).expect("fill should parse");

        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.block_number, 1052527672);
        assert_eq!(row.coin, "SOL");
        assert_eq!(row.px, decimal_from_str("72.153").unwrap());
        assert_eq!(row.start_position, decimal_from_str("-162.6").unwrap());
        assert_eq!(row.fee, decimal_from_str("-0.001255").unwrap());
        assert_eq!(row.liquidation_method.as_deref(), Some("market"));
        assert_eq!(
            row.liquidation_mark_px,
            Some(decimal_from_str("72.149").unwrap())
        );
        assert!(row.liquidation_liquidated_user.is_some());
        assert!(row.cloid.is_some());
    }
}
