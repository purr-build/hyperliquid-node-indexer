use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};
use std::collections::HashMap;
use types::{
    ActionResponse, BlockData, ExecutionResponse, NodeFillEvent, NodeFillsData, object_without_type,
};

use crate::storage::{
    ActionRow, BlockRow, Decimal, NodeFillRow, SignedActionBundleRow, address_from_hex,
    cloid_from_hex, decimal_from_str, hash, hash_from_hex, sig_from_hex,
};

fn parse_datetime_nanos(value: &str) -> Result<DateTime<Utc>> {
    Ok(NaiveDateTime::parse_from_str(value, "%Y-%m-%dT%H:%M:%S%.f")?.and_utc())
}

fn parse_millis(value: u64) -> Result<DateTime<Utc>> {
    let value = i64::try_from(value).map_err(|_| anyhow::anyhow!("millis timestamp overflow"))?;
    Utc.timestamp_millis_opt(value)
        .single()
        .ok_or_else(|| anyhow::anyhow!("invalid millis timestamp"))
}

fn parse_decimal_field(value: &str, field: &str) -> Result<Decimal> {
    decimal_from_str(value).map_err(|e| anyhow::anyhow!(format!("parse {field}: {e}")))
}

fn parse_optional_decimal_field(value: Option<&str>, field: &str) -> Result<Option<Decimal>> {
    value
        .map(|value| parse_decimal_field(value, field))
        .transpose()
}

pub fn parse_node_fills(mut line: Vec<u8>) -> Result<Vec<NodeFillRow>> {
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

pub struct ReplicaCmdsRows {
    pub block: BlockRow,
    pub bundles: Vec<SignedActionBundleRow>,
    pub actions: Vec<ActionRow>,
}

pub fn parse_replica_cmds(height: u64, mut line: Vec<u8>) -> Result<ReplicaCmdsRows> {
    let block_hash = hash(&line);
    let block: BlockData = simd_json::serde::from_slice(&mut line)?;
    let abci = &block.abci_block;
    let block_time = parse_datetime_nanos(&abci.time)?;
    let proposer = address_from_hex(abci.proposer.as_str())
        .map_err(|e| anyhow::anyhow!(format!("parse proposer {}", e)))?;
    let round = abci.round;

    let block_row = BlockRow {
        number: height,
        hash: block_hash,
        proposer,
        time: block_time,
        round,
        parent_round: abci.parent_round,
        hardfork_version: abci.hardfork.as_ref().map(|h| h.version),
        hardfork_round: abci.hardfork.as_ref().map(|h| h.round),
    };

    let responses: HashMap<&str, &[ActionResponse]> = block
        .resps
        .as_ref()
        .and_then(|r| r.full.as_ref())
        .map(|entries| entries.iter().map(|e| (e.hash(), e.responses())).collect())
        .unwrap_or_default();

    let mut bundles = Vec::with_capacity(abci.signed_action_bundles.len());
    let mut actions = Vec::new();

    for entry in &abci.signed_action_bundles {
        let bundle = entry.bundle();
        let bundle_hash = hash_from_hex(entry.hash()).map_err(|e| anyhow::anyhow!(e))?;
        let broadcaster = address_from_hex(&bundle.broadcaster)
            .map_err(|e| anyhow::anyhow!(format!("parse broadcaster: {}", e)))?;
        let broadcaster_nonce = bundle.broadcaster_nonce;

        bundles.push(SignedActionBundleRow {
            block_number: height,
            hash: bundle_hash,
            broadcaster,
            broadcaster_nonce,
        });

        let bundle_responses = responses.get(entry.hash()).copied().unwrap_or_default();

        for (i, signed) in bundle.signed_actions.iter().enumerate() {
            let (status, user, response_type) = match bundle_responses.get(i) {
                Some(resp) => {
                    let user =
                        if resp.user.is_empty() {
                            None
                        } else {
                            Some(address_from_hex(&resp.user).map_err(|e| {
                                anyhow::anyhow!(format!("parse response user: {}", e))
                            })?)
                        };
                    let response_type = match &resp.res.response {
                        Some(ExecutionResponse::Typed(t)) => t.response_type.clone(),
                        _ => String::new(),
                    };
                    (resp.res.status.clone(), user, response_type)
                }
                None => (String::new(), None, String::new()),
            };

            let vault_address = signed
                .vault_address
                .as_deref()
                .map(address_from_hex)
                .transpose()
                .map_err(|e| anyhow::anyhow!(format!("parse vault_address: {}", e)))?;

            let payload = match object_without_type(serde_json::to_value(&signed.action)?) {
                Some(obj) => serde_json::to_string(&obj)?,
                None => String::new(),
            };

            actions.push(ActionRow {
                block_time,
                round,
                proposer,
                bundle_hash,
                broadcaster,
                broadcaster_nonce,
                nonce: signed.nonce,
                vault_address,
                expires_after: signed.expires_after,
                sig_r: sig_from_hex(&signed.signature.r)
                    .map_err(|e| anyhow::anyhow!(format!("parse sig_r: {}", e)))?,
                sig_s: sig_from_hex(&signed.signature.s)
                    .map_err(|e| anyhow::anyhow!(format!("parse sig_s: {}", e)))?,
                sig_v: signed.signature.v,
                action_type: signed.action.action_type().to_string(),
                status,
                user,
                response_type,
                payload,
            });
        }
    }

    Ok(ReplicaCmdsRows {
        block: block_row,
        bundles,
        actions,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_node_fill_with_liquidation() {
        let line = br#"{"local_time":"2026-06-28T08:43:53.687645997","block_time":"2026-06-28T08:43:53.257028820","block_number":1052527672,"events":[["0xa775d1bd91ee2cc4cae6113e5fd9c0c17b355277",{"coin":"SOL","px":"72.153","sz":"0.58","side":"A","time":1782636233257,"startPosition":"-162.6","dir":"Open Short","closedPnl":"0.0","hash":"0xdc25d330b40799afdd9f043ebc4c38000060eb164f0ab8817fee7e83730b739a","oid":481702553903,"crossed":false,"fee":"-0.001255","tid":340554939716632,"cloid":"0x00000000000000000000019f08a8bdd8","liquidation":{"liquidatedUser":"0x5ef843ccf26810073ee689a954f6d05f5b48fccb","markPx":"72.149","method":"market"},"feeToken":"USDC","twapId":null}]]}"#;

        let rows = parse_node_fills(line.to_vec()).expect("fill should parse");

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
