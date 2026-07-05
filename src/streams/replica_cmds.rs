use std::collections::HashMap;

use chrono::Utc;
use clickhouse::{Client, Row, inserter::Inserter};
use serde::{Deserialize, Serialize};
use tailer::Event;
use types::{ActionResponse, BlockData, ExecutionResponse, object_without_type};

use crate::{
    metrics::Metrics,
    storage::{Address, Hash, address_from_hex, hash, hash_from_hex, new_inserter, sig_from_hex},
    streams::{Stream, commit_metered, parse_datetime_nanos},
    websocket::WsPublisher,
};

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

pub struct ReplicaCmdsRows {
    pub block: BlockRow,
    pub bundles: Vec<SignedActionBundleRow>,
    pub actions: Vec<ActionRow>,
}

pub fn parse(height: u64, mut line: Vec<u8>) -> anyhow::Result<ReplicaCmdsRows> {
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

pub struct ReplicaCmds;

pub struct ReplicaCmdsSinks {
    blocks: Inserter<BlockRow>,
    bundles: Inserter<SignedActionBundleRow>,
    actions: Inserter<ActionRow>,
    ws: Option<WsPublisher>,
}

impl ReplicaCmdsSinks {
    pub fn new(ch: &Client, ws: Option<WsPublisher>) -> Self {
        Self {
            blocks: new_inserter(ch, "blocks"),
            bundles: new_inserter(ch, "signed_action_bundle"),
            actions: new_inserter(ch, "actions"),
            ws,
        }
    }
}

impl Stream for ReplicaCmds {
    const NAME: &'static str = "replica_cmds";
    const SOURCE_DIR: &'static str = "replica_cmds";

    type Rows = ReplicaCmdsRows;
    type Sinks = ReplicaCmdsSinks;

    fn parse(event: Event) -> anyhow::Result<Self::Rows> {
        let filename: u64 = event
            .file
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| anyhow::anyhow!("bad replica_cmds file name"))?
            .parse()?;
        parse(filename + event.position.line_number as u64, event.line)
    }

    async fn write(
        sinks: &mut Self::Sinks,
        rows: &Self::Rows,
        metrics: &Metrics,
    ) -> anyhow::Result<()> {
        let lag = (Utc::now() - rows.block.time).as_seconds_f64();
        metrics.record_ingested(Self::NAME, "block");
        metrics.record_lag(Self::NAME, lag);
        sinks.blocks.write(&rows.block).await?;

        if let Some(ws) = &sinks.ws {
            ws.publish_block(&rows.block);
        }

        for b in &rows.bundles {
            metrics.record_ingested(Self::NAME, "bundle");
            sinks.bundles.write(b).await?;
        }

        for a in &rows.actions {
            metrics.record_ingested(Self::NAME, "action");
            sinks.actions.write(a).await?;
        }

        Ok(())
    }

    async fn commit(sinks: &mut Self::Sinks, metrics: &Metrics, force: bool) -> anyhow::Result<()> {
        commit_metered(&mut sinks.blocks, "blocks", metrics, force).await?;
        commit_metered(&mut sinks.bundles, "signed_action_bundle", metrics, force).await?;
        commit_metered(&mut sinks.actions, "actions", metrics, force).await?;
        Ok(())
    }
}
