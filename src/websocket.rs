use std::{net::SocketAddr, sync::Arc};

use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::broadcast,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        Message, Utf8Bytes,
        handshake::server::{Request, Response},
    },
};
use tracing::{debug, info, warn};

use crate::{
    storage::{Address, Cloid, DECIMAL_MULTIPLIER, Decimal, Hash},
    streams::{node_fills::NodeFillRow, replica_cmds::BlockRow},
};

const CHANNEL_CAPACITY: usize = 4096;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "method")]
enum ClientRequest {
    Subscribe { subscription: Subscription },
    Unsubscribe { subscription: Subscription },
    Ping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
enum Subscription {
    Blocks,
    NodeFills,
}

impl Subscription {
    fn from_query_value(value: &str) -> Option<Self> {
        match value {
            "blocks" => Some(Self::Blocks),
            "nodeFills" => Some(Self::NodeFills),
            _ => None,
        }
    }
}

/// Parse `subscription` query params from the request path, e.g.
/// `/ws?subscription=nodeFills&subscription=blocks`. Unknown values are ignored.
fn subscriptions_from_query(query: &str) -> Vec<Subscription> {
    let mut out = Vec::new();
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() != Some("subscription") {
            continue;
        }
        let Some(value) = kv.next() else { continue };
        if let Some(sub) = Subscription::from_query_value(value)
            && !out.contains(&sub)
        {
            out.push(sub);
        }
    }
    out
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionAck<'a> {
    method: &'a str,
    subscription: Subscription,
}

fn channel_msg<T: Serialize>(channel: &str, data: &T) -> Utf8Bytes {
    let msg = serde_json::json!({ "channel": channel, "data": data });
    Utf8Bytes::from(msg.to_string())
}

fn hex_string(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for b in bytes {
        out.push(char::from_digit((b >> 4) as u32, 16).unwrap());
        out.push(char::from_digit((b & 0xf) as u32, 16).unwrap());
    }
    out
}

fn address_string(a: &Address) -> String {
    hex_string(a)
}

fn hash_string(h: &Hash) -> String {
    hex_string(h)
}

fn cloid_string(c: &Cloid) -> String {
    hex_string(c)
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
struct BlockMsg {
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
            hash: hash_string(&row.hash),
            proposer: address_string(&row.proposer),
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
struct NodeFillMsg {
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

impl From<&NodeFillRow> for NodeFillMsg {
    fn from(row: &NodeFillRow) -> Self {
        let liquidation = match (
            &row.liquidation_liquidated_user,
            row.liquidation_mark_px,
            &row.liquidation_method,
        ) {
            (Some(user), Some(mark_px), Some(method)) => Some(LiquidationMsg {
                liquidated_user: address_string(user),
                mark_px: decimal_string(mark_px),
                method: method.clone(),
            }),
            _ => None,
        };

        Self {
            local_time: row.local_time,
            block_time: row.block_time,
            block_number: row.block_number,
            user: address_string(&row.user),
            coin: row.coin.clone(),
            px: decimal_string(row.px),
            sz: decimal_string(row.sz),
            side: row.side.clone(),
            time: row.time.timestamp_millis(),
            start_position: decimal_string(row.start_position),
            dir: row.dir.clone(),
            closed_pnl: decimal_string(row.closed_pnl),
            hash: hash_string(&row.hash),
            oid: row.oid,
            crossed: row.crossed,
            liquidation,
            fee: decimal_string(row.fee),
            builder_fee: row.builder_fee.map(decimal_string),
            tid: row.tid,
            cloid: row.cloid.as_ref().map(cloid_string),
            fee_token: row.fee_token.clone(),
            builder: row.builder.as_ref().map(address_string),
            twap_id: row.twap_id,
            deployer_fee: row.deployer_fee.map(decimal_string),
            priority_gas: row.priority_gas.map(decimal_string),
        }
    }
}

struct Channels {
    blocks: broadcast::Sender<Utf8Bytes>,
    node_fills: broadcast::Sender<Utf8Bytes>,
}

pub struct WsServer {
    channels: Arc<Channels>,
}

#[derive(Clone)]
pub struct WsPublisher {
    channels: Arc<Channels>,
}

impl WsServer {
    pub fn new() -> Self {
        let (blocks, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (node_fills, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            channels: Arc::new(Channels { blocks, node_fills }),
        }
    }

    pub fn publisher(&self) -> WsPublisher {
        WsPublisher {
            channels: self.channels.clone(),
        }
    }

    pub async fn run(self, addr: SocketAddr) -> anyhow::Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("websocket server listening on {addr}");

        loop {
            let (stream, peer) = listener.accept().await?;
            let channels = self.channels.clone();
            tokio::spawn(async move {
                debug!(%peer, "websocket connection opened");
                if let Err(e) = handle_connection(stream, &channels).await {
                    debug!(%peer, error = %e, "websocket connection error");
                }
                debug!(%peer, "websocket connection closed");
            });
        }
    }
}

impl WsPublisher {
    pub fn publish_block(&self, block: &BlockRow) {
        if self.channels.blocks.receiver_count() == 0 {
            return;
        }
        let msg = channel_msg("blocks", &BlockMsg::from(block));
        let _ = self.channels.blocks.send(msg);
    }

    pub fn publish_fills(&self, fills: &[NodeFillRow]) {
        if fills.is_empty() || self.channels.node_fills.receiver_count() == 0 {
            return;
        }
        let msgs: Vec<NodeFillMsg> = fills.iter().map(NodeFillMsg::from).collect();
        let msg = channel_msg("nodeFills", &msgs);
        let _ = self.channels.node_fills.send(msg);
    }
}

async fn recv_opt(
    rx: &mut Option<broadcast::Receiver<Utf8Bytes>>,
) -> Result<Utf8Bytes, broadcast::error::RecvError> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

// The `accept_hdr_async` callback's Err variant is large by design; we never return it.
#[allow(clippy::result_large_err)]
async fn handle_connection(stream: TcpStream, channels: &Channels) -> anyhow::Result<()> {
    let mut query = String::new();
    let ws = accept_hdr_async(stream, |req: &Request, resp: Response| {
        if let Some(q) = req.uri().query() {
            query = q.to_string();
        }
        Ok(resp)
    })
    .await?;
    let (mut sink, mut source) = ws.split();

    let mut blocks_rx: Option<broadcast::Receiver<Utf8Bytes>> = None;
    let mut fills_rx: Option<broadcast::Receiver<Utf8Bytes>> = None;

    // Subscriptions requested via query string, e.g. `?subscription=nodeFills`.
    for subscription in subscriptions_from_query(&query) {
        match subscription {
            Subscription::Blocks => blocks_rx = Some(channels.blocks.subscribe()),
            Subscription::NodeFills => fills_rx = Some(channels.node_fills.subscribe()),
        }
        let ack = channel_msg(
            "subscriptionResponse",
            &SubscriptionAck {
                method: "subscribe",
                subscription,
            },
        );
        sink.send(Message::Text(ack)).await?;
    }

    loop {
        tokio::select! {
            msg = source.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    let response = match serde_json::from_str::<ClientRequest>(text.as_str()) {
                        Ok(ClientRequest::Subscribe { subscription }) => {
                            let rx = match subscription {
                                Subscription::Blocks => &mut blocks_rx,
                                Subscription::NodeFills => &mut fills_rx,
                            };
                            *rx = Some(match subscription {
                                Subscription::Blocks => channels.blocks.subscribe(),
                                Subscription::NodeFills => channels.node_fills.subscribe(),
                            });
                            channel_msg(
                                "subscriptionResponse",
                                &SubscriptionAck { method: "subscribe", subscription },
                            )
                        }
                        Ok(ClientRequest::Unsubscribe { subscription }) => {
                            match subscription {
                                Subscription::Blocks => blocks_rx = None,
                                Subscription::NodeFills => fills_rx = None,
                            }
                            channel_msg(
                                "subscriptionResponse",
                                &SubscriptionAck { method: "unsubscribe", subscription },
                            )
                        }
                        Ok(ClientRequest::Ping) => channel_msg("pong", &()),
                        Err(e) => channel_msg("error", &format!("invalid request: {e}")),
                    };
                    sink.send(Message::Text(response)).await?;
                }
                Some(Ok(Message::Ping(payload))) => {
                    sink.send(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(e.into()),
            },
            item = recv_opt(&mut blocks_rx) => match item {
                Ok(msg) => sink.send(Message::Text(msg)).await?,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("websocket client lagged on blocks, dropped {n} messages");
                    let msg = channel_msg("error", &format!("lagged: dropped {n} blocks messages"));
                    sink.send(Message::Text(msg)).await?;
                }
                Err(broadcast::error::RecvError::Closed) => blocks_rx = None,
            },
            item = recv_opt(&mut fills_rx) => match item {
                Ok(msg) => sink.send(Message::Text(msg)).await?,
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!("websocket client lagged on nodeFills, dropped {n} messages");
                    let msg = channel_msg("error", &format!("lagged: dropped {n} nodeFills messages"));
                    sink.send(Message::Text(msg)).await?;
                }
                Err(broadcast::error::RecvError::Closed) => fills_rx = None,
            },
        }
    }

    Ok(())
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
    fn hex_string_formats() {
        assert_eq!(hex_string(&[0x00, 0xff, 0x1a]), "0x00ff1a");
    }

    #[tokio::test]
    async fn subscribe_and_receive_block() {
        let server = WsServer::new();
        let publisher = server.publisher();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        tokio::spawn(server.run(addr));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}"))
            .await
            .unwrap();

        ws.send(Message::Text(
            r#"{"method":"subscribe","subscription":{"type":"blocks"}}"#.into(),
        ))
        .await
        .unwrap();

        let ack = ws.next().await.unwrap().unwrap();
        let ack: serde_json::Value = serde_json::from_str(ack.to_text().unwrap()).unwrap();
        assert_eq!(ack["channel"], "subscriptionResponse");
        assert_eq!(ack["data"]["subscription"]["type"], "blocks");

        publisher.publish_block(&BlockRow {
            number: 42,
            hash: [1; 32],
            proposer: [2; 20],
            time: Utc::now(),
            round: 7,
            parent_round: 6,
            hardfork_version: None,
            hardfork_round: None,
        });

        let msg = ws.next().await.unwrap().unwrap();
        let msg: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(msg["channel"], "blocks");
        assert_eq!(msg["data"]["number"], 42);
        assert_eq!(msg["data"]["round"], 7);
        assert!(msg["data"]["hash"].as_str().unwrap().starts_with("0x0101"));
    }

    #[test]
    fn parses_subscriptions_from_query() {
        assert_eq!(
            subscriptions_from_query("subscription=nodeFills"),
            vec![Subscription::NodeFills]
        );
        assert_eq!(
            subscriptions_from_query("foo=bar&subscription=blocks&subscription=nodeFills"),
            vec![Subscription::Blocks, Subscription::NodeFills]
        );
        // duplicates and unknown values are ignored
        assert_eq!(
            subscriptions_from_query("subscription=blocks&subscription=blocks&subscription=nope"),
            vec![Subscription::Blocks]
        );
        assert!(subscriptions_from_query("").is_empty());
    }

    #[tokio::test]
    async fn subscribe_via_query_param() {
        let server = WsServer::new();
        let publisher = server.publisher();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        tokio::spawn(server.run(addr));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("ws://{addr}/ws?subscription=nodeFills"))
                .await
                .unwrap();

        let ack = ws.next().await.unwrap().unwrap();
        let ack: serde_json::Value = serde_json::from_str(ack.to_text().unwrap()).unwrap();
        assert_eq!(ack["channel"], "subscriptionResponse");
        assert_eq!(ack["data"]["method"], "subscribe");
        assert_eq!(ack["data"]["subscription"]["type"], "nodeFills");

        publisher.publish_fills(&[NodeFillRow {
            local_time: Utc::now(),
            block_time: Utc::now(),
            block_number: 99,
            user: [3; 20],
            coin: "BTC".to_string(),
            px: 50_000 * DECIMAL_MULTIPLIER,
            sz: DECIMAL_MULTIPLIER / 2,
            side: "B".to_string(),
            time: Utc::now(),
            start_position: 0,
            dir: "Open Long".to_string(),
            closed_pnl: 0,
            hash: [4; 32],
            oid: 1,
            crossed: true,
            liquidation_liquidated_user: None,
            liquidation_mark_px: None,
            liquidation_method: None,
            fee: 0,
            builder_fee: None,
            tid: 5,
            cloid: None,
            fee_token: "USDC".to_string(),
            builder: None,
            twap_id: None,
            deployer_fee: None,
            priority_gas: None,
        }]);

        let msg = ws.next().await.unwrap().unwrap();
        let msg: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(msg["channel"], "nodeFills");
        assert_eq!(msg["data"][0]["blockNumber"], 99);
        assert_eq!(msg["data"][0]["px"], "50000");
        assert_eq!(msg["data"][0]["sz"], "0.5");
    }
}
