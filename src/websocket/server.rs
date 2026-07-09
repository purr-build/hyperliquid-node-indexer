//! Websocket server: broadcast channels, connection handling, and publishing.

use std::{net::SocketAddr, sync::Arc};

use futures_util::{SinkExt, StreamExt};
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
    streams::{
        hip3_oracle_updates::Hip3OracleUpdateRow, node_fills::NodeFillRow, replica_cmds::BlockRow,
    },
    websocket::{
        messages::{BlockMsg, NodeFillMsg, channel_msg},
        subscription::{
            ClientRequest, SubscriptionAck, SubscriptionKind, subscriptions_from_query,
        },
    },
};

const CHANNEL_CAPACITY: usize = 4096;

struct Channels {
    blocks: broadcast::Sender<Utf8Bytes>,
    node_fills: broadcast::Sender<Utf8Bytes>,
    hip3_oracle_updates: broadcast::Sender<Utf8Bytes>,
}

impl Channels {
    fn sender(&self, kind: SubscriptionKind) -> &broadcast::Sender<Utf8Bytes> {
        match kind {
            SubscriptionKind::Blocks => &self.blocks,
            SubscriptionKind::NodeFills => &self.node_fills,
            SubscriptionKind::Hip3OracleUpdates => &self.hip3_oracle_updates,
        }
    }
}

pub enum WsData<'a> {
    Blocks(&'a BlockRow),
    NodeFills(&'a [NodeFillRow]),
    Hip3OracleUpdates(&'a [Hip3OracleUpdateRow]),
}

impl WsData<'_> {
    fn kind(&self) -> SubscriptionKind {
        match self {
            Self::Blocks(_) => SubscriptionKind::Blocks,
            Self::NodeFills(_) => SubscriptionKind::NodeFills,
            Self::Hip3OracleUpdates(_) => SubscriptionKind::Hip3OracleUpdates,
        }
    }
}

#[derive(Clone)]
pub struct WsServer {
    channels: Arc<Channels>,
}

impl WsServer {
    pub fn new() -> Self {
        let (blocks, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (node_fills, _) = broadcast::channel(CHANNEL_CAPACITY);
        let (hip3_oracle_updates, _) = broadcast::channel(CHANNEL_CAPACITY);

        Self {
            channels: Arc::new(Channels {
                blocks,
                node_fills,
                hip3_oracle_updates,
            }),
        }
    }

    pub fn send(&self, data: WsData<'_>) {
        let kind = data.kind();
        let tx = self.channels.sender(kind);
        if tx.receiver_count() == 0 {
            return;
        }
        let channel = kind.to_string();
        let msg = match data {
            WsData::Blocks(block) => channel_msg(&channel, &BlockMsg::from(block)),
            WsData::NodeFills(fills) => {
                if fills.is_empty() {
                    return;
                }
                let msgs: Vec<NodeFillMsg> = fills.iter().map(NodeFillMsg::from).collect();
                channel_msg(&channel, &msgs)
            }
            WsData::Hip3OracleUpdates(updates) => {
                if updates.is_empty() {
                    return;
                }
                channel_msg(&channel, &updates)
            }
        };
        let _ = tx.send(msg);
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
    let mut hip3_oracle_updates_rx: Option<broadcast::Receiver<Utf8Bytes>> = None;

    // Subscriptions requested via query string, e.g. `?subscription=nodeFills`.
    for subscription in subscriptions_from_query(&query) {
        let rx = match subscription {
            SubscriptionKind::Blocks => &mut blocks_rx,
            SubscriptionKind::NodeFills => &mut fills_rx,
            SubscriptionKind::Hip3OracleUpdates => &mut hip3_oracle_updates_rx,
        };

        *rx = Some(channels.sender(subscription).subscribe());
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
                                SubscriptionKind::Blocks => &mut blocks_rx,
                                SubscriptionKind::NodeFills => &mut fills_rx,
                                SubscriptionKind::Hip3OracleUpdates => &mut hip3_oracle_updates_rx,
                            };
                            *rx = Some(channels.sender(subscription).subscribe());
                            channel_msg(
                                "subscriptionResponse",
                                &SubscriptionAck { method: "subscribe", subscription },
                            )
                        }
                        Ok(ClientRequest::Unsubscribe { subscription }) => {
                            match subscription {
                                SubscriptionKind::Blocks => blocks_rx = None,
                                SubscriptionKind::NodeFills => fills_rx = None,
                                SubscriptionKind::Hip3OracleUpdates => hip3_oracle_updates_rx = None,
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
    use crate::storage::DECIMAL_MULTIPLIER;
    use chrono::Utc;

    #[tokio::test]
    async fn subscribe_and_receive_block() {
        let server = WsServer::new();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        tokio::spawn(server.clone().run(addr));
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

        server.send(WsData::Blocks(&BlockRow {
            number: 42,
            hash: [1; 32],
            proposer: [2; 20],
            time: Utc::now(),
            round: 7,
            parent_round: 6,
            hardfork_version: None,
            hardfork_round: None,
        }));

        let msg = ws.next().await.unwrap().unwrap();
        let msg: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(msg["channel"], "blocks");
        assert_eq!(msg["data"]["number"], 42);
        assert_eq!(msg["data"]["round"], 7);
        assert!(msg["data"]["hash"].as_str().unwrap().starts_with("0x0101"));
    }

    #[tokio::test]
    async fn subscribe_via_query_param() {
        let server = WsServer::new();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);

        tokio::spawn(server.clone().run(addr));
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

        server.send(WsData::NodeFills(&[NodeFillRow {
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
        }]));

        let msg = ws.next().await.unwrap().unwrap();
        let msg: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
        assert_eq!(msg["channel"], "nodeFills");
        assert_eq!(msg["data"][0]["blockNumber"], 99);
        assert_eq!(msg["data"][0]["px"], "50000");
        assert_eq!(msg["data"][0]["sz"], "0.5");
    }
}
