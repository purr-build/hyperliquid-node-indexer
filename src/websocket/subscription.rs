//! Subscription kinds and client-facing protocol types.

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", tag = "method")]
pub enum ClientRequest {
    Subscribe { subscription: SubscriptionKind },
    Unsubscribe { subscription: SubscriptionKind },
    Ping,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SubscriptionKind {
    Blocks,
    NodeFills,
    Hip3OracleUpdates,
}

impl SubscriptionKind {
    fn from_query_value(value: &str) -> Option<Self> {
        match value {
            "blocks" => Some(Self::Blocks),
            "nodeFills" => Some(Self::NodeFills),
            _ => None,
        }
    }
}

impl std::fmt::Display for SubscriptionKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Blocks => write!(f, "blocks"),
            Self::NodeFills => write!(f, "nodeFills"),
            Self::Hip3OracleUpdates => write!(f, "hip3OracleUpdates"),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionAck<'a> {
    pub method: &'a str,
    pub subscription: SubscriptionKind,
}

/// Parse `subscription` query params from the request path, e.g.
/// `/ws?subscription=nodeFills&subscription=blocks`. Unknown values are ignored.
pub fn subscriptions_from_query(query: &str) -> Vec<SubscriptionKind> {
    let mut out = Vec::new();
    for pair in query.split('&') {
        let mut kv = pair.splitn(2, '=');
        if kv.next() != Some("subscription") {
            continue;
        }
        let Some(value) = kv.next() else { continue };
        if let Some(sub) = SubscriptionKind::from_query_value(value)
            && !out.contains(&sub)
        {
            out.push(sub);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_subscriptions_from_query() {
        assert_eq!(
            subscriptions_from_query("subscription=nodeFills"),
            vec![SubscriptionKind::NodeFills]
        );
        assert_eq!(
            subscriptions_from_query("foo=bar&subscription=blocks&subscription=nodeFills"),
            vec![SubscriptionKind::Blocks, SubscriptionKind::NodeFills]
        );
        // duplicates and unknown values are ignored
        assert_eq!(
            subscriptions_from_query("subscription=blocks&subscription=blocks&subscription=nope"),
            vec![SubscriptionKind::Blocks]
        );
        assert!(subscriptions_from_query("").is_empty());
    }
}
