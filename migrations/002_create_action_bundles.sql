CREATE TABLE IF NOT EXISTS signed_action_bundle 
(
    block_number UInt64,
    hash FixedString(32),
    broadcaster FixedString(20),
    broadcaster_nonce UInt64,
    created_at DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(created_at)
ORDER BY (block_number, hash)
TTL created_at + INTERVAL 1 DAYS DELETE;
