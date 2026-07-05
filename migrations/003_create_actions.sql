CREATE TABLE IF NOT EXISTS actions
(
    block_time         DateTime64(9, 'UTC'),
    round              UInt64,
    proposer           FixedString(20),
    bundle_hash        FixedString(32),
    broadcaster        FixedString(20),
    broadcaster_nonce  UInt64,
    nonce              UInt64,
    vault_address      Nullable(FixedString(20)),
    expires_after      Nullable(UInt64),
    sig_r              FixedString(32),
    sig_s              FixedString(32),
    sig_v              UInt8,
    action_type        LowCardinality(String),
    status             LowCardinality(String),
    user               Nullable(FixedString(20)),
    response_type      LowCardinality(String) DEFAULT '',
    payload            JSON,
    created_at         DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (action_type, block_time, sig_r, sig_s)
TTL block_time + INTERVAL 1 DAYS DELETE;
