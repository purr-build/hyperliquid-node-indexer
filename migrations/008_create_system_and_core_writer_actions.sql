CREATE TABLE IF NOT EXISTS system_and_core_writer_actions
(
    local_time   DateTime64(9, 'UTC'),
    block_time   DateTime64(9, 'UTC'),
    block_number UInt64,
    event_index  UInt32,
    user         FixedString(20),
    nonce        UInt64,
    evm_tx_hash  FixedString(32),
    action_type  LowCardinality(String),
    payload      JSON,
    created_at   DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (block_number, nonce, event_index, evm_tx_hash)
TTL block_time + INTERVAL 1 DAYS DELETE;
