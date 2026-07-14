CREATE TABLE IF NOT EXISTS evm_receipts
(
    block_time            DateTime64(9, 'UTC'),
    block_number          UInt64,
    transaction_index     UInt32,
    transaction_hash      Nullable(FixedString(32)),
    is_system             Bool,
    transaction_type      LowCardinality(String),
    success               Bool,
    cumulative_gas_used   UInt64,
    gas_used              UInt64,
    log_count             UInt32,
    created_at            DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (block_number, transaction_index)
TTL block_time + INTERVAL 1 DAYS DELETE;
