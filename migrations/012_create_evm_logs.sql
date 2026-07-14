CREATE TABLE IF NOT EXISTS evm_logs
(
    block_time          DateTime64(9, 'UTC'),
    block_number        UInt64,
    transaction_index   UInt32,
    transaction_hash    Nullable(FixedString(32)),
    is_system           Bool,
    log_index           UInt32,
    address             FixedString(20),
    topics              Array(FixedString(32)),
    data                String,
    created_at          DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (address, block_number, transaction_index, log_index)
TTL block_time + INTERVAL 1 DAYS DELETE;
