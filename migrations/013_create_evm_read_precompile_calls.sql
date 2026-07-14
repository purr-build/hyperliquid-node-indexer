CREATE TABLE IF NOT EXISTS evm_read_precompile_calls
(
    block_time          DateTime64(9, 'UTC'),
    block_number        UInt64,
    precompile_index    UInt32,
    call_index          UInt32,
    address             FixedString(20),
    input               String,
    gas_limit           UInt64,
    success             Bool,
    gas_used            Nullable(UInt64),
    output              String,
    error               String,
    created_at          DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (block_number, precompile_index, call_index)
TTL block_time + INTERVAL 1 DAYS DELETE;
