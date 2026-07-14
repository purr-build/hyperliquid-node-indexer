CREATE TABLE IF NOT EXISTS evm_transactions
(
    block_time                    DateTime64(9, 'UTC'),
    block_number                  UInt64,
    transaction_index             UInt32,
    hash                          Nullable(FixedString(32)),
    is_system                     Bool,
    transaction_type              LowCardinality(String),
    chain_id                      Nullable(UInt64),
    nonce                         UInt64,
    gas                           UInt64,
    gas_price                     Nullable(FixedString(32)),
    max_fee_per_gas               Nullable(FixedString(32)),
    max_priority_fee_per_gas      Nullable(FixedString(32)),
    from_address                  Nullable(FixedString(20)),
    to_address                    Nullable(FixedString(20)),
    value                         FixedString(32),
    input                         String,
    access_list                   JSON,
    signature_r                   Nullable(FixedString(32)),
    signature_s                   Nullable(FixedString(32)),
    signature_y_parity            Nullable(UInt8),
    created_at                    DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (block_number, transaction_index)
TTL block_time + INTERVAL 1 DAYS DELETE;
