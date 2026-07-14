CREATE TABLE IF NOT EXISTS evm_blocks
(
    local_time                   DateTime64(9, 'UTC'),
    block_time                   DateTime64(9, 'UTC'),
    block_number                 UInt64,
    hash                         FixedString(32),
    parent_hash                  FixedString(32),
    sha3_uncles                  FixedString(32),
    miner                        FixedString(20),
    state_root                   FixedString(32),
    transactions_root            FixedString(32),
    receipts_root                FixedString(32),
    logs_bloom                   String,
    difficulty                   FixedString(32),
    gas_limit                    UInt64,
    gas_used                     UInt64,
    extra_data                   String,
    mix_hash                     FixedString(32),
    nonce                        UInt64,
    base_fee_per_gas             Nullable(FixedString(32)),
    withdrawals_root             Nullable(FixedString(32)),
    blob_gas_used                Nullable(UInt64),
    excess_blob_gas              Nullable(UInt64),
    parent_beacon_block_root      Nullable(FixedString(32)),
    highest_precompile_address   FixedString(20),
    transaction_count            UInt32,
    system_transaction_count     UInt32,
    ommers                       JSON,
    withdrawals                  JSON,
    created_at                   DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY block_number
TTL block_time + INTERVAL 1 DAYS DELETE;
