CREATE TABLE IF NOT EXISTS blocks
(
    number           UInt64,
    hash             FixedString(32),
    proposer         FixedString(20),
    time             DateTime64(9, 'UTC'),
    round            UInt64,
    parent_round     UInt64,
    hardfork_version Nullable(UInt64),
    hardfork_round   Nullable(UInt64),
    
    created_at DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(time)
ORDER BY number
TTL time + INTERVAL 1 DAYS DELETE;
