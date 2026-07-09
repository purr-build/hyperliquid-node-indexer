CREATE TABLE IF NOT EXISTS node_twap_statuses
(
    local_time   DateTime64(9, 'UTC'),
    block_time   DateTime64(9, 'UTC'),
    block_number UInt64,
    event_index  UInt32,
    time         DateTime64(9, 'UTC'),
    twap_id      UInt64,
    coin         LowCardinality(String),
    user         FixedString(20),
    side         LowCardinality(String),
    sz           Decimal(38, 18),
    executed_sz  Decimal(38, 18),
    executed_ntl Decimal(38, 18),
    minutes      UInt64,
    reduce_only  Bool,
    randomize    Bool,
    timestamp    DateTime64(3, 'UTC'),
    status       LowCardinality(String),
    error        Nullable(String),
    created_at   DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(time)
ORDER BY (time, twap_id, event_index)
TTL time + INTERVAL 1 DAYS DELETE;
