CREATE TABLE IF NOT EXISTS misc_events
(
    id           FixedString(32),
    local_time   DateTime64(9, 'UTC'),
    block_time   DateTime64(9, 'UTC'),
    block_number UInt64,
    event_index  UInt32,
    time         DateTime64(9, 'UTC'),
    hash         FixedString(32),
    event_type   LowCardinality(String),
    payload      JSON,
    created_at   DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(time)
ORDER BY (event_type, time, id)
TTL time + INTERVAL 1 DAYS DELETE;
