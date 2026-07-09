CREATE TABLE IF NOT EXISTS hip3_oracle_updates
(
    local_time                DateTime64(9, 'UTC'),
    block_time                DateTime64(9, 'UTC'),
    block_number              UInt64,
    coin                      LowCardinality(String),
    update_class              LowCardinality(String),

    oracle_px                 Decimal(38, 18),
    oracle_last_update_time   DateTime64(9, 'UTC'),
    oracle_daily_px           Decimal(38, 18),

    mark_px                   Decimal(38, 18),
    mark_last_update_time     DateTime64(9, 'UTC'),
    mark_daily_px             Decimal(38, 18),

    external_px               Decimal(38, 18),
    external_last_update_time DateTime64(9, 'UTC'),
    external_daily_px         Decimal(38, 18),

    spot_px                   Decimal(38, 18),
    created_at                DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (block_time, block_number, coin, update_class)
TTL block_time + INTERVAL 1 DAYS DELETE;
