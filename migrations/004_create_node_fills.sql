CREATE TABLE IF NOT EXISTS node_fills
(
    local_time     DateTime64(9, 'UTC'),
    block_time     DateTime64(9, 'UTC'),
    block_number   UInt64,
    user           FixedString(20),
    coin           LowCardinality(String),
    px             Decimal(38, 18),
    sz             Decimal(38, 18),
    side           LowCardinality(String),
    time           DateTime64(3, 'UTC'),
    start_position Decimal(38, 18),
    dir            LowCardinality(String),
    closed_pnl     Decimal(38, 18),
    hash           FixedString(32),
    oid            UInt64,
    crossed        Bool,
    liquidation_liquidated_user Nullable(FixedString(20)),
    liquidation_mark_px         Nullable(Decimal(38, 18)),
    liquidation_method          LowCardinality(Nullable(String)),
    fee            Decimal(38, 18),
    builder_fee    Nullable(Decimal(38, 18)),
    tid            UInt64,
    cloid          Nullable(FixedString(16)),
    fee_token      LowCardinality(String),
    builder        Nullable(FixedString(20)),
    twap_id        Nullable(UInt64),
    deployer_fee   Nullable(Decimal(38, 18)),
    priority_gas   Nullable(Decimal(38, 18)),
    created_at     DateTime64(9, 'UTC') DEFAULT now64(9, 'UTC')
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMMDD(block_time)
ORDER BY (block_time, tid, user, hash, oid)
TTL block_time + INTERVAL 1 DAYS DELETE;
