CREATE TABLE IF NOT EXISTS transactions
(
    us_state LowCardinality(String),
    cat_id LowCardinality(String),
    amount Decimal64(2)
)
ENGINE = MergeTree
ORDER BY (us_state, amount);

CREATE TABLE IF NOT EXISTS max_category_by_state
(
    us_state LowCardinality(String),
    category_state AggregateFunction(argMax, String, Decimal64(2))
)
ENGINE = AggregatingMergeTree
ORDER BY us_state;

CREATE MATERIALIZED VIEW IF NOT EXISTS max_category_by_state_mv
TO max_category_by_state AS
SELECT us_state, argMaxState(toString(cat_id), amount) AS category_state
FROM transactions
GROUP BY us_state;

CREATE TABLE IF NOT EXISTS transactions_kafka
(
    raw String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'transactions_clickhouse',
         kafka_group_name = 'clickhouse_transactions',
         kafka_format = 'JSONAsString',
         kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS transactions_kafka_mv
TO transactions AS
SELECT
    JSONExtractString(raw, 'us_state') AS us_state,
    JSONExtractString(raw, 'cat_id') AS cat_id,
    toDecimal64(JSONExtractString(raw, 'amount'), 2) AS amount
FROM transactions_kafka;
