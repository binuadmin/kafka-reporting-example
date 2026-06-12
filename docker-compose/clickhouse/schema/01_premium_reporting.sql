-- =============================================================================
-- Basic Kafka CSV ingest with raw storage and aggregated reporting
--
-- Pipeline:
--   premium_reporting_kafka  (Kafka engine — raw CSV strings)
--     ├── mv_kafka_to_raw    → premium_reporting_raw     (MergeTree, typed, as-is)
--           └── mv_kafka_to_rollup → premium_reporting_hourly  (AggregatingMergeTree)
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Kafka Engine Table
-- Reads CSV rows from Kafka. Each message is a single CSV row (no header).
-- All columns are String to match raw CSV parsing.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS premium_reporting_kafka (
    request_timestamp    String,
    device_id            String,
    app_id               String,
    ipv4_address         String,
    country_code         String,
    payment_type         String,
    http_status_code     String,
    http_status_msg      String,
    request_bytes        String,
    response_bytes       String,
    url                  String,
    hni                  String,
    connect_endpoint_ip  String,
    client_uid           String,
    logproc_run_id       String,
    logproc_time         String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = '{kafka_broker_list}',
    kafka_topic_list = '{kafka_topic}',
    kafka_group_name = '{kafka_consumer_group}',
    kafka_format = 'CSV',
    kafka_num_consumers = 2;
    --input_format_csv_allow_variable_number_of_columns = 1; -- future compatibility


-- -----------------------------------------------------------------------------
-- Raw MergeTree — typed "as-is" storage
-- All rows are stored with type conversions applied. Partitioned by day.
-- ORDER BY (app_id, request_timestamp) suits queries filtering by app_id and
-- a date range.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS premium_reporting_raw (
    request_timestamp    DateTime64(3) DEFAULT toDateTime64(0, 3),
    device_id            String DEFAULT '',
    app_id               UInt32 DEFAULT 0,
    ipv4_address         IPv4 DEFAULT toIPv4('0.0.0.0'),
    country_code         LowCardinality(String) DEFAULT '',
    payment_type         LowCardinality(String) DEFAULT '',
    http_status_code     UInt16 DEFAULT 0,
    http_status_msg      String DEFAULT '',
    request_bytes        UInt64 DEFAULT 0,
    response_bytes       UInt64 DEFAULT 0,
    url                  String DEFAULT '',
    hni                  LowCardinality(String) DEFAULT '',
    connect_endpoint_ip  IPv4 DEFAULT toIPv4('0.0.0.0'),
    client_uid           String DEFAULT '',
    logproc_run_id       UInt32 DEFAULT 0,
    logproc_time         DateTime64(3) DEFAULT toDateTime64(0, 3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(request_timestamp)
ORDER BY (app_id, ipv4_address, device_id, url, request_timestamp)
TTL request_timestamp + INTERVAL 30 DAY
SETTINGS ttl_only_drop_parts = 1;


-- -----------------------------------------------------------------------------
-- MV: Kafka → Raw MergeTree
-- Parses and type-casts all fields, stores exactly what is received (as-is).
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_raw
TO premium_reporting_raw
AS
SELECT
    parseDateTime64BestEffortOrZero(request_timestamp, 3)   AS request_timestamp,
    device_id,
    toUInt32OrZero(app_id)                                  AS app_id,
    toIPv4OrDefault(ipv4_address)                           AS ipv4_address,
    country_code,
    payment_type,
    toUInt16OrZero(http_status_code)                        AS http_status_code,
    http_status_msg,
    toUInt64OrZero(request_bytes)                           AS request_bytes,
    toUInt64OrZero(response_bytes)                          AS response_bytes,
    url,
    hni,
    toIPv4OrDefault(connect_endpoint_ip)                    AS connect_endpoint_ip,
    client_uid,
    toUInt32OrZero(logproc_run_id)                          AS logproc_run_id,
    parseDateTime64BestEffortOrZero(logproc_time, 3)        AS logproc_time
FROM premium_reporting_kafka;


-- -----------------------------------------------------------------------------
-- Aggregated MergeTree — per-minute rollup by app_id
-- Sums event_count, total_request_bytes, total_response_bytes.
-- SimpleAggregateFunction columns are merged on INSERT / OPTIMIZE with FINAL.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS premium_reporting_rollup (
    request_timestamp    DateTime DEFAULT toDateTime(0),
    app_id               UInt32 DEFAULT 0,
    event_count          SimpleAggregateFunction(sum, UInt64),
    total_request_bytes  SimpleAggregateFunction(sum, UInt64),
    total_response_bytes SimpleAggregateFunction(sum, UInt64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(request_timestamp)
ORDER BY (app_id, request_timestamp)
SETTINGS ttl_only_drop_parts = 1;


-- -----------------------------------------------------------------------------
-- MV: Kafka → Aggregated MergeTree
-- Rolls up count, request_bytes, response_bytes grouped by app_id + minute.
-- Both MVs read from the same Kafka engine table independently.
-- -----------------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_kafka_to_rollup
TO premium_reporting_rollup
AS
SELECT
    toStartOfInterval(request_timestamp, INTERVAL 1 MINUTE) AS request_timestamp,
    app_id,
    1 AS event_count,
    request_bytes AS total_request_bytes,
    response_bytes AS total_response_bytes
FROM premium_reporting_raw;
