-- Hive table definitions for the telemetry data lake
-- Run these after batch processing to enable SQL queries on HDFS data

CREATE DATABASE IF NOT EXISTS telemetry_db;
USE telemetry_db;

-- ── Raw Telemetry Events ────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS raw_app_events (
    event_id        STRING,
    event_type      STRING,
    `timestamp`     TIMESTAMP,
    device_id       STRING,
    device_type     STRING,
    os              STRING,
    app_version     STRING,
    region          STRING,
    payload         MAP<STRING, STRING>
)
STORED AS PARQUET
LOCATION '/data/raw/app_events';

CREATE EXTERNAL TABLE IF NOT EXISTS raw_crashes (
    event_id        STRING,
    event_type      STRING,
    `timestamp`     TIMESTAMP,
    device_id       STRING,
    device_type     STRING,
    os              STRING,
    app_version     STRING,
    region          STRING,
    payload         MAP<STRING, STRING>
)
STORED AS PARQUET
LOCATION '/data/raw/crashes';

CREATE EXTERNAL TABLE IF NOT EXISTS raw_performance (
    event_id        STRING,
    event_type      STRING,
    `timestamp`     TIMESTAMP,
    device_id       STRING,
    device_type     STRING,
    os              STRING,
    app_version     STRING,
    region          STRING,
    payload         MAP<STRING, STRING>
)
STORED AS PARQUET
LOCATION '/data/raw/performance';

-- ── Processed Aggregates ────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS daily_trends (
    `date`              DATE,
    daily_active_users  BIGINT,
    total_events        BIGINT,
    total_sessions      BIGINT
)
STORED AS PARQUET
LOCATION '/data/processed/batch/daily_trends';

CREATE EXTERNAL TABLE IF NOT EXISTS feature_adoption (
    `date`          DATE,
    feature_name    STRING,
    unique_users    BIGINT,
    total_usage     BIGINT,
    avg_duration_ms DOUBLE,
    adoption_rate   DOUBLE
)
STORED AS PARQUET
LOCATION '/data/processed/batch/feature_adoption';

CREATE EXTERNAL TABLE IF NOT EXISTS feature_ranking (
    feature_name        STRING,
    unique_users        BIGINT,
    total_usage         BIGINT,
    avg_duration_ms     DOUBLE,
    avg_interactions    DOUBLE,
    `rank`              INT
)
STORED AS PARQUET
LOCATION '/data/processed/batch/feature_ranking';

CREATE EXTERNAL TABLE IF NOT EXISTS crash_analysis (
    device_type      STRING,
    os               STRING,
    app_version      STRING,
    crash_count      BIGINT,
    affected_devices BIGINT,
    fatal_crashes    BIGINT,
    total_devices    BIGINT,
    crash_rate       DOUBLE
)
STORED AS PARQUET
LOCATION '/data/processed/batch/crash_analysis';

CREATE EXTERNAL TABLE IF NOT EXISTS device_performance (
    device_type      STRING,
    os               STRING,
    app_version      STRING,
    avg_metric_value DOUBLE,
    p50              DOUBLE,
    p95              DOUBLE,
    p99              DOUBLE,
    sample_count     BIGINT
)
STORED AS PARQUET
LOCATION '/data/processed/batch/device_performance';

-- ── Example Queries (Enterprise BI Workflows) ───────────────────────

-- Most used features
-- SELECT feature_name, total_usage, unique_users, adoption_rate
-- FROM feature_ranking
-- ORDER BY rank
-- LIMIT 10;

-- Crash rates by device type
-- SELECT device_type, os, crash_rate, crash_count, fatal_crashes
-- FROM crash_analysis
-- WHERE crash_rate > 0.01
-- ORDER BY crash_rate DESC;

-- Usage by region over time
-- SELECT date, region, daily_active_users, total_events
-- FROM regional_trends
-- WHERE region = 'us-east'
-- ORDER BY date DESC;

-- Daily trend with week-over-week comparison
-- SELECT date, daily_active_users,
--        LAG(daily_active_users, 7) OVER (ORDER BY date) as prev_week_dau,
--        (daily_active_users - LAG(daily_active_users, 7) OVER (ORDER BY date))
--            / LAG(daily_active_users, 7) OVER (ORDER BY date) * 100 as wow_change_pct
-- FROM daily_trends
-- ORDER BY date DESC;
