USE performance_monitoring;

-- =============================================================================
-- MASTER ANALYTICS (PRODUCTION-READY)
-- =============================================================================

-- Shared assumptions:
-- 1) execution_time is stored in milliseconds in system_logs.
-- 2) All latency outputs are standardized to seconds with 3-decimal precision.
-- 3) SLA breach threshold is 0.5 sec.

SET @sla_threshold_sec = 0.5;

-- =============================================================================
-- BASIC ANALYTICS
-- =============================================================================

WITH base AS (
    SELECT
        id,
        ip,
        endpoint,
        status,
        `timestamp`,
        DATE(`timestamp`) AS request_date,
        DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:%i:00') AS minute_bucket,
        execution_time / 1000.0 AS exec_sec,
        CASE WHEN status = 200 THEN 1 ELSE 0 END AS is_success,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error,
        CASE WHEN status = 404 THEN 1 ELSE 0 END AS is_not_found,
        CASE WHEN (execution_time / 1000.0) > @sla_threshold_sec THEN 1 ELSE 0 END AS is_sla_breach
    FROM system_logs
)
SELECT
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(MAX(exec_sec), 3) AS max_latency_sec,
    ROUND(MIN(exec_sec), 3) AS min_latency_sec,
    ROUND(SUM(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
    ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
    ROUND(SUM(CASE WHEN is_not_found = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS not_found_rate_pct,
    ROUND(SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_rate_pct
FROM base;

WITH base AS (
    SELECT
        endpoint,
        DATE(`timestamp`) AS request_date,
        execution_time / 1000.0 AS exec_sec,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error,
        CASE WHEN (execution_time / 1000.0) > @sla_threshold_sec THEN 1 ELSE 0 END AS is_sla_breach
    FROM system_logs
)
SELECT
    request_date,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
    ROUND(SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_rate_pct
FROM base
GROUP BY request_date
ORDER BY request_date;

WITH base AS (
    SELECT
        DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:%i:00') AS minute_bucket
    FROM system_logs
)
SELECT
    minute_bucket,
    COUNT(*) AS requests_per_minute
FROM base
GROUP BY minute_bucket
ORDER BY minute_bucket DESC
LIMIT 60;

WITH base AS (
    SELECT
        endpoint,
        execution_time / 1000.0 AS exec_sec,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error,
        CASE WHEN (execution_time / 1000.0) > @sla_threshold_sec THEN 1 ELSE 0 END AS is_sla_breach
    FROM system_logs
)
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(MAX(exec_sec), 3) AS max_latency_sec,
    ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
    ROUND(SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_rate_pct
FROM base
GROUP BY endpoint
ORDER BY total_requests DESC, avg_latency_sec DESC
LIMIT 10;

-- =============================================================================
-- ADVANCED ANALYTICS
-- =============================================================================

WITH base AS (
    SELECT
        endpoint,
        `timestamp`,
        DATE(`timestamp`) AS request_date,
        DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:%i:00') AS minute_bucket,
        execution_time / 1000.0 AS exec_sec,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error,
        CASE WHEN (execution_time / 1000.0) > @sla_threshold_sec THEN 1 ELSE 0 END AS is_sla_breach
    FROM system_logs
)
SELECT
    CASE
        WHEN exec_sec <= 0.100 THEN 'Excellent'
        WHEN exec_sec <= 0.300 THEN 'Good'
        WHEN exec_sec <= 0.700 THEN 'Moderate'
        ELSE 'Slow'
    END AS latency_bucket,
    COUNT(*) AS request_count
FROM base
GROUP BY latency_bucket
ORDER BY request_count DESC;

WITH base AS (
    SELECT
        endpoint,
        execution_time / 1000.0 AS exec_sec
    FROM system_logs
),
ranked AS (
    SELECT
        endpoint,
        exec_sec,
        CUME_DIST() OVER (ORDER BY exec_sec) AS cd_global,
        CUME_DIST() OVER (PARTITION BY endpoint ORDER BY exec_sec) AS cd_endpoint
    FROM base
)
SELECT
    ROUND(MIN(CASE WHEN cd_global >= 0.95 THEN exec_sec END), 3) AS p95_latency_sec,
    ROUND(MIN(CASE WHEN cd_global >= 0.99 THEN exec_sec END), 3) AS p99_latency_sec
FROM ranked;

WITH base AS (
    SELECT
        endpoint,
        execution_time / 1000.0 AS exec_sec
    FROM system_logs
),
ranked AS (
    SELECT
        endpoint,
        exec_sec,
        CUME_DIST() OVER (PARTITION BY endpoint ORDER BY exec_sec) AS cd
    FROM base
)
SELECT
    endpoint,
    ROUND(MIN(CASE WHEN cd >= 0.95 THEN exec_sec END), 3) AS p95_latency_sec,
    ROUND(MIN(CASE WHEN cd >= 0.99 THEN exec_sec END), 3) AS p99_latency_sec
FROM ranked
GROUP BY endpoint
ORDER BY p99_latency_sec DESC, p95_latency_sec DESC;

-- 7-day rolling averages (traffic, latency, error)
WITH base AS (
    SELECT
        DATE(`timestamp`) AS request_date,
        execution_time / 1000.0 AS exec_sec,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error
    FROM system_logs
),
daily AS (
    SELECT
        request_date,
        COUNT(*) AS total_requests,
        ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
        ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct
    FROM base
    GROUP BY request_date
)
SELECT
    request_date,
    total_requests,
    avg_latency_sec,
    error_rate_pct,
    ROUND(AVG(total_requests) OVER (
        ORDER BY request_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS traffic_rolling_7d,
    ROUND(AVG(avg_latency_sec) OVER (
        ORDER BY request_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 3) AS latency_rolling_7d_sec,
    ROUND(AVG(error_rate_pct) OVER (
        ORDER BY request_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS error_rate_rolling_7d_pct
FROM daily
ORDER BY request_date;

-- Error spike detection (daily z-score style against prior 7-day baseline)
WITH base AS (
    SELECT
        DATE(`timestamp`) AS request_date,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error
    FROM system_logs
),
daily AS (
    SELECT
        request_date,
        ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct
    FROM base
    GROUP BY request_date
),
baseline AS (
    SELECT
        request_date,
        error_rate_pct,
        AVG(error_rate_pct) OVER (
            ORDER BY request_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS prior_7d_avg_error_rate,
        STDDEV_SAMP(error_rate_pct) OVER (
            ORDER BY request_date
            ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
        ) AS prior_7d_std_error_rate
    FROM daily
)
SELECT
    request_date,
    error_rate_pct,
    ROUND(prior_7d_avg_error_rate, 2) AS prior_7d_avg_error_rate,
    ROUND(prior_7d_std_error_rate, 2) AS prior_7d_std_error_rate,
    CASE
        WHEN prior_7d_std_error_rate IS NULL OR prior_7d_std_error_rate = 0 THEN 0
        WHEN error_rate_pct > prior_7d_avg_error_rate + (2 * prior_7d_std_error_rate) THEN 1
        ELSE 0
    END AS is_error_spike
FROM baseline
ORDER BY request_date;

-- Traffic surge detection (minute-level current vs prior 60-minute average)
WITH base AS (
    SELECT
        DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:%i:00') AS minute_bucket
    FROM system_logs
),
minute_agg AS (
    SELECT
        minute_bucket,
        COUNT(*) AS requests_per_minute
    FROM base
    GROUP BY minute_bucket
),
surge AS (
    SELECT
        minute_bucket,
        requests_per_minute,
        AVG(requests_per_minute) OVER (
            ORDER BY minute_bucket
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS prior_60m_avg
    FROM minute_agg
)
SELECT
    minute_bucket,
    requests_per_minute,
    ROUND(prior_60m_avg, 2) AS prior_60m_avg,
    CASE
        WHEN prior_60m_avg IS NULL THEN 0
        WHEN requests_per_minute >= (prior_60m_avg * 1.5) THEN 1
        ELSE 0
    END AS is_traffic_surge
FROM surge
ORDER BY minute_bucket DESC
LIMIT 180;

-- Endpoint risk scoring (weighted by error, SLA breach, and latency)
WITH base AS (
    SELECT
        endpoint,
        execution_time / 1000.0 AS exec_sec,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error,
        CASE WHEN (execution_time / 1000.0) > @sla_threshold_sec THEN 1 ELSE 0 END AS is_sla_breach
    FROM system_logs
),
endpoint_metrics AS (
    SELECT
        endpoint,
        COUNT(*) AS total_requests,
        ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
        ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
        ROUND(SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_rate_pct
    FROM base
    GROUP BY endpoint
)
SELECT
    endpoint,
    total_requests,
    avg_latency_sec,
    error_rate_pct,
    sla_breach_rate_pct,
    ROUND(
        (error_rate_pct * 0.50) +
        (sla_breach_rate_pct * 0.35) +
        (LEAST(avg_latency_sec / NULLIF(@sla_threshold_sec, 0), 2) * 100 * 0.15),
    2) AS endpoint_risk_score
FROM endpoint_metrics
ORDER BY endpoint_risk_score DESC, total_requests DESC;

-- =============================================================================
-- KPI & ETL ANALYTICS
-- =============================================================================

WITH base AS (
    SELECT
        execution_time / 1000.0 AS exec_sec,
        CASE WHEN status = 200 THEN 1 ELSE 0 END AS is_success,
        CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error,
        CASE WHEN (execution_time / 1000.0) > @sla_threshold_sec THEN 1 ELSE 0 END AS is_sla_breach
    FROM system_logs
)
SELECT
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(SUM(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
    ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
    ROUND(SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_rate_pct,
    ROUND(
        (
            (SUM(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) * 0.50) +
            ((1 - (SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0))) * 0.30) +
            ((1 - (SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0))) * 0.20)
        ) * 100,
    2) AS health_score_pct
FROM base;

SELECT
    run_id,
    source_type,
    total_rows,
    inserted_rows,
    rejected_rows,
    ROUND(inserted_rows * 100.0 / NULLIF(total_rows, 0), 2) AS inserted_pct,
    ROUND(rejected_rows * 100.0 / NULLIF(total_rows, 0), 2) AS rejected_pct,
    load_time
FROM etl_metrics
ORDER BY load_time DESC;

SELECT
    reason,
    COUNT(*) AS rejected_count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM rejected_logs), 0), 2) AS rejected_share_pct
FROM rejected_logs
GROUP BY reason
ORDER BY rejected_count DESC;

SELECT
    ROUND(TIMESTAMPDIFF(SECOND, MAX(load_time), NOW()) / 60.0, 2) AS minutes_since_last_load
FROM etl_metrics;

-- =============================================================================
-- QUERY CATEGORY PARTITION (REPRESENTATIVE FILES)
-- =============================================================================

SELECT
    1 AS category_order,
    'BASIC ANALYTICS' AS query_category,
    'sql/01_basic_analytics.sql' AS representative_file,
    16 AS query_count
UNION ALL
SELECT
    2 AS category_order,
    'ADVANCED ANALYTICS' AS query_category,
    'sql/03_advanced_analytics.sql' AS representative_file,
    8 AS query_count
UNION ALL
SELECT
    3 AS category_order,
    'KPI & HEALTH METRICS' AS query_category,
    'sql/02_kpi_analytics.sql' AS representative_file,
    4 AS query_count
UNION ALL
SELECT
    4 AS category_order,
    'MASTER ANALYTICS' AS query_category,
    'sql/04_master_analytics.sql' AS representative_file,
    28 AS query_count
ORDER BY category_order;
