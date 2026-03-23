USE performance_monitoring;

-- =============================================================================
-- PRODUCTION PROOF: PERFORMANCE + QUALITY + BUSINESS IMPACT
-- =============================================================================

-- -----------------------------------------------------------------------------
-- A) PERFORMANCE OPTIMIZATION PROOF (EXPLAIN ANALYZE)
-- Purpose: demonstrate index impact with controlled before vs after runs.
-- -----------------------------------------------------------------------------

-- A1. Time-based analytics query (before index usage hint).
EXPLAIN ANALYZE
SELECT
    DATE(`timestamp`) AS request_date,
    COUNT(*) AS total_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec
FROM system_logs IGNORE INDEX (idx_timestamp)
WHERE `timestamp` >= NOW() - INTERVAL 7 DAY
GROUP BY DATE(`timestamp`)
ORDER BY request_date;

-- A2. Time-based analytics query (after timestamp index usage hint).
EXPLAIN ANALYZE
SELECT
    DATE(`timestamp`) AS request_date,
    COUNT(*) AS total_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec
FROM system_logs USE INDEX (idx_timestamp)
WHERE `timestamp` >= NOW() - INTERVAL 7 DAY
GROUP BY DATE(`timestamp`)
ORDER BY request_date;

-- A3. Endpoint aggregation query (before endpoint index usage hint).
EXPLAIN ANALYZE
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec
FROM system_logs IGNORE INDEX (idx_endpoint)
GROUP BY endpoint
ORDER BY total_requests DESC
LIMIT 10;

-- A4. Endpoint aggregation query (after endpoint index usage hint).
EXPLAIN ANALYZE
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec
FROM system_logs USE INDEX (idx_endpoint)
GROUP BY endpoint
ORDER BY total_requests DESC
LIMIT 10;

-- Review EXPLAIN ANALYZE output for reduced rows examined and lower execution time.

-- -----------------------------------------------------------------------------
-- B) DATA QUALITY ENGINEERING CHECKS
-- Validation rules:
-- 1) status IN (200, 404, 500)
-- 2) execution_time > 0
-- 3) endpoint is not null/blank
-- -----------------------------------------------------------------------------

-- B1. Rule-violation summary in trusted table.
SELECT
    COUNT(*) AS total_rows_checked,
    SUM(CASE WHEN status NOT IN (200, 404, 500) THEN 1 ELSE 0 END) AS invalid_status_rows,
    SUM(CASE WHEN execution_time <= 0 THEN 1 ELSE 0 END) AS invalid_execution_time_rows,
    SUM(CASE WHEN endpoint IS NULL OR TRIM(endpoint) = '' THEN 1 ELSE 0 END) AS invalid_endpoint_rows
FROM system_logs;

-- B2. Rejected-log reason distribution (proof of captured invalid data reasons).
SELECT
    reason,
    COUNT(*) AS rejected_count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM rejected_logs), 0), 2) AS rejected_share_pct
FROM rejected_logs
GROUP BY reason
ORDER BY rejected_count DESC;

-- B3. Rejection accounting consistency vs ETL metrics.
SELECT
    COALESCE(SUM(rejected_rows), 0) AS etl_reported_rejected_rows,
    (SELECT COUNT(*) FROM rejected_logs) AS rejected_logs_rows,
    COALESCE(SUM(rejected_rows), 0) - (SELECT COUNT(*) FROM rejected_logs) AS rejection_count_gap
FROM etl_metrics;

-- -----------------------------------------------------------------------------
-- C) BUSINESS IMPACT LAYER
-- Purpose: translate technical metrics into user and business risk signals.
-- -----------------------------------------------------------------------------

-- C1. Slowest endpoints impacting user experience.
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(MAX(exec_sec), 3) AS max_latency_sec,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean
GROUP BY endpoint
HAVING COUNT(*) >= 10
ORDER BY avg_latency_sec DESC, sla_breach_rate_pct DESC
LIMIT 10;

-- C2. Highest-error endpoints impacting reliability.
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec
FROM vw_system_logs_clean
GROUP BY endpoint
HAVING COUNT(*) >= 10
ORDER BY error_rate_pct DESC, total_requests DESC
LIMIT 10;

-- C3. Traffic spike candidates for capacity planning.
WITH minute_traffic AS (
    SELECT
        minute_bucket,
        COUNT(*) AS requests_per_minute
    FROM vw_system_logs_clean
    GROUP BY minute_bucket
),
scored AS (
    SELECT
        minute_bucket,
        requests_per_minute,
        AVG(requests_per_minute) OVER (
            ORDER BY minute_bucket
            ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING
        ) AS prior_60m_avg
    FROM minute_traffic
)
SELECT
    minute_bucket,
    requests_per_minute,
    ROUND(prior_60m_avg, 2) AS prior_60m_avg,
    ROUND(
        CASE
            WHEN prior_60m_avg IS NULL OR prior_60m_avg = 0 THEN NULL
            ELSE (requests_per_minute / prior_60m_avg)
        END,
    2) AS spike_multiplier
FROM scored
WHERE prior_60m_avg IS NOT NULL
  AND requests_per_minute >= (prior_60m_avg * 1.5)
ORDER BY minute_bucket DESC
LIMIT 120;
