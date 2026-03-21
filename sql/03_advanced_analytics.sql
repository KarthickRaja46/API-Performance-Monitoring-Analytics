USE performance_monitoring;

-- =============================================================================
-- ADVANCED ANALYTICS
-- =============================================================================

-- Latency Distribution
SELECT
    CASE
        WHEN execution_time <= 100 THEN 'Excellent'
        WHEN execution_time <= 300 THEN 'Good'
        WHEN execution_time <= 700 THEN 'Moderate'
        ELSE 'Slow'
    END AS latency_bucket,
    COUNT(*) AS request_count
FROM system_logs
GROUP BY latency_bucket
ORDER BY request_count DESC;

-- P95 Latency (seconds)
WITH ranked AS (
    SELECT
        execution_time,
        CUME_DIST() OVER (ORDER BY execution_time) AS cd
    FROM system_logs
)
SELECT ROUND(
    MIN(CASE WHEN cd >= 0.95 THEN execution_time END) / 1000.0,
3) AS p95_latency_sec
FROM ranked;

-- P99 Latency (seconds)
WITH ranked AS (
    SELECT
        execution_time,
        CUME_DIST() OVER (ORDER BY execution_time) AS cd
    FROM system_logs
)
SELECT ROUND(
    MIN(CASE WHEN cd >= 0.99 THEN execution_time END) / 1000.0,
3) AS p99_latency_sec
FROM ranked;

-- Endpoint P95 Latency
WITH endpoint_ranked AS (
    SELECT
        endpoint,
        execution_time,
        CUME_DIST() OVER (PARTITION BY endpoint ORDER BY execution_time) AS cd
    FROM system_logs
)
SELECT
    endpoint,
    ROUND(MIN(CASE WHEN cd >= 0.95 THEN execution_time END) / 1000.0, 3) AS p95_latency_sec
FROM endpoint_ranked
GROUP BY endpoint
ORDER BY p95_latency_sec DESC;

-- Endpoint Risk Mix
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(SUM(CASE WHEN status = 500 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
    ROUND(SUM(CASE WHEN execution_time > 500 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_pct
FROM system_logs
GROUP BY endpoint
ORDER BY error_rate_pct DESC, sla_breach_pct DESC;

-- Rejected Distribution
SELECT
    reason,
    COUNT(*) AS rejected_count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM rejected_logs), 0), 2) AS rejected_share_pct
FROM rejected_logs
GROUP BY reason
ORDER BY rejected_count DESC;
