USE performance_monitoring;

-- =============================================================================
-- ADVANCED ANALYTICS (ONLY ADVANCED INSIGHTS)
-- =============================================================================

-- Latency distribution buckets
SELECT
    CASE
        WHEN exec_sec <= 0.100 THEN 'Excellent'
        WHEN exec_sec <= 0.300 THEN 'Good'
        WHEN exec_sec <= 0.700 THEN 'Moderate'
        ELSE 'Slow'
    END AS latency_bucket,
    COUNT(*) AS request_count
FROM vw_system_logs_clean
GROUP BY latency_bucket
ORDER BY request_count DESC;

-- Global P95 latency
WITH ranked AS (
    SELECT
        exec_sec,
        CUME_DIST() OVER (ORDER BY exec_sec) AS cd
    FROM vw_system_logs_clean
)
SELECT ROUND(
    MIN(CASE WHEN cd >= 0.95 THEN exec_sec END),
3) AS p95_latency_sec
FROM ranked;

-- Global P99 latency
WITH ranked AS (
    SELECT
        exec_sec,
        CUME_DIST() OVER (ORDER BY exec_sec) AS cd
    FROM vw_system_logs_clean
)
SELECT ROUND(
    MIN(CASE WHEN cd >= 0.99 THEN exec_sec END),
3) AS p99_latency_sec
FROM ranked;

-- Endpoint risk scoring
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct,
    ROUND(
        (AVG(is_error) * 100 * 0.50) +
        (AVG(is_sla_breach) * 100 * 0.35) +
        (LEAST(AVG(exec_sec) / 0.5, 2) * 100 * 0.15),
    2) AS endpoint_risk_score
FROM vw_system_logs_clean
GROUP BY endpoint
ORDER BY endpoint_risk_score DESC, total_requests DESC;

-- Daily trend with 7-day rolling latency and error
WITH daily AS (
    SELECT
        request_date,
        COUNT(*) AS total_requests,
        ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
        ROUND(AVG(is_error) * 100, 2) AS error_rate_pct
    FROM vw_system_logs_clean
    GROUP BY request_date
)
SELECT
    request_date,
    total_requests,
    avg_latency_sec,
    error_rate_pct,
    ROUND(
        AVG(avg_latency_sec) OVER (
            ORDER BY request_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
    3) AS latency_rolling_7d_sec,
    ROUND(
        AVG(error_rate_pct) OVER (
            ORDER BY request_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
    2) AS error_rate_rolling_7d_pct
FROM daily
ORDER BY request_date;
