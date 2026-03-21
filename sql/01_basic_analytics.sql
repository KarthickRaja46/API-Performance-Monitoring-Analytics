USE performance_monitoring;
-- =============================================================================
-- BASIC ANALYTICS
-- =============================================================================

-- Total Requests
SELECT COUNT(*) AS total_requests
FROM system_logs;

-- Success Rate (%)
SELECT ROUND(
    SUM(CASE WHEN status = 200 THEN 1 ELSE 0 END) * 100.0
    / NULLIF(COUNT(*), 0),
2) AS success_rate_pct
FROM system_logs;

-- Error Rate (%)
SELECT ROUND(
    SUM(CASE WHEN status = 500 THEN 1 ELSE 0 END) * 100.0
    / NULLIF(COUNT(*), 0),
2) AS error_rate_pct
FROM system_logs;

-- Not Found Rate (%)
SELECT ROUND(
    SUM(CASE WHEN status = 404 THEN 1 ELSE 0 END) * 100.0
    / NULLIF(COUNT(*), 0),
2) AS not_found_rate_pct
FROM system_logs;

-- Average Response Time (seconds)
SELECT ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec
FROM system_logs;

-- Slow Requests (>1 sec)
SELECT COUNT(*) AS slow_requests
FROM system_logs
WHERE (execution_time / 1000.0) > 1.0;

-- Daily Average Latency
SELECT
    DATE(`timestamp`) AS request_date,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec
FROM system_logs
GROUP BY request_date
ORDER BY request_date;

-- Requests Per Minute
SELECT
    DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:%i:00') AS minute_bucket,
    COUNT(*) AS requests_per_minute
FROM system_logs
GROUP BY minute_bucket
ORDER BY minute_bucket DESC
LIMIT 60;

-- Peak Minute Traffic
SELECT
    DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:%i:00') AS minute_bucket,
    COUNT(*) AS request_count
FROM system_logs
GROUP BY minute_bucket
ORDER BY request_count DESC, minute_bucket DESC
LIMIT 1;

-- Hourly Error Rate
SELECT
    HOUR(`timestamp`) AS hour,
    ROUND(
        SUM(CASE WHEN status = 500 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0),
    2) AS error_rate_pct
FROM system_logs
GROUP BY hour
ORDER BY hour;

-- Daily Status Trend
SELECT
    DATE(`timestamp`) AS request_date,
    status,
    COUNT(*) AS request_count
FROM system_logs
GROUP BY request_date, status
ORDER BY request_date DESC;

-- Top Endpoints
SELECT endpoint, COUNT(*) AS request_count
FROM system_logs
GROUP BY endpoint
ORDER BY request_count DESC
LIMIT 10;

-- Top Slow Endpoints (>0.5 sec)
SELECT
    endpoint,
    COUNT(*) AS slow_request_count,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec
FROM system_logs
WHERE (execution_time / 1000.0) > 0.5
GROUP BY endpoint
ORDER BY slow_request_count DESC, avg_latency_sec DESC
LIMIT 10;

-- Endpoint Latency Summary
SELECT
    endpoint,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec,
    ROUND(MAX(execution_time) / 1000.0, 3) AS max_latency_sec
FROM system_logs
GROUP BY endpoint
ORDER BY avg_latency_sec DESC;

-- Endpoint Error Rate
SELECT
    endpoint,
    ROUND(
        SUM(CASE WHEN status = 500 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0),
    2) AS error_rate_pct
FROM system_logs
GROUP BY endpoint
ORDER BY error_rate_pct DESC;

-- SLA Breach Rate (>0.5 sec)
SELECT
    endpoint,
    ROUND(
        SUM(CASE WHEN (execution_time / 1000.0) > 0.5 THEN 1 ELSE 0 END) * 100.0
        / NULLIF(COUNT(*), 0),
    2) AS sla_breach_pct
FROM system_logs
GROUP BY endpoint
ORDER BY sla_breach_pct DESC;
