USE performance_monitoring;

-- =============================================================================
-- BASIC ANALYTICS (ONLY SIMPLE CHECKS)
-- =============================================================================

-- Total requests
SELECT COUNT(*) AS total_requests
FROM vw_system_logs_clean;

-- Average latency in seconds
SELECT ROUND(AVG(exec_sec), 3) AS avg_latency_sec
FROM vw_system_logs_clean;

-- Total error requests
SELECT COUNT(*) AS error_requests
FROM vw_system_logs_clean
WHERE is_error = 1;