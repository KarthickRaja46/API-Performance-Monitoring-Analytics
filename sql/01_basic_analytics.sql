USE performance_monitoring;

-- Total requests
SELECT COUNT(*) AS total_requests
FROM vw_system_logs_clean;

-- Success rate
SELECT ROUND(AVG(is_success) * 100, 2) AS success_rate_pct
FROM vw_system_logs_clean;

-- Error rate
SELECT ROUND(AVG(is_error) * 100, 2) AS error_rate_pct
FROM vw_system_logs_clean;

-- Not found rate
SELECT ROUND(AVG(is_not_found) * 100, 2) AS not_found_rate_pct
FROM vw_system_logs_clean;

-- Average execution time (seconds)
SELECT ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec
FROM vw_system_logs_clean;

-- Total error requests
SELECT COUNT(*) AS error_requests
FROM vw_system_logs_clean
WHERE is_error = 1;

-- Top endpoints by traffic(used)
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct
FROM vw_system_logs_clean
GROUP BY endpoint
ORDER BY total_requests DESC
LIMIT 10;

-- Top slow endpoints
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(MAX(exec_sec), 3) AS max_execution_time_sec
FROM vw_system_logs_clean
GROUP BY endpoint
HAVING COUNT(*) >= 10
ORDER BY avg_execution_time_sec DESC, total_requests DESC
LIMIT 10;

-- Hourly traffic with error rate
SELECT
    HOUR(`timestamp`) AS hour_of_day,
    COUNT(*) AS total_requests,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec
FROM vw_system_logs_clean
GROUP BY HOUR(`timestamp`)
ORDER BY hour_of_day;

-- Status distribution
SELECT
    status,
    COUNT(*) AS total_requests,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM vw_system_logs_clean), 0), 2) AS request_share_pct
FROM vw_system_logs_clean
GROUP BY status
ORDER BY total_requests DESC;