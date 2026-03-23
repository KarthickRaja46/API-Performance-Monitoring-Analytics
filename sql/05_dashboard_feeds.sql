USE performance_monitoring;
-- Summary KPI block 
SELECT
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    SUM(is_error) AS error_count,
    ROUND(AVG(is_success) * 100, 2) AS success_rate_pct,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_not_found) * 100, 2) AS not_found_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct,
    ROUND(
        (AVG(is_success) * 50) +
        ((1 - AVG(is_error)) * 30) +
        ((1 - AVG(is_sla_breach)) * 20),
    2) AS health_score_pct
FROM vw_system_logs_clean;
-- Daily trend for charts
SELECT
    request_date,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean
GROUP BY request_date
ORDER BY request_date;
-- Top endpoints table
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(MAX(exec_sec), 3) AS max_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean
GROUP BY endpoint
ORDER BY total_requests DESC, avg_execution_time_sec DESC
LIMIT 10;
-- Slow endpoints table (for slow endpoint chart)
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(MAX(exec_sec), 3) AS max_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean
GROUP BY endpoint
HAVING COUNT(*) >= 10
ORDER BY avg_execution_time_sec DESC, total_requests DESC
LIMIT 10;
-- Status code distribution (for status chart)
SELECT
    status,
    COUNT(*) AS total_requests,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM vw_system_logs_clean), 0), 2) AS request_share_pct
FROM vw_system_logs_clean
GROUP BY status
ORDER BY total_requests DESC;
-- DB load view by hour (traffic + scan + join pressure)
SELECT
    HOUR(`timestamp`) AS hour_of_day,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(AVG(COALESCE(rows_scanned, 0)), 2) AS avg_rows_scanned,
    ROUND(AVG(COALESCE(joins_count, 0)), 2) AS avg_joins_count
FROM vw_system_logs_clean
GROUP BY HOUR(`timestamp`)
ORDER BY hour_of_day;
