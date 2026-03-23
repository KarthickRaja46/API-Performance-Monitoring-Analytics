USE performance_monitoring;

-- =============================================================================
-- MASTER DASHBOARD OUTPUTS (FINAL FEEDS ONLY)
-- =============================================================================

-- 1) SUMMARY QUERY (single-row KPI block)
SELECT
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(AVG(is_success) * 100, 2) AS success_rate_pct,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_not_found) * 100, 2) AS not_found_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct,
    ROUND(
        (
            (AVG(is_success) * 0.50) +
            ((1 - AVG(is_error)) * 0.30) +
            ((1 - AVG(is_sla_breach)) * 0.20)
        ) * 100,
    2) AS health_score_pct
FROM vw_system_logs_clean;

-- 2) TREND QUERY (daily dashboard line charts)
SELECT
    request_date,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean
GROUP BY request_date
ORDER BY request_date;

-- 3) ENDPOINT QUERY (top endpoint table)
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_latency_sec,
    ROUND(MAX(exec_sec), 3) AS max_latency_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean
GROUP BY endpoint
ORDER BY total_requests DESC, avg_latency_sec DESC
LIMIT 10;
