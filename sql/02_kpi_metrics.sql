
USE performance_monitoring;

-/*- Success rate
SELECT ROUND(AVG(is_success) * 100, 2) AS success_rate_pct
FROM vw_system_logs_clean;--*/

/*-- Error rate
SELECT ROUND(AVG(is_error) * 100, 2) AS error_rate_pct
FROM vw_system_logs_clean;  --*/

-- SLA breach rate
SELECT ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean;

-- ETL inserted vs rejected share
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

-- ETL freshness (minutes)
SELECT ROUND(
    TIMESTAMPDIFF(SECOND, MAX(load_time), NOW()) / 60.0,
2) AS minutes_since_last_load
FROM etl_metrics;

-- Overall health score
SELECT
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
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
