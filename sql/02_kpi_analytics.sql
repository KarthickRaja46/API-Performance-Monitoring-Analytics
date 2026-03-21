USE performance_monitoring;

SET @sla_threshold_sec = 0.5;

-- =============================================================================
-- KPI & ETL ANALYTICS
-- =============================================================================

-- ETL Inserted vs Rejected (%)
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

-- Rejected Data by Reason
SELECT
    reason,
    COUNT(*) AS rejected_count
FROM rejected_logs
GROUP BY reason
ORDER BY rejected_count DESC;

-- ETL Freshness (minutes since last load)
SELECT ROUND(
    TIMESTAMPDIFF(SECOND, MAX(load_time), NOW()) / 60.0,
2) AS minutes_since_last_load
FROM etl_metrics;

-- Overall System Health Score
WITH base AS (
    SELECT
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
    ROUND(SUM(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
    ROUND(SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
    ROUND(SUM(CASE WHEN is_not_found = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS not_found_rate_pct,
    ROUND(SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_rate_pct,
    ROUND(
        (
            (SUM(CASE WHEN is_success = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) * 0.50) +
            ((1 - (SUM(CASE WHEN is_error = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0))) * 0.30) +
            ((1 - (SUM(CASE WHEN is_sla_breach = 1 THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0))) * 0.20)
        ) * 100,
    2) AS health_score_pct
FROM base;

-- Query Category Summary
SELECT 'basic_query_count' AS metric, 16 AS value
UNION ALL
SELECT 'advanced_query_count' AS metric, 8 AS value
UNION ALL
SELECT 'kpi_query_count' AS metric, 4 AS value
UNION ALL
SELECT 'total_query_count' AS metric, 28 AS value;
