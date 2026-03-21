USE performance_monitoring;

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
SELECT
    COUNT(*) AS total_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_latency_sec,
    ROUND(SUM(status = 200) * 100.0 / NULLIF(COUNT(*), 0), 2) AS success_rate_pct,
    ROUND(SUM(status = 500) * 100.0 / NULLIF(COUNT(*), 0), 2) AS error_rate_pct,
    ROUND(SUM(execution_time > 500) * 100.0 / NULLIF(COUNT(*), 0), 2) AS sla_breach_pct,
    ROUND(
        (
            (SUM(status = 200) * 1.0 / NULLIF(COUNT(*), 0) * 0.5) +
            ((1 - SUM(status = 500) * 1.0 / NULLIF(COUNT(*), 0)) * 0.3) +
            ((1 - SUM(execution_time > 500) * 1.0 / NULLIF(COUNT(*), 0)) * 0.2)
        ) * 100,
    2) AS health_score_pct
FROM system_logs;

-- Query Category Summary
SELECT 'basic_query_count' AS metric, 14 AS value
UNION ALL
SELECT 'advanced_query_count' AS metric, 6 AS value
UNION ALL
SELECT 'other_query_count' AS metric, 0 AS value
UNION ALL
SELECT 'total_query_count' AS metric, 20 AS value;
