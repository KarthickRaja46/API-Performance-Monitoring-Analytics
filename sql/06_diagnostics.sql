USE performance_monitoring;

CALL sp_system_health_check();

-- Slow query evidence
SELECT
    id,
    endpoint,
    status,
    `timestamp`,
    ROUND(execution_time / 1000.0, 3) AS execution_time_sec,
    rows_scanned,
    joins_count,
    'Slow query' AS problem,
    'High execution time' AS cause,
    'Rewrite query, optimize endpoint logic, and add caching' AS recommended_solution
FROM system_logs
WHERE execution_time > 5000
ORDER BY execution_time DESC
LIMIT 200;

-- High-load query evidence
SELECT
    id,
    endpoint,
    status,
    `timestamp`,
    ROUND(execution_time / 1000.0, 3) AS execution_time_sec,
    rows_scanned,
    joins_count,
    'High load query' AS problem,
    'rows_scanned high' AS cause,
    'Add index and tighten WHERE filters' AS recommended_solution
FROM system_logs
WHERE COALESCE(rows_scanned, 0) >= 5000
ORDER BY rows_scanned DESC, execution_time DESC
LIMIT 200;

-- Join-heavy query evidence
SELECT
    id,
    endpoint,
    status,
    `timestamp`,
    ROUND(execution_time / 1000.0, 3) AS execution_time_sec,
    rows_scanned,
    joins_count,
    'Complex query' AS problem,
    'joins_count > 2' AS cause,
    'Reduce joins and pre-aggregate intermediate data' AS recommended_solution
FROM system_logs
WHERE COALESCE(joins_count, 0) > 2
ORDER BY joins_count DESC, execution_time DESC
LIMIT 200;

/*-- Daily diagnostics trend (last 7 days)--*/
SELECT
    DATE(`timestamp`) AS request_date,
    COUNT(*) AS total_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_execution_time_sec,
    ROUND(AVG(CASE WHEN status >= 400 THEN 1 ELSE 0 END) * 100, 2) AS error_rate_pct,
    ROUND(AVG(CASE WHEN execution_time > 500 THEN 1 ELSE 0 END) * 100, 2) AS sla_breach_rate_pct
FROM system_logs
WHERE `timestamp` >= NOW() - INTERVAL 7 DAY
GROUP BY DATE(`timestamp`)
ORDER BY request_date;

/*-- System-level problem summary with action--*/
SELECT
    'system_overview' AS scope,
    CASE
        WHEN AVG(is_sla_breach) * 100 >= 40 THEN 'High SLA breach rate'
        WHEN AVG(is_sla_breach) * 100 >= 20 THEN 'Moderate SLA breach rate'
        ELSE 'SLA mostly stable'
    END AS problem_statement,
    CASE
        WHEN AVG(is_sla_breach) * 100 >= 40 THEN 'HIGH'
        WHEN AVG(is_sla_breach) * 100 >= 20 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS impact_level,
    CASE
        WHEN AVG(is_sla_breach) * 100 >= 40 THEN 'Add index on heavy filters, reduce join complexity, tune slow endpoints first'
        WHEN AVG(is_sla_breach) * 100 >= 20 THEN 'Review top slow endpoints, optimize query plans, add targeted caching'
        ELSE 'Continue monitoring and keep current tuning baseline'
    END AS recommended_solution,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct
FROM vw_system_logs_clean;

/*-- Alert severity classification--*/
SELECT
    CASE
        WHEN exec_sec > 5 THEN 'HIGH'
        WHEN exec_sec >= 3 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END AS alert_severity,
    COUNT(*) AS request_count,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(MAX(exec_sec), 3) AS max_execution_time_sec
FROM vw_system_logs_clean
GROUP BY
    CASE
        WHEN exec_sec > 5 THEN 'HIGH'
        WHEN exec_sec >= 3 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END
ORDER BY request_count DESC;

/*-- High-error endpoints--*/
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    CASE
        WHEN AVG(is_error) * 100 >= 10 THEN 'Error handling issue'
        WHEN AVG(exec_sec) >= 0.60 THEN 'Latency + reliability issue'
        ELSE 'Stable error profile'
    END AS root_cause,
    CASE
        WHEN AVG(is_error) * 100 >= 10 THEN 'Inspect app logs, retries, and upstream dependency failures'
        WHEN AVG(exec_sec) >= 0.60 THEN 'Optimize endpoint queries and add timeout-safe fallback'
        ELSE 'No urgent fix needed'
    END AS recommended_fix
FROM vw_system_logs_clean
GROUP BY endpoint
HAVING COUNT(*) >= 10
    AND (AVG(is_error) * 100 >= 5 OR AVG(exec_sec) >= 0.60)
ORDER BY error_rate_pct DESC, avg_execution_time_sec DESC
LIMIT 20;

/*-- Endpoint-level action matrix--*/
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(MAX(exec_sec), 3) AS max_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    ROUND(AVG(is_sla_breach) * 100, 2) AS sla_breach_rate_pct,
    CASE
        WHEN AVG(is_error) * 100 >= 8 THEN 'High error endpoint'
        WHEN AVG(is_sla_breach) * 100 >= 35 THEN 'High SLA breach endpoint'
        WHEN AVG(exec_sec) >= 0.60 THEN 'Slow endpoint'
        ELSE 'Endpoint stable'
    END AS problem_statement,
    CASE
        WHEN AVG(is_error) * 100 >= 8 OR AVG(is_sla_breach) * 100 >= 35 THEN 'HIGH'
        WHEN AVG(exec_sec) >= 0.45 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS impact_level,
    CASE
        WHEN AVG(is_error) * 100 >= 8 THEN 'Check app logs and retries; review error-prone code paths'
        WHEN AVG(is_sla_breach) * 100 >= 35 THEN 'Add index on endpoint/time filters; reduce expensive joins and scans'
        WHEN AVG(exec_sec) >= 0.60 THEN 'Optimize SQL and API logic; add caching for hot endpoint'
        ELSE 'No immediate action needed'
    END AS recommended_solution
FROM vw_system_logs_clean
GROUP BY endpoint
HAVING COUNT(*) >= 10
ORDER BY impact_level DESC, avg_execution_time_sec DESC, total_requests DESC;

/*-- Hour-level bottleneck guidance--*/

SELECT
    HOUR(`timestamp`) AS hour_of_day,
    COUNT(*) AS total_requests,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    CASE
        WHEN COUNT(*) >= 1000 AND AVG(exec_sec) >= 0.50 THEN 'Peak-hour overload'
        WHEN AVG(is_error) * 100 >= 8 THEN 'Error spike hour'
        ELSE 'Normal hour'
    END AS problem_statement,
    CASE
        WHEN COUNT(*) >= 1000 AND AVG(exec_sec) >= 0.50 THEN 'HIGH'
        WHEN AVG(is_error) * 100 >= 8 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS impact_level,
    CASE
        WHEN COUNT(*) >= 1000 AND AVG(exec_sec) >= 0.50 THEN 'Scale resources in this hour window; pre-warm cache and optimize heavy queries'
        WHEN AVG(is_error) * 100 >= 8 THEN 'Inspect deployment/incident logs for this hour and stabilize failing dependencies'
        ELSE 'No special hour-level action required'
    END AS recommended_solution
FROM vw_system_logs_clean
GROUP BY HOUR(`timestamp`)
ORDER BY impact_level DESC, total_requests DESC;

/*-- Heavy minute buckets for immediate action--*/
SELECT
    minute_bucket,
    COUNT(*) AS requests_per_minute,
    ROUND(AVG(exec_sec), 3) AS avg_execution_time_sec,
    ROUND(AVG(is_error) * 100, 2) AS error_rate_pct,
    CASE
        WHEN COUNT(*) >= 50 AND AVG(exec_sec) >= 0.60 THEN 'Traffic burst causing latency'
        WHEN AVG(is_error) * 100 >= 8 THEN 'Error spike in minute bucket'
        ELSE 'Normal minute bucket'
    END AS root_cause,
    CASE
        WHEN COUNT(*) >= 50 AND AVG(exec_sec) >= 0.60 THEN 'Scale resources and cache hot endpoints for this time window'
        WHEN AVG(is_error) * 100 >= 8 THEN 'Investigate deployment logs and dependency health for this period'
        ELSE 'No immediate fix needed'
    END AS recommended_fix
FROM vw_system_logs_clean
GROUP BY minute_bucket
HAVING COUNT(*) >= 20
ORDER BY minute_bucket DESC
LIMIT 120;


/*-- Query-complexity diagnostics--*/
SELECT
    endpoint,
    COUNT(*) AS sampled_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_execution_time_sec,
    ROUND(AVG(COALESCE(rows_scanned, 0)), 2) AS avg_rows_scanned,
    ROUND(AVG(COALESCE(joins_count, 0)), 2) AS avg_joins_count,
    CASE
        WHEN AVG(COALESCE(rows_scanned, 0)) >= 5000 THEN 'High table scan pressure'
        WHEN AVG(COALESCE(joins_count, 0)) >= 5 THEN 'Join-heavy query pattern'
        ELSE 'Query complexity acceptable'
    END AS problem_statement,
    CASE
        WHEN AVG(COALESCE(rows_scanned, 0)) >= 5000 OR AVG(COALESCE(joins_count, 0)) >= 5 THEN 'HIGH'
        WHEN AVG(COALESCE(rows_scanned, 0)) >= 2000 OR AVG(COALESCE(joins_count, 0)) >= 3 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS impact_level,
    CASE
        WHEN AVG(COALESCE(rows_scanned, 0)) >= 5000 THEN 'Add selective indexes, avoid SELECT *, and tighten WHERE filters'
        WHEN AVG(COALESCE(joins_count, 0)) >= 5 THEN 'Reduce joins, denormalize hot paths, or pre-aggregate intermediate results'
        ELSE 'No complexity action needed now'
    END AS recommended_solution
FROM system_logs
GROUP BY endpoint
HAVING COUNT(*) >= 10
ORDER BY impact_level DESC, avg_rows_scanned DESC, avg_joins_count DESC;

/*-- Root-cause analysis by endpoint--*/
SELECT
    endpoint,
    COUNT(*) AS total_requests,
    ROUND(AVG(execution_time) / 1000.0, 3) AS avg_execution_time_sec,
    ROUND(AVG(COALESCE(rows_scanned, 0)), 2) AS avg_rows_scanned,
    ROUND(AVG(COALESCE(joins_count, 0)), 2) AS avg_joins_count,
    CASE
        WHEN AVG(execution_time) / 1000.0 > 5 THEN 'Slow query'
        WHEN AVG(COALESCE(rows_scanned, 0)) >= 5000 THEN 'High load'
        WHEN AVG(COALESCE(joins_count, 0)) > 2 THEN 'Complex query'
        ELSE 'No critical issue'
    END AS problem,
    CASE
        WHEN AVG(execution_time) / 1000.0 > 5 THEN 'High execution time'
        WHEN AVG(COALESCE(rows_scanned, 0)) >= 5000 THEN 'rows_scanned high'
        WHEN AVG(COALESCE(joins_count, 0)) > 2 THEN 'joins_count > 2'
        ELSE 'Stable behavior'
    END AS cause,
    CASE
        WHEN AVG(COALESCE(rows_scanned, 0)) >= 5000 THEN 'Create selective indexes on endpoint/timestamp/status filters'
        WHEN AVG(COALESCE(joins_count, 0)) > 2 THEN 'Simplify join path and pre-aggregate heavy transformations'
        WHEN AVG(execution_time) / 1000.0 > 5 THEN 'Refactor expensive SQL and optimize endpoint logic'
        ELSE 'Continue monitoring baseline behavior'
    END AS recommended_solution
FROM system_logs
GROUP BY endpoint
HAVING COUNT(*) >= 10
   AND (
       AVG(execution_time) / 1000.0 > 3
       OR AVG(COALESCE(rows_scanned, 0)) >= 2000
       OR AVG(COALESCE(joins_count, 0)) > 2
   )
ORDER BY avg_execution_time_sec DESC, avg_rows_scanned DESC, avg_joins_count DESC;

/*-- Rejection reason to action map--*/
SELECT
    reason,
    COUNT(*) AS rejected_count,
    ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM rejected_logs), 0), 2) AS rejected_share_pct,
    CASE
        WHEN COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM rejected_logs), 0) >= 30 THEN 'HIGH'
        WHEN COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM rejected_logs), 0) >= 15 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS impact_level,
    CASE
        WHEN LOWER(reason) LIKE '%status%' THEN 'Fix status mapping and add strict API contract validation'
        WHEN LOWER(reason) LIKE '%execution%' THEN 'Clamp invalid latency values and validate execution_time at source'
        WHEN LOWER(reason) LIKE '%endpoint%' THEN 'Validate endpoint format and block blanks before insert'
        ELSE 'Review rejection sample payloads and add source-side validation rules'
    END AS recommended_solution
FROM rejected_logs
GROUP BY reason
ORDER BY rejected_count DESC;

/*-- ETL delay diagnostics--*/
SELECT
    MAX(load_time) AS last_etl_run_time,
    TIMESTAMPDIFF(MINUTE, MAX(load_time), NOW()) AS etl_delay_minutes,
    CASE
        WHEN MAX(load_time) IS NULL THEN 'NO_RUN_HISTORY'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(load_time), NOW()) <= 15 THEN 'HEALTHY'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(load_time), NOW()) <= 60 THEN 'DELAYED'
        ELSE 'CRITICAL_DELAY'
    END AS pipeline_status,
    CASE
        WHEN MAX(load_time) IS NULL THEN 'Run simulator/ETL to generate fresh data'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(load_time), NOW()) <= 15 THEN 'No action needed'
        WHEN TIMESTAMPDIFF(MINUTE, MAX(load_time), NOW()) <= 60 THEN 'Check ingestion scheduler and DB write backlog'
        ELSE 'Escalate incident, restart ingestion job, and validate source availability'
    END AS recommended_fix
FROM etl_metrics;
