USE performance_monitoring;

-- =============================================================================
-- REUSABLE VIEW LAYER
-- =============================================================================

-- Centralized cleaned log view used by all analytics files.
CREATE OR REPLACE VIEW vw_system_logs_clean AS
SELECT
    id,
    endpoint,
    status,
    `timestamp`,
    DATE(`timestamp`) AS request_date,
    DATE_FORMAT(`timestamp`, '%Y-%m-%d %H:%i:00') AS minute_bucket,
    execution_time / 1000.0 AS exec_sec,
    CASE WHEN status = 200 THEN 1 ELSE 0 END AS is_success,
    CASE WHEN status = 500 THEN 1 ELSE 0 END AS is_error,
    CASE WHEN status = 404 THEN 1 ELSE 0 END AS is_not_found,
    CASE WHEN execution_time > 500 THEN 1 ELSE 0 END AS is_sla_breach
FROM system_logs;
