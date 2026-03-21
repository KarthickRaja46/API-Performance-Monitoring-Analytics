USE performance_monitoring;

-- =============================================================================
-- FULL DATA RESET (KEEPS TABLE STRUCTURE)
-- =============================================================================
-- Use when you want a completely fresh dataset.

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE alerts;
TRUNCATE TABLE alert_threshold_config;
TRUNCATE TABLE rejected_logs;
TRUNCATE TABLE system_logs_archive;
TRUNCATE TABLE system_logs;
TRUNCATE TABLE etl_metrics;

SET FOREIGN_KEY_CHECKS = 1;
