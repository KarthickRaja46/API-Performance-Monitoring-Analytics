-- =============================================================================
-- DATABASE
-- =============================================================================
CREATE DATABASE IF NOT EXISTS performance_monitoring;
USE performance_monitoring;

-- =============================================================================
-- 1. SYSTEM LOGS
-- =============================================================================
CREATE TABLE system_logs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    ip VARCHAR(45) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    status SMALLINT NOT NULL,
    `timestamp` DATETIME(3) NOT NULL,
    execution_time INT NOT NULL,
    rows_scanned INT NULL,
    joins_count INT NULL,
    etl_run_id VARCHAR(36) NULL,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

    CHECK (execution_time >= 0),
    CHECK (rows_scanned IS NULL OR rows_scanned >= 0),
    CHECK (joins_count IS NULL OR joins_count >= 0),
    CHECK (status IN (200, 404, 500))
);

-- Indexes for system_logs
CREATE INDEX idx_timestamp ON system_logs(`timestamp`);
CREATE INDEX idx_endpoint ON system_logs(endpoint);
CREATE INDEX idx_status ON system_logs(status);
CREATE INDEX idx_logs_etl_run ON system_logs(etl_run_id);
CREATE INDEX idx_logs_endpoint_timestamp ON system_logs(endpoint, `timestamp`);


-- =============================================================================
-- 2. ETL METRICS
-- =============================================================================
CREATE TABLE etl_metrics (
    run_id VARCHAR(36) NOT NULL,
    source_type VARCHAR(20) NOT NULL,
    total_rows INT NOT NULL,
    inserted_rows INT NOT NULL,
    rejected_rows INT NOT NULL,
    load_time DATETIME(3) NOT NULL,
    notes VARCHAR(500),

    PRIMARY KEY (run_id),

    CHECK (total_rows >= 0),
    CHECK (inserted_rows >= 0),
    CHECK (rejected_rows >= 0),
    CHECK (source_type IN ('csv', 'api', 'batch', 'manual'))
);

-- Indexes for etl_metrics
CREATE INDEX idx_etl_load_time ON etl_metrics(load_time);


-- =============================================================================
-- 3. REJECTED LOGS
-- =============================================================================
CREATE TABLE rejected_logs (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    etl_run_id VARCHAR(36) NOT NULL,
    source_type VARCHAR(20) NOT NULL,
    line_number INT NOT NULL,
    reason VARCHAR(100) NOT NULL,
    raw_payload JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id),

    CONSTRAINT fk_rejected_logs_run
    FOREIGN KEY (etl_run_id) REFERENCES etl_metrics(run_id)
);

-- Indexes for rejected_logs
CREATE INDEX idx_rejected_reason ON rejected_logs(reason);
CREATE INDEX idx_rejected_etl_run ON rejected_logs(etl_run_id);


-- =============================================================================
-- 4. SYSTEM LOGS ARCHIVE
-- =============================================================================
CREATE TABLE system_logs_archive (
    id BIGINT UNSIGNED NOT NULL,
    ip VARCHAR(45) NOT NULL,
    endpoint VARCHAR(255) NOT NULL,
    status SMALLINT NOT NULL,
    `timestamp` DATETIME(3) NOT NULL,
    execution_time INT NOT NULL,
    rows_scanned INT NULL,
    joins_count INT NULL,
    etl_run_id VARCHAR(36) NULL,
    ingested_at TIMESTAMP NOT NULL,
    archived_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id)
);

-- Indexes for archive
CREATE INDEX idx_archive_timestamp ON system_logs_archive(`timestamp`);


-- =============================================================================
-- 5. ALERT CONFIGURATION
-- =============================================================================
CREATE TABLE alert_threshold_config (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    threshold_value DECIMAL(10,2) NOT NULL,
    comparison_operator VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =============================================================================
-- 6. ALERTS TABLE
-- =============================================================================
CREATE TABLE alerts (
    alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    endpoint VARCHAR(255),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    severity VARCHAR(20),
    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for alerts
CREATE INDEX idx_alerts_severity ON alerts(severity);


-- =============================================================================
-- ARCHIVE PROCEDURE
-- =============================================================================
DROP PROCEDURE IF EXISTS sp_archive_old_system_logs;

DELIMITER $$

CREATE PROCEDURE sp_archive_old_system_logs()
BEGIN
    INSERT IGNORE INTO system_logs_archive (
        id, ip, endpoint, status, `timestamp`,
        execution_time, rows_scanned, joins_count,
        etl_run_id, ingested_at
    )
    SELECT 
        id, ip, endpoint, status, `timestamp`,
        execution_time, rows_scanned, joins_count,
        etl_run_id, ingested_at
    FROM system_logs
    WHERE `timestamp` < NOW() - INTERVAL 90 DAY;

    DELETE FROM system_logs
    WHERE `timestamp` < NOW() - INTERVAL 90 DAY;
END $$

DELIMITER ;