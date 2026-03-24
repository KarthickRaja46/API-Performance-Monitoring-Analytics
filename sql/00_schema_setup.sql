CREATE DATABASE performance_monitoring;
USE performance_monitoring;

-- SYSTEM LOGS TABLE
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

CREATE INDEX idx_timestamp ON system_logs(`timestamp`);
CREATE INDEX idx_endpoint ON system_logs(endpoint);
CREATE INDEX idx_status ON system_logs(status);
CREATE INDEX idx_logs_etl_run ON system_logs(etl_run_id);
CREATE INDEX idx_logs_endpoint_timestamp ON system_logs(endpoint, `timestamp`);

-- ETL METRICS TABLE
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

CREATE INDEX idx_etl_load_time ON etl_metrics(load_time);

-- REJECTED LOGS TABLE
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

CREATE INDEX idx_rejected_reason ON rejected_logs(reason);
CREATE INDEX idx_rejected_etl_run ON rejected_logs(etl_run_id);

-- ARCHIVE TABLE
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

CREATE INDEX idx_archive_timestamp ON system_logs_archive(`timestamp`);

-- ALERT CONFIG TABLE
CREATE TABLE alert_threshold_config (
    config_id INT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    threshold_value DECIMAL(10,2) NOT NULL,
    comparison_operator VARCHAR(10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- ALERTS TABLE
CREATE TABLE alerts (
    alert_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    endpoint VARCHAR(255),
    metric_name VARCHAR(100),
    metric_value DECIMAL(10,2),
    severity VARCHAR(20),
    alert_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_alerts_severity ON alerts(severity);


-- ARCHIVE PROCEDURE
DROP PROCEDURE IF EXISTS sp_archive_old_system_logs;

DELIMITER $$

CREATE PROCEDURE sp_archive_old_system_logs()
BEGIN
    DECLARE v_archived_rows BIGINT DEFAULT 0;
    DECLARE v_deleted_rows BIGINT DEFAULT 0;

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

    SET v_archived_rows = ROW_COUNT();

    DELETE FROM system_logs
    WHERE `timestamp` < NOW() - INTERVAL 90 DAY;

    SET v_deleted_rows = ROW_COUNT();

    SELECT
        'archive_completed' AS status,
        v_archived_rows AS archived_rows,
        v_deleted_rows AS deleted_rows,
        NOW() AS run_time;
END $$

DELIMITER ;

-- SYSTEM HEALTH CHECK PROCEDURE
DROP PROCEDURE IF EXISTS sp_system_health_check;

DELIMITER $$

CREATE PROCEDURE sp_system_health_check()
BEGIN
    DECLARE v_total_requests BIGINT DEFAULT 0;
    DECLARE v_error_rate_pct DECIMAL(10,2) DEFAULT 0.00;
    DECLARE v_avg_execution_time_sec DECIMAL(10,3) DEFAULT 0.000;
    DECLARE v_sla_breach_rate_pct DECIMAL(10,2) DEFAULT 0.00;
    DECLARE v_alerts_inserted INT DEFAULT 0;

    SELECT
        COUNT(*) AS total_requests,
        COALESCE(ROUND(AVG(is_error) * 100, 2), 0),
        COALESCE(ROUND(AVG(exec_sec), 3), 0),
        COALESCE(ROUND(AVG(is_sla_breach) * 100, 2), 0)
    INTO
        v_total_requests,
        v_error_rate_pct,
        v_avg_execution_time_sec,
        v_sla_breach_rate_pct
    FROM vw_system_logs_clean
    WHERE `timestamp` >= NOW() - INTERVAL 60 MINUTE;

    IF v_total_requests = 0 THEN
        INSERT INTO alerts (endpoint, metric_name, metric_value, severity)
        SELECT NULL, 'no_traffic_last_60m', 0, 'INFO'
        WHERE NOT EXISTS (
            SELECT 1
            FROM alerts
            WHERE metric_name = 'no_traffic_last_60m'
              AND alert_time >= NOW() - INTERVAL 10 MINUTE
        );
        SET v_alerts_inserted = v_alerts_inserted + ROW_COUNT();
    ELSE
        IF v_error_rate_pct > 5 THEN
            INSERT INTO alerts (endpoint, metric_name, metric_value, severity)
            SELECT NULL, 'error_rate_pct', v_error_rate_pct, 'HIGH'
            WHERE NOT EXISTS (
                SELECT 1
                FROM alerts
                WHERE metric_name = 'error_rate_pct'
                  AND alert_time >= NOW() - INTERVAL 10 MINUTE
            );
            SET v_alerts_inserted = v_alerts_inserted + ROW_COUNT();
        END IF;

        IF v_avg_execution_time_sec > 1 THEN
            INSERT INTO alerts (endpoint, metric_name, metric_value, severity)
            SELECT NULL, 'avg_execution_time_sec', v_avg_execution_time_sec, 'HIGH'
            WHERE NOT EXISTS (
                SELECT 1
                FROM alerts
                WHERE metric_name = 'avg_execution_time_sec'
                  AND alert_time >= NOW() - INTERVAL 10 MINUTE
            );
            SET v_alerts_inserted = v_alerts_inserted + ROW_COUNT();
        END IF;

        IF v_sla_breach_rate_pct > 10 THEN
            INSERT INTO alerts (endpoint, metric_name, metric_value, severity)
            SELECT NULL, 'sla_breach_rate_pct', v_sla_breach_rate_pct, 'MEDIUM'
            WHERE NOT EXISTS (
                SELECT 1
                FROM alerts
                WHERE metric_name = 'sla_breach_rate_pct'
                  AND alert_time >= NOW() - INTERVAL 10 MINUTE
            );
            SET v_alerts_inserted = v_alerts_inserted + ROW_COUNT();
        END IF;
    END IF;

    SELECT
        'health_check_completed' AS status,
        v_total_requests AS total_requests_60m,
        v_error_rate_pct AS error_rate_pct,
        v_avg_execution_time_sec AS avg_execution_time_sec,
        v_sla_breach_rate_pct AS sla_breach_rate_pct,
        v_alerts_inserted AS alerts_inserted,
        NOW() AS run_time;
END $$

DELIMITER ;