USE performance_monitoring;

SET FOREIGN_KEY_CHECKS = 0;

TRUNCATE TABLE alerts;
TRUNCATE TABLE alert_threshold_config;
TRUNCATE TABLE rejected_logs;
TRUNCATE TABLE system_logs_archive;
TRUNCATE TABLE system_logs;
TRUNCATE TABLE etl_metrics;

SET FOREIGN_KEY_CHECKS = 1;

SET GLOBAL local_infile = 1 ;
SHOW GLOBAL VARIABLES LIKE 'local_infile';

LOAD DATA LOCAL INFILE 'D:/SQL_PROJECT/data/processed/cleaned_logs.csv'
INTO TABLE system_logs
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ip, endpoint, status, @ts, execution_time, @rows_scanned, @joins_count)
SET
    `timestamp` = STR_TO_DATE(@ts, '%Y-%m-%d %H:%i:%s.%f'),
    rows_scanned = NULLIF(@rows_scanned, ''),
    joins_count = NULLIF(@joins_count, ''),
    etl_run_id = NULL;

SET GLOBAL local_infile = 0 ;

