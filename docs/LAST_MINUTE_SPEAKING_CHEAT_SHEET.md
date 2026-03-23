# Last-Minute Speaking Cheat Sheet (1 Page)

## 1) 30-Second Project Intro
This project is an end-to-end API observability pipeline using Python, MySQL, SQL analytics, and Power BI.
It simulates live logs, validates data quality, stores clean and rejected records separately, and produces KPI plus advanced performance insights.
Goal: monitor reliability, latency, SLA compliance, and ETL quality in one workflow.

## 2) Speak This Flow (Core Story)
1. Generate logs in scripts/log_simulator.py.
2. Save all raw records to data/raw/log.csv.
3. Validate each record (status, execution_time, rows_scanned).
4. Send valid rows to system_logs and data/processed/cleaned_logs.csv.
5. Send invalid rows to rejected_logs and data/processed/rejected_logs.csv with reason.
6. Update etl_metrics (total, inserted, rejected, load_time).
7. Run SQL analytics and show Power BI results.

## 3) Tables in One Line Each
- system_logs: clean fact table for all performance analytics.
- rejected_logs: invalid rows with reason and raw payload for audit.
- etl_metrics: ETL run-level quality and freshness tracking.
- system_logs_archive: old data retention support.

## 4) KPI Lines to Say on Dashboard Slide
- Total Requests tells overall workload.
- Success Rate tells reliability.
- Error Rate shows server failures.
- Avg Latency shows response speed.
- SLA Breach Rate shows % requests above threshold (0.5 sec).

Current sample values:
- Total Requests: 19.97K
- Success Rate: 79.27%
- Error Rate: 10.07%
- Avg Latency: 0.494 sec
- SLA Breach Rate: 48.43%

## 5) Interpretation Lines (Fast)
- Traffic is healthy in volume, but bursty by minute.
- Reliability is moderate, not excellent yet.
- SLA breach is high, so latency optimization is priority one.
- Endpoint-level risk and P95/P99 guide where to optimize first.

## 6) Must-Mention Advanced Points
- Average latency is not enough; P95/P99 expose tail pain.
- Rejected logs are not deleted; they are audited for governance.
- Defense-in-depth quality: Python validation + DB constraints.
- Orphan check ensures rejected rows map to valid ETL run.

## 7) 10-Second Formula Line
Health Score = 50% success + 30% inverse error + 20% inverse SLA breach.

## 8) Top 8 Viva Questions with Ready Answers
1. Why simulation?  
Controlled, repeatable testing without production data risk.

2. Why rejected table?  
For auditability, root-cause analysis, and quality improvement.

3. Why P95/P99?  
They capture tail latency that averages hide.

4. Why both app and DB validation?  
To catch errors early and still guarantee storage integrity.

5. Why etl_run_id?  
It gives run-level lineage across clean and rejected records.

6. Why indexes?  
To speed common filters and aggregations on time, endpoint, and status.

7. Biggest issue from BI?  
High SLA breach rate, so performance tuning is top priority.

8. Next production steps?  
Scheduling, alerts, partitioning, RBAC, CI query checks.

## 9) 45-Second Closing Line
This project demonstrates a complete data engineering and analytics cycle: synthetic ingestion, quality enforcement, SQL intelligence, and BI storytelling.
It is technically strong because it combines performance monitoring with data governance and actionable optimization priorities.
