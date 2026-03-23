# SQL Project Full Preparation Brief

## 1) Project Goal in One Line
Build a mini observability pipeline that simulates API logs, validates data quality, stores clean and rejected records separately, and produces analytics/KPIs for performance monitoring.

## 2) End-to-End Flow (Raw Log to Final Insight)
1. Log simulation starts in `scripts/log_simulator.py`.
2. Synthetic API events are generated with fields like endpoint, status, timestamp, and execution_time.
3. Every generated event is first buffered for raw CSV output to `data/raw/log.csv`.
4. Validation rules are applied:
   - execution_time must be >= 0
   - status must be one of 200, 404, 500
   - rows_scanned must be NULL or >= 0
5. Valid rows:
   - inserted into `system_logs`
   - appended to `data/processed/cleaned_logs.csv`
6. Invalid rows:
   - inserted into `rejected_logs` with reason and raw JSON payload
   - appended to `data/processed/rejected_logs.csv`
7. ETL counters are upserted in `etl_metrics` (total_rows, inserted_rows, rejected_rows, load_time).
8. SQL analytics scripts read from these tables and generate KPI outputs used by dashboard visuals.

## 3) Source and Destination Layers
- Raw generated layer: `data/raw/log.csv`
- Clean validated layer: `data/processed/cleaned_logs.csv` and DB table `system_logs`
- Rejection layer: `data/processed/rejected_logs.csv` and DB table `rejected_logs`
- ETL audit layer: DB table `etl_metrics`
- Alerting support layer: `alerts`, `alert_threshold_config`
- Historical layer: `system_logs_archive`

## 4) Database Objects and Purpose
- `system_logs`: main fact table for request analytics
- `etl_metrics`: ETL run-level quality summary
- `rejected_logs`: bad records and reasons (data quality governance)
- `system_logs_archive`: older data storage target (90+ day archive procedure)
- `alerts`: generated alert records
- `alert_threshold_config`: configurable threshold definitions

## 5) Main Columns You Should Explain
In `system_logs`:
- id: surrogate key
- ip: request client IP
- endpoint: API path (/login, /search, etc.)
- status: 200, 404, 500
- timestamp: event time
- execution_time: latency in milliseconds
- rows_scanned, joins_count: complexity hints
- etl_run_id: link back to ETL run
- ingested_at: insertion time

In `rejected_logs`:
- etl_run_id, source_type, line_number
- reason (why validation failed)
- raw_payload (original bad row JSON)

In `etl_metrics`:
- run_id
- total_rows, inserted_rows, rejected_rows
- load_time

## 6) Simulator Behavior (What to Say in Demo)
- Traffic is generated in bursts (1 to 5 events each cycle).
- Endpoint is randomly chosen from 5 endpoints.
- Statuses are generated with weighted probabilities and occasional invalid status (999) for rejection testing.
- execution_time is generated from normal distribution and intentionally includes some invalid negatives.
- rows_scanned sometimes becomes negative intentionally to test rejection rules.
- Database inserts are batched for performance.
- CSV writes are batched to reduce I/O overhead.
- Keyboard interrupt performs clean flush and final ETL upsert.

## 7) SQL File-by-File: What Query Is What

### A) `sql/schema.sql`
Purpose: Create database, tables, constraints, indexes, and archive procedure.

What to highlight:
- CHECK constraints enforce data validity at DB level too.
- Indexes on timestamp, endpoint, status, etl_run_id improve analytics speed.
- Procedure `sp_archive_old_system_logs()` moves data older than 90 days to archive table.

### B) `sql/reset.sql`
Purpose: Reset all analytical tables and optionally reload clean CSV into `system_logs`.

What to highlight:
- Disables FK checks for truncate safety.
- Truncates alerts, configs, rejected, archive, system logs, ETL metrics.
- Uses `LOAD DATA LOCAL INFILE` to bulk load cleaned logs CSV.

### C) `sql/basic.sql` (16 practical queries)
1. Total Requests -> "How many total API calls did we handle?"
2. Success Rate % -> "What percent returned HTTP 200?"
3. Error Rate % -> "What percent returned HTTP 500?"
4. Not Found Rate % -> "What percent returned HTTP 404?"
5. Average Response Time (sec) -> "How fast is system on average?"
6. Slow Requests (>1s) -> "How many requests are user-visible slow?"
7. Daily Average Latency -> "How latency changes day by day?"
8. Requests Per Minute -> "Traffic intensity each minute"
9. Peak Minute Traffic -> "Most loaded minute"
10. Hourly Error Rate -> "At which hour failures rise?"
11. Daily Status Trend -> "Daily volume split by status"
12. Top Endpoints -> "Most used APIs"
13. Top Slow Endpoints -> "Endpoints with slow behavior"
14. Endpoint Latency Summary -> "Avg and max latency by endpoint"
15. Endpoint Error Rate -> "Which endpoint fails more"
16. SLA Breach Rate (>0.5s) -> "Endpoint-wise SLA non-compliance"

### D) `sql/advanced.sql` (8 advanced queries)
1. Latency Distribution -> bucket as Excellent/Good/Moderate/Slow
2. Global P95 Latency -> tail performance indicator
3. Global P99 Latency -> extreme tail latency
4. Endpoint P95 -> per-endpoint tail latency
5. Endpoint P99 -> per-endpoint worst tail
6. Endpoint Risk Mix -> combined error + SLA profile by endpoint
7. Rejected Distribution -> data quality issue share by reason
8. Traffic + Error by Hour -> hour-wise traffic, errors, error rate, avg latency

### E) `sql/kpi.sql` (4 KPI/quality queries)
1. ETL Inserted vs Rejected % -> pipeline quality
2. Rejected Data by Reason -> dominant data issue
3. ETL Freshness -> staleness in minutes since last load
4. Overall System Health Score -> weighted health metric

Health score formula:
- 50% success contribution
- 30% inverse error contribution
- 20% inverse SLA-breach contribution

### F) `sql/dashboard.sql`
Purpose: Final dashboard-ready output script (summary, trend, endpoint).

What to highlight:
- Exactly three dashboard feeds for BI use.
- Uses centralized cleaned view instead of repeated CASE logic.
- Keeps dashboard consumption layer isolated from advanced exploration.

## 8) Dashboard Mapping (Visual to Query Logic)
For a dashboard like your screenshot:
- Card: Total Requests -> COUNT(*) from `system_logs`
- Card: Success Rate % -> status=200 share
- Card: Error Rate % -> status=500 share
- Card: Avg Latency Sec -> AVG(execution_time)/1000
- Card: SLA Breach Rate % -> execution_time/1000 > 0.5 share
- Chart: Total Requests by Hour -> GROUP BY HOUR(timestamp)
- Chart: Total Requests by Minute -> GROUP BY minute bucket
- Chart: Requests by Endpoint -> GROUP BY endpoint
- Stacked Chart: Endpoint Status Mix -> endpoint x status distribution
- Slicers: endpoint, status

## 9) Why Rejected Logs Matter (Interview Point)
Rejected logs prove data governance maturity:
- You do not silently drop bad data.
- You preserve root cause (`reason`) and original payload.
- You can measure ETL quality over time.
- You can improve upstream systems using rejection patterns.

## 10) Typical Presentation Story (2-3 Minutes)
1. Start with objective: monitor API performance and ETL quality.
2. Explain pipeline: generator -> validation -> clean/rejected split -> ETL metrics.
3. Explain schema and constraints: valid data protected at app and DB level.
4. Show KPI cards: volume, reliability, speed, SLA.
5. Drill down by endpoint and hour to show bottlenecks.
6. Show advanced metrics: P95/P99 and risk scoring.
7. End with governance: rejected reason analysis + ETL freshness + health score.

## 11) Common Viva/Interview Questions and Short Answers
Q1. Why store execution_time in milliseconds?
A: Higher precision and easier threshold conversion; analytics outputs convert to seconds for readability.

Q2. Why separate rejected data table?
A: For auditability, debugging, and quality trend tracking without polluting trusted analytics table.

Q3. Why use both app validation and DB constraints?
A: Defense in depth. App catches most issues early; DB guarantees integrity if app logic misses something.

Q4. Why P95/P99 instead of only average?
A: Average hides long-tail latency. P95/P99 reveal worst user experience segments.

Q5. What is etl_run_id used for?
A: Traceability: link each accepted/rejected row to a specific ingestion run.

## 12) Final Pre-Submission Checklist
- Run schema script successfully.
- Run simulator and generate enough data.
- Verify `system_logs`, `rejected_logs`, `etl_metrics` all have rows.
- Run basic, advanced, and KPI analytics scripts with no errors.
- Ensure latency units are consistently shown in seconds in outputs.
- Verify dashboard cards match SQL results.
- Keep one clean screenshot and one short explanation per visual.

## 13) One-Line Conclusion
This project demonstrates full-cycle data engineering and analytics: synthetic generation, quality validation, ETL observability, SQL KPI design, and dashboard-ready operational insights.

## 14) 3-Minute Speaking Script (Ready to Present)
Hello everyone. This project is a database performance insights system built using Python and MySQL. The goal is to monitor API behavior and ETL quality from end to end.

The pipeline starts with a real-time log simulator. It generates realistic API traffic fields like endpoint, status code, timestamp, and execution time. All generated records first go into a raw CSV layer, then validation rules are applied.

Valid records are inserted into system_logs and also written to data/processed/cleaned_logs.csv. Invalid records are not discarded. They are captured in rejected_logs with a rejection reason and raw payload. This gives traceability and data governance.

At the same time, ETL run metrics are tracked in etl_metrics, including total rows, inserted rows, rejected rows, and load time. So we can measure both system performance and pipeline health.

For analytics, I divided SQL into basic, advanced, and KPI scripts. Basic analytics includes total requests, success rate, error rate, not-found rate, average latency, endpoint trends, and SLA breach rate. Advanced analytics adds P95 and P99 latency, risk mix by endpoint, rejected-data distribution, and traffic plus error behavior by hour. KPI analytics focuses on ETL quality, freshness, and a weighted system health score.

On the dashboard, top KPI cards summarize volume, reliability, speed, and SLA compliance. Then charts provide drill-down by hour, minute, endpoint, and status mix. This helps identify bottlenecks quickly.

Overall, the project demonstrates full-cycle implementation: data simulation, validation, quality control, SQL analytics, and dashboard-ready insights for operational monitoring.

## 15) One-Page Quick Revision Sheet

### A) Pipeline in 5 Steps
1. Generate logs in scripts/log_simulator.py
2. Validate each row
3. Store valid rows in system_logs
4. Store invalid rows in rejected_logs
5. Run SQL analytics for dashboard/KPI outputs

### B) 5 KPI Cards You Must Remember
1. Total Requests
2. Success Rate %
3. Error Rate %
4. Avg Latency Sec
5. SLA Breach Rate %

### C) Key Thresholds
- SLA threshold = 0.5 sec
- Slow request example threshold = 1.0 sec
- Tail latency focus = P95, P99

### D) Health Score Weights
- 50% Success rate
- 30% Inverse error rate
- 20% Inverse SLA breach rate

### E) 10-Second Definitions
- P95 latency: latency below which 95% of requests complete
- P99 latency: latency below which 99% of requests complete
- Rejected logs: invalid rows captured with reason for quality auditing
- ETL freshness: minutes since latest load_time

### F) Most Important Interview Line
I used both application-level validation and database constraints for defense in depth, so bad data is blocked early and integrity is still guaranteed at storage time.

## 16) Dashboard Visual Narration (Line-by-Line)

### KPI Cards
- Total Requests: This card shows overall workload handled by the platform.
- Success Rate %: This indicates reliability by measuring HTTP 200 share.
- Error Rate %: This tracks server-side failures using HTTP 500 share.
- Avg Latency Sec: This reflects average user-perceived response speed.
- SLA Breach Rate %: This shows percentage of requests crossing 0.5 seconds.

### Total Requests by Hour
- This visual shows traffic distribution over hours.
- It helps identify peak operational load windows.

### Total Requests by Minute
- This gives minute-level traffic granularity.
- It helps detect short bursts and sudden spikes.

### Requests by Endpoint
- This compares endpoint demand volume.
- It helps prioritize optimization for highest-traffic APIs.

### Endpoint Status Mix (Stacked)
- This shows quality mix per endpoint across 200, 404, and 500 statuses.
- It identifies endpoints with relatively high failure proportions.

### Slicers (endpoint, status)
- These are interactive filters to isolate behavior by API and status.
- They support root-cause exploration during analysis.

## 17) 60-Second Backup Script (If Time Is Very Short)
This project simulates API logs and builds a complete monitoring pipeline. Logs are generated in real time, validated, and split into clean and rejected paths. Clean data is stored in system_logs for analytics, while bad rows are stored in rejected_logs with reasons for auditability. ETL metrics track insertion quality and freshness. SQL scripts are divided into basic, advanced, and KPI analytics, including latency, success and error rates, SLA breach, P95/P99 tail latency, and endpoint risk. The dashboard summarizes these as KPI cards and drill-down visuals by time and endpoint. The result is an end-to-end, production-style observability workflow.

## 18) How to Run the Project (Step-by-Step)

### A) Prerequisites
- Python 3.10+
- MySQL 8.0+ (running locally)
- Python packages: pandas, mysql-connector-python

Install dependencies:

```bash
pip install pandas mysql-connector-python
```

### B) Initialize Database
1. Open MySQL Workbench or MySQL CLI.
2. Run schema script:

```sql
source sql/schema.sql;
```

This creates all required tables, indexes, and the archive procedure.

### C) Start Log Ingestion Simulator
From project root:

```bash
python scripts/log_simulator.py
```

When prompted, enter MySQL password.

What happens while it runs:
- Generates synthetic logs continuously
- Writes raw rows to data/raw/log.csv
- Validates records
- Inserts valid rows into system_logs and data/processed/cleaned_logs.csv
- Inserts invalid rows into rejected_logs and data/processed/rejected_logs.csv
- Updates etl_metrics periodically

Stop safely with CTRL+C.

### D) Run Analytics Queries
Use MySQL client and execute in this order:
1. Optional reset: sql/reset.sql (run only when simulator is stopped)
2. Build reusable layer: sql/views.sql
3. Basic analytics: sql/basic.sql
4. KPI analytics: sql/kpi.sql
5. Advanced analytics: sql/advanced.sql

Alternative single run:
- sql/dashboard.sql

### E) Build/Refresh Dashboard
In your BI tool:
1. Connect to performance_monitoring database.
2. Load system_logs (and optionally rejected_logs, etl_metrics).
3. Create cards and visuals mapped in Section 8.
4. Add slicers: endpoint and status.
5. Cross-check card values with SQL results.

### F) Pre-Publish Validation
Run final checks using available scripts and quick SQL validations:

```sql
USE performance_monitoring;

-- Data presence checks
SELECT COUNT(*) AS system_logs_count FROM system_logs;
SELECT COUNT(*) AS etl_metrics_count FROM etl_metrics;

-- Rejected rows should always map to a valid ETL run (no orphans)
SELECT COUNT(*) AS orphan_rejected_rows
FROM rejected_logs r
LEFT JOIN etl_metrics e ON e.run_id = r.etl_run_id
WHERE e.run_id IS NULL;
```

Then execute:
- sql/basic.sql
- sql/advanced.sql
- sql/kpi.sql

Expected highlights:
- Scripts execute without errors
- orphan_rejected_rows = 0
- KPI cards match SQL outputs in dashboard

## 19) Expected Tricky Examiner Questions with Strong Best-Answer Lines

1. Why did you simulate data instead of using real production logs?
Best answer: This project focuses on architecture, validation, and analytics design. Simulation gave controlled variability, repeatability, and safe testing of error paths without exposing sensitive production data.

2. How do you ensure data quality is not compromised?
Best answer: I implemented two layers of protection: Python validation before insert and SQL constraints at table level, then audited failures in rejected_logs with reason and payload.

3. Why keep rejected records instead of deleting them?
Best answer: Rejected records are critical for governance and root-cause analysis. They let us quantify quality issues and continuously improve upstream producers.

4. Why is average latency not enough?
Best answer: Averages hide long-tail pain. P95 and P99 reveal worst-case user experience, which is often where incidents and SLA breaches occur.

5. Why use etl_run_id in both accepted and rejected flows?
Best answer: It gives end-to-end lineage. We can trace exactly which run produced which clean and rejected records, making audits and debugging much faster.

6. How do indexes help in this project?
Best answer: Most queries filter or aggregate by timestamp, endpoint, status, and run_id. Indexes on these columns reduce scan cost and improve dashboard refresh performance.

7. What is the logic behind your health score weighting?
Best answer: I prioritized reliability first (success 50%), then stability (inverse error 30%), then responsiveness/compliance (inverse SLA breach 20%), giving a balanced but business-focused score.

8. What happens if ETL table is truncated while simulator is running?
Best answer: The simulator recreates parent run rows with INSERT IGNORE before writing rejected rows, so foreign-key inserts remain safe after external resets.

9. Why store execution_time in milliseconds and report in seconds?
Best answer: Milliseconds preserve precision at ingestion. Seconds are better for business readability in reports, so conversion is done consistently in analytics.

10. How would you make this production-ready next?
Best answer: I would add scheduler/orchestration, partitioning by date, alert automation from thresholds, role-based access, and CI checks for query correctness and performance.

## 20) Mock Q&A Round (10 Questions for Practice)

1. Q: Walk me through your pipeline in 30 seconds.
A: Logs are generated by simulator, validated, and split into clean vs rejected paths. Clean rows go to system_logs, invalid rows go to rejected_logs with reasons, and etl_metrics tracks ingestion quality. Analytics SQL then computes KPI, advanced latency, and risk insights for dashboard use.

2. Q: Which table is the main analytics fact table and why?
A: system_logs, because it contains validated request-level facts such as endpoint, status, timestamp, and latency needed for performance metrics.

3. Q: How do you calculate success and error rates?
A: Using conditional aggregation: success is status=200 count over total, error is status=500 count over total, both expressed as percentages with divide-by-zero protection using NULLIF.

4. Q: What does SLA breach mean in your project?
A: Any request with execution_time above 0.5 seconds. It is calculated globally and endpoint-wise to detect where responsiveness violates target.

5. Q: Why did you include hourly and minute-level traffic queries?
A: Hourly shows broader load patterns, while minute-level reveals short spikes and burst behavior that hourly aggregation can hide.

6. Q: How do you identify problematic endpoints?
A: I use endpoint error rate, endpoint SLA breach rate, top slow endpoint lists, and advanced endpoint risk scoring that combines these dimensions.

7. Q: What insights do P95 and P99 add over average latency?
A: They capture tail latency. Even with good average, poor P95/P99 indicates a meaningful subset of users still faces slow experiences.

8. Q: What evidence shows your ETL is healthy?
A: High inserted percentage, low rejected percentage, acceptable freshness (low minutes since last load), and stable rejection reasons over time.

9. Q: If rejected rows increase suddenly, what would you do first?
A: Check rejection reason distribution, identify dominant failure type, trace affected etl_run_id window, inspect raw payload examples, and apply upstream or validation-rule fixes.

10. Q: What is the strongest technical point of this project?
A: Full lifecycle completeness: synthetic data generation, quality enforcement, lineage, KPI analytics, and dashboard-ready operational intelligence in one coherent workflow.

## 21) Ready Answer Bank (Use These Exact Lines)

1. Question: What problem does your project solve?
Answer: It provides an end-to-end monitoring pipeline for API performance and ETL quality, so teams can track reliability, speed, data quality, and SLA compliance from one system.

2. Question: Why did you use a simulator?
Answer: It allowed controlled, repeatable testing of both normal and failure scenarios, including invalid records, without requiring sensitive production data.

3. Question: Why do you keep three layers: raw, clean, rejected?
Answer: Raw preserves source truth, clean supports trusted analytics, and rejected preserves failed records for audit and root-cause analysis.

4. Question: How is data quality enforced?
Answer: First in Python with validation rules, then in MySQL with constraints. This defense-in-depth approach prevents bad data from entering analytics tables.

5. Question: Why track ETL metrics separately?
Answer: ETL metrics give measurable pipeline health, including throughput, rejection rate, and freshness, which are critical for operational trust.

6. Question: How do you compute success rate?
Answer: Success rate is count of status 200 divided by total requests, multiplied by 100, using NULLIF to avoid divide-by-zero errors.

7. Question: Why do you separate 404 from 500?
Answer: 404 often indicates routing or client-side path issues, while 500 indicates server-side failure. They require different corrective actions.

8. Question: Why is SLA breach threshold set at 0.5 seconds?
Answer: It represents a practical responsiveness target for this project, and we consistently use it across KPI and endpoint-level analyses.

9. Question: Why not rely only on average latency?
Answer: Average can hide slow tail behavior; P95 and P99 expose worst-case user experience and are better early warning indicators.

10. Question: What does endpoint risk score represent?
Answer: It is a weighted severity index combining error rate, SLA breach rate, and latency pressure to prioritize endpoints for optimization.

11. Question: How do indexes improve this solution?
Answer: They speed up frequent filters and aggregations on timestamp, endpoint, status, and run_id, which improves analytics and dashboard responsiveness.

12. Question: What happens during reset and reload?
Answer: Analytical tables are truncated safely with FK checks controlled, and cleaned CSV can be bulk loaded back to system_logs for deterministic testing.

13. Question: How do you prove rejected records are validly linked?
Answer: I run an orphan check between rejected_logs and etl_metrics; the expected result is orphan_rejected_rows equals zero.

14. Question: What is your strongest governance feature?
Answer: Every rejected row stores both reason and raw payload, so quality failures are measurable, explainable, and actionable.

15. Question: If you get one week more, what will you add?
Answer: I would add scheduled orchestration, alert automation, partitioning, role-based access, and CI-based SQL validation for production hardening.

## 22) Power BI Results Snapshot (Sample Run)

### KPI Cards Observed
- Total Requests: 19.97K
- Success Rate: 79.27%
- Error Rate: 10.07%
- Avg Latency: 0.494 sec
- SLA Breach Rate: 48.43%

### Chart-Level Findings
- Requests by hour are concentrated across two active hour buckets, showing uneven load windows.
- Minute-level traffic shows bursts, with peaks around 1.2K+ requests/min in high-activity periods.
- Endpoint request volume is relatively balanced, each around ~4K requests in the sample.
- Endpoint status mix is mostly HTTP 200, with visible 404 and 500 shares that need tuning.

### Business Interpretation
- Reliability is acceptable but not strong yet because success is below 80%.
- SLA breach is high at 48.43%, indicating latency optimization is the top priority.
- Error and not-found rates suggest endpoint-level debugging and contract validation are required.

### Action Plan Based on BI Results
1. Prioritize endpoint tuning using risk score and P95/P99 latency.
2. Reduce SLA breaches with query optimization and caching for hot endpoints.
3. Investigate status-code outliers by endpoint and hour bucket.
4. Track week-over-week trend using the same KPI card set for measurable improvement.

