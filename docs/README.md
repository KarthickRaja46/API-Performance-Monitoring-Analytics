# Database Performance Insights

A Python + MySQL project to simulate real-time system logs, store valid/rejected events, and run structured SQL analytics.

## Project Structure

## 📁 Project Structure

SQL_PROJECT/
|
|- data/           # Raw and processed log data
|- sql/            # All SQL scripts (core, analytics, automation)
|- scripts/        # ETL simulation scripts
|- dashboard/      # Power BI dashboard files
|- docs/           # Documentation and preparation material

- `scripts/log_simulator.py` - Live log simulator with validation, DB insert, and CSV export
- `.gitignore` - Repository ignore rules for generated/local files
- `data/raw/log.csv` - Raw generated logs (before validation)
- `data/processed/cleaned_logs.csv` - Cleaned/valid logs (after validation)
- `data/processed/rejected_logs.csv` - Rejected log export (header + data)
- `dashboard/performance_monitoring.pbix` - Power BI dashboard file
- `dashboard/powerbi_screenshot.png` - Dashboard snapshot
- `sql/schema.sql` - Database schema and indexes
- `sql/reset.sql` - Full data reset (truncate generated data)
- `sql/views.sql` - Reusable cleaned view for analytics
- `sql/basic.sql` - Basic analytics queries
- `sql/kpi.sql` - KPI and ETL quality queries
- `sql/advanced.sql` - Advanced analytics queries
- `sql/dashboard.sql` - Final dashboard output queries
- `sql/run.sql` - Main SQL runner
- `sql/future/ops.sql` - Optional operational automation script
- `sql/future/proof.sql` - Archived production proof script
- `sql/future/run.sql` - Future-layer runner

## Prerequisites

- Python 3.10+
- MySQL Server 8.0+ running locally
- Python packages:
  - `pandas`
  - `mysql-connector-python`

Install packages:

```bash
pip install pandas mysql-connector-python
```

## Database Setup

1. Open MySQL client or Workbench.
2. Run schema setup:

```sql
source sql/schema.sql;
```

This creates:
- `system_logs`
- `rejected_logs`
- `etl_metrics`
- `alerts`
- `alert_threshold_config`
- `system_logs_archive`

## Run Simulator

From project root:

```bash
python scripts/log_simulator.py
```

When prompted, enter your MySQL password.

What it does:
- Generates random realistic logs continuously
- Writes all generated rows to `data/raw/log.csv` (raw layer)
- Validates data quality rules
- Inserts valid rows into `system_logs`
- Writes valid rows to `data/processed/cleaned_logs.csv`
- Inserts invalid rows into `rejected_logs`
- Tracks run metrics in `etl_metrics`
- Writes rejected rows to `data/processed/rejected_logs.csv`

Stop with `CTRL+C` for clean shutdown and buffer flush.

## Run Analytics

Run in this order:

1. `sql/reset.sql` (optional, for clean run)
2. `sql/views.sql`
3. `sql/kpi.sql`
4. `sql/dashboard.sql`
5. `sql/future/ops.sql` (optional)

Final dashboard feeds:

- `sql/dashboard.sql`

Or run all scripts:

- `sql/run.sql` (for mysql CLI / MySQL Shell where `SOURCE` is supported)

Important:
- `SOURCE` is a mysql-client command, not server-side SQL.
- If you run `SOURCE ...` in a GUI query tab, MySQL returns `Error 1064`.

## Pre-Publish Verification

Run this final checklist in MySQL Workbench before publishing:

- `sql/views.sql` executes successfully
- Analytics scripts execute with no SQL errors

Expected checks:
- No missing required tables
- `orphan_rejected_rows = 0`
- Dashboard output queries return data

## Notes

- CSV headers are auto-ensured in simulator startup.
- Analytics files are split for clarity and ordered execution.
- Analytics latency outputs are standardized in seconds.
- If you run `sql/reset.sql`, stop the simulator first and restart it after reset.
- Main branch is configured for the active GitHub repository.

## Sample Output

Simulator (live):

```text
Live stream started (CTRL+C to stop)
Inserted 3 rows
Inserted 1 rows
Inserted 4 rows
Logs=50 Inserted=42 Rejected=8
```

On stop:

```text
Stopping...
Stream stopped cleanly
```

Example analytics result (basic):

```text
total_requests | 5000
success_rate_pct | 89.40
error_rate_pct | 10.60
avg_latency_sec | 0.482
```
