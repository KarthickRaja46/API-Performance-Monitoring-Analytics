# 🚀 Database Performance Monitoring System

📌 Built to simulate real-world production monitoring systems
📌 Focus: Data quality + performance analytics + system reliability

A real-time **ETL + SQL Analytics pipeline** that simulates system logs, enforces data quality, monitors pipeline health, and generates performance insights using MySQL and Power BI.

---

# 🎯 Problem Statement

Modern applications generate massive volumes of logs, but:

* Raw logs are often **noisy, inconsistent, and unreliable**
* Poor data quality leads to **incorrect analytics and decisions**
* Performance issues remain hidden without **structured monitoring**

👉 This project solves it by building an **end-to-end data pipeline**:

**Generate → Validate → Store → Analyze → Monitor**

---

# 💡 What Makes This Project Stand Out

* Simulates real-world system behavior (latency, retries, failures)
* Implements strong data validation to prevent bad data entry
* Separates **clean vs rejected data** for reliable analytics
* Tracks pipeline health using ETL metrics
* Computes advanced KPIs (SLA breach, latency, health score)
* End-to-end pipeline from raw data → dashboard insights

---

# 🔄 System Architecture (Data Flow)

```
Log Simulator (Python)
        ↓
Raw Logs (CSV Buffer)
        ↓
ETL Validation Layer
   ↙                         ↘
Valid Data                  Rejected Data
(system_logs)              (rejected_logs)
   ↓                         ↓
Cleaned CSV                Rejected CSV
        ↘                   ↙
           ETL Metrics Tracking
                    ↓
        SQL Analytics Layer
 (Views → Basic → KPI → Advanced → Dashboard)
                    ↓
           Power BI Dashboard
```

---

# 🧩 Key Design Decisions

* Separated rejected data to avoid corrupt analytics
* Used CSV as raw layer for traceability and debugging
* Designed modular SQL scripts for scalability
* Introduced ETL metrics to monitor pipeline health
* Structured queries into layers (basic → KPI → advanced)

---

# 🧱 Core Components

## 1. Log Simulator (Python)

* Generates realistic system logs continuously
* Simulates latency, errors, retries, and edge cases

## 2. ETL Pipeline

* Validates incoming data using rules
* Separates:

  * ✅ Valid records → `system_logs`
  * ❌ Invalid records → `rejected_logs`
* Tracks pipeline stats in `etl_metrics`

## 3. SQL Analytics Layer

* Modular SQL scripts:

  * Basic metrics
  * KPI calculations
  * Advanced performance analysis
* Optimized using views and structured queries

## 4. Dashboard Layer

* Power BI dashboard for:

  * System health monitoring
  * Latency trends
  * Error tracking
  * SLA compliance

---

# 🗂️ Project Structure

```
SQL_PROJECT/
│
├── data/
│   ├── raw/                  # Raw generated logs
│   └── processed/            # Cleaned & rejected logs
│
├── sql/                      # Schema, analytics, automation
├── scripts/                  # Python ETL + simulator
├── dashboard/                # Power BI files
```

---

# 🧾 Database Design (Simplified)

| Table                    | Purpose                           |
| ------------------------ | --------------------------------- |
| `system_logs`            | Clean data used for analytics     |
| `rejected_logs`          | Stores invalid data for debugging |
| `etl_metrics`            | Tracks pipeline performance       |
| `alerts`                 | Stores detected anomalies         |
| `alert_threshold_config` | Defines alert rules               |
| `system_logs_archive`    | Historical storage                |

---

# ⚙️ Tech Stack

* **Python** → Data simulation & ETL
* **MySQL** → Storage & analytics
* **SQL** → KPI computation
* **Power BI** → Visualization

---

# 🛠️ Setup & Execution

## 1. Setup Database

```sql
source sql/00_schema.sql;
```

---

## 2. Run Simulator

```bash
python scripts/log_simulator.py
```

This will:

* Generate logs continuously
* Validate data
* Store clean & rejected records
* Track ETL performance

---

## 3. Run Analytics

```text
1. sql/07_reset.sql (optional)
2. sql/03_views.sql
3. sql/01_basic.sql
4. sql/02_kpi.sql
5. sql/04_advanced.sql
6. sql/05_dashboard.sql
```

Or run all:

```sql
source sql/06_run.sql;
```

⚠️ `SOURCE` works only in MySQL CLI

---

# 📊 Key Metrics (Business Meaning)

| Metric       | Why It Matters                                              |
| ------------ | ----------------------------------------------------------- |
| Success Rate | Measures system reliability                                 |
| Error Rate   | Identifies failure frequency                                |
| Avg Latency  | Overall performance indicator                               |
| SLA Breach   | % of requests exceeding acceptable latency (e.g., >0.5 sec) |
| Health Score | Combined indicator of system reliability and performance    |

---

# 📈 Sample Output

### Simulator

```
Inserted 3 rows
Inserted 1 rows
Logs=50 Inserted=42 Rejected=8
```

### Analytics

```
total_requests: 5000
success_rate_pct: 89.40
error_rate_pct: 10.60
avg_latency_sec: 0.482
```

---

# 🚀 Business Impact

* Prevents **bad data from corrupting analytics**
* Enables **real-time system monitoring**
* Helps detect:

  * Performance bottlenecks
  * Data quality issues
  * SLA violations

👉 This mirrors real-world monitoring systems used in production environments.

---

# ⚠️ Important Notes

* Stop simulator before reset (`sql/07_reset.sql`)
* CSV headers are auto-managed
* Queries are modular and reusable

---

# 🔮 Future Enhancements

* Kafka-based real-time streaming
* API ingestion layer
* Automated alerts (Email/Slack)
* Query optimization engine

---

# 👨‍💻 Author

**Karthick**
Aspiring Data Analyst / Data Engineer

---

# ⭐ Final Takeaway

This project demonstrates a **complete data pipeline**, not just SQL queries.

👉 It covers:

* Data generation
* Data cleaning
* Data storage
* Data analysis
* Performance monitoring

🔥 This project shows how raw data becomes actionable insights through a structured pipeline.
