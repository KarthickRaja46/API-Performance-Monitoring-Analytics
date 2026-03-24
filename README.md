# 🚀 Database Performance Monitoring & Analytics System

🚀 End-to-end data pipeline to monitor system performance, ensure data quality, and generate actionable insights.

**Real-world ETL + SQL analytics system to monitor database performance, detect SLA breaches, and optimize system health.**

📌 Built to simulate real-world production monitoring systems used in backend and API environments
📌 Focus: Data quality • Performance analytics • System reliability

---

## 🔹 Executive Summary

Modern applications generate massive logs that are noisy and inconsistent. Poor data quality leads to wrong decisions, while hidden performance issues impact reliability.

👉 This project demonstrates how to **Generate → Validate → Store → Analyze → Monitor** logs using a complete pipeline.

---

## 📌 Key Highlights

* End-to-end ETL pipeline (Python + SQL)
* Real-time log simulation with failures & latency
* Data validation with rejected data handling
* SLA, P95, P99 performance tracking
* 40+ SQL queries (basic → advanced → diagnostics)
* Power BI dashboard for system monitoring

---

## 🎯 Problem Statement

* Raw logs are **noisy, inconsistent, unreliable**
* Poor data quality → **incorrect analytics & decisions**
* Performance issues remain hidden without **structured monitoring**

---

## 💡 Solution Overview

* Logs generated with realistic latency, retries, failures
* ETL pipeline validates & cleans data
* Invalid records stored separately for debugging
* SQL analytics computes KPIs & diagnostics
* Power BI dashboard visualizes system health

---

## 🔄 System Architecture

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
 (Basic → KPI → Advanced → Diagnostics)
                    ↓
           Power BI Dashboard
```

---

## 🧱 Core Components

**1. Log Simulator (Python)** → Generates realistic logs with latency/errors
**2. ETL Pipeline** → Validates data, separates valid/invalid, tracks metrics
**3. SQL Analytics Layer** → KPIs, advanced diagnostics, optimized queries
**4. Dashboard Layer (Power BI)** → SLA monitoring, latency trends, error tracking

---

## 📸 Dashboard Preview

![Dashboard](dashboard/powerbi_screenshot.png)

**Key Insights:**

* SLA breach trends
* High latency endpoints
* Error rate spikes
* System health score

---

## 📊 Sample Insights

* Identified high latency endpoints impacting SLA
* Detected error spikes during peak load
* Improved performance via query optimization

---

## 📈 Results

* Processed ***20K+*** simulated system log records
* Achieved high data quality through ETL validation
* Identified slow endpoints impacting SLA compliance
* Improved performance using SQL query optimization

---

## 🗂️ Project Structure

```
SQL_PROJECT/
│
├── data/
│   ├── raw/
│   └── processed/
├── sql/
├── scripts/
├── dashboard/
```

---

## 🧾 Database Design

| Table         | Purpose                  |
| ------------- | ------------------------ |
| system_logs   | Clean data for analytics |
| rejected_logs | Invalid data storage     |
| etl_metrics   | Pipeline tracking        |
| alerts        | Detected issues          |

---

## ⚙️ Tech Stack

![Python](https://img.shields.io/badge/Python-3.9-blue)
![SQL](https://img.shields.io/badge/SQL-MySQL-orange)
![PowerBI](https://img.shields.io/badge/PowerBI-Dashboard-yellow)

---

## ⚡ Quick Start

1. Setup database:

   ```sql
   source sql/00_schema_setup.sql;
   ```

2. Run log simulator:

   ```bash
   python scripts/log_simulator.py
   ```

3. Execute analytics:
   Run SQL scripts from `sql/` folder in order

4. Open dashboard:
   Load `dashboard/performance_monitoring.pbix` in Power BI

---

## 🧮 Key Metrics

| Metric       | Purpose               |
| ------------ | --------------------- |
| Success Rate | System reliability    |
| Error Rate   | Failure tracking      |
| Avg Latency  | Performance           |
| SLA Breach   | Performance threshold |
| Health Score | Overall system status |

---

## 🌍 Real-World Use Cases

* API performance monitoring
* Backend system health tracking
* Production log analytics
* Incident detection systems

👉 Similar to monitoring systems at Amazon, Netflix, and Google

---

## 🚀 Business Impact

* Prevents bad data from corrupting analytics
* Enables real-time monitoring
* Detects performance bottlenecks
* Improves system reliability

---

## 🔮 Future Enhancements

* Kafka-based real-time streaming
* API ingestion
* Automated alerts (Email/Slack)
* Query optimization engine

---

## 👨‍💻 Author

**Karthick Raja**
-
Data Analyst specializing in SQL, ETL pipelines, and performance analytics
---

## ⭐ Final Takeaway

This project demonstrates a **complete data pipeline**, not just SQL queries.

👉 Covers:

* Data generation
* Data cleaning
* Data storage
* Data analysis
* Performance monitoring

🔥 Shows how raw data becomes actionable insights through a structured system.
