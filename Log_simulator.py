# =============================================================================
# REAL-TIME LOG ETL SIMULATOR (FINAL PRODUCTION WITH DATA QUALITY)
# =============================================================================

import pandas as pd
import random
import uuid
import math
import time
from datetime import datetime
import mysql.connector
import os
import json

# =============================================================================
# 1. PASSWORD INPUT
# =============================================================================
db_password = input("Enter MySQL Password: ")

# =============================================================================
# 2. DB CONNECTION
# =============================================================================
def get_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password=db_password,
        database="performance_monitoring",
        autocommit=False
    )

def reconnect():
    global conn, cursor
    print("⚠️ Reconnecting...")
    conn = get_connection()
    cursor = conn.cursor()


def ensure_etl_parent(run_id):
    cursor.execute(
        """
        INSERT IGNORE INTO etl_metrics
        (run_id, source_type, total_rows, inserted_rows, rejected_rows, load_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (run_id, "api", 0, 0, 0, datetime.now())
    )
    conn.commit()


def upsert_etl_metrics(run_id, total_rows, inserted_rows, rejected_rows):
    cursor.execute(
        """
        INSERT INTO etl_metrics
        (run_id, source_type, total_rows, inserted_rows, rejected_rows, load_time)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_rows = VALUES(total_rows),
            inserted_rows = VALUES(inserted_rows),
            rejected_rows = VALUES(rejected_rows),
            load_time = VALUES(load_time)
        """,
        (run_id, "api", total_rows, inserted_rows, rejected_rows, datetime.now())
    )
    conn.commit()

conn = get_connection()
cursor = conn.cursor()

# =============================================================================
# 3. DATA DIRECTORY
# =============================================================================
DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)

raw_csv = os.path.join(DATA_DIR, "log.csv")
cleaned_csv = os.path.join(DATA_DIR, "cleaned_logs.csv")
rejected_csv = os.path.join(DATA_DIR, "rejected_logs.csv")

# =============================================================================
# 4. HEADERS (FIX)
# =============================================================================
RAW_COLUMNS = [
    "ip", "endpoint", "status", "timestamp",
    "execution_time", "rows_scanned", "joins_count"
]

VALID_COLUMNS = [
    "ip", "endpoint", "status", "timestamp",
    "execution_time", "rows_scanned", "joins_count"
]

REJECTED_COLUMNS = [
    "ip", "endpoint", "status", "timestamp",
    "execution_time", "rows_scanned", "joins_count",
    "line_number", "reason"
]


def needs_header(file_path):
    return (not os.path.exists(file_path)) or os.path.getsize(file_path) == 0


def ensure_csv_header(file_path, columns):
    expected_header = ",".join(columns)

    if not os.path.exists(file_path):
        pd.DataFrame(columns=columns).to_csv(file_path, index=False)
        return

    if os.path.getsize(file_path) == 0:
        pd.DataFrame(columns=columns).to_csv(file_path, index=False)
        return

    with open(file_path, "r", encoding="utf-8") as f:
        first_line = f.readline().strip()

    if first_line != expected_header:
        with open(file_path, "r", encoding="utf-8") as f:
            existing_content = f.read()
        with open(file_path, "w", encoding="utf-8", newline="") as f:
            f.write(expected_header + "\n")
            f.write(existing_content)

# create/repair headers
ensure_csv_header(raw_csv, RAW_COLUMNS)
ensure_csv_header(cleaned_csv, VALID_COLUMNS)
ensure_csv_header(rejected_csv, REJECTED_COLUMNS)

# =============================================================================
# 5. HELPERS
# =============================================================================
def fix_timestamp(ts):
    return ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts

def validate(row):
    if row["execution_time"] < 0:
        return "Invalid execution_time"
    if row["status"] not in [200, 404, 500]:
        return "Invalid status"
    if row["rows_scanned"] is not None and row["rows_scanned"] < 0:
        return "Invalid rows_scanned"
    return None

def safe_json(row):
    return json.dumps({
        k: (None if pd.isna(v) or (isinstance(v, float) and not math.isfinite(v)) else v)
        for k, v in row.items()
    }, default=str, allow_nan=False)

# =============================================================================
# USER PROFILES
# =============================================================================
USER_PROFILES = [
    {"ip_prefix": "10.0", "latency_ms": (80, 900)},
    {"ip_prefix": "172.16", "latency_ms": (50, 1200)},
    {"ip_prefix": "192.168", "latency_ms": (40, 700)}
]

def generate_log(profile):
    endpoints = ["/login", "/search", "/checkout", "/profile", "/api/data"]
    endpoint = random.choice(endpoints)

    # Inject invalid status sometimes
    status = random.choices(
        [200, 404, 500, 999],
        [75, 10, 10, 5]
    )[0]

    min_ms, max_ms = profile["latency_ms"]
    execution_time = int(random.gauss((min_ms + max_ms) / 2, 150))

    # Inject bad execution_time
    if random.random() < 0.05:
        execution_time = -abs(execution_time)
    else:
        execution_time = max(20, execution_time)

    rows_scanned = random.choice([None, random.randint(10, 5000)])

    # Inject bad rows_scanned
    if random.random() < 0.03:
        rows_scanned = -100

    return {
        "ip": f"{profile['ip_prefix']}.{random.randint(0,255)}.{random.randint(1,255)}",
        "endpoint": endpoint,
        "status": status,
        "timestamp": datetime.now(),
        "execution_time": execution_time,
        "rows_scanned": rows_scanned,
        "joins_count": random.choice([None, random.randint(1, 5)])
    }

# =============================================================================
# 6. INIT
# =============================================================================
run_id = str(uuid.uuid4())

valid_buffer = []
reject_buffer = []
csv_raw_buffer = []
csv_valid_buffer = []
csv_reject_buffer = []

total = inserted = rejected = 0

print("🚀 Live stream started (CTRL+C to stop)")
ensure_etl_parent(run_id)

# =============================================================================
# 7. MAIN LOOP
# =============================================================================
try:
    while True:
        try:
            # 🔥 Random traffic burst
            cycle_size = random.randint(1, 5)
            cycle_inserted = 0

            for _ in range(cycle_size):
                profile = random.choice(USER_PROFILES)
                row = generate_log(profile)
                total += 1
                csv_raw_buffer.append(row)

                reason = validate(row)

                if reason:
                    rejected += 1
                    reject_buffer.append((run_id, "api", total, reason, safe_json(row)))
                    csv_reject_row = dict(row)
                    csv_reject_row["line_number"] = total
                    csv_reject_row["reason"] = reason
                    csv_reject_buffer.append(csv_reject_row)
                else:
                    inserted += 1
                    cycle_inserted += 1

                    valid_buffer.append((
                        row["ip"], row["endpoint"], row["status"],
                        fix_timestamp(row["timestamp"]),
                        row["execution_time"],
                        row["rows_scanned"],
                        row["joins_count"],
                        run_id
                    ))
                    csv_valid_buffer.append(row)

            print(f"Inserted {cycle_inserted} rows")

            # =============================================================================
            # BATCH INSERTS
            # =============================================================================
            if len(valid_buffer) >= 20:
                cursor.executemany("""
                    INSERT INTO system_logs
                    (ip, endpoint, status, timestamp, execution_time, rows_scanned, joins_count, etl_run_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, valid_buffer)
                valid_buffer.clear()
                conn.commit()

            if len(reject_buffer) >= 10:
                # Recreate parent ETL row if it was removed externally (for example after TRUNCATE).
                ensure_etl_parent(run_id)
                cursor.executemany("""
                    INSERT INTO rejected_logs
                    (etl_run_id, source_type, line_number, reason, raw_payload)
                    VALUES (%s,%s,%s,%s,%s)
                """, reject_buffer)
                reject_buffer.clear()
                conn.commit()

            # =============================================================================
            # CSV BATCH WRITE (NO HEADER DUPLICATION)
            # =============================================================================
            if len(csv_raw_buffer) >= 25:
                pd.DataFrame(csv_raw_buffer)[RAW_COLUMNS].to_csv(
                    raw_csv,
                    mode="a",
                    header=needs_header(raw_csv),
                    index=False
                )
                csv_raw_buffer.clear()

            if len(csv_valid_buffer) >= 25:
                pd.DataFrame(csv_valid_buffer)[VALID_COLUMNS].to_csv(
                    cleaned_csv,
                    mode="a",
                    header=needs_header(cleaned_csv),
                    index=False
                )
                csv_valid_buffer.clear()

            if len(csv_reject_buffer) >= 25:
                pd.DataFrame(csv_reject_buffer)[REJECTED_COLUMNS].to_csv(
                    rejected_csv,
                    mode="a",
                    header=needs_header(rejected_csv),
                    index=False
                )
                csv_reject_buffer.clear()

            # =============================================================================
            # METRICS UPDATE
            # =============================================================================
            if total % 50 == 0:
                assert total >= inserted + rejected

                upsert_etl_metrics(run_id, total, inserted, rejected)

                print(f"Logs={total} Inserted={inserted} Rejected={rejected}")

            time.sleep(random.uniform(0.1, 0.4))

        except mysql.connector.Error as e:
            print("❌ DB Error:", e)
            try:
                conn.rollback()
            except Exception:
                pass
            reconnect()
            ensure_etl_parent(run_id)

# =============================================================================
# 8. CLEAN EXIT
# =============================================================================
except KeyboardInterrupt:
    print("\n🛑 Stopping...")

    if valid_buffer:
        cursor.executemany("""
            INSERT INTO system_logs
            (ip, endpoint, status, timestamp, execution_time, rows_scanned, joins_count, etl_run_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, valid_buffer)
        valid_buffer.clear()

    if reject_buffer:
        ensure_etl_parent(run_id)
        cursor.executemany("""
            INSERT INTO rejected_logs
            (etl_run_id, source_type, line_number, reason, raw_payload)
            VALUES (%s,%s,%s,%s,%s)
        """, reject_buffer)
        reject_buffer.clear()

    if csv_raw_buffer:
        pd.DataFrame(csv_raw_buffer)[RAW_COLUMNS].to_csv(
            raw_csv,
            mode="a",
            header=needs_header(raw_csv),
            index=False
        )

    if csv_valid_buffer:
        pd.DataFrame(csv_valid_buffer)[VALID_COLUMNS].to_csv(
            cleaned_csv,
            mode="a",
            header=needs_header(cleaned_csv),
            index=False
        )

    if csv_reject_buffer:
        pd.DataFrame(csv_reject_buffer)[REJECTED_COLUMNS].to_csv(
            rejected_csv,
            mode="a",
            header=needs_header(rejected_csv),
            index=False
        )

    upsert_etl_metrics(run_id, total, inserted, rejected)
    cursor.close()
    conn.close()

    print("✅ Stream stopped cleanly")