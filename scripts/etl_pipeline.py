
import csv
import json
import logging
import uuid
from datetime import datetime
import mysql.connector
from mysql.connector import Error
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    try:
        # Read password from environment variable
        password = os.environ.get('MYSQL_PASSWORD', '')
        return mysql.connector.connect(
            host='127.0.0.1',
            database='performance_monitoring',
            user='root',
            password=password
        )
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def validate_row(row):
    """
    Validates a single log row. Returns (is_valid, reason).
    """
    try:
        ip, endpoint, status, timestamp, execution_time, rows_scanned, joins_count = row
        
        if not endpoint:
            return False, "Missing endpoint"
            
        status = int(status)
        if status not in (200, 404, 500):
            return False, f"Invalid status code: {status}"
            
        execution_time = int(execution_time)
        if execution_time < 0:
            return False, "Negative execution_time"
            
        rows_scanned = int(rows_scanned) if rows_scanned else 0
        if rows_scanned < 0:
            return False, "Negative rows_scanned"
            
        joins_count = int(joins_count) if joins_count else 0
        if joins_count < 0:
            return False, "Negative joins_count"
            
        # Optional timestamp format validation
        datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
        
    except ValueError as e:
        return False, f"Data type parsing error: {str(e)}"
    except Exception as e:
        return False, f"Validation error: {str(e)}"
        
    return True, "Valid"

def run_etl(csv_file_path):
    conn = get_db_connection()
    if not conn:
        logging.error("Failed to connect to DB. Exiting ETL.")
        return

    cursor = conn.cursor()
    run_id = str(uuid.uuid4())
    load_time = datetime.now()
    
    total_rows = 0
    inserted_rows = 0
    rejected_rows = 0
    
    valid_records = []
    rejected_records = []
    
    logging.info(f"Starting ETL Run ID: {run_id}")
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)  # Skip header
            
            line_number = 1
            for row in reader:
                line_number += 1
                total_rows += 1
                
                if len(row) != 7:
                    rejected_records.append((run_id, 'csv', line_number, "Invalid column count", json.dumps(row)))
                    rejected_rows += 1
                    continue
                    
                is_valid, reason = validate_row(row)
                
                if is_valid:
                    valid_records.append((
                        row[0], row[1], int(row[2]), row[3], 
                        int(row[4]), int(row[5]), int(row[6]), run_id
                    ))
                    inserted_rows += 1
                else:
                    payload = dict(zip(header, row))
                    rejected_records.append((run_id, 'csv', line_number, reason, json.dumps(payload)))
                    rejected_rows += 1

        # Insert into etl_metrics
        cursor.execute('''
            INSERT INTO etl_metrics (run_id, source_type, total_rows, inserted_rows, rejected_rows, load_time, notes)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (run_id, 'csv', total_rows, inserted_rows, rejected_rows, load_time, 'Completed successfully'))

        # Batch insert valid records
        if valid_records:
            insert_query = '''
                INSERT INTO system_logs 
                (ip, endpoint, status, timestamp, execution_time, rows_scanned, joins_count, etl_run_id) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            '''
            cursor.executemany(insert_query, valid_records)

        # Batch insert rejected records
        if rejected_records:
            reject_query = '''
                INSERT INTO rejected_logs 
                (etl_run_id, source_type, line_number, reason, raw_payload) 
                VALUES (%s, %s, %s, %s, %s)
            '''
            cursor.executemany(reject_query, rejected_records)

        conn.commit()
        logging.info(f"ETL completed. Total: {total_rows}, Inserted: {inserted_rows}, Rejected: {rejected_rows}")

    except Exception as e:
        conn.rollback()
        logging.error(f"ETL Pipeline Failed: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == '__main__':
    run_etl('../data/raw_api_logs.csv')
