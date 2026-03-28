import csv
import random
import time
from datetime import datetime, timedelta
import os
import uuid

# Configuration
NUM_RECORDS = 20000
OUTPUT_FILE = '../data/raw_api_logs.csv'
ENDPOINTS = [
    '/api/v1/users',
    '/api/v1/orders',
    '/api/v1/products',
    '/api/v1/checkout',
    '/api/v1/search',
    '/api/v1/payment'
]
STATUS_CODES = [200, 200, 200, 200, 200, 404, 500] # weighted
IP_PREFIXES = ['192.168.1.', '10.0.0.', '172.16.0.']

def generate_logs():
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['ip', 'endpoint', 'status', 'timestamp', 'execution_time', 'rows_scanned', 'joins_count'])
        
        start_time = datetime.now() - timedelta(days=7)
        
        for i in range(NUM_RECORDS):
            # 5% chance of generating a "bad" record for ETL to reject
            is_bad = random.random() < 0.05
            
            ip = f"{random.choice(IP_PREFIXES)}{random.randint(1, 255)}"
            endpoint = random.choice(ENDPOINTS)
            status = random.choice(STATUS_CODES)
            
            # Progress time sequentially
            current_time = start_time + timedelta(seconds=i * random.uniform(0.1, 5.0))
            timestamp = current_time.strftime('%Y-%m-%d %H:%M:%S.000')
            
            # Base logic for performance metrics
            execution_time = int(random.lognormvariate(3.0, 0.5)) 
            rows_scanned = random.randint(0, 1000)
            joins_count = random.randint(0, 5)
            
            if endpoint == '/api/v1/checkout':
                execution_time += 1500 # Slower endpoint
            if endpoint == '/api/v1/search':
                rows_scanned += 5000
                joins_count += 3
                
            if is_bad:
                # Introduce errors
                error_type = random.choice(['bad_status', 'neg_execution', 'neg_rows', 'missing_endpoint'])
                if error_type == 'bad_status':
                    status = 999
                elif error_type == 'neg_execution':
                    execution_time = -100
                elif error_type == 'neg_rows':
                    rows_scanned = -5
                elif error_type == 'missing_endpoint':
                    endpoint = ''
                    
            writer.writerow([ip, endpoint, status, timestamp, execution_time, rows_scanned, joins_count])

    print(f"✅ Successfully generated {NUM_RECORDS} log records in {OUTPUT_FILE}")

if __name__ == '__main__':
    print("🚀 Starting API Log Simulator...")
    generate_logs()
    
    # Trigger the ETL pipeline automatically
    print("⏳ Triggering ETL Pipeline...")
    import etl_pipeline
    etl_pipeline.run_etl(OUTPUT_FILE)
