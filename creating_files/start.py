import os
import json
from confluent_kafka import Producer
import schedule
import time
from loader_S3.loader import check_and_upload_csv_files
from files_generator.csv_generator import create_sales_csv

# Kafka producer configuration
producer_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD')
}
producer = Producer(producer_conf)

def notify_etl(file_name, s3_path):
    """Send Kafka notification with file details."""
    message = json.dumps({'file_name': file_name, 's3_path': s3_path}).encode('utf-8')
    producer.produce(topic='sales', value=message)
    producer.flush()
    print(f"Notification sent for {file_name} in S3 path: {s3_path}")

# Schedule CSV creation and upload
def create_and_upload():
    # Step 1: Create a new CSV file
    file_name = create_sales_csv()
    
    # Step 2: Check and upload CSV files to S3
    s3_paths = check_and_upload_csv_files()
    
    # Step 3: Notify ETL process of new uploads
    for s3_path in s3_paths:
        notify_etl(file_name, s3_path)

# Schedule to run every 10 seconds
schedule.every(10).seconds.do(create_and_upload)

# Run the scheduler
if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
