import os
from etl.extract import download_csv_files_from_s3
from etl.transform import validate_and_clean_data
from etl.load import load_data_to_postgres
from confluent_kafka import Consumer
import json

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'group.id': 'streamlit-app',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD')
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['sales'])

def run_etl(file_name):
    # Step 1: Download CSV files from S3
    csv_files = download_csv_files_from_s3(file_name)

    if not csv_files:
        print("No CSV files found in the S3 bucket under the 'CSV/' prefix.")
        return

    # Step 2: Validate and clean the DataFrames
    cleaned_data = validate_and_clean_data(csv_files)

    # Step 3: Save the combined DataFrame as a new CSV file
    output_path = 'data/combined_cleaned_data.csv'
    os.makedirs('data', exist_ok=True)
    cleaned_data.to_csv(output_path, index=False)
    print(f"Combined and cleaned data saved to {output_path}")

    file_path = 'data/combined_cleaned_data.csv'

    # Step 4: Load data into PostgreSQL
    load_data_to_postgres(file_path)

# Run the Kafka consumer loop
if __name__ == "__main__":
    print("Starting ETL Kafka consumer...")
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Parse the Kafka message and start the ETL process
        data = json.loads(msg.value().decode('utf-8'))
        file_name = data['file_name']
        print(f"Received notification for new file: {file_name}")
        run_etl(file_name)

    consumer.close()
