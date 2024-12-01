import threading
import time
import os
import pandas as pd
from kafka_etl.kafka_producer import KafkaProducer
from kafka_etl.kafka_consumer import KafkaConsumer
from backend.creating_files.files_generator.csv_generator import create_sales_csv
from backend.creating_files.loader_S3.loader import check_and_upload_csv_files
from backend.etl.extract import download_csv_files_from_s3
from backend.etl.transform import validate_and_clean_data
from backend.etl.load import load_data_to_postgres, create_sales_data_table

# Define paths and folders
data_folder = "data_consolidated"
os.makedirs(data_folder, exist_ok=True)

def generate_csv_and_notify(producer):
    """Generate a CSV file every 60 seconds and send a Kafka message."""
    try:
        while True:
            try:
                print("Generating new CSV file...")
                create_sales_csv()
                print("CSV file generated.")

                # Send Kafka message to start pipeline
                producer.send_message(
                    "file-generated",
                    {"CSV_GENERATED": "New CSV generated and ready for processing"},
                    key="csv"
                )
                print("Notification sent to Kafka.")
            except Exception as e:
                print(f"Error generating or notifying about CSV: {e}")
            time.sleep(60)
    finally:
        producer.close()

def run_etl_process():
    """Run the ETL process."""
    create_sales_data_table()  # Ensure the table exists
    print("Checking for new data in S3...")

    try:
        # Step 1: Download new CSV files from S3
        new_dataframes = download_csv_files_from_s3()
        if not new_dataframes:
            print("No new files to process.")
            return

        # Step 2: Validate and clean data
        cleaned_data = validate_and_clean_data(new_dataframes)

        # Optional: Save cleaned data locally as a backup
        output_path = os.path.join(data_folder, "last_csv_updated.csv")
        cleaned_data.to_csv(output_path, index=False)
        print(f"Cleaned data saved to {output_path}")

        # Step 3: Load cleaned data into PostgreSQL
        load_data_to_postgres(output_path)
        print("Data loaded to PostgreSQL successfully.")
    except Exception as e:
        print(f"ETL process encountered an error: {e}")

def start_consumer():
    """Start Kafka consumer to process messages."""
    consumer = KafkaConsumer(topics=["file-generated", "S3-bucket", "postgres-db"])
    try:
        consumer.consume_messages()
    except Exception as e:
        print(f"Consumer encountered an error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    producer = KafkaProducer()

    # Run the CSV generator in one thread
    csv_thread = threading.Thread(target=generate_csv_and_notify, args=(producer,), daemon=True)

    # Run the Kafka consumer in another thread
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)

    # Start threads
    csv_thread.start()
    consumer_thread.start()

    # Keep the main program alive while threads run
    csv_thread.join()
    consumer_thread.join()
