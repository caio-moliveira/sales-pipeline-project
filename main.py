import time
import os
import pandas as pd
from creating_files.files_generator.csv_generator import create_sales_csv
from creating_files.loader_S3.loader import check_and_upload_csv_files
from etl.extract import download_csv_files_from_s3
from etl.transform import validate_and_clean_data
from etl.load import load_data_to_postgres
from kafka_etl.kafka_producer import KafkaProducer


# Define paths and folders
data_folder = "data_consolidated"
os.makedirs(data_folder, exist_ok=True)

# Initialize Kafka producer
producer = KafkaProducer()

def run_etl_process():
    """Check for new CSV files in S3, run the ETL pipeline, and load cleaned data into PostgreSQL."""
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

    # Send a Kafka message to indicate the data has been loaded into PostgreSQL
    producer.send_message("postgres-db", {"UPLOADED": "Data uploaded to Postgres", "file": output_path}, key="upload")

# Scheduled task runner
if __name__ == "__main__":
    last_csv_generation = time.time()
    last_s3_check = time.time()

    while True:
        current_time = time.time()

        # Generate a new CSV every 30 seconds
        if current_time - last_csv_generation >= 30:
            print("Generating new CSV file...")
            create_sales_csv()
            last_csv_generation = current_time
            
            # Send a Kafka message to the "S3-bucket" topic after generating a new CSV
            producer.send_message("S3-bucket", {"CSV_GENERATED": "New CSV generated and ready for upload"}, key='csv')

        # Check and upload CSV files to S3 every 15 seconds
        if current_time - last_s3_check >= 35:
            print("Checking for new files to upload to S3...")
            check_and_upload_csv_files()
            last_s3_check = current_time
            
            # Send a Kafka message to the "S3-bucket" topic after uploading a file to S3
            producer.send_message("S3-bucket", {"UPLOADED": "File uploaded to S3"}, key='upload')

        # Run ETL process to check for new data in S3 and load it to Postgres
        print("Running ETL process to check for new data in S3...")
        run_etl_process()

        # Wait before the next iteration
        time.sleep(5)
