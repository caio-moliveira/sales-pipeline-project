import time
import os
import pandas as pd
from backend.creating_files.files_generator.csv_generator import create_sales_csv
from backend.creating_files.loader_S3.loader import check_and_upload_csv_files
from backend.etl.extract import download_csv_files_from_s3
from backend.etl.transform import validate_and_clean_data
from backend.etl.load import load_data_to_postgres, create_sales_data_table
from kafka_etl.kafka_producer import KafkaProducer


# Define paths and folders
data_folder = "data_consolidated"
os.makedirs(data_folder, exist_ok=True)

# Initialize Kafka producer
producer = KafkaProducer()

def run_etl_process():

    # Step 0: Create the sales_data table if it doesn't already exist
    create_sales_data_table()
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


if __name__ == "__main__":
    last_csv_generation = time.time()
    last_s3_check = time.time()

    while True:
        current_time = time.time()

        # Generate a new CSV every 30 seconds
        if current_time - last_csv_generation >= 30:
            try:
                print("Generating new CSV file...")
                create_sales_csv()
                last_csv_generation = current_time
                
                # Send a Kafka message after generating a new CSV
                producer.send_message("S3-bucket", {"CSV_GENERATED": "New CSV generated and ready for upload"}, key='csv')
            except Exception as e:
                print(f"Error generating CSV: {e}")

        # Check and upload CSV files to S3 every 35 seconds
        if current_time - last_s3_check >= 35:
            try:
                print("Checking for new files to upload to S3...")
                check_and_upload_csv_files()
                last_s3_check = current_time
                
                # Send a Kafka message after uploading a file to S3
                producer.send_message("S3-bucket", {"UPLOADED": "File uploaded to S3"}, key='upload')
            except Exception as e:
                print(f"Error uploading to S3: {e}")

        # Run ETL process to check for new data in S3 and load it to Postgres
        try:
            print("Running ETL process to check for new data in S3...")
            run_etl_process()
        except Exception as e:
            print(f"Error in ETL process: {e}")

        # Wait before the next iteration
        time.sleep(5)
