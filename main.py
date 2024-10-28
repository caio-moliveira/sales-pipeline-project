import time
from creating_files.files_generator.csv_generator import create_sales_csv
from creating_files.loader_S3.loader import check_and_upload_csv_files
from etl.extract import download_csv_files_from_s3
from etl.transform import validate_and_clean_data
from etl.load import load_data_to_postgres
import os
import pandas as pd

# Define paths and folders
data_folder = "data_consolidated"
os.makedirs(data_folder, exist_ok=True)

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
    output_path = os.path.join(data_folder, "combined_cleaned_data.csv")
    cleaned_data.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

    # Step 3: Load cleaned data into PostgreSQL
    load_data_to_postgres(output_path)

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

        # Check and upload CSV files to S3 every 15 seconds
        if current_time - last_s3_check >= 15:
            print("Checking for new files to upload to S3...")
            check_and_upload_csv_files()
            last_s3_check = current_time

        # Run ETL process
        print("Running ETL process to check for new data in S3...")
        run_etl_process()

        # Wait before the next iteration
        time.sleep(5)
