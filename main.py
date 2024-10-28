import os
import glob
import pandas as pd
from etl.extract import download_csv_files_from_s3
from etl.transform import validate_and_clean_data
from etl.load import load_data_to_postgres
from confluent_kafka import Consumer
import json

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'group.id': 'etl-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD')
}
consumer = Consumer(consumer_conf)
consumer.subscribe(['sales'])

def get_latest_file(directory, extension='csv'):
    """Get the latest file in the specified directory with the specified extension."""
    directory = os.path.abspath(directory)
    print(f"Checking directory: {directory}")
    
    list_of_files = glob.glob(f"{directory}/*.{extension}")
    if not list_of_files:
        print("No files found.")
        return None
    latest_file = max(list_of_files, key=os.path.getmtime)
    print(f"Latest file found: {latest_file}")
    return latest_file

def run():
    # Step 1: Check for the latest file in the 'data' folder
    latest_file = get_latest_file('data')
    if not latest_file:
        print("No CSV files found in the 'data' folder.")
        return

    print(f"Processing latest file: {latest_file}")

    # Step 2: Load the CSV into a DataFrame
    try:
        df = pd.read_csv(latest_file)
    except Exception as e:
        print(f"Failed to load {latest_file} into DataFrame: {e}")
        return

    # Step 3: Validate and clean the DataFrame
    cleaned_data = validate_and_clean_data([df])

    # Optional: Save the cleaned data (if needed for backup or other purposes)
    output_path = 'data_consolidated/combined_cleaned_data.csv'
    os.makedirs('data_consolidated', exist_ok=True)
    cleaned_data.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

    # Step 4: Load data into PostgreSQL
    load_data_to_postgres(output_path)


# Run the Kafka consumer loop
if __name__ == "__main__":
    run()
