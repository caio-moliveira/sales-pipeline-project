import os
from etl.extract import download_csv_files_from_s3
from etl.transform import validate_and_clean_data
from etl.load import load_data_to_postgres



def run():

    # Step 1: Download CSV files from the S3 'CSV/' folder
    csv_files = download_csv_files_from_s3()

    if not csv_files:
        print("No CSV files found in the S3 bucket under the 'CSV/' prefix.")
        return

    # Step 2: Validate and clean the DataFrames
    cleaned_data = validate_and_clean_data(csv_files)

    # Optional: Save the combined DataFrame as a new CSV file
    output_path = 'data/combined_cleaned_data.csv'
    os.makedirs('data', exist_ok=True)
    cleaned_data.to_csv(output_path, index=False)
    print(f"Combined and cleaned data saved to {output_path}")

    # Step 3: Define the path to the cleaned and combined CSV file
    file_path = 'data/combined_cleaned_data.csv'
    
    # Step 4: Load data into PostgreSQL
    load_data_to_postgres(file_path)


if __name__ == "__main__":
    run()
