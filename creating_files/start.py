import schedule
import time
from loader_S3.loader import check_and_upload_csv_files
from files_generator.csv_generator import create_sales_csv


# Schedule CSV creation and upload
def create_and_upload():
    # Step 1: Create a new CSV file
    create_sales_csv()
    
    # Step 2: Check and upload any CSV files to S3
    check_and_upload_csv_files()


# Schedule to run every 10 seconds
schedule.every(10).seconds.do(create_and_upload)

# Run the scheduler
if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)
