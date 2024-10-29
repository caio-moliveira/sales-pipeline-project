import boto3
import os
from pydantic import BaseModel, FilePath, constr, ValidationError
from dotenv import load_dotenv

# Load environment variables for AWS credentials
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Pydantic model to validate input parameters
class S3UploadModel(BaseModel):
    file_path: FilePath                 # Ensures the file exists locally
    bucket_name: constr(min_length=3)   # Minimum length for S3 bucket names
    object_name: str                    # Object name for storing in S3

def check_and_upload_csv_files():
    """Check for CSV files in 'data' and upload them to the S3 bucket under 'CSV/' prefix."""
    # Path to the data folder
    local_data_folder = 'data'
    
    # Verify that the local data folder exists
    if not os.path.exists(local_data_folder):
        print(f"The folder '{local_data_folder}' does not exist.")
        return []

    # Get all CSV files in the data folder
    csv_files = [f for f in os.listdir(local_data_folder) if f.endswith('.csv')]
    
    # Check if there are any CSV files to upload
    if not csv_files:
        print(f"No CSV files found in '{local_data_folder}' folder.")
        return []

    # Configure the S3 client with credentials
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # List to keep track of successfully uploaded file paths
    uploaded_paths = []
    
    # Process each CSV file
    for csv_file in csv_files:
        file_path = os.path.join(local_data_folder, csv_file)
        
        # Set the S3 object name with the 'CSV/' prefix
        object_name = f"CSV/{csv_file}"
        
        # Validate the parameters with Pydantic
        try:
            validated_data = S3UploadModel(
                file_path=file_path,
                bucket_name=BUCKET_NAME,
                object_name=object_name
            )
        except ValidationError as ve:
            print(f"Validation error for file '{file_path}': {ve}")
            continue
        
        # Attempt to upload the file to S3
        try:
            s3_client.upload_file(
                validated_data.file_path,
                validated_data.bucket_name,
                validated_data.object_name
            )
            print(f"File '{validated_data.file_path}' successfully uploaded to S3 bucket '{validated_data.bucket_name}' as '{validated_data.object_name}'.")
            uploaded_paths.append(validated_data.object_name)  # Add to the list of uploaded paths
        except Exception as e:
            print(f"Error uploading file '{file_path}' to S3: {e}")

    # Return the list of uploaded file paths
    return uploaded_paths
