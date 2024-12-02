import os
import pandas as pd
import boto3
from pydantic import BaseModel, Field
from datetime import date
from dotenv import load_dotenv
from typing import List, Optional
from io import BytesIO
import chardet

# Load environment variables for AWS credentials
load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('BUCKET_NAME')

# Initialize S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# Track files that have already been downloaded
downloaded_files = set()

# Define Pydantic model for row validation
class SalesRecord(BaseModel):
    sale_date: date = Field(alias="Sale Date")
    sale_id: str = Field(alias="Sale ID")
    product_id: str = Field(alias="Product ID")
    product_name: str = Field(alias="Product Name")
    product_category: str = Field(alias="Product Category")
    quantity_sold: int = Field(alias="Quantity Sold", ge=1)
    unit_price: float = Field(alias="Unit Price", ge=0)
    discount: Optional[int] = Field(alias="Discount (%)", ge=0, le=100) 
    total_value: float = Field(alias="Total Value (with Discount)", ge=0)
    unit_cost: float = Field(alias="Unit Cost", ge=0)
    total_cost: float = Field(alias="Total Cost", ge=0)
    gross_profit: float = Field(alias="Gross Profit")
    payment_method: str = Field(alias="Payment Method")
    payment_status: str = Field(alias="Payment Status")
    payment_date: Optional[date] = Field(alias="Payment Date")
    customer_id: str = Field(alias="Customer ID")
    customer_name: str = Field(alias="Customer Name")
    sales_channel: str = Field(alias="Sales Channel")
    sales_region: str = Field(alias="Sales Region")
    sales_rep: str = Field(alias="Sales Representative")
    customer_rating: Optional[str] = Field(alias="Customer Rating")
    shipping_cost: float = Field(alias="Shipping Cost", ge=0)
    delivery_status: str = Field(alias="Delivery Status")
    delivery_date: Optional[date] = Field(alias="Delivery Date")

def download_csv_files_from_s3(prefix='CSV/'):
    """Download new CSV files from the S3 'CSV/' folder."""
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    csv_files = []

    for item in response.get('Contents', []):
        file_key = item['Key']
        if file_key.endswith('.csv') and file_key not in downloaded_files:
            print(f"Downloading new file {file_key} from S3 bucket.")
            csv_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)

            # Detect encoding using chardet
            raw_content = csv_obj['Body'].read()
            encoding = chardet.detect(raw_content)['encoding']

            # Decode and load CSV content into DataFrame with detected encoding
            csv_df = pd.read_csv(BytesIO(raw_content), encoding=encoding)
            csv_files.append(csv_df)

            # Mark the file as downloaded
            downloaded_files.add(file_key)

    return csv_files

