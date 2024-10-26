import os
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from dotenv import load_dotenv

# Load environment variables for PostgreSQL credentials
load_dotenv()

# PostgreSQL configuration
POSTGRES_USER = os.getenv('POSTGRES_USER')  
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

# Create a connection string
db_url = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Initialize database engine
engine = create_engine(db_url)

def load_data_to_postgres(file_path, table_name='sales_data'):
    """Load data from a CSV file to a PostgreSQL database table."""
    try:
        # Load CSV into a DataFrame with UTF-8 encoding
        df = pd.read_csv(file_path, encoding='utf-8')
        
        # Insert data into the specified PostgreSQL table
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Data from {file_path} loaded successfully into the '{table_name}' table.")
    except Exception as e:
        print(f"An error occurred while loading data to PostgreSQL: {e}")