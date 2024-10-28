import os
import pandas as pd
from sqlalchemy import create_engine
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
    """Load new data from a CSV file to a PostgreSQL database table, avoiding duplicates via unique constraint."""
    try:
        # Load CSV into a DataFrame
        df = pd.read_csv(file_path, encoding='utf-8')

        # Insert new data into the PostgreSQL table, handling duplicates with the unique constraint
        with engine.connect() as connection:
            try:
                # Insert directly, allowing PostgreSQL to enforce the unique constraint
                df.to_sql(table_name, connection, if_exists='append', index=False)
                print(f"New data from {file_path} loaded successfully into the '{table_name}' table.")
            except Exception as e:
                print(f"An error occurred while inserting data to PostgreSQL: {e}")
    except Exception as e:
        print(f"An error occurred while loading data to PostgreSQL: {e}")


