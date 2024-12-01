import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


# Load environment variables for PostgreSQL credentials
load_dotenv()

# PostgreSQL configuration
POSTGRES_USER = os.getenv("POSTGRES_USER")  
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
#POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DB = os.getenv("POSTGRES_DB")

# Create a connection string
db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@postgres_db/{POSTGRES_DB}"

# Initialize database engine
engine = create_engine(db_url)
Base = declarative_base()
# Configurar a sessão do SQLAlchemy
Session = sessionmaker(bind=engine)

# Define the SalesData table based on SalesRecord fields
class SalesData(Base):
    __tablename__ = 'sales_data'
    
    sale_date = Column(Date, nullable=False)  # Changed to Date
    sale_id = Column(String, primary_key=True, nullable=False)
    product_id = Column(String, nullable=False)
    product_name = Column(String, nullable=False)
    product_category = Column(String, nullable=False)
    quantity_sold = Column(Integer, nullable=False)
    unit_price = Column(Float, nullable=False)
    discount = Column(Integer, nullable=True)  # Changed to Integer
    total_value = Column(Float, nullable=False)
    unit_cost = Column(Float, nullable=False)
    total_cost = Column(Float, nullable=False)
    gross_profit = Column(Float, nullable=False)
    payment_method = Column(String, nullable=False)
    payment_status = Column(String, nullable=False)
    payment_date = Column(Date, nullable=True)  # Changed to Date
    customer_id = Column(String, nullable=False)
    customer_name = Column(String, nullable=False)
    sales_channel = Column(String, nullable=False)
    sales_region = Column(String, nullable=False)
    sales_rep = Column(String, nullable=False)
    customer_rating = Column(String, nullable=True)
    shipping_cost = Column(Float, nullable=False)
    delivery_status = Column(String, nullable=False)
    delivery_date = Column(Date, nullable=True)  # Changed to Date

    # Function to create the sales_data table if it doesn't exist
def create_sales_data_table():
    try:
        # Create the table
        Base.metadata.create_all(engine)
        print("Table sales_data created successfully or already exists.")
    except SQLAlchemyError as e:
        print(f"Error creating table: {e}")


def load_data_to_postgres(file_path, table_name='sales_data'):
    """Load new data from a CSV file to a PostgreSQL database table, avoiding duplicates via unique constraint."""
    try:
        # Load CSV into a DataFrame
        df = pd.read_csv(file_path, encoding='utf-8')

        # Insert new data into the PostgreSQL table, handling duplicates with the unique constraint
        with engine.connect() as connection:
                # Insert directly, allowing PostgreSQL to enforce the unique constraint
                df.to_sql(table_name, connection, if_exists='append', index=False)
                print(f"New data from {file_path} loaded successfully into the '{table_name}' table.")
    except Exception as e:
        print(f"An error occurred while loading data to PostgreSQL: {e}")


