from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables for PostgreSQL credentials
load_dotenv()

# PostgreSQL configuration
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

# Create a connection string
db_url = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def test_postgres_connection():
    """Test connection to the PostgreSQL database."""
    try:
        # Initialize database engine
        engine = create_engine(db_url)

        # Connect and execute a test query
        with engine.connect() as connection:
            result = connection.execute("SELECT 1")
            print("Connection successful! Test query result:", result.scalar())

    except Exception as e:
        print("Failed to connect to the database:", e)

if __name__ == "__main__":
    test_postgres_connection()
