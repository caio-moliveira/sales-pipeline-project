# Sales ETL and Real-Time KPI Dashboard

This project demonstrates an end-to-end ETL pipeline, from data generation to a real-time KPI dashboard, showcasing data engineering and processing skills. The project leverages a combination of cloud storage, ETL practices, and real-time data visualization to simulate a sales pipeline. This README details the project’s architecture, main components, and instructions for running the project locally or in Docker containers.

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies and Libraries Used](#technologies-and-libraries-used)
- [Architecture](#architecture)
- [Setup Instructions](#setup-instructions)
- [Data Flow](#data-flow)
- [Key Performance Indicators (KPIs)](#key-performance-indicators-kpis)

---

## Project Overview

The project simulates a daily sales sheet generation system where a CSV file is created to represent daily sales data. This data is uploaded to an AWS S3 bucket, which acts as the staging area. A scheduled process monitors this bucket for new files. Once detected, an ETL pipeline is initiated to:
1. **Extract**: Retrieve the file from the S3 bucket.
2. **Transform**: Clean and validate data using `pandas` and `pydantic` for data integrity.
3. **Load**: Insert the data into a PostgreSQL database.

The data is then visualized in a Streamlit-powered web application that accesses the PostgreSQL database and displays key sales KPIs in real time.

![architecture](![image](![app](https://github.com/user-attachments/assets/066cd9d0-4056-4812-9109-7f7ad9c09028)

## Technologies and Libraries Used

### Infrastructure
- **AWS S3**: Storage of sales data files, acting as the data source for ETL.
- **PostgreSQL**: Storage for transformed data, enabling real-time data retrieval.
- **Docker**: Containerization of services for easy deployment and management.
- **Kafka**: Message queuing system for monitoring and triggering ETL processes.

### Development and Analysis Tools
- **DBeaver**: Database management and querying.
  
### Main Python Libraries
- **pandas**: Data manipulation and transformation.
- **boto3**: AWS SDK for Python, used to interact with S3.
- **Faker**: Simulation of sales data.
- **pydantic**: Data validation, ensuring data quality in each pipeline step.
- **sqlalchemy**: ORM for data insertion into PostgreSQL.
- **streamlit**: Real-time KPI dashboard.
- **confluent_kafka**: Interface for Kafka, handling event-driven ETL execution.

## Architecture

This project follows a modular ETL pipeline and visualization architecture. Each component is responsible for a specific function:

![architecture](![image](https://github.com/user-attachments/assets/2e2d9a63-51da-42b4-b245-65fdf8cd7941)


1. **Data Generation**: `csv_generator.py` creates a sales data CSV with simulated daily sales.
2. **S3 Storage and Monitoring**: Files are stored in an S3 bucket and monitored by a Kafka topic that triggers the ETL process upon file arrival.
3. **ETL Pipeline**:
   - **Extraction**: Pulls CSV from S3.
   - **Transformation**: Validates and cleans data using `pydantic`.
   - **Loading**: Inserts data into PostgreSQL.
4. **Visualization**: A Streamlit application that pulls from PostgreSQL to provide real-time KPIs.

## Setup Instructions

### Prerequisites

- Docker
- AWS CLI configured with S3 access
- PostgreSQL
- Kafka
  
### Running the Project

1. **Clone the repository**:
   ```bash
   git clone git@github.com/caio-moliveira/sales-pipeline-project.git
   cd sales-pipeline-project
   ```
2. **Create and configure** `.env` **file** for PostgreSQL, AWS, and Kafka settings.


3. **Run Docker Services**
  
  ```bash
  docker-compose up --build
  ```

4. **Environment Variables**

This project requires specific environment variables to be set for proper functioning. Below is a description of each variable and its purpose. Make sure to include them in a `.env` file at the root of the project.

### AWS Configuration
- **`AWS_ACCESS_KEY_ID`**: Your AWS access key for authentication.
- **`AWS_SECRET_ACCESS_KEY`**: Your AWS secret access key.
- **`AWS_REGION`**: The AWS region where your services are hosted (e.g., `us-east-1`).
- **`BUCKET_NAME`**: The name of your AWS S3 bucket.

### PostgreSQL Database Configuration
- **`POSTGRES_USER`**: The username for your PostgreSQL database.
- **`POSTGRES_PASSWORD`**: The password for your PostgreSQL database.
- **`POSTGRES_HOST`**: The host address of your PostgreSQL database.
- **`POSTGRES_DB`**: The name of your PostgreSQL database.

### Kafka Configuration
- **`BOOTSTRAP_SERVERS`**: The Kafka broker(s) to connect to (e.g., `broker1:9092,broker2:9092`).
- **`SASL_USERNAME`**: Your Kafka username.
- **`SASL_PASSWORD`**: Your Kafka password.
- **`CLIENT_ID`**: The unique identifier for the Kafka client.

#### Setting Up
1. Create a `.env` file in the root of the project.
2. Copy the example below and replace placeholder values with your actual credentials.
3. Ensure the `.env` file is excluded from version control using `.gitignore` to keep sensitive information private.

### Example `.env` File
```env
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_REGION=your_aws_region
BUCKET_NAME=your_bucket_name

POSTGRES_USER=your_postgres_username
POSTGRES_PASSWORD=your_postgres_password
POSTGRES_HOST=your_postgres_host
POSTGRES_DB=your_postgres_database

BOOTSTRAP_SERVERS=your_kafka_bootstrap_servers
SASL_USERNAME=your_kafka_username
SASL_PASSWORD=your_kafka_password
CLIENT_ID=your_kafka_client_id
```

 ## Data Flow

1. **Data Generation**: 
   - Uses `Faker` to simulate realistic sales data, stored as a CSV in S3.
2. **Extraction**:
   - `boto3` fetches the latest sales CSV from S3.
3. **Transformation**:
   - `pandas` and `pydantic` perform basic data cleaning and validation.
4. **Loading**:
   - `sqlalchemy` ORM maps data to PostgreSQL.
5. **Visualization**:
   - Streamlit dashboard connects to PostgreSQL, presenting KPIs in real time.

## Key Performance Indicators (KPIs)

The dashboard displays the following KPIs:
- **Total Sales**: Sum of daily sales.
- **Average Transaction Value**: Average value of transactions.
- **Top-selling Products**: Highest volume products for the day.
- **Sales by Category**: Breakdown of sales by product category.
- **Sales Trend**: A real-time chart tracking sales over time.

