# Sales ETL and Real-Time KPI Dashboard

This project demonstrates an end-to-end ETL pipeline, from data generation to a real-time KPI dashboard, showcasing data engineering and processing skills. The project leverages a combination of cloud storage, ETL practices, and real-time data visualization to simulate a sales pipeline. This README details the project’s architecture, main components, and instructions for running the project locally or in Docker containers.

## Table of Contents

- [Project Overview](#project-overview)
- [Technologies and Libraries Used](#technologies-and-libraries-used)
- [Architecture](#architecture)
- [Setup Instructions](#setup-instructions)
- [Data Flow](#data-flow)
- [Key Performance Indicators (KPIs)](#key-performance-indicators-kpis)
- [Future Improvements](#future-improvements)

---

## Project Overview

The project simulates a daily sales sheet generation system where a CSV file is created to represent daily sales data. This data is uploaded to an AWS S3 bucket, which acts as the staging area. A scheduled process monitors this bucket for new files. Once detected, an ETL pipeline is initiated to:
1. **Extract**: Retrieve the file from the S3 bucket.
2. **Transform**: Clean and validate data using `pandas` and `pydantic` for data integrity.
3. **Load**: Insert the data into a PostgreSQL database.

The data is then visualized in a Streamlit-powered web application that accesses the PostgreSQL database and displays key sales KPIs in real time.

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

1. **Data Generation**: `csv_generator.py` creates a sales data CSV with simulated daily sales.
2. **S3 Storage and Monitoring**: Files are stored in an S3 bucket and monitored by a Kafka topic that triggers the ETL process upon file arrival.
3. **ETL Pipeline**:
   - **Extraction**: Pulls CSV from S3.
   - **Transformation**: Validates and cleans data using `pydantic`.
   - **Loading**: Inserts data into PostgreSQL.
4. **Visualization**: A Streamlit application that pulls from PostgreSQL to provide real-time KPIs.

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- AWS CLI configured with S3 access
- PostgreSQL and Kafka should be accessible (or run through Docker)
  
### Running the Project

1. **Clone the repository**:
   ```bash
   git clone https://github.com/username/sales-etl-dashboard.git
   cd sales-etl-dashboard
