# Use a lightweight Python image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main ETL script and the Kafka files
COPY main.py .
COPY kafka_etl/kafka_consumer.py .
COPY kafka_etl/kafka_producer.py .

# Run the ETL script
CMD ["python", "main.py"]
