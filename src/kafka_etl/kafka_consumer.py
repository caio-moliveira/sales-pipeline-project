import json
import logging
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException
from backend.creating_files.loader_S3.loader import check_and_upload_csv_files
from backend.etl.extract import download_csv_files_from_s3
from backend.etl.transform import validate_and_clean_data
from backend.etl.load import load_data_to_postgres
from kafka_etl.kafka_producer import KafkaProducer
import os

# Kafka consumer configuration
local_conf = {
    'bootstrap.servers': 'sales:9092',
    'group.id': 'etl-group',
    'auto.offset.reset': 'earliest',
}

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

class KafkaConsumer:
    def __init__(self, topics: list):
        self.consumer = ConfluentConsumer(local_conf)
        self.consumer.subscribe(topics)

    def consume_messages(self):
        """Consume messages and trigger pipeline steps."""
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Consumer error: %s", msg.error())
                    continue

                # Decode message
                message_value = json.loads(msg.value().decode('utf-8'))
                logger.info("Received message from %s: %s", msg.topic(), message_value)

                # Trigger appropriate step based on the topic
                if msg.topic() == "file-generated":
                    self.handle_file_generated(message_value)
                elif msg.topic() == "S3-bucket":
                    self.handle_s3_upload(message_value)
                elif msg.topic() == "postgres-db":
                    self.handle_postgres_upload(message_value)

        except Exception as e:
            logger.error(f"Error during message consumption: {e}")
        finally:
            self.close()

    def handle_file_generated(self, message):
        """Handle 'file-generated' topic."""
        try:
            check_and_upload_csv_files()

            # Send Kafka message after S3 upload
            
            producer = KafkaProducer()
            producer.send_message(
                "S3-bucket",
                {"UPLOADED": "File uploaded to S3"},
                key="s3_upload"
            )
            producer.close()
        except Exception as e:
            logger.error(f"Error handling file-generated: {e}")

    def handle_s3_upload(self, message):
        """Handle 'S3-bucket' topic."""
        try:
            logger.info("Running ETL pipeline...")
            # Step 1: Download CSV files from S3
            new_dataframes = download_csv_files_from_s3()
            if not new_dataframes:
                logger.info("No new files to process.")
                return

            # Step 2: Validate and clean data
            cleaned_data = validate_and_clean_data(new_dataframes)

            # Save cleaned data locally for reference
            output_path = os.path.join("data_consolidated", "last_csv_updated.csv")
            cleaned_data.to_csv(output_path, index=False)

            # Step 3: Load cleaned data into PostgreSQL
            load_data_to_postgres(output_path)

            # Send Kafka message after successful data load
            producer = KafkaProducer()
            producer.send_message(
                "postgres-db",
                {"UPLOADED": "Data uploaded to Postgres"},
                key="db_upload"
            )
            producer.close()
        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}")
    def handle_postgres_upload(self, message):
        """Handle 'postgres-db' topic."""
        logger.info("Pipeline completed! Dashboard has been updated!")

    def close(self):
        """Close the consumer."""
        self.consumer.close()
