import threading
import time
import os
from kafka_etl.kafka_producer import KafkaProducer
from kafka_etl.kafka_consumer import KafkaConsumer
from backend.creating_files.files_generator.csv_generator import create_sales_csv

# Ensure the consolidated data folder exists
data_folder = "data_consolidated"
os.makedirs(data_folder, exist_ok=True)

def generate_csv_and_notify():
    """Generate a CSV file every 60 seconds and notify Kafka."""
    producer = KafkaProducer()
    try:
        while True:
            try:
                print("Generating new CSV file...")
                create_sales_csv()
                print("CSV file generated.")

                # Notify Kafka to trigger pipeline
                producer.send_message(
                    "file-generated",
                    {"CSV_GENERATED": "New CSV generated and ready for processing"},
                    key="csv"
                )
                print("Notification sent to Kafka.")
            except Exception as e:
                print(f"Error generating or notifying about CSV: {e}")
            time.sleep(60)
    finally:
        producer.close()

def start_consumer():
    """Starting Kafka consumer to process messages."""
    consumer = KafkaConsumer(topics=["file-generated", "S3-bucket", "postgres-db"])
    try:
        consumer.consume_messages()
    except Exception as e:
        print(f"Consumer encountered an error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    # Start CSV generation and notification
    csv_thread = threading.Thread(target=generate_csv_and_notify, daemon=True)

    # Start Kafka consumer
    consumer_thread = threading.Thread(target=start_consumer, daemon=True)

    try:
        # Start both threads
        csv_thread.start()
        consumer_thread.start()

        # Wait for threads to complete
        csv_thread.join()
        consumer_thread.join()
    except KeyboardInterrupt:
        print("Pipeline execution interrupted.")
    except Exception as e:
        print(f"Error in main execution: {e}")
