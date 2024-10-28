from confluent_kafka import Producer as ConfluentProducer
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'group.id': 'etl-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD')
}

# Configuring logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

class KafkaProducer:
    def __init__(self):
        # Configure the producer with SASL_SSL for Confluent Cloud
        self.producer = ConfluentProducer(conf)

    def send_message(self, topic: str, message: dict):
        """Send message to 'S3-bucket' topic when a CSV file reaches the S3 bucket."""
        try:
            self.producer.produce(topic, json.dumps(message).encode("utf-8"))
            logger.info("Message sent to %s: %s", topic, message)

            self.producer.poll(0)
        except Exception as e:
            logger.error("Failed to send message to %s: %s", topic,e)

    def close(self):
        self.producer.flush()
        self.producer.close()


