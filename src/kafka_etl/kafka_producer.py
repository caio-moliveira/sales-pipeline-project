from confluent_kafka import Producer as ConfluentProducer
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()


# Configuration for Confluent Cloud
confluent_conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'group.id': 'etl-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD')
}

# Configuration for Local Kafka Broker
local_conf = {
    'bootstrap.servers': 'sales:9092',
    'group.id': 'etl-group',
    'auto.offset.reset': 'earliest'
}

# Configuring logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

class KafkaProducer:
    def __init__(self):
        # Configure the producer with SASL_SSL for Confluent Cloud
        self.producer = ConfluentProducer(local_conf) # or use confluent_conf 

    def send_message(self, topic: str, message: dict, key: str = None):
        """Send message to 'S3-bucket' topic when a CSV file reaches the S3 bucket."""
        try:
            serialized_message = json.dumps(message).encode("utf-8")
            serialized_key = key.encode("utf-8") if key else None

            self.producer.produce(topic, serialized_message, key=serialized_key)
            self.producer.poll(0)
            logger.info("Message sent to %s: %s with key: %s", topic, message, key)
        except Exception as e:
            logger.error("Failed to send message to %s: %s", topic,e)

    def close(self):
        self.producer.flush()



