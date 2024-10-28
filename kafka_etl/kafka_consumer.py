import json
import logging
import os
from dotenv import load_dotenv
from confluent_kafka import Consumer as ConfluentConsumer, KafkaException

# Load environment variables for Kafka credentials
load_dotenv()

# Kafka consumer configuration
conf = {
    'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
    'group.id': 'etl-consumer-group',
    'auto.offset.reset': 'earliest',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('SASL_USERNAME'),
    'sasl.password': os.getenv('SASL_PASSWORD')
}

# Configuring logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaConsumer")

class KafkaConsumer:
    def __init__(self, topics: list):
        # Initialize the Confluent consumer with the configuration and subscribe to topics
        self.consumer = ConfluentConsumer(conf)
        self.consumer.subscribe(topics)

    def consume_messages(self):
        """Continuously listen to messages from subscribed topics."""
        try:
            while True:
                msg = self.consumer.poll(1.0)  # Poll for new messages
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaException._PARTITION_EOF:
                        logger.info("End of partition reached %s %s", msg.topic(), msg.partition())
                    else:
                        logger.error("Consumer error: %s", msg.error())
                    continue

                # Decode and log the message
                message_value = json.loads(msg.value().decode('utf-8'))
                logger.info("Received message from %s: %s", msg.topic(), message_value)
        except KeyboardInterrupt:
            pass
        finally:
            self.close()

    def close(self):
        """Close the Kafka consumer connection."""
        self.consumer.close()
        
