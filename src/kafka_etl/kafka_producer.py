from confluent_kafka import Producer as ConfluentProducer
import json
import logging
import os
from dotenv import load_dotenv

load_dotenv()

local_conf = {
    'bootstrap.servers': 'sales:9092'
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaProducer")

class KafkaProducer:
    def __init__(self):
        self.producer = ConfluentProducer(local_conf)

    def send_message(self, topic: str, message: dict, key: str = None):
        try:
            serialized_message = json.dumps(message).encode("utf-8")
            serialized_key = key.encode("utf-8") if key else None
            self.producer.produce(topic, serialized_message, key=serialized_key)
            self.producer.poll(0)
            logger.info("Message sent to %s: %s with key: %s", topic, message, key)
        except Exception as e:
            logger.error("Failed to send message to %s: %s", topic, e)

    def close(self):
        self.producer.flush()
