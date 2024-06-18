import os
import json
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError
import logging

logging.basicConfig(level=logging.INFO)
producer = Producer({'bootstrap.servers': os.environ.get("KAFKA_HOST", "kafka-01:9092")})


def send_to_kafka(message: str, topic: str) -> None:
    """Send a message kafka topic."""

    try:
        message = json.dumps(message).encode('utf-8')
        producer.produce(topic, value=message)
        producer.flush()
        logging.info(f"Sent message to Kafka topic: {topic}")
    except KafkaException as e:
        logging.info(f"Failed to send message to Kafka: {e}")
    except Exception as e:
        logging.info(f"Failed to send message to Kafka: {e}")