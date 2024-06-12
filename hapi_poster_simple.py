"""Connect to kafka, consume messages, and post to HAPI FHIR server."""
import json
import logging
import sys
from confluent_kafka import Consumer, KafkaException, KafkaError
import requests
import time

logging.basicConfig(level=logging.INFO)

HAPI_FHIR_URL = "http://hapi-fhir:8080/fhir"
HAPI_FHIR_BUNDLE_SIZE: int = 72
TOPIC = "migration"
KAFKA_HOST = "kafka-01:9092"
KAFKA_GROUP_ID = "clickhouse-migration"


running = True



def convert_to_json(bundle):
    return json.loads(bundle.decode('utf-8'))


def enrich_bundle(bundle: dict) -> dict:
    """Add FHIR Request metadata."""

    for entry in bundle.get("entry"):
        entry["request"] = {
        "method": "PUT" if entry.get("resource").get("id") else "POST",
        "url": f"{entry['resource']['resourceType']}/{entry['resource']['id']}"
      }

    return bundle


def post_to_hapi_fhir(fhir_url: str, fhir_bundle: list) -> None:
    """Post a bundle of messages to the HAPI FHIR server."""

    logging.info("Posting to HAPI FHIR")

    headers= {
      'Content-Type': 'application/fhir+json',
      'X-Upsert-Existence-Check': 'disabled',
      'Authorization': 'Custom test',
    }

    response = requests.post(fhir_url, json=fhir_bundle, headers=headers)
    if response.status_code == 200:
        logging.info("Successfully posted to HAPI FHIR server")
    else:
        logging.error(f"Failed to post to HAPI FHIR server: {response.status_code} - {response.text}")
    


def consume_messages(consumer: Consumer, topic: str) -> None:
    """Consume messages from the migration topic and post them to the HAPI FHIR server."""

    try:
        logging.info("Consuming messages from migration")
        consumer.subscribe([topic])

        total_processed: int = 0
        empty_message_counter: int = 0

        while running:
            if empty_message_counter == 10:
                logging.info("No messages to consume, exiting")
                break
            else:
                msg = consumer.poll(timeout=3.0)
                if msg is None:
                    empty_message_counter += 1
                    logging.info("No message to consume")
                    logging.info(f"Empty message counter: {empty_message_counter}")
                    continue

                else:
                    total_processed += 1
                    logging.info(f"Consumed messages: {total_processed}")
                    # Enrich message
                    bundle = enrich_bundle(convert_to_json(msg.value()))
                    post_to_hapi_fhir(HAPI_FHIR_URL, bundle)
                    # quit()

    except Exception as e:
        logging.error(f"Error consuming messages: {e}")
    finally:
        logging.info(f"Finished, processed {total_processed} messages")
        consumer.close()



def main(consumer: Consumer, topic: str) -> None:
   
    
    logging.info("Starting main function")
    start = time.perf_counter()
    consume_messages(consumer, topic)

    elapsed = time.perf_counter() - start
    logging.info(f"Elapsed time: {elapsed:0.2f} seconds")


if __name__ == "__main__":
     
    conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'smallest'
    }

    consumer = Consumer(conf)
    main(consumer, TOPIC)