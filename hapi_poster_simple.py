"""Connect to kafka, consume messages, and post to HAPI FHIR server."""
import json
import logging
from typing import Generator
import asyncio
import aiohttp

from confluent_kafka import Consumer, KafkaException, KafkaError
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

def get_resource_order_index(resource_type: str) -> int:
    """Get the index of the resource type in the resource order list."""
    
    resource_order = ["Organization", "Patient", "Encounter", "DiagnosticReport", "OperationOutcome"]
    
    try:
        return resource_order.index(resource_type)
    except ValueError:
        return len(resource_order)

async def post_to_hapi_fhir(fhir_url: str, fhir_bundle: list, session) -> None:
    """Post a bundle of messages to the HAPI FHIR server."""

    logging.info("Posting to HAPI FHIR")


    headers= {
      'Content-Type': 'application/fhir+json',
      'X-Upsert-Existence-Check': 'disabled',
      'Authorization': 'Custom test',
    }

    
    response = await session.post(fhir_url, json=fhir_bundle, headers=headers)
    if response.status == 200:
        logging.info("Successfully posted to HAPI FHIR server")
    else:
        response_text = await response.text()
        logging.error(f"Failed to post to HAPI FHIR server: {response.status} - {response_text}")
        # TODO: Send to dead letter queue




def consume_messages(consumer: Consumer, topic: str, batch_size: int) -> Generator:
    """Consume messages from the migration topic and post them to the HAPI FHIR server."""

    try:
        logging.info("Consuming messages from migration")
        consumer.subscribe([topic])

        total_processed: int = 0
        empty_message_counter: int = 0
        fhir_bundle_batch: list[dict] = []

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
                    fhir_bundle_batch.append(enrich_bundle(convert_to_json(msg.value())))
                    if len(fhir_bundle_batch) == batch_size:
                        fhir_bundle_batch.sort(key=lambda x: get_resource_order_index(x.get("entry")[0].get("resource").get("resourceType")))
                        yield fhir_bundle_batch
                        fhir_bundle_batch = []

    except Exception as e:
        logging.error(f"Error consuming messages: {e}")
    finally:
        if fhir_bundle_batch:
            yield fhir_bundle_batch

        logging.info(f"Finished, processed {total_processed} messages")
        consumer.close()




async def main(topic: str) -> None:
   
    
    logging.info("Starting main function")
    tasks = []
    start = time.perf_counter()
    batch_size = 50

    conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'smallest'
    }

    consumer = Consumer(conf)
    
    
    async with aiohttp.ClientSession() as session:

        for batch in consume_messages(consumer, topic, batch_size):
            tasks = [asyncio.create_task(post_to_hapi_fhir(HAPI_FHIR_URL, bundle, session)) for bundle in batch]

            await asyncio.gather(*tasks)


    elapsed = time.perf_counter() - start
    logging.info(f"Elapsed time: {elapsed:0.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main(TOPIC))