"""Connect to kafka, consume messages, and post to HAPI FHIR server."""
import os
import time
import logging
from typing import Generator
import asyncio
import aiohttp
import aiofiles

from kafka import send_to_kafka
from utils import convert_to_json, add_request_metadata, get_resource_order_index
from confluent_kafka import Consumer


logging.basicConfig(level=logging.INFO)


HAPI_FHIR_URL = os.getenv("HAPI_FHIR_URL", "http://hapi-fhir:8080/fhir")
HAPI_FHIR_BUNDLE_SIZE = os.getenv("HAPI_FHIR_BUNDLE_SIZE", 72)
TOPIC = os.getenv("TOPIC", "migration")
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka-01:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "clickhouse-migration")
ERROR_TOPIC = os.getenv("ERROR_TOPIC", "migration-errors")
HAPI_FHIR_BATCH_SIZE = os.getenv("HAPI_FHIR_BATCH_SIZE", 150)


async def post_to_hapi_fhir(fhir_url: str, fhir_bundle: dict, session) -> None:
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
        
        # Write Fhir Entry IDs to a file for later processing
        fhir_resource_ids: list[str] = [entry.get("resource").get("id")for entry in fhir_bundle.get("entry")]
        async with aiofiles.open("posted_fhir_resource_ids.txt", mode='a') as f:
            await f.write("\n".join(fhir_resource_ids) + "\n")
    else:
        response_text = await response.text()
        logging.error(f"Failed to post to HAPI FHIR server: {response.status} - {response_text}")
        
        # If posting to HAPI FHIR server fails, log resource ID and send bundle to error topic
        fhir_resource_ids: list[str] = [entry.get("resource").get("id")for entry in fhir_bundle.get("entry")]
        async with aiofiles.open("failed_fhir_resource_ids.txt", mode='a') as f:
            await f.write("\n".join(fhir_resource_ids) + "\n")
        
        send_to_kafka(fhir_bundle, ERROR_TOPIC)



def consume_messages(consumer: Consumer, topic: str, batch_size: int) -> Generator:
    """Consume messages from the migration topic and return message batches to the caller."""

    try:
        logging.info("Consuming messages from migration")
        consumer.subscribe([topic])

        total_processed: int = 0
        empty_message_counter: int = 0
        fhir_bundle_batch: list[dict] = []

       
        while True:
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
                    
                    # Add request metadata to the message and append to the batch
                    fhir_bundle_batch.append(add_request_metadata(convert_to_json(msg.value())))
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
    start = time.perf_counter()

    conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': KAFKA_GROUP_ID,
        'auto.offset.reset': 'smallest'
    }

    consumer = Consumer(conf)

    async with aiohttp.ClientSession() as session:

        for batch in consume_messages(consumer, topic, HAPI_FHIR_BATCH_SIZE):
            tasks = [asyncio.create_task(post_to_hapi_fhir(HAPI_FHIR_URL, bundle, session)) for bundle in batch]

            await asyncio.gather(*tasks)


    elapsed = time.perf_counter() - start
    logging.info(f"Elapsed time: {elapsed:0.2f} seconds")


if __name__ == "__main__":
    asyncio.run(main(TOPIC))