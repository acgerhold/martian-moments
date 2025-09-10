import json
import urllib.parse
from kafka import KafkaProducer
from datetime import datetime, timezone
import os

def parse_message(args, logger):
    message = args[-1]
    try:
        val = json.loads(message.value())
        key = urllib.parse.unquote(val.get('Key', ''))

        logger.info(f"File uploaded - Key: {key}")
        return {"filepath": key, "event": val}
    except Exception as e:
        logger.error(f"Error parsing message - Error: {e}")
        return {"error": str(e)}
    
def extract_filepath_from_message(events, logger):
    for event in events:
        logger.info(f"Message: {event}")

        minio_filepath = event.extra.get('payload', {}).get('filepath')
        logger.info(f"Filepath extracted - Path: {minio_filepath}")

        if not minio_filepath:
            logger.info("No file to process")
            return None
        
        return minio_filepath
    
def produce_kafka_message(topic, message_data, logger):
    logger.info(f"Attempting to produce event to topic {topic}")
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        producer.send(topic, value=message_data)
        producer.flush()
        logger.info(f"Produced message - Topic: {topic}")
    except Exception as e:
        logger.error(f"Failed to produce message - Topic: {topic}, Error: {e}")
        raise
    finally:
        producer.close()

def generate_load_complete_message(minio_filepath, logger):
    logger.info("Generating load complete message")
    message = {
        "filepath": minio_filepath,
        "status": "success",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    }

    return message