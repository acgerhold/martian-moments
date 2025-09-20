import json
import urllib.parse
from kafka import KafkaProducer
from datetime import datetime, timezone
import os

def parse_kafka_message(args, topic_name, logger):
    payload = json.loads(args[-1].value())

    match topic_name:
        case "minio-events":
            return urllib.parse.unquote(payload.get('Key', ''))
        case "ingestion-scheduling":
            return payload.get('schedule')
        case "snowflake-load-complete":
            return payload.get('filepath')
        case _:
            logger.warning(f"Unknown topic: {topic_name}")
            return payload

def parse_message(args, logger):
    message = args[-1]
    try:
        val = json.loads(message.value())
        key = urllib.parse.unquote(val.get('Key', ''))

        logger.info(f"Message received - Key: {key}")
        return {"data": key, "event": val}
    except Exception as e:
        logger.error(f"Error Parsing Message - Key: {key}, Value: {val}, Error: {e}")
        return {"error": str(e)}
    
def extract_filepath_from_message(events, logger):
    logger.info(f"Attempting to extract data from message - {events}")
    for event in events:
        filepath = event.extra.get('payload', {}).get('data')

        if not filepath:
            logger.info("No data to process")
            return None
        
        logger.info(f"Filepath extracted - Data: {filepath}")
        return filepath
    
def extract_ingestion_schedule_from_message(events, logger):
    logger.info(f"Attempting to extract data from message - {events}")
    for event in events:
        ingestion_schedule_msg = event.extra.get('payload', {}).get('event')
        ingestion_schedule = ingestion_schedule_msg.get('schedule')

        if not ingestion_schedule:
            logger.info("No data to process")
            return None
        
        logger.info(f"Ingestion schedule extracted - {ingestion_schedule}")
        return ingestion_schedule
            
def produce_kafka_message(topic, message_data, logger):
    logger.info(f"Attempting to produce event - Topic: {topic}")
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
    logger.info(f"Attempting to generate load complete message - Path: {minio_filepath}")
    message = {
        "filepath": minio_filepath,
        "event": "success",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    }

    logger.info(f"Message produced - Message: {message}")
    return message

def generate_ingestion_schedule_message(ingestion_schedule, logger):
    logger.info(f"Attempting to generate ingestion schedule message - {ingestion_schedule}")
    message = {
        "schedule": ingestion_schedule,
        "event": "success",
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    }

    logger.info(f"Message produced - Message: {message}")
    return message
