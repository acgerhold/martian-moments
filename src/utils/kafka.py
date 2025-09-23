import json
import urllib.parse
from kafka import KafkaProducer

def parse_kafka_message(topic_name, args, logger):
    payload = json.loads(args[-1].value())
    logger.info(f"Processing Message - Topic: {topic_name}, Payload: {payload}")

    match topic_name:
        case "minio-events":
            minio_upload_path = urllib.parse.unquote(payload.get('Key', ''))
            logger.info(f"Parsed Message - Topic: {topic_name}, Upload Path: {minio_upload_path}")
            return minio_upload_path
        case "snowflake-load-complete":
            tmp_jsonl_staging_path = payload.get('tmp_jsonl_staging_path')
            logger.info(f"Parsed Message - Topic: {topic_name}, Staging Path: {tmp_jsonl_staging_path}")
            return tmp_jsonl_staging_path
        case "ingestion-scheduling":
            ingestion_schedule = payload.get('ingestion_schedule')
            logger.info(f"Parsed Message - Topic: {topic_name}, Ingestion Schedule: {ingestion_schedule}")
            return ingestion_schedule
        case _:
            logger.warning(f"Unknown topic: {topic_name}")
            return payload

def unwrap_airflow_asset_payload(events, logger):
    logger.info(f"Unwrapping data from AssetWatcher - Events: {events}")

    for event in events:
        payload = event.extra.get('payload')

        if not payload:
            logger.info("No payload found in AssetWatcher event")
            continue

        logger.info(f"Extracted data from AssetWatcher- Payload: {payload}")
        return payload
    
    logger.info("No event to process")
    return None
            
def produce_kafka_message(topic, message_data, logger):
    logger.info(f"Attempting to produce message - Topic: {topic}, Message: {message_data}")
    producer = KafkaProducer(
        bootstrap_servers='kafka:9092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        producer.send(topic, value=message_data)
        producer.flush()
        logger.info(f"Produced message - Topic: {topic}, Message: {message_data}")
    except Exception as e:
        logger.error(f"Failed to produce message - Topic: {topic}, Message: {message_data}, Error: {e}")
        raise
    finally:
        producer.close()