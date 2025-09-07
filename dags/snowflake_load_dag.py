from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import json
import urllib.parse
import sys 

sys.path.append('/opt/airflow')
from src.utils.minio import get_minio_client, extract_json_as_jsonl_from_minio
from src.utils.snowflake import get_snowflake_connection, copy_photos_to_snowflake, create_photos_bronze_table
from src.utils.logger import setup_logger

def apply_function(*args, **kwargs):
    message = args[-1]
    try:
        val = json.loads(message.value())
        key = urllib.parse.unquote(val.get('Key', ''))
        print(f"File uploaded: {key}")
        return {"filepath": key, "event": val}
    except Exception as e:
        print(f"Error parsing message: {e}")
        return {"error": str(e)}

trigger = MessageQueueTrigger(
    queue="kafka://kafka:9092/minio-events",
    apply_function="snowflake_load_dag.apply_function"
)

kafka_topic_asset = Asset(
    name="kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)]
)

@dag(schedule=[kafka_topic_asset])
def load_photos_to_snowflake_dag():
    
    @task
    def process_message(triggering_asset_events=None):
        logger = setup_logger('process_message_task', 'snowflake_load_dag.log', 'loading')

        logger.info("Message recieved, beginning load process")
        for event in triggering_asset_events[kafka_topic_asset]:
            logger.info(f"Message: {event}")

            logger.info("Extracting MinIO filepath")
            minio_filepath = event.extra.get('payload', {}).get('filepath')
            logger.info(f"Filepath recieved from event: {minio_filepath}")

            return minio_filepath
        
    @task
    def load_to_snowflake(minio_filepath: str):
        logger = setup_logger('load_to_snowflake_task', 'snowflake_load_dag.log', 'loading')
        if not minio_filepath:
            logger.info("No file to process")
            return
        
        logger.info(f"Extracting {minio_filepath} from MinIO")
        minio_client = get_minio_client()
        photos_data_jsonl = extract_json_as_jsonl_from_minio(minio_client, minio_filepath)
        
        logger.info(f"Connecting to Snowflake")
        snowflake_connection = get_snowflake_connection()
        with snowflake_connection.cursor() as snowflake_cursor:
            try:
                logger.info(f"Creating bronze tables")
                create_photos_bronze_table(snowflake_cursor)

                logger.info(f"Copying data to Snowflake")
                copy_photos_to_snowflake(snowflake_cursor, photos_data_jsonl)
            finally:
                snowflake_cursor.close()
                snowflake_connection.close()    

    minio_filepath = process_message()
    load_to_snowflake(minio_filepath)

load_photos_to_snowflake_dag()