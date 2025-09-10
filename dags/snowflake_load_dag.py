from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import sys 

sys.path.append('/opt/airflow')
from src.utils.minio import get_minio_client, extract_json_as_jsonl_from_minio
from src.utils.kafka import parse_message, extract_filepath_from_message
from src.utils.snowflake import get_snowflake_connection, copy_file_to_snowflake
from src.utils.logger import setup_logger

def apply_function(*args, **kwargs):
    logger = setup_logger('apply_function_task', 'snowflake_load_dag.log', 'loading')
    filepath = parse_message(args, logger)
    return filepath

trigger = MessageQueueTrigger(
    queue="kafka://kafka:9092/minio-events",
    apply_function="snowflake_load_dag.apply_function"
)

kafka_topic_asset = Asset(
    name="kafka_topic_asset", watchers=[AssetWatcher(name="kafka_watcher", trigger=trigger)]
)

@dag(
    dag_id="load_to_snowflake",
    schedule=[kafka_topic_asset],
    tags=["Loading", "Kafka", "MinIO", "Snowflake"]
)
def load_photos_to_snowflake_dag():
        
    @task
    def load_to_snowflake_task(triggering_asset_events=None):
        logger = setup_logger('load_to_snowflake_task', 'snowflake_load_dag.log', 'loading')
        minio_filepath = extract_filepath_from_message(triggering_asset_events[kafka_topic_asset], logger)
        minio_client = get_minio_client()
        photos_data_jsonl_path = extract_json_as_jsonl_from_minio(minio_client, minio_filepath, logger)        
        snowflake_connection = get_snowflake_connection()
        copy_file_to_snowflake(snowflake_connection, photos_data_jsonl_path, logger)

    load_to_snowflake_task()

load_photos_to_snowflake_dag()