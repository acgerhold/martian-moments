from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import sys 

sys.path.append('/opt/airflow')
from src.utils.minio import get_minio_client, extract_json_as_jsonl_from_minio
from src.utils.kafka import parse_message, extract_filepath_from_message, produce_kafka_message, generate_load_complete_message
from src.utils.snowflake import get_snowflake_connection, copy_file_to_snowflake
from src.utils.logger import setup_logger
from src.config import LOAD_COMPLETE_TOPIC

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
    def extract_filepath_from_message_task(triggering_asset_events=None):
        logger = setup_logger('extract_filepath_from_message_task', 'snowflake_load_dag.log', 'loading')
        minio_filepath = extract_filepath_from_message(triggering_asset_events[kafka_topic_asset], logger)
        return minio_filepath

    @task
    def extract_json_as_jsonl_from_minio_task(minio_filepath):
        logger = setup_logger('extract_json_as_jsonl_from_minio_task', 'snowflake_load_dag.log', 'loading')
        minio_client = get_minio_client()
        photos_data_jsonl_path = extract_json_as_jsonl_from_minio(minio_client, minio_filepath, logger)
        return photos_data_jsonl_path        

    @task
    def load_to_snowflake_task(tmp_jsonl_filepath):
        logger = setup_logger('load_to_snowflake_task', 'snowflake_load_dag.log', 'loading')      
        snowflake_connection = get_snowflake_connection()
        copy_file_to_snowflake(snowflake_connection, tmp_jsonl_filepath, logger)
        return tmp_jsonl_filepath
    
    @task
    def produce_load_complete_message_task(tmp_jsonl_filepath):
        logger = setup_logger('produce_load_complete_message_task', 'snowflake_load_dag.log', 'loading')
        event_message = generate_load_complete_message(tmp_jsonl_filepath, logger)
        produce_kafka_message(LOAD_COMPLETE_TOPIC, event_message, logger)


    minio_filepath = extract_filepath_from_message_task()
    tmp_jsonl_filepath = extract_json_as_jsonl_from_minio_task(minio_filepath)
    copied_tmp_jsonl_filepath = load_to_snowflake_task(tmp_jsonl_filepath)
    produce_load_complete_message_task(copied_tmp_jsonl_filepath)
    
load_photos_to_snowflake_dag()