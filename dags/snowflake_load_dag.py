from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import sys 

sys.path.append('/opt/airflow')
from src.config import MINIO_EVENTS_TOPIC, LOAD_COMPLETE_TOPIC
from src.utils.kafka import parse_kafka_message, unwrap_airflow_asset_payload, produce_kafka_message
from src.utils.minio import extract_json_as_jsonl_from_minio
from src.utils.snowflake import copy_file_to_snowflake
from src.utils.logger import setup_logger

def apply_function(*args, **kwargs):
    logger = setup_logger('apply_function_task', 'snowflake_load_dag.log', 'loading')
    file_events_msg = parse_kafka_message(MINIO_EVENTS_TOPIC, args, logger)
    return file_events_msg

trigger = MessageQueueTrigger(
    queue=f"kafka://kafka:9092/{MINIO_EVENTS_TOPIC}",
    apply_function="snowflake_load_dag.apply_function"
)

minio_events_asset = Asset(
    name="minio_events_topic_asset", watchers=[AssetWatcher(name="minio_events_watcher", trigger=trigger)]
)

@dag(
    dag_id="load_to_snowflake",
    schedule=[minio_events_asset],
    catchup=False,
    tags=["Loading", "Kafka", "MinIO", "Snowflake"]
)
def load_photos_to_snowflake_dag():
    
    @task
    def extract_minio_upload_path_from_payload_task(triggering_asset_events=None):
        logger = setup_logger('extract_minio_upload_path_from_payload_task', 'snowflake_load_dag.log', 'loading')
        minio_upload_path = unwrap_airflow_asset_payload(triggering_asset_events[minio_events_asset], logger)
        return minio_upload_path

    @task
    def extract_json_as_jsonl_from_minio_task(minio_upload_path):
        logger = setup_logger('extract_json_as_jsonl_from_minio_task', 'snowflake_load_dag.log', 'loading')
        tmp_jsonl_staging_path = extract_json_as_jsonl_from_minio(minio_upload_path, logger)
        return tmp_jsonl_staging_path         

    @task
    def load_to_snowflake_task(tmp_jsonl_staging_path):
        logger = setup_logger('load_to_snowflake_task', 'snowflake_load_dag.log', 'loading')      
        load_complete_message = copy_file_to_snowflake(tmp_jsonl_staging_path, logger)
        return load_complete_message
    
    @task
    def produce_load_complete_message_task(load_complete_message):
        logger = setup_logger('produce_load_complete_message_task', 'snowflake_load_dag.log', 'loading')
        produce_kafka_message(LOAD_COMPLETE_TOPIC, load_complete_message, logger)

    minio_upload_path = extract_minio_upload_path_from_payload_task()
    tmp_jsonl_staging_path = extract_json_as_jsonl_from_minio_task(minio_upload_path)
    load_complete_message = load_to_snowflake_task(tmp_jsonl_staging_path)
    produce_load_complete_message_task(load_complete_message)
    
dag = load_photos_to_snowflake_dag()