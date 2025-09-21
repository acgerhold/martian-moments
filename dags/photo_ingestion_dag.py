from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import sys

sys.path.append('/opt/airflow')
from src.ingestion.photos import extract_photos_from_nasa, create_final_photos_json, generate_tasks_for_photos_batch
from src.config import INGESTION_SCHEDULING_TOPIC
from src.utils.minio import upload_json_to_minio
from src.utils.logger import setup_logger
from src.utils.kafka import parse_kafka_message, extract_ingestion_schedule_from_message, parse_kafka_message

def apply_function(*args, **kwargs):
    logger = setup_logger('apply_function_task', 'snowflake_load_dag.log', 'loading')
    ingestion_schedule_msg = parse_kafka_message(INGESTION_SCHEDULING_TOPIC, args, logger)
    return ingestion_schedule_msg

trigger = MessageQueueTrigger(
    queue=f"kafka://kafka:9092/{INGESTION_SCHEDULING_TOPIC}",
    apply_function="photo_ingestion_dag.apply_function"
)

ingestion_scheduling_asset = Asset(
    name="ingestion_scheduling_topic_asset", watchers=[AssetWatcher(name="ingestion_scheduling_watcher", trigger=trigger)]
)

@dag(
    dag_id="mars_rover_photos_ingestion",
    schedule=[ingestion_scheduling_asset],
    tags=["Ingestion", "Photos", "MinIO"]
)
def mars_rover_photos_ingestion_dag():
   
    @task
    def generate_tasks_for_batch_task(triggering_asset_events=None):
        logger = setup_logger('get_ingestion_config_task', 'photo_ingestion_dag.log', 'ingestion')     
        batch = generate_tasks_for_photos_batch(triggering_asset_events, logger)  
        return batch

    @task
    def extract_tasks_from_batch(batch):
        return batch["tasks"]

    @task
    def fetch_and_collect_rover_photos_task(rover: str, sol: int):
        logger = setup_logger('fetch_and_collect_rover_photos_task', 'photo_ingestion_dag.log', 'ingestion')            
        photos_result = extract_photos_from_nasa(rover, sol, logger)
        return photos_result
    
    @task 
    def extract_sol_range_from_batch(batch):
        return batch["sol_range"]

    @task
    def create_combined_batch_file_task(all_rover_photo_results: list, sol_range):
        logger = setup_logger('create_combined_batch_file_task', 'photo_ingestion_dag.log', 'ingestion')
        final_photos_json = create_final_photos_json(all_rover_photo_results, sol_range, logger)
        upload_json_to_minio(final_photos_json, logger)

    batch = generate_tasks_for_batch_task()
    tasks = extract_tasks_from_batch(batch)
    sol_range = extract_sol_range_from_batch(batch)
    all_rover_photo_results = fetch_and_collect_rover_photos_task.expand_kwargs(tasks)
    create_combined_batch_file_task(all_rover_photo_results, sol_range)

dag = mars_rover_photos_ingestion_dag()