from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import sys

sys.path.append('/opt/airflow')
from src.utils.logger import setup_logger
from src.utils.dbt import run_dbt_models_by_tag
from src.utils.snowflake import fetch_results_from_silver_schema
from src.ingestion.scheduling import generate_ingestion_batches_from_table_results
from src.utils.kafka import parse_kafka_message, produce_kafka_message, generate_ingestion_schedule_message
from src.config import GOLD_TAG, INGESTION_PLANNING_VIEW, LOAD_COMPLETE_TOPIC, INGESTION_SCHEDULING_TOPIC

def apply_function(*args, **kwargs):
    logger = setup_logger('apply_function_task', 'transformation_dag.log', 'transformation')
    load_complete_msg = parse_kafka_message(LOAD_COMPLETE_TOPIC, args, logger)
    return load_complete_msg

trigger = MessageQueueTrigger(
    queue=f"kafka://kafka:9092/{LOAD_COMPLETE_TOPIC}",
    apply_function="transformation_dag.apply_function"
)

load_complete_asset = Asset(
    name="load_complete_topic_asset", watchers=[AssetWatcher(name="load_complete_watcher", trigger=trigger)]
)

@dag(
    dag_id="run_dbt_models",
    schedule=[load_complete_asset],
    tags=["Transformation", "Scheduling", "Snowflake", "dbt"]
)
def run_dbt_models_dag():

    @task
    def run_dbt_transformations_task():
        logger = setup_logger('run_dbt_transformations_task', 'transformation_dag.log', 'transformation')
        dbt_result = run_dbt_models_by_tag(GOLD_TAG, logger)
        return dbt_result

    @task
    def fetch_ingestion_planning_results_task():
        logger = setup_logger('fetch_ingestion_schedule_task', 'transformation_dag.log', 'transformation')
        table_results = fetch_results_from_silver_schema(INGESTION_PLANNING_VIEW, logger)
        return table_results
    
    @task
    def generate_ingestion_batches_task(table_results):
        logger = setup_logger('generate_ingestion_batches_task', 'transformation_dag.log', 'transformation')
        batches = generate_ingestion_batches_from_table_results(table_results, logger)
        return batches
    
    @task
    def generate_ingestion_schedule_message_task(batches):
        logger = setup_logger('generate_ingestion_scheduling_message_task', 'transformation_dag.log', 'transformation')
        message = generate_ingestion_schedule_message(batches, logger)
        return message

    @task
    def produce_ingestion_schedule_message_task(message):
        logger = setup_logger('produce_ingestion_schedule_task', 'transformation_dag.log', 'transformation')
        produce_kafka_message(INGESTION_SCHEDULING_TOPIC, message, logger)

    dbt_result = run_dbt_transformations_task()
    table_results = fetch_ingestion_planning_results_task()
    batches = generate_ingestion_batches_task(table_results)
    message = generate_ingestion_schedule_message_task(batches)
    produce_ingestion_schedule_message_task(message)
    
    dbt_result >> table_results

dag = run_dbt_models_dag()