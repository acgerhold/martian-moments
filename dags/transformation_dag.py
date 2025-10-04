from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
import sys

sys.path.append('/opt/airflow')
from src.config import LOAD_COMPLETE_TOPIC, GOLD_TAG, SILVER_TAG, INGESTION_SCHEDULING_TOPIC
from src.utils.kafka import parse_kafka_message, produce_kafka_message
from src.utils.dbt import run_dbt_models_by_tag
from src.utils.snowflake import fetch_next_ingestion_batch, generate_ingestion_batch_tasks
from src.utils.logger import setup_logger


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
    catchup=False,
    tags=["Transformation", "Scheduling", "Snowflake", "dbt"]
)
def run_dbt_models_dag():

    @task
    def run_dbt_flatten_task():
        logger = setup_logger('run_dbt_flatten_task', 'transformation_dag.log', 'transformation')
        flatten_models_success = run_dbt_models_by_tag(SILVER_TAG, logger)
        return flatten_models_success

    @task
    def run_dbt_transformation_task():
        logger = setup_logger('run_dbt_transformations_task', 'transformation_dag.log', 'transformation')
        aggregate_models_success = run_dbt_models_by_tag(GOLD_TAG, logger)
        return aggregate_models_success

    @task
    def fetch_next_ingestion_batch_task(run_dbt_models_success):
        logger = setup_logger('fetch_next_ingestion_batch_task', 'transformation_dag.log', 'transformation')
        ingestion_batch_dataframe = fetch_next_ingestion_batch(run_dbt_models_success, logger)
        return ingestion_batch_dataframe
    
    @task
    def generate_tasks_for_batch_task(ingestion_batch_dataframe):
        logger = setup_logger('generate_tasks_for_batch_task', 'transformation_dag.log', 'transformation')
        ingestion_batch_tasks = generate_ingestion_batch_tasks(ingestion_batch_dataframe, logger)
        return ingestion_batch_tasks

    @task
    def produce_ingestion_batch_message_task(ingestion_batch_tasks):
        logger = setup_logger('produce_ingestion_schedule_task', 'transformation_dag.log', 'transformation')
        produce_kafka_message(INGESTION_SCHEDULING_TOPIC, ingestion_batch_tasks, logger)

    flatten_models_success = run_dbt_flatten_task()
    aggregate_models_success = run_dbt_transformation_task()
    ingestion_batch_dataframe = fetch_next_ingestion_batch_task(aggregate_models_success)
    ingestion_batch_tasks = generate_tasks_for_batch_task(ingestion_batch_dataframe)
    produce_ingestion_batch_message_task(ingestion_batch_tasks)
    
    flatten_models_success >> aggregate_models_success >> ingestion_batch_dataframe

dag = run_dbt_models_dag()