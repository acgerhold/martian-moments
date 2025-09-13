from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from airflow.sdk import dag, task, Asset, AssetWatcher
from datetime import datetime
import sys

sys.path.append('/opt/airflow')
from src.utils.logger import setup_logger
from src.utils.dbt import run_dbt_models_by_tag
from src.utils.kafka import parse_message
from src.config import GOLD_TAG, LOAD_COMPLETE_TOPIC

def apply_function(*args, **kwargs):
    logger = setup_logger('apply_function_task', 'transformation_dag.log', 'transformation')
    filepath = parse_message(args, logger)
    return filepath

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
    tags=["Transformation", "Snowflake", "dbt"]
)
def run_dbt_models_dag():

    @task
    def run_dbt_transformations():
        logger = setup_logger('run_dbt_transformations_task', 'transformation_dag.log', 'transformation')
        run_dbt_models_by_tag(GOLD_TAG, logger)

    run_dbt_transformations()

dag = run_dbt_models_dag()