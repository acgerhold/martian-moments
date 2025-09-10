from airflow.sdk import dag, task
from datetime import datetime
import sys

sys.path.append('/opt/airflow')
from src.utils.logger import setup_logger
from src.utils.dbt import run_dbt_models_by_tag
from src.config import GOLD_TAG

@dag(
    dag_id="run_dbt_models",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["Transformation", "Snowflake", "dbt"]
)
def run_dbt_models_dag():

    @task
    def run_dbt_transformations():
        logger = setup_logger('run_dbt_transformations_task', 'transformation_dag.log', 'transformation')
        run_dbt_models_by_tag(GOLD_TAG, logger)

    run_dbt_transformations()

dag = run_dbt_models_dag()