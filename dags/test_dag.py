from airflow import DAG
from airflow.decorators import task
from datetime import datetime

with DAG(
    dag_id="test_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    @task
    def hello_world():
        print("Hello from Airflow!")

    hello_world()