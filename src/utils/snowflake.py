import os
import snowflake.connector
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.config import PHOTOS_TABLE_NAME, COORDINATES_TABLE_NAME, MANIFESTS_TABLE_NAME, BATCH_SIZE

load_dotenv()

def get_snowflake_connection():
    snowflake_connection = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        user=os.getenv('SNOWFLAKE_USER'),
        role=os.getenv('SNOWFLAKE_ROLE'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA_BRONZE')     
    )

    return snowflake_connection

def copy_file_to_snowflake(tmp_jsonl_staging_path, logger):
    logger.info(f"Attempting copy to Snowflake - File: {tmp_jsonl_staging_path}")
    snowflake_connection = get_snowflake_connection()
    snowflake_cursor = snowflake_connection.cursor()
    snowflake_cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_BRONZE')};")

    filename = os.path.basename(tmp_jsonl_staging_path)
    match filename:
        case name if name.startswith("mars_rover_photos"):
            table_name = PHOTOS_TABLE_NAME
        case name if name.startswith("mars_rover_coordinates"):
            table_name = COORDINATES_TABLE_NAME
        case name if name.startswith("mars_rover_manifests"):
            table_name = MANIFESTS_TABLE_NAME
        case _:
            table_name = "UNKNOWN"

    snowflake_cursor.execute(f"REMOVE @%{table_name} PATTERN='.*';")
    
    try:
        snowflake_cursor.execute(f"PUT file://{tmp_jsonl_staging_path} @%{table_name} OVERWRITE = TRUE")        
        snowflake_cursor.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT = (TYPE = 'JSON')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """)
        
    finally:
        logger.info(f"Copied to Snowflake - File: {tmp_jsonl_staging_path}")    
        if os.path.exists(tmp_jsonl_staging_path):
            os.remove(tmp_jsonl_staging_path)
            
        snowflake_cursor.close()
        snowflake_connection.close()

        return {
            "tmp_jsonl_staging_path": tmp_jsonl_staging_path,
            "status": "success",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        } 
 

def fetch_next_ingestion_batch(run_dbt_models_success, logger):
    if run_dbt_models_success:
        logger.info(f"Attempting to fetch next ingestion batch")
        snowflake_connection = get_snowflake_connection()
        snowflake_cursor = snowflake_connection.cursor()

        try: 
            snowflake_cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_SILVER')};")          
            rows = snowflake_cursor.execute(f"SELECT rover_name, sol FROM VALIDATION_PHOTO_GAPS WHERE validation_status = 'MISSING_SOL' AND rover_name != 'Perseverance' AND sol >= 4600 ORDER BY sol LIMIT {BATCH_SIZE}").fetchall()
            columns = [desc[0] for desc in snowflake_cursor.description]
            ingestion_batch_dataframe = pd.DataFrame(rows, columns=columns)

            logger.info(f"Fetched ingestion batch - Results: {ingestion_batch_dataframe}")
            return ingestion_batch_dataframe
        except Exception as e:
            logger.error(f"Error fetching ingestion batch - Error: {e}")
        finally:
            snowflake_cursor.close()
            snowflake_connection.close()
    else:
        logger.error("Unsuccessful dbt run, cancelling next ingestion batch")
        
def generate_ingestion_batch_tasks(ingestion_batch_dataframe, logger):
    if not ingestion_batch_dataframe.empty:
        logger.info("Attempting to generate tasks from ingestion batch")
        ingestion_batch = {"tasks": [], "sol_range": []}
        sol_range = list(range(ingestion_batch_dataframe['SOL'].min(), ingestion_batch_dataframe['SOL'].max() + 1))

        ingestion_tasks = []
        for _, row in ingestion_batch_dataframe.iterrows():
            task = {
                "rover_name": row['ROVER_NAME'],
                "sol": row['SOL'],
            }
            ingestion_tasks.append(task)
    
        ingestion_batch = {"tasks": ingestion_tasks, "sol_range": sol_range}

        logger.info(f"Generated ingestion batch tasks - Tasks: {len(ingestion_tasks)}, Sol Range: {sol_range[0]} to {sol_range[-1]}")
        return {
            "ingestion_schedule": ingestion_batch,
            "status": "success",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        }
    else:
        logger.info("No tasks found, photos data up-to-date")