import os
import snowflake.connector
from dotenv import load_dotenv

from src.config import PHOTOS_TABLE_NAME, COORDINATES_TABLE_NAME, MANIFESTS_TABLE_NAME

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

def copy_file_to_snowflake(tmp_jsonl_filepath, logger):
    logger.info(f"Attempting copy to Snowflake - File: {tmp_jsonl_filepath}")
    snowflake_connection = get_snowflake_connection()
    snowflake_cursor = snowflake_connection.cursor()
    snowflake_cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_BRONZE')};")

    filename = os.path.basename(tmp_jsonl_filepath)
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
        snowflake_cursor.execute(f"PUT file://{tmp_jsonl_filepath} @%{table_name} OVERWRITE = TRUE")        
        snowflake_cursor.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT = (TYPE = 'JSON')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """)
        
    finally:
        logger.info(f"Copied to Snowflake - File: {tmp_jsonl_filepath}")    
        if os.path.exists(tmp_jsonl_filepath):
            os.remove(tmp_jsonl_filepath)
            
        snowflake_cursor.close()
        snowflake_connection.close()

def fetch_results_from_silver_schema(table_name, logger):
    logger.info(f"Attempting to fetch results - Table: {table_name}")
    snowflake_connection = get_snowflake_connection()
    snowflake_cursor = snowflake_connection.cursor()

    try:
        snowflake_cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_SILVER')};")
        table_results = snowflake_cursor.execute(f"SELECT * FROM {table_name}").fetchall()

    finally:
        snowflake_cursor.close()
        snowflake_connection.close()

    logger.info(f"Fetched results successfully - Table: {table_name}")
    return table_results