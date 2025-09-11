import os
import snowflake.connector
from dotenv import load_dotenv

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

def copy_file_to_snowflake(snowflake_connection, tmp_jsonl_filepath, logger):
    logger.info(f"Attempting copy to Snowflake - File: {tmp_jsonl_filepath}")
    snowflake_cursor = snowflake_connection.cursor()
    snowflake_cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_BRONZE')};")    
    snowflake_cursor.execute("REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';")
    
    try:
        snowflake_cursor.execute(f"PUT file://{tmp_jsonl_filepath} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE")        
        snowflake_cursor.execute("""
            COPY INTO RAW_PHOTO_RESPONSE
            FROM @%RAW_PHOTO_RESPONSE
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