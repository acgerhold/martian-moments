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

def create_photos_bronze_table(cursor):
    tables_sql = {
        'raw_photos': """
            CREATE TABLE IF NOT EXISTS RAW_PHOTO_RESPONSE (
                photos VARIANT,
                ingestion_date TIMESTAMP_NTZ,
                sol INTEGER,
                rover STRING,
                filename STRING
            )
        """
    }

    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_BRONZE')};")

    for table_name, sql in tables_sql.items():
        cursor.execute(sql)

def copy_photos_to_snowflake(cursor, jsonl_file_path):
    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_BRONZE')};")
    
    cursor.execute("REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';")
    
    try:
        cursor.execute(f"PUT file://{jsonl_file_path} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE")
        
        cursor.execute("""
            COPY INTO RAW_PHOTO_RESPONSE
            FROM @%RAW_PHOTO_RESPONSE
            FILE_FORMAT = (TYPE = 'JSON')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """)
        
    finally:
        if os.path.exists(jsonl_file_path):
            os.remove(jsonl_file_path)


def copy_photos_to_snowflake_batch(cursor, file_paths):
    cursor.execute(f"USE SCHEMA {os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('SNOWFLAKE_SCHEMA_BRONZE')};")

    cursor.execute("REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';")

    try:
        cursor.execute(f"PUT file://{file_paths['photo_response']} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE")

        cursor.execute("""
            COPY INTO RAW_PHOTO_RESPONSE
            FROM @%RAW_PHOTO_RESPONSE
            FILE_FORMAT = (TYPE = 'JSON')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """)

    finally:
        for path in file_paths.values():
            os.remove(path)