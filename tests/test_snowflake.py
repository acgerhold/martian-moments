from unittest.mock import patch, MagicMock
import pytest
import os
import pandas as pd
from src.utils.snowflake import get_snowflake_connection, copy_file_to_snowflake, fetch_next_ingestion_batch, generate_ingestion_batch_tasks

@pytest.fixture
def mock_logger():
    return MagicMock()

# ====== GET_SNOWFLAKE_CONNECTION TESTS ======

@patch.dict(os.environ, {
    'SNOWFLAKE_ACCOUNT': 'test_account',
    'SNOWFLAKE_PASSWORD': 'test_password',
    'SNOWFLAKE_USER': 'test_user',
    'SNOWFLAKE_ROLE': 'test_role',
    'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
    'SNOWFLAKE_DATABASE': 'test_database',
    'SNOWFLAKE_SCHEMA_BRONZE': 'bronze_schema'
})
def test_get_snowflake_connection_success():
    """Test successful Snowflake connection creation"""
    with patch('src.utils.snowflake.snowflake.connector.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        result = get_snowflake_connection()
        
        mock_connect.assert_called_once_with(
            account='test_account',
            password='test_password',
            user='test_user',
            role='test_role',
            warehouse='test_warehouse',
            database='test_database',
            schema='bronze_schema'
        )
        assert result == mock_connection

@patch.dict(os.environ, {}, clear=True)
def test_get_snowflake_connection_missing_env_vars():
    """Test Snowflake connection creation with missing environment variables"""
    with patch('src.utils.snowflake.snowflake.connector.connect') as mock_connect:
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection
        
        result = get_snowflake_connection()
        
        mock_connect.assert_called_once_with(
            account=None,
            password=None,
            user=None,
            role=None,
            warehouse=None,
            database=None,
            schema=None
        )
        assert result == mock_connection

# ====== COPY_FILE_TO_SNOWFLAKE TESTS ======

def test_copy_file_to_snowflake_photos_success(mock_logger):
    """Test successful photo file copy to Snowflake"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    jsonl_file_path = "/tmp/mars_rover_photos_batch_sol_100_to_100_2025-09-13T15:30:00.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove') as mock_remove, \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
        
        result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
        # Verify return value
        expected_result = {
            "tmp_jsonl_staging_path": jsonl_file_path,
            "status": "success", 
            "timestamp": "2025-09-22T15:30:00"
        }
        assert result == expected_result
        
        # Verify connection and cursor setup
        mock_connection.cursor.assert_called_once()
        
        # Verify SQL commands were executed in correct order
        expected_calls = [
            "USE SCHEMA TEST_DB.BRONZE;",
            "REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';",
            f"PUT file://{jsonl_file_path} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE"
        ]
        
        # Check the first three execute calls
        assert mock_cursor.execute.call_count >= 4  # USE SCHEMA, REMOVE, PUT, COPY INTO
        for i, expected_call in enumerate(expected_calls):
            actual_call = mock_cursor.execute.call_args_list[i][0][0]
            assert actual_call == expected_call
        
        # Verify COPY INTO command was executed (4th call)
        copy_command = mock_cursor.execute.call_args_list[3][0][0]
        assert "COPY INTO RAW_PHOTO_RESPONSE" in copy_command
        assert "FROM @%RAW_PHOTO_RESPONSE" in copy_command
        assert "FILE_FORMAT = (TYPE = 'JSON')" in copy_command
        assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in copy_command
        assert "ON_ERROR = 'CONTINUE'" in copy_command
        
        # Verify cleanup
        mock_remove.assert_called_once_with(jsonl_file_path)
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_any_call(f"Attempting copy to Snowflake - File: {jsonl_file_path}")
        mock_logger.info.assert_any_call(f"Copied to Snowflake - File: {jsonl_file_path}")

def test_copy_file_to_snowflake_coordinates_success(mock_logger):
    """Test successful coordinate file copy to Snowflake"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    jsonl_file_path = "/tmp/mars_rover_coordinates_2025-09-13T15:30:00.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove') as mock_remove, \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
        
        result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
        # Verify return value
        expected_result = {
            "tmp_jsonl_staging_path": jsonl_file_path,
            "status": "success",
            "timestamp": "2025-09-22T15:30:00"
        }
        assert result == expected_result
        
        # Verify coordinate-specific table routing
        expected_calls = [
            "USE SCHEMA TEST_DB.BRONZE;",
            "REMOVE @%RAW_COORDINATE_RESPONSE PATTERN='.*';",
            f"PUT file://{jsonl_file_path} @%RAW_COORDINATE_RESPONSE OVERWRITE = TRUE"
        ]
        
        for i, expected_call in enumerate(expected_calls):
            actual_call = mock_cursor.execute.call_args_list[i][0][0]
            assert actual_call == expected_call
        
        # Verify COPY INTO uses coordinate table
        copy_command = mock_cursor.execute.call_args_list[3][0][0]
        assert "COPY INTO RAW_COORDINATE_RESPONSE" in copy_command
        assert "FROM @%RAW_COORDINATE_RESPONSE" in copy_command

def test_copy_file_to_snowflake_manifests_success(mock_logger):
    """Test successful manifest file copy to Snowflake"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    jsonl_file_path = "/tmp/mars_rover_manifests_2025-09-13T15:30:00.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove') as mock_remove, \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
        
        result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
        # Verify return value
        expected_result = {
            "tmp_jsonl_staging_path": jsonl_file_path,
            "status": "success",
            "timestamp": "2025-09-22T15:30:00"
        }
        assert result == expected_result
        
        # Verify manifest-specific table routing
        expected_calls = [
            "USE SCHEMA TEST_DB.BRONZE;",
            "REMOVE @%RAW_MANIFEST_RESPONSE PATTERN='.*';",
            f"PUT file://{jsonl_file_path} @%RAW_MANIFEST_RESPONSE OVERWRITE = TRUE"
        ]
        
        for i, expected_call in enumerate(expected_calls):
            actual_call = mock_cursor.execute.call_args_list[i][0][0]
            assert actual_call == expected_call
        
        # Verify COPY INTO uses manifest table
        copy_command = mock_cursor.execute.call_args_list[3][0][0]
        assert "COPY INTO RAW_MANIFEST_RESPONSE" in copy_command
        assert "FROM @%RAW_MANIFEST_RESPONSE" in copy_command

def test_copy_file_to_snowflake_unknown_filename(mock_logger):
    """Test file copy with unknown filename pattern"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    jsonl_file_path = "/tmp/unknown_file_type_2025-09-13T15:30:00.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove'), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
        
        result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
        # Verify return value
        expected_result = {
            "tmp_jsonl_staging_path": jsonl_file_path,
            "status": "success",
            "timestamp": "2025-09-22T15:30:00"
        }
        assert result == expected_result
        
        # Verify UNKNOWN table is used as fallback
        expected_calls = [
            "USE SCHEMA TEST_DB.BRONZE;",
            "REMOVE @%UNKNOWN PATTERN='.*';",
            f"PUT file://{jsonl_file_path} @%UNKNOWN OVERWRITE = TRUE"
        ]
        
        for i, expected_call in enumerate(expected_calls):
            actual_call = mock_cursor.execute.call_args_list[i][0][0]
            assert actual_call == expected_call
        
        # Verify COPY INTO uses UNKNOWN table
        copy_command = mock_cursor.execute.call_args_list[3][0][0]
        assert "COPY INTO UNKNOWN" in copy_command
        assert "FROM @%UNKNOWN" in copy_command

def test_copy_file_to_snowflake_file_not_exists(mock_logger):
    """Test file copy when file doesn't exist for cleanup"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    jsonl_file_path = "/tmp/nonexistent_file.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('os.path.exists', return_value=False), \
    patch('os.remove') as mock_remove, \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
        
        result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
        # Verify return value
        expected_result = {
            "tmp_jsonl_staging_path": jsonl_file_path,
            "status": "success",
            "timestamp": "2025-09-22T15:30:00"
        }
        assert result == expected_result
        
        # Verify SQL operations still completed
        assert mock_cursor.execute.call_count >= 4
        
        # Verify connections were properly closed
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # Verify file removal was not attempted since file doesn't exist
        mock_remove.assert_not_called()

def test_copy_file_to_snowflake_sql_error_cleanup(mock_logger):
    """Test that cleanup happens even if SQL operations fail"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Make the COPY INTO command fail
    def execute_side_effect(sql):
        if "COPY INTO" in sql:
            raise Exception("SQL Error")
        # Let other SQL commands pass through normally
        return None
    
    mock_cursor.execute.side_effect = execute_side_effect
    
    jsonl_file_path = "/tmp/mars_rover_photos_error_test.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove') as mock_remove, \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
        
        # The function should handle the exception internally and continue with cleanup
        try:
            result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
            
            # If function completes without raising, verify return value
            expected_result = {
                "tmp_jsonl_staging_path": jsonl_file_path,
                "status": "success",
                "timestamp": "2025-09-22T15:30:00"
            }
            assert result == expected_result
            
        except Exception as e:
            # If an exception bubbles up, that's expected behavior - 
            # we still want to verify cleanup happened
            assert "SQL Error" in str(e)
        
        # Verify cleanup still happened despite error
        mock_remove.assert_called_once_with(jsonl_file_path)
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # Verify logging still happened (finally block)
        mock_logger.info.assert_any_call(f"Copied to Snowflake - File: {jsonl_file_path}")

def test_copy_file_to_snowflake_different_environments(mock_logger):
    """Test that different database and schema configurations work"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    jsonl_file_path = "/tmp/mars_rover_photos_env_test.jsonl"
    
    test_cases = [
        ('PROD_DB', 'RAW_LAYER'),
        ('DEV_DATABASE', 'STAGING'),
        ('ANALYTICS_DB', 'BRONZE_SCHEMA')
    ]
    
    for database, schema in test_cases:
        mock_cursor.reset_mock()
        mock_connection.reset_mock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_DATABASE': database,
            'SNOWFLAKE_SCHEMA_BRONZE': schema
        }), \
        patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
        patch('os.path.exists', return_value=True), \
        patch('os.remove'), \
        patch('src.utils.snowflake.datetime') as mock_datetime:
            
            mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
            
            result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
            
            # Verify return value
            expected_result = {
                "tmp_jsonl_staging_path": jsonl_file_path,
                "status": "success",
                "timestamp": "2025-09-22T15:30:00"
            }
            assert result == expected_result
            
            # Verify correct schema usage
            use_schema_call = mock_cursor.execute.call_args_list[0][0][0]
            assert use_schema_call == f"USE SCHEMA {database}.{schema};"

def test_copy_file_to_snowflake_command_structure(mock_logger):
    """Test that all SQL commands have correct structure"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    jsonl_file_path = "/tmp/mars_rover_photos_structure_test.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove'), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
        
        result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
        # Verify return value
        expected_result = {
            "tmp_jsonl_staging_path": jsonl_file_path,
            "status": "success",
            "timestamp": "2025-09-22T15:30:00"
        }
        assert result == expected_result
        
        # Verify exactly 4 SQL operations were executed
        assert mock_cursor.execute.call_count == 4
        
        executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        
        # 1. USE SCHEMA
        assert executed_sqls[0] == "USE SCHEMA TEST_DB.BRONZE;"
        
        # 2. REMOVE files
        assert executed_sqls[1] == "REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';"
        
        # 3. PUT file
        assert executed_sqls[2] == f"PUT file://{jsonl_file_path} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE"
        
        # 4. COPY INTO - verify it contains all required parameters
        copy_command = executed_sqls[3]
        assert "COPY INTO RAW_PHOTO_RESPONSE" in copy_command
        assert "FROM @%RAW_PHOTO_RESPONSE" in copy_command
        assert "FILE_FORMAT = (TYPE = 'JSON')" in copy_command
        assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in copy_command
        assert "ON_ERROR = 'CONTINUE'" in copy_command

def test_copy_file_to_snowflake_filename_routing_edge_cases(mock_logger):
    """Test edge cases in filename routing logic"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    test_cases = [
        ("/tmp/mars_rover_photos.jsonl", "RAW_PHOTO_RESPONSE"),  # Minimal photos filename
        ("/tmp/mars_rover_coordinates.jsonl", "RAW_COORDINATE_RESPONSE"),  # Minimal coordinates filename
        ("/tmp/mars_rover_photos_extra_long_filename_with_details.jsonl", "RAW_PHOTO_RESPONSE"),  # Long photos filename
        ("/tmp/coordinates_mars_rover.jsonl", "UNKNOWN"),  # Doesn't start with mars_rover_coordinates
        ("/tmp/photos_mars_rover.jsonl", "UNKNOWN"),  # Doesn't start with mars_rover_photos
        ("/tmp/mars_rover_other_data.jsonl", "UNKNOWN"),  # Different data type
        ("/tmp/MARS_ROVER_PHOTOS.jsonl", "UNKNOWN"),  # Case sensitive check
    ]
    
    for jsonl_file_path, expected_table in test_cases:
        mock_cursor.reset_mock()
        mock_connection.reset_mock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_DATABASE': 'TEST_DB',
            'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
        }), \
        patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
        patch('os.path.exists', return_value=True), \
        patch('os.remove'), \
        patch('src.utils.snowflake.datetime') as mock_datetime:
            
            mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
            
            result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
            
            # Verify return value
            expected_result = {
                "tmp_jsonl_staging_path": jsonl_file_path,
                "status": "success",
                "timestamp": "2025-09-22T15:30:00"
            }
            assert result == expected_result
            
            # Verify correct table routing
            remove_call = mock_cursor.execute.call_args_list[1][0][0]
            put_call = mock_cursor.execute.call_args_list[2][0][0]
            copy_call = mock_cursor.execute.call_args_list[3][0][0]
            
            assert f"@%{expected_table}" in remove_call
            assert f"@%{expected_table}" in put_call
            assert f"COPY INTO {expected_table}" in copy_call

def test_copy_file_to_snowflake_path_handling(mock_logger):
    """Test that file paths are handled correctly regardless of directory structure"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    test_paths = [
        "/tmp/mars_rover_photos_test.jsonl",  # Simple path
        "/home/user/data/processing/mars_rover_photos_batch.jsonl",  # Nested path
        "/var/tmp/airflow/mars_rover_photos_2025.jsonl",  # Different tmp location
        "mars_rover_photos_relative.jsonl",  # Relative path
    ]
    
    for jsonl_file_path in test_paths:
        mock_cursor.reset_mock()
        mock_connection.reset_mock()
        mock_connection.cursor.return_value = mock_cursor
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_DATABASE': 'TEST_DB',
            'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
        }), \
        patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
        patch('os.path.exists', return_value=True), \
        patch('os.remove'), \
        patch('src.utils.snowflake.datetime') as mock_datetime:
            
            mock_datetime.now.return_value.strftime.return_value = "2025-09-22T15:30:00"
            
            result = copy_file_to_snowflake(jsonl_file_path, mock_logger)
            
            # Verify return value
            expected_result = {
                "tmp_jsonl_staging_path": jsonl_file_path,
                "status": "success",
                "timestamp": "2025-09-22T15:30:00"
            }
            assert result == expected_result
            
            # Verify PUT command uses full path
            put_call = mock_cursor.execute.call_args_list[2][0][0]
            assert f"PUT file://{jsonl_file_path}" in put_call
            
            # Verify table routing works based on filename only (not path)
            assert "RAW_PHOTO_RESPONSE" in put_call


def test_fetch_next_ingestion_batch_success_with_data(mock_logger):
    """Test successful fetch returning DataFrame with missing sols"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock fetchall result for VALIDATION_PHOTO_GAPS query
    mock_cursor.fetchall.return_value = [
        ('Curiosity', 2800),
        ('Curiosity', 2801), 
        ('Opportunity', 2900),
        ('Opportunity', 2901),
        ('Perseverance', 1700)
    ]
    
    # Mock cursor description for DataFrame columns
    mock_cursor.description = [
        ('ROVER_NAME',), ('SOL',)
    ]
    
    # Mock the execute method to return the mock cursor for method chaining
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection):
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify connection and cursor setup
        mock_connection.cursor.assert_called_once()
        
        # Verify SQL commands - updated to match actual implementation with LIMIT
        expected_calls = [
            f"USE SCHEMA TEST_DB.SILVER;",
            f"SELECT rover_name, sol FROM VALIDATION_PHOTO_GAPS LIMIT 200"
        ]
        
        for i, expected_call in enumerate(expected_calls):
            actual_call = mock_cursor.execute.call_args_list[i][0][0]
            assert actual_call == expected_call
        
        # Verify result is a DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 5
        assert list(result.columns) == ['ROVER_NAME', 'SOL']
        
        # Verify DataFrame content
        assert result.iloc[0]['ROVER_NAME'] == 'Curiosity'
        assert result.iloc[0]['SOL'] == 2800
        assert result.iloc[4]['ROVER_NAME'] == 'Perseverance'
        assert result.iloc[4]['SOL'] == 1700
        
        # Verify cleanup
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # Verify logging - updated to match actual log message
        mock_logger.info.assert_any_call("Attempting to fetch next ingestion batch")
        # Verify DataFrame was logged (the actual logging format)
        logged_messages = [call.args[0] for call in mock_logger.info.call_args_list]
        assert any("Fetched ingestion batch - Results:" in msg for msg in logged_messages)

def test_fetch_next_ingestion_batch_empty_results(mock_logger):
    """Test fetch when no missing sols are found"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Empty query results
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = [
        ('ROVER_NAME',), ('SOL',)
    ]
    
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection):
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify result is empty DataFrame
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 0
        assert list(result.columns) == ['ROVER_NAME', 'SOL']
        
        # The function doesn't have specific logging for empty results, just the general log
        mock_logger.info.assert_any_call("Attempting to fetch next ingestion batch")
        logged_messages = [call.args[0] for call in mock_logger.info.call_args_list]
        assert any("Fetched ingestion batch - Results:" in msg for msg in logged_messages)

def test_fetch_next_ingestion_batch_sql_error(mock_logger):
    """Test fetch when SQL query fails"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Make the SELECT query fail
    mock_cursor.execute.side_effect = Exception("SQL query failed")
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection):
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify error logging - updated to match actual error message format
        mock_logger.error.assert_called_once()
        error_msg = mock_logger.error.call_args[0][0]
        assert "Error fetching ingestion batch - Error:" in error_msg
        assert "SQL query failed" in error_msg
        
        # Verify cleanup still happens
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # The function doesn't return anything on error, it returns None
        assert result is None

def test_fetch_next_ingestion_batch_large_dataset(mock_logger):
    """Test fetch with large dataset handling (with LIMIT in actual implementation)"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Generate 73 rows
    mock_data = [(f'Rover{i%4}', 1000 + i) for i in range(73)]
    mock_cursor.fetchall.return_value = mock_data
    
    mock_cursor.description = [
        ('ROVER_NAME',), ('SOL',)
    ]
    
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection):
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify query has LIMIT (your actual implementation uses LIMIT)
        query_call = mock_cursor.execute.call_args_list[1][0][0]
        assert "LIMIT" in query_call  # Changed assertion
        assert "SELECT rover_name, sol FROM VALIDATION_PHOTO_GAPS LIMIT 200" == query_call
        
        # Verify result contains exactly 73 rows
        assert isinstance(result, pd.DataFrame)
        assert len(result) == 73
        
        # Verify logging shows the DataFrame
        logged_messages = [call.args[0] for call in mock_logger.info.call_args_list]
        assert any("Fetched ingestion batch - Results:" in msg for msg in logged_messages)

def test_fetch_next_ingestion_batch_false_trigger(mock_logger):
    """Test fetch when run_dbt_models_success is False"""
    # When run_dbt_models_success is False, function should not execute
    
    with patch('src.utils.snowflake.get_snowflake_connection') as mock_get_conn:
        result = fetch_next_ingestion_batch(False, mock_logger)
        
        # Should return None and not attempt any database operations
        assert result is None
        mock_get_conn.assert_not_called()
        
        # Verify the error log message
        mock_logger.error.assert_called_once_with("Unsuccessful dbt run, cancelling next ingestion batch")

# ====== GENERATE_INGESTION_BATCH_TASKS TESTS ======

def test_generate_ingestion_batch_tasks_success_with_data(mock_logger):
    """Test successful task generation with DataFrame containing missing sols"""
    # Create test DataFrame
    test_data = {
        'ROVER_NAME': ['Curiosity', 'Curiosity', 'Opportunity', 'Perseverance', 'Spirit'],
        'SOL': [2800, 2801, 2900, 1700, 380]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T15:30:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify return structure
        assert isinstance(result, dict)
        assert "ingestion_schedule" in result
        assert "status" in result
        assert "timestamp" in result
        
        # Verify top-level fields
        assert result["status"] == "success"
        assert result["timestamp"] == "2025-10-02T15:30:00"
        
        # Verify ingestion_schedule structure
        ingestion_schedule = result["ingestion_schedule"]
        assert "tasks" in ingestion_schedule
        assert "sol_range" in ingestion_schedule
        
        # Verify tasks content
        tasks = ingestion_schedule["tasks"]
        assert len(tasks) == 5
        
        # Check first task
        assert tasks[0]["rover_name"] == "Curiosity"
        assert tasks[0]["sol"] == 2800
        
        # Check last task
        assert tasks[4]["rover_name"] == "Spirit"
        assert tasks[4]["sol"] == 380
        
        # Verify sol_range (should be from min to max)
        sol_range = ingestion_schedule["sol_range"]
        expected_range = list(range(380, 2901))  # 380 to 2900 inclusive
        assert sol_range == expected_range
        
        # Verify logging
        mock_logger.info.assert_any_call("Attempting to generate tasks from ingestion batch")
        mock_logger.info.assert_any_call("Generated ingestion batch tasks - Tasks: 5, Sol Range: 380 to 2900")

def test_generate_ingestion_batch_tasks_empty_dataframe(mock_logger):
    """Test task generation with empty DataFrame"""
    # Create empty DataFrame
    empty_dataframe = pd.DataFrame(columns=['ROVER_NAME', 'SOL'])
    
    result = generate_ingestion_batch_tasks(empty_dataframe, mock_logger)
    
    # Should return None for empty DataFrame
    assert result is None
    
    # Verify logging
    mock_logger.info.assert_called_once_with("No tasks found, photos data up-to-date")

def test_generate_ingestion_batch_tasks_single_record(mock_logger):
    """Test task generation with single record DataFrame"""
    # Create single record DataFrame
    test_data = {
        'ROVER_NAME': ['Perseverance'],
        'SOL': [1500]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T16:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify structure
        assert isinstance(result, dict)
        assert result["status"] == "success"
        assert result["timestamp"] == "2025-10-02T16:00:00"
        
        # Verify single task
        tasks = result["ingestion_schedule"]["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["rover_name"] == "Perseverance"
        assert tasks[0]["sol"] == 1500
        
        # Verify sol_range for single record
        sol_range = result["ingestion_schedule"]["sol_range"]
        assert sol_range == [1500]  # Range from 1500 to 1500
        
        # Verify logging
        mock_logger.info.assert_any_call("Generated ingestion batch tasks - Tasks: 1, Sol Range: 1500 to 1500")

def test_generate_ingestion_batch_tasks_multiple_rovers_same_sol(mock_logger):
    """Test task generation with multiple rovers having same sol"""
    # Create DataFrame with same sol for different rovers
    test_data = {
        'ROVER_NAME': ['Curiosity', 'Opportunity', 'Spirit', 'Perseverance'],
        'SOL': [1000, 1000, 1000, 1000]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T17:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify all tasks created
        tasks = result["ingestion_schedule"]["tasks"]
        assert len(tasks) == 4
        
        # Verify all tasks have same sol but different rovers
        rovers = [task["rover_name"] for task in tasks]
        sols = [task["sol"] for task in tasks]
        
        assert set(rovers) == {'Curiosity', 'Opportunity', 'Spirit', 'Perseverance'}
        assert all(sol == 1000 for sol in sols)
        
        # Verify sol_range for same sol
        sol_range = result["ingestion_schedule"]["sol_range"]
        assert sol_range == [1000]  # Range from 1000 to 1000
        
        # Verify logging
        mock_logger.info.assert_any_call("Generated ingestion batch tasks - Tasks: 4, Sol Range: 1000 to 1000")

def test_generate_ingestion_batch_tasks_wide_sol_range(mock_logger):
    """Test task generation with wide sol range"""
    # Create DataFrame with wide sol range but few records
    test_data = {
        'ROVER_NAME': ['Curiosity', 'Opportunity', 'Spirit'],
        'SOL': [100, 2000, 4500]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T18:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify tasks
        tasks = result["ingestion_schedule"]["tasks"]
        assert len(tasks) == 3
        
        # Verify sol_range spans full range
        sol_range = result["ingestion_schedule"]["sol_range"]
        expected_range = list(range(100, 4501))  # 100 to 4500 inclusive
        assert sol_range == expected_range
        assert len(sol_range) == 4401  # 4500 - 100 + 1
        
        # Verify logging shows correct range
        mock_logger.info.assert_any_call("Generated ingestion batch tasks - Tasks: 3, Sol Range: 100 to 4500")

def test_generate_ingestion_batch_tasks_dataframe_column_access(mock_logger):
    """Test that function correctly accesses DataFrame columns"""
    # Create DataFrame with specific column names and data types
    test_data = {
        'ROVER_NAME': ['Test_Rover_1', 'Test_Rover_2'],
        'SOL': [999, 1001]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T19:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify DataFrame column access works correctly
        tasks = result["ingestion_schedule"]["tasks"]
        
        # Check rover names are accessed correctly
        assert tasks[0]["rover_name"] == "Test_Rover_1"
        assert tasks[1]["rover_name"] == "Test_Rover_2"
        
        # Check sols are accessed correctly
        assert tasks[0]["sol"] == 999
        assert tasks[1]["sol"] == 1001
        
        # Verify min/max operations work
        sol_range = result["ingestion_schedule"]["sol_range"]
        assert min(sol_range) == 999
        assert max(sol_range) == 1001
        assert sol_range == [999, 1000, 1001]

def test_generate_ingestion_batch_tasks_task_structure(mock_logger):
    """Test that each task has correct structure"""
    # Create test DataFrame
    test_data = {
        'ROVER_NAME': ['Curiosity', 'Perseverance'],
        'SOL': [2500, 1800]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T20:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        tasks = result["ingestion_schedule"]["tasks"]
        
        # Verify each task has exactly the required fields
        for task in tasks:
            assert set(task.keys()) == {"rover_name", "sol"}
            assert isinstance(task["rover_name"], str)
            assert isinstance(task["sol"], (int, pd.Int64Dtype))

def test_generate_ingestion_batch_tasks_logging_verification(mock_logger):
    """Test that all logging messages are correct"""
    # Create test DataFrame
    test_data = {
        'ROVER_NAME': ['Curiosity', 'Opportunity', 'Spirit'],
        'SOL': [1500, 1600, 1700]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T21:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify all expected log messages
        expected_log_messages = [
            "Attempting to generate tasks from ingestion batch",
            "Generated ingestion batch tasks - Tasks: 3, Sol Range: 1500 to 1700"
        ]
        
        actual_log_messages = [call.args[0] for call in mock_logger.info.call_args_list]
        
        for expected_msg in expected_log_messages:
            assert expected_msg in actual_log_messages

def test_generate_ingestion_batch_tasks_return_value_completeness(mock_logger):
    """Test that return value contains all required fields"""
    # Create test DataFrame
    test_data = {
        'ROVER_NAME': ['Perseverance'],
        'SOL': [1200]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T22:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify top-level structure
        required_top_level_keys = {"ingestion_schedule", "status", "timestamp"}
        assert set(result.keys()) == required_top_level_keys
        
        # Verify ingestion_schedule structure
        ingestion_schedule = result["ingestion_schedule"]
        required_schedule_keys = {"tasks", "sol_range"}
        assert set(ingestion_schedule.keys()) == required_schedule_keys
        
        # Verify data types
        assert isinstance(result["status"], str)
        assert isinstance(result["timestamp"], str)
        assert isinstance(ingestion_schedule["tasks"], list)
        assert isinstance(ingestion_schedule["sol_range"], list)

def test_generate_ingestion_batch_tasks_edge_case_large_dataset(mock_logger):
    """Test task generation with large dataset"""
    # Create large DataFrame (100 records)
    rovers = ['Curiosity', 'Opportunity', 'Spirit', 'Perseverance']
    test_data = {
        'ROVER_NAME': [rovers[i % 4] for i in range(100)],
        'SOL': list(range(1000, 1100))
    }
    test_dataframe = pd.DataFrame(test_data)
    
    with patch('src.utils.snowflake.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-10-02T23:00:00"
        
        result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
        
        # Verify all tasks created
        tasks = result["ingestion_schedule"]["tasks"]
        assert len(tasks) == 100
        
        # Verify sol_range is correct for large dataset
        sol_range = result["ingestion_schedule"]["sol_range"]
        expected_range = list(range(1000, 1100))  # 1000 to 1099
        assert sol_range == expected_range
        
        # Verify logging shows correct count
        mock_logger.info.assert_any_call("Generated ingestion batch tasks - Tasks: 100, Sol Range: 1000 to 1099")

def test_generate_ingestion_batch_tasks_timestamp_format(mock_logger):
    """Test that timestamp format is consistent"""
    # Create test DataFrame
    test_data = {
        'ROVER_NAME': ['Curiosity'],
        'SOL': [1500]
    }
    test_dataframe = pd.DataFrame(test_data)
    
    # Test multiple timestamp formats
    test_timestamps = [
        "2025-10-02T15:30:00",
        "2025-12-31T23:59:59", 
        "2025-01-01T00:00:00"
    ]
    
    for test_timestamp in test_timestamps:
        with patch('src.utils.snowflake.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = test_timestamp
            
            result = generate_ingestion_batch_tasks(test_dataframe, mock_logger)
            
            # Verify timestamp format
            assert result["timestamp"] == test_timestamp
            
            # Verify timestamp follows ISO format pattern
            assert len(result["timestamp"]) == 19
            assert result["timestamp"][4] == "-"
            assert result["timestamp"][7] == "-"
            assert result["timestamp"][10] == "T"
            assert result["timestamp"][13] == ":"
            assert result["timestamp"][16] == ":"