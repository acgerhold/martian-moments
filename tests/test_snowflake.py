from unittest.mock import patch, MagicMock
import pytest
import os
from src.utils.snowflake import get_snowflake_connection, copy_file_to_snowflake, fetch_next_ingestion_batch

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


# ====== FETCH_NEXT_INGESTION_BATCH TESTS ======

def test_fetch_next_ingestion_batch_success_with_batches(mock_logger):
    """Test successful fetch with ingestion batches available"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock fetchall result
    mock_cursor.fetchall.return_value = [
        ('Curiosity', 2800, 2900, 2799, 4666, False, 1867, 19, '2025-09-22 11:09:22.480'),
        ('Opportunity', 2800, 2900, 2799, 5111, False, 2312, 24, '2025-09-22 11:09:22.480'),
        ('Perseverance', 1700, 1631, 1631, 1631, True, 0, 0, '2025-09-22 11:09:22.480'),
        ('Spirit', 2300, 2208, 2207, 2208, False, 1, 1, '2025-09-22 11:09:22.480')
    ]
    
    # Mock cursor description for DataFrame columns
    mock_cursor.description = [
        ('ROVER_NAME',), ('START_SOL',), ('END_SOL',), ('PHOTOS_MAX_SOL',), 
        ('MANIFEST_MAX_SOL',), ('UP_TO_DATE',), ('TOTAL_SOLS_NEEDED',), 
        ('ESTIMATED_BATCHES',), ('TASK_CREATED_AT',)
    ]
    
    # Mock the execute method to return the mock cursor for method chaining
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T21:47:59"
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify connection and cursor setup
        mock_connection.cursor.assert_called_once()
        
        # Verify SQL commands
        expected_calls = [
            f"USE SCHEMA TEST_DB.SILVER;",
            f"SELECT * FROM INGESTION_PLANNING;"
        ]
        
        for i, expected_call in enumerate(expected_calls):
            actual_call = mock_cursor.execute.call_args_list[i][0][0]
            assert actual_call == expected_call
        
        # Verify result structure
        assert result["status"] == "success"
        assert result["timestamp"] == "2025-09-22T21:47:59"
        assert "ingestion_schedule" in result
        
        # Verify only non-up-to-date batches are included
        expected_batches = [
            {"rover_name": "Curiosity", "sol_start": 2800, "sol_end": 2900},
            {"rover_name": "Opportunity", "sol_start": 2800, "sol_end": 2900},
            {"rover_name": "Spirit", "sol_start": 2300, "sol_end": 2208}
        ]
        assert result["ingestion_schedule"] == expected_batches
        
        # Verify cleanup
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_any_call("Attempting to fetch next ingestion batch")
        mock_logger.info.assert_any_call(f"Fetched next ingestion batch - Batch: {expected_batches}")

def test_fetch_next_ingestion_batch_all_up_to_date(mock_logger):
    """Test fetch when all rovers are up to date"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # All rovers are up to date
    mock_cursor.fetchall.return_value = [
        ('Curiosity', 2800, 2800, 2800, 2800, True, 0, 0, '2025-09-22 11:09:22.480'),
        ('Perseverance', 1700, 1700, 1700, 1700, True, 0, 0, '2025-09-22 11:09:22.480')
    ]
    
    mock_cursor.description = [
        ('ROVER_NAME',), ('START_SOL',), ('END_SOL',), ('PHOTOS_MAX_SOL',), 
        ('MANIFEST_MAX_SOL',), ('UP_TO_DATE',), ('TOTAL_SOLS_NEEDED',), 
        ('ESTIMATED_BATCHES',), ('TASK_CREATED_AT',)
    ]
    
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T21:47:59"
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify empty ingestion schedule
        assert result["ingestion_schedule"] == []
        assert result["status"] == "success"
        
        # Verify logging shows empty batch
        mock_logger.info.assert_any_call("Fetched next ingestion batch - Batch: []")

def test_fetch_next_ingestion_batch_false_trigger(mock_logger):
    """Test fetch when run_dbt_models_success is False"""
    # When run_dbt_models_success is False, function should not execute
    
    with patch('src.utils.snowflake.get_snowflake_connection') as mock_get_conn:
        result = fetch_next_ingestion_batch(False, mock_logger)
        
        # Should return None and not attempt any database operations
        assert result is None
        mock_get_conn.assert_not_called()
        mock_logger.info.assert_not_called()

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
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T21:47:59"
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify error logging
        mock_logger.error.assert_called_once()
        error_msg = mock_logger.error.call_args[0][0]
        assert "Error fetching results from INGESTION_PLANNING" in error_msg
        assert "SQL query failed" in error_msg
        
        # Verify cleanup still happens
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # Verify final result still returned (with empty schedule)
        assert result["status"] == "success"
        assert result["ingestion_schedule"] == []

def test_fetch_next_ingestion_batch_empty_results(mock_logger):
    """Test fetch when query returns no results"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Empty query results
    mock_cursor.fetchall.return_value = []
    mock_cursor.description = [
        ('ROVER_NAME',), ('START_SOL',), ('END_SOL',), ('PHOTOS_MAX_SOL',), 
        ('MANIFEST_MAX_SOL',), ('UP_TO_DATE',), ('TOTAL_SOLS_NEEDED',), 
        ('ESTIMATED_BATCHES',), ('TASK_CREATED_AT',)
    ]
    
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T21:47:59"
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify empty ingestion schedule
        assert result["ingestion_schedule"] == []
        assert result["status"] == "success"

def test_fetch_next_ingestion_batch_mixed_data_types(mock_logger):
    """Test fetch with mixed data types and edge cases"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mixed data with various edge cases
    mock_cursor.fetchall.return_value = [
        ('Curiosity', 0, 100, 0, 100, False, 100, 1, '2025-09-22 11:09:22.480'),  # Starting from sol 0
        ('TestRover', 5000, 5000, 5000, 5000, True, 0, 0, '2025-09-22 11:09:22.480'),  # High sol numbers, up to date
        ('EdgeCase', 1, 2, 1, 2, False, 1, 1, '2025-09-22 11:09:22.480'),  # Single sol difference
    ]
    
    mock_cursor.description = [
        ('ROVER_NAME',), ('START_SOL',), ('END_SOL',), ('PHOTOS_MAX_SOL',), 
        ('MANIFEST_MAX_SOL',), ('UP_TO_DATE',), ('TOTAL_SOLS_NEEDED',), 
        ('ESTIMATED_BATCHES',), ('TASK_CREATED_AT',)
    ]
    
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T21:47:59"
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify only non-up-to-date batches
        expected_batches = [
            {"rover_name": "Curiosity", "sol_start": 0, "sol_end": 100},
            {"rover_name": "EdgeCase", "sol_start": 1, "sol_end": 2}
        ]
        assert result["ingestion_schedule"] == expected_batches

def test_fetch_next_ingestion_batch_environment_variations(mock_logger):
    """Test fetch with different environment configurations"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    mock_cursor.fetchall.return_value = [
        ('Curiosity', 100, 200, 100, 200, False, 100, 1, '2025-09-22 11:09:22.480')
    ]
    
    mock_cursor.description = [
        ('ROVER_NAME',), ('START_SOL',), ('END_SOL',), ('PHOTOS_MAX_SOL',), 
        ('MANIFEST_MAX_SOL',), ('UP_TO_DATE',), ('TOTAL_SOLS_NEEDED',), 
        ('ESTIMATED_BATCHES',), ('TASK_CREATED_AT',)
    ]
    
    mock_cursor.execute.return_value = mock_cursor
    
    test_environments = [
        ('PROD_DB', 'ANALYTICS'),
        ('DEV_DATABASE', 'STAGING_SILVER'),
        ('TEST_DB', 'TRANSFORMED_LAYER')
    ]
    
    for database, schema in test_environments:
        mock_cursor.reset_mock()
        mock_connection.reset_mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.return_value = mock_cursor
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_DATABASE': database,
            'SNOWFLAKE_SCHEMA_SILVER': schema
        }), \
        patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
        patch('src.utils.snowflake.datetime') as mock_datetime:
            
            mock_datetime.now.return_value.strftime.return_value = "2025-09-22T21:47:59"
            
            result = fetch_next_ingestion_batch(True, mock_logger)
            
            # Verify correct schema usage
            use_schema_call = mock_cursor.execute.call_args_list[0][0][0]
            assert use_schema_call == f"USE SCHEMA {database}.{schema};"
            
            # Verify result is still properly formatted
            assert result["status"] == "success"
            assert len(result["ingestion_schedule"]) == 1

def test_fetch_next_ingestion_batch_dataframe_processing(mock_logger):
    """Test DataFrame processing logic with pandas operations"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Test data with specific focus on UP_TO_DATE filtering
    mock_cursor.fetchall.return_value = [
        ('RoverA', 100, 200, 100, 200, False, 100, 1, '2025-09-22 11:09:22.480'),
        ('RoverB', 300, 400, 300, 400, True, 0, 0, '2025-09-22 11:09:22.480'),
        ('RoverC', 500, 600, 500, 600, False, 100, 1, '2025-09-22 11:09:22.480'),
        ('RoverD', 700, 800, 700, 800, True, 0, 0, '2025-09-22 11:09:22.480'),
        ('RoverE', 900, 1000, 900, 1000, False, 100, 1, '2025-09-22 11:09:22.480')
    ]
    
    mock_cursor.description = [
        ('ROVER_NAME',), ('START_SOL',), ('END_SOL',), ('PHOTOS_MAX_SOL',), 
        ('MANIFEST_MAX_SOL',), ('UP_TO_DATE',), ('TOTAL_SOLS_NEEDED',), 
        ('ESTIMATED_BATCHES',), ('TASK_CREATED_AT',)
    ]
    
    mock_cursor.execute.return_value = mock_cursor
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('src.utils.snowflake.datetime') as mock_datetime:
        
        mock_datetime.now.return_value.strftime.return_value = "2025-09-22T21:47:59"
        
        result = fetch_next_ingestion_batch(True, mock_logger)
        
        # Verify only False UP_TO_DATE entries are processed
        expected_batches = [
            {"rover_name": "RoverA", "sol_start": 100, "sol_end": 200},
            {"rover_name": "RoverC", "sol_start": 500, "sol_end": 600},
            {"rover_name": "RoverE", "sol_start": 900, "sol_end": 1000}
        ]
        assert result["ingestion_schedule"] == expected_batches
        assert len(result["ingestion_schedule"]) == 3
