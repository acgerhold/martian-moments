from unittest.mock import patch, MagicMock
import pytest
import os
from src.utils.snowflake import get_snowflake_connection, copy_file_to_snowflake, fetch_results_from_silver_schema

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
    patch('os.remove') as mock_remove:
        
        copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
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
    patch('os.remove') as mock_remove:
        
        copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
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
    patch('os.remove'):
        
        copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
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
    patch('os.remove') as mock_remove:
        
        copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
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
    patch('os.remove') as mock_remove:
        
        # The function should handle the exception internally and continue with cleanup
        try:
            copy_file_to_snowflake(jsonl_file_path, mock_logger)
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
        patch('os.remove'):
            
            copy_file_to_snowflake(jsonl_file_path, mock_logger)
            
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
    patch('os.remove'):
        
        copy_file_to_snowflake(jsonl_file_path, mock_logger)
        
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
        patch('os.remove'):
            
            copy_file_to_snowflake(jsonl_file_path, mock_logger)
            
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
        patch('os.remove'):
            
            copy_file_to_snowflake(jsonl_file_path, mock_logger)
            
            # Verify PUT command uses full path
            put_call = mock_cursor.execute.call_args_list[2][0][0]
            assert f"PUT file://{jsonl_file_path}" in put_call
            
            # Verify table routing works based on filename only (not path)
            assert "RAW_PHOTO_RESPONSE" in put_call

# ====== FETCH_RESULTS_FROM_SILVER_SCHEMA TESTS ======

def test_fetch_results_from_silver_schema_success(mock_logger):
    """Test successful data fetch from silver schema"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock query results
    mock_results = [
        (1, 'Curiosity', '2025-09-15', 'FHAZ'),
        (2, 'Perseverance', '2025-09-15', 'NAVCAM'),
        (3, 'Opportunity', '2025-09-14', 'PANCAM')
    ]
    
    # Mock column descriptions
    mock_cursor.description = [
        ('ID', 'NUMBER'),
        ('ROVER_NAME', 'VARCHAR'),
        ('DATE', 'DATE'),
        ('CAMERA', 'VARCHAR')
    ]
    
    # Mock the chained call: execute().fetchall()
    mock_execute_result = MagicMock()
    mock_execute_result.fetchall.return_value = mock_results
    mock_cursor.execute.return_value = mock_execute_result
    
    table_name = "PHOTOS_SILVER"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('pandas.DataFrame') as mock_dataframe:
        
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df
        
        result = fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify connection and cursor setup
        mock_connection.cursor.assert_called_once()
        
        # Verify SQL commands were executed
        assert mock_cursor.execute.call_count == 2
        
        # Verify USE SCHEMA command
        use_schema_call = mock_cursor.execute.call_args_list[0][0][0]
        assert use_schema_call == "USE SCHEMA TEST_DB.SILVER;"
        
        # Verify SELECT query
        select_call = mock_cursor.execute.call_args_list[1][0][0]
        assert select_call == f"SELECT * FROM {table_name}"
        
        # Verify fetchall was called on the execute result
        mock_execute_result.fetchall.assert_called_once()
        
        # Verify DataFrame was created with correct data
        expected_columns = ['ID', 'ROVER_NAME', 'DATE', 'CAMERA']
        mock_dataframe.assert_called_once_with(mock_results, columns=expected_columns)
        
        # Verify cleanup
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        # Verify logging
        mock_logger.info.assert_any_call(f"Attempting to fetch results - Table: {table_name}")
        mock_logger.info.assert_any_call(f"Fetched results successfully - Table: {table_name}")
        
        # Verify return value
        assert result == mock_df

def test_fetch_results_from_silver_schema_different_table(mock_logger):
    """Test data fetch with different table name"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock different table results
    mock_results = [
        (100, 25.5, -30.2, 'Curiosity'),
        (101, 26.1, -29.8, 'Perseverance')
    ]
    
    mock_cursor.description = [
        ('SOL', 'NUMBER'),
        ('LATITUDE', 'FLOAT'),
        ('LONGITUDE', 'FLOAT'),
        ('ROVER', 'VARCHAR')
    ]
    
    # Mock the chained call: execute().fetchall()
    mock_execute_result = MagicMock()
    mock_execute_result.fetchall.return_value = mock_results
    mock_cursor.execute.return_value = mock_execute_result
    
    table_name = "COORDINATES_SILVER"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'MARS_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'PROCESSED'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('pandas.DataFrame') as mock_dataframe:
        
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df
        
        result = fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify correct schema usage
        use_schema_call = mock_cursor.execute.call_args_list[0][0][0]
        assert use_schema_call == "USE SCHEMA MARS_DB.PROCESSED;"
        
        # Verify correct table query
        select_call = mock_cursor.execute.call_args_list[1][0][0]
        assert select_call == f"SELECT * FROM {table_name}"
        
        # Verify DataFrame created with coordinate data
        expected_columns = ['SOL', 'LATITUDE', 'LONGITUDE', 'ROVER']
        mock_dataframe.assert_called_once_with(mock_results, columns=expected_columns)

def test_fetch_results_from_silver_schema_empty_results(mock_logger):
    """Test data fetch when table has no results"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock empty results
    mock_results = []
    
    mock_cursor.description = [
        ('ID', 'NUMBER'),
        ('NAME', 'VARCHAR')
    ]
    
    # Mock the chained call: execute().fetchall()
    mock_execute_result = MagicMock()
    mock_execute_result.fetchall.return_value = mock_results
    mock_cursor.execute.return_value = mock_execute_result
    
    table_name = "EMPTY_TABLE"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('pandas.DataFrame') as mock_dataframe:
        
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df
        
        result = fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify DataFrame was still created with empty data
        expected_columns = ['ID', 'NAME']
        mock_dataframe.assert_called_once_with([], columns=expected_columns)
        
        # Verify logging still occurred
        mock_logger.info.assert_any_call(f"Attempting to fetch results - Table: {table_name}")
        mock_logger.info.assert_any_call(f"Fetched results successfully - Table: {table_name}")
        
        assert result == mock_df

def test_fetch_results_from_silver_schema_single_row(mock_logger):
    """Test data fetch with single row result"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock single row result
    mock_results = [
        ('MANIFEST_001', 'Curiosity', 3000, '2025-09-15T10:30:00')
    ]
    
    mock_cursor.description = [
        ('MANIFEST_ID', 'VARCHAR'),
        ('ROVER_NAME', 'VARCHAR'),
        ('MAX_SOL', 'NUMBER'),
        ('LAST_UPDATED', 'TIMESTAMP')
    ]
    
    # Mock the chained call: execute().fetchall()
    mock_execute_result = MagicMock()
    mock_execute_result.fetchall.return_value = mock_results
    mock_cursor.execute.return_value = mock_execute_result
    
    table_name = "MANIFESTS_SILVER"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('pandas.DataFrame') as mock_dataframe:
        
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df
        
        result = fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify DataFrame was created with single row
        expected_columns = ['MANIFEST_ID', 'ROVER_NAME', 'MAX_SOL', 'LAST_UPDATED']
        mock_dataframe.assert_called_once_with(mock_results, columns=expected_columns)
        
        assert result == mock_df

def test_fetch_results_from_silver_schema_query_error_cleanup(mock_logger):
    """Test that cleanup happens even if query fails"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Make the SELECT query fail
    def execute_side_effect(sql):
        if "SELECT" in sql:
            raise Exception("Query failed")
        # Let USE SCHEMA pass through
        return None
    
    mock_cursor.execute.side_effect = execute_side_effect
    
    table_name = "ERROR_TABLE"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection):
        
        # The function should raise the exception but still cleanup
        with pytest.raises(Exception, match="Query failed"):
            fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify cleanup still happened despite error
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

def test_fetch_results_from_silver_schema_connection_error_cleanup(mock_logger):
    """Test that cleanup happens even if connection fails"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock the chained call but make fetchall fail
    mock_execute_result = MagicMock()
    mock_execute_result.fetchall.side_effect = Exception("Connection error")
    mock_cursor.execute.return_value = mock_execute_result
    
    table_name = "CONNECTION_ERROR_TABLE"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection):
        
        # The function should raise the exception but still cleanup
        with pytest.raises(Exception, match="Connection error"):
            fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify cleanup still happened despite error
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()

def test_fetch_results_from_silver_schema_complex_data_types(mock_logger):
    """Test data fetch with complex data types"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Mock results with various data types
    mock_results = [
        (1, 'Curiosity', 3.14159, True, '2025-09-15 10:30:00', '{"camera": "FHAZ", "sol": 100}'),
        (2, 'Perseverance', 2.71828, False, '2025-09-14 15:45:30', '{"camera": "NAVCAM", "sol": 50}')
    ]
    
    mock_cursor.description = [
        ('ID', 'NUMBER'),
        ('ROVER_NAME', 'VARCHAR'),
        ('COORDINATE', 'FLOAT'),
        ('IS_ACTIVE', 'BOOLEAN'),
        ('TIMESTAMP', 'TIMESTAMP_NTZ'),
        ('METADATA', 'VARIANT')
    ]
    
    # Mock the chained call: execute().fetchall()
    mock_execute_result = MagicMock()
    mock_execute_result.fetchall.return_value = mock_results
    mock_cursor.execute.return_value = mock_execute_result
    
    table_name = "COMPLEX_DATA_TABLE"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('pandas.DataFrame') as mock_dataframe:
        
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df
        
        result = fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify DataFrame was created with complex data types
        expected_columns = ['ID', 'ROVER_NAME', 'COORDINATE', 'IS_ACTIVE', 'TIMESTAMP', 'METADATA']
        mock_dataframe.assert_called_once_with(mock_results, columns=expected_columns)
        
        assert result == mock_df

def test_fetch_results_from_silver_schema_different_environments(mock_logger):
    """Test fetch with different database and schema configurations"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    mock_results = [('test', 'data')]
    mock_cursor.description = [('COL1', 'VARCHAR'), ('COL2', 'VARCHAR')]
    
    table_name = "TEST_TABLE"
    
    test_cases = [
        ('PROD_DB', 'SILVER_LAYER'),
        ('DEV_DATABASE', 'PROCESSED'),
        ('ANALYTICS_DB', 'CURATED_SCHEMA')
    ]
    
    for database, schema in test_cases:
        mock_cursor.reset_mock()
        mock_connection.reset_mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.description = [('COL1', 'VARCHAR'), ('COL2', 'VARCHAR')]
        
        # Mock the chained call: execute().fetchall()
        mock_execute_result = MagicMock()
        mock_execute_result.fetchall.return_value = mock_results
        mock_cursor.execute.return_value = mock_execute_result
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_DATABASE': database,
            'SNOWFLAKE_SCHEMA_SILVER': schema
        }), \
        patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
        patch('pandas.DataFrame') as mock_dataframe:
            
            mock_df = MagicMock()
            mock_dataframe.return_value = mock_df
            
            result = fetch_results_from_silver_schema(table_name, mock_logger)
            
            # Verify correct schema usage
            use_schema_call = mock_cursor.execute.call_args_list[0][0][0]
            assert use_schema_call == f"USE SCHEMA {database}.{schema};"

def test_fetch_results_from_silver_schema_table_name_variations(mock_logger):
    """Test fetch with different table name formats"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    mock_results = [('test',)]
    mock_cursor.description = [('COL', 'VARCHAR')]
    
    test_table_names = [
        "SIMPLE_TABLE",
        "schema.table_name",  # Qualified table name
        "MARS_ROVER_PHOTOS_PROCESSED",  # Long descriptive name
        "T1",  # Short name
        "photos_2025_q3_final",  # With underscores and numbers
    ]
    
    for table_name in test_table_names:
        mock_cursor.reset_mock()
        mock_connection.reset_mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.description = [('COL', 'VARCHAR')]
        
        # Mock the chained call: execute().fetchall()
        mock_execute_result = MagicMock()
        mock_execute_result.fetchall.return_value = mock_results
        mock_cursor.execute.return_value = mock_execute_result
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_DATABASE': 'TEST_DB',
            'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
        }), \
        patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
        patch('pandas.DataFrame') as mock_dataframe:
            
            mock_df = MagicMock()
            mock_dataframe.return_value = mock_df
            
            result = fetch_results_from_silver_schema(table_name, mock_logger)
            
            # Verify SELECT query uses exact table name
            select_call = mock_cursor.execute.call_args_list[1][0][0]
            assert select_call == f"SELECT * FROM {table_name}"
            
            # Verify logging includes table name
            mock_logger.info.assert_any_call(f"Attempting to fetch results - Table: {table_name}")
            mock_logger.info.assert_any_call(f"Fetched results successfully - Table: {table_name}")

def test_fetch_results_from_silver_schema_large_dataset_simulation(mock_logger):
    """Test fetch simulation with larger dataset"""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()
    mock_connection.cursor.return_value = mock_cursor
    
    # Simulate larger dataset (1000 rows)
    mock_results = [(i, f'rover_{i%3}', f'camera_{i%5}', f'2025-09-{15+(i%10)}') for i in range(1000)]
    
    mock_cursor.description = [
        ('ID', 'NUMBER'),
        ('ROVER', 'VARCHAR'),
        ('CAMERA', 'VARCHAR'),
        ('DATE', 'DATE')
    ]
    
    # Mock the chained call: execute().fetchall()
    mock_execute_result = MagicMock()
    mock_execute_result.fetchall.return_value = mock_results
    mock_cursor.execute.return_value = mock_execute_result
    
    table_name = "LARGE_PHOTOS_TABLE"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_SILVER': 'SILVER'
    }), \
    patch('src.utils.snowflake.get_snowflake_connection', return_value=mock_connection), \
    patch('pandas.DataFrame') as mock_dataframe:
        
        mock_df = MagicMock()
        mock_dataframe.return_value = mock_df
        
        result = fetch_results_from_silver_schema(table_name, mock_logger)
        
        # Verify DataFrame was called with large dataset
        expected_columns = ['ID', 'ROVER', 'CAMERA', 'DATE']
        mock_dataframe.assert_called_once_with(mock_results, columns=expected_columns)
        
        # Verify proper cleanup even with large dataset
        mock_cursor.close.assert_called_once()
        mock_connection.close.assert_called_once()
        
        assert result == mock_df