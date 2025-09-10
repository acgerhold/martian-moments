from unittest.mock import patch, MagicMock
import pytest
import os
from src.utils.snowflake import get_snowflake_connection, copy_file_to_snowflake

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def mock_snowflake_connection():
    return MagicMock()

@pytest.fixture
def mock_snowflake_cursor():
    cursor = MagicMock()
    return cursor

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

def test_copy_file_to_snowflake_success(mock_snowflake_connection, mock_logger):
    """Test successful file copy to Snowflake"""
    mock_cursor = MagicMock()
    mock_snowflake_connection.cursor.return_value = mock_cursor
    jsonl_file_path = "/tmp/test_file.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove') as mock_remove:
        
        copy_file_to_snowflake(mock_snowflake_connection, jsonl_file_path, mock_logger)
        
        # Verify cursor was created
        mock_snowflake_connection.cursor.assert_called_once()
        
        # Verify SQL commands were executed in correct order
        expected_calls = [
            "USE SCHEMA TEST_DB.BRONZE;",
            "REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';",
            f"PUT file://{jsonl_file_path} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE"
        ]
        
        # Check the first three execute calls
        assert mock_cursor.execute.call_count >= 3
        for i, expected_call in enumerate(expected_calls):
            actual_call = mock_cursor.execute.call_args_list[i][0][0]
            assert actual_call == expected_call
        
        # Verify COPY INTO command was executed (4th call)
        copy_command = mock_cursor.execute.call_args_list[3][0][0]
        assert "COPY INTO RAW_PHOTO_RESPONSE" in copy_command
        assert "FROM @%RAW_PHOTO_RESPONSE" in copy_command
        assert "FILE_FORMAT = (TYPE = 'JSON')" in copy_command
        
        # Verify cleanup
        mock_remove.assert_called_once_with(jsonl_file_path)
        mock_cursor.close.assert_called_once()
        mock_snowflake_connection.close.assert_called_once()

def test_copy_file_to_snowflake_file_not_exists(mock_snowflake_connection, mock_logger):
    """Test file copy when JSONL file doesn't exist for cleanup"""
    mock_cursor = MagicMock()
    mock_snowflake_connection.cursor.return_value = mock_cursor
    jsonl_file_path = "/tmp/nonexistent_file.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('os.path.exists', return_value=False), \
    patch('os.remove') as mock_remove:
        
        copy_file_to_snowflake(mock_snowflake_connection, jsonl_file_path, mock_logger)
        
        # Verify SQL operations still executed
        assert mock_cursor.execute.call_count >= 3
        
        # Verify file removal was not attempted since file doesn't exist
        mock_remove.assert_not_called()
        
        # Verify connections were still closed
        mock_cursor.close.assert_called_once()
        mock_snowflake_connection.close.assert_called_once()

def test_copy_file_to_snowflake_sql_error(mock_snowflake_connection, mock_logger):
    """Test file copy when SQL error occurs"""
    mock_cursor = MagicMock()
    mock_snowflake_connection.cursor.return_value = mock_cursor
    
    # Make the PUT command fail
    def execute_side_effect(sql):
        if "PUT file://" in sql:
            raise Exception("Snowflake SQL error")
        return None
    
    mock_cursor.execute.side_effect = execute_side_effect
    jsonl_file_path = "/tmp/test_file.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove') as mock_remove, \
    pytest.raises(Exception):
        
        copy_file_to_snowflake(mock_snowflake_connection, jsonl_file_path, mock_logger)
        
        # Verify cleanup still happens in finally block
        mock_remove.assert_called_once_with(jsonl_file_path)
        mock_cursor.close.assert_called_once()
        mock_snowflake_connection.close.assert_called_once()

def test_copy_file_to_snowflake_schema_usage(mock_snowflake_connection, mock_logger):
    """Test correct schema usage in SQL commands"""
    mock_cursor = MagicMock()
    mock_snowflake_connection.cursor.return_value = mock_cursor
    jsonl_file_path = "/tmp/schema_test.jsonl"
    
    test_cases = [
        ('PROD_DB', 'RAW_LAYER'),
        ('DEV_DATABASE', 'STAGING'),
        ('ANALYTICS_DB', 'BRONZE_SCHEMA')
    ]
    
    for database, schema in test_cases:
        mock_cursor.reset_mock()
        mock_snowflake_connection.reset_mock()
        mock_snowflake_connection.cursor.return_value = mock_cursor
        
        with patch.dict(os.environ, {
            'SNOWFLAKE_DATABASE': database,
            'SNOWFLAKE_SCHEMA_BRONZE': schema
        }), \
        patch('os.path.exists', return_value=True), \
        patch('os.remove'):
            
            copy_file_to_snowflake(mock_snowflake_connection, jsonl_file_path, mock_logger)
            
            # Verify correct schema is used
            use_schema_call = mock_cursor.execute.call_args_list[0][0][0]
            expected_schema_command = f"USE SCHEMA {database}.{schema};"
            assert use_schema_call == expected_schema_command

def test_copy_file_to_snowflake_copy_command_structure(mock_snowflake_connection, mock_logger):
    """Test the structure of the COPY INTO command"""
    mock_cursor = MagicMock()
    mock_snowflake_connection.cursor.return_value = mock_cursor
    jsonl_file_path = "/tmp/copy_test.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'TEST_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'BRONZE'
    }), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove'):
        
        copy_file_to_snowflake(mock_snowflake_connection, jsonl_file_path, mock_logger)
        
        # Find the COPY INTO command
        copy_command = None
        for call in mock_cursor.execute.call_args_list:
            sql = call[0][0]
            if "COPY INTO RAW_PHOTO_RESPONSE" in sql:
                copy_command = sql
                break
        
        assert copy_command is not None
        
        # Verify key components of COPY command
        assert "COPY INTO RAW_PHOTO_RESPONSE" in copy_command
        assert "FROM @%RAW_PHOTO_RESPONSE" in copy_command
        assert "FILE_FORMAT = (TYPE = 'JSON')" in copy_command
        assert "MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE" in copy_command
        assert "ON_ERROR = 'CONTINUE'" in copy_command

def test_copy_file_to_snowflake_cleanup_on_cursor_error(mock_snowflake_connection, mock_logger):
    """Test cleanup when cursor operations fail"""
    # Simulate cursor creation failing
    mock_snowflake_connection.cursor.side_effect = Exception("Cursor creation failed")
    jsonl_file_path = "/tmp/cursor_error_test.jsonl"
    
    with patch('os.path.exists', return_value=True), \
         patch('os.remove') as mock_remove, \
         pytest.raises(Exception):
        
        copy_file_to_snowflake(mock_snowflake_connection, jsonl_file_path, mock_logger)
        
        # Even when cursor creation fails, file should not be removed
        # because the finally block only removes if cursor was created
        mock_remove.assert_not_called()
        mock_snowflake_connection.close.assert_not_called()

def test_copy_file_to_snowflake_multiple_operations(mock_snowflake_connection, mock_logger):
    """Test all SQL operations are executed in sequence"""
    mock_cursor = MagicMock()
    mock_snowflake_connection.cursor.return_value = mock_cursor
    jsonl_file_path = "/tmp/operations_test.jsonl"
    
    with patch.dict(os.environ, {
        'SNOWFLAKE_DATABASE': 'OPERATIONS_DB',
        'SNOWFLAKE_SCHEMA_BRONZE': 'OPERATIONS_SCHEMA'
    }), \
    patch('os.path.exists', return_value=True), \
    patch('os.remove'):
        
        copy_file_to_snowflake(mock_snowflake_connection, jsonl_file_path, mock_logger)
        
        # Verify exactly 4 SQL operations were executed
        assert mock_cursor.execute.call_count == 4
        
        # Verify the sequence of operations
        executed_sqls = [call[0][0] for call in mock_cursor.execute.call_args_list]
        
        # 1. USE SCHEMA
        assert executed_sqls[0] == "USE SCHEMA OPERATIONS_DB.OPERATIONS_SCHEMA;"
        
        # 2. REMOVE files
        assert executed_sqls[1] == "REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';"
        
        # 3. PUT file
        assert executed_sqls[2] == f"PUT file://{jsonl_file_path} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE"
        
        # 4. COPY INTO
        assert "COPY INTO RAW_PHOTO_RESPONSE" in executed_sqls[3]
