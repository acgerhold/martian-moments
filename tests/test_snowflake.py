import pytest
from unittest.mock import patch, MagicMock, mock_open
import os
from src.utils.snowflake import (
    get_snowflake_connection,
    copy_photos_to_snowflake
)

@pytest.fixture
def mock_cursor():
    """Create a mock cursor for Snowflake operations"""
    return MagicMock()

@pytest.fixture
def sample_jsonl_file_path(tmp_path):
    """Create a temporary JSONL file for testing"""
    test_file = tmp_path / "test_data.jsonl"
    test_file.write_text('{"photos": [{"id": 1, "img_src": "test.jpg"}]}\n')
    return str(test_file)

def test_get_snowflake_connection(monkeypatch):
    """Test Snowflake connection initialization with environment variables"""
    # Mock environment variables
    env_vars = {
        'SNOWFLAKE_ACCOUNT': 'test_account',
        'SNOWFLAKE_PASSWORD': 'test_password',
        'SNOWFLAKE_USER': 'test_user',
        'SNOWFLAKE_ROLE': 'test_role',
        'SNOWFLAKE_WAREHOUSE': 'test_warehouse',
        'SNOWFLAKE_DATABASE': 'test_database',
        'SNOWFLAKE_SCHEMA_BRONZE': 'test_schema'
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    # Mock load_dotenv and snowflake.connector.connect
    with patch('src.utils.snowflake.load_dotenv'):
        with patch('src.utils.snowflake.snowflake.connector.connect') as mock_connect:
            mock_connection = MagicMock()
            mock_connect.return_value = mock_connection
            
            result = get_snowflake_connection()
            
            # Verify connection was called with correct parameters
            mock_connect.assert_called_once_with(
                account='test_account',
                password='test_password',
                user='test_user',
                role='test_role',
                warehouse='test_warehouse',
                database='test_database',
                schema='test_schema'
            )
            assert result == mock_connection

def test_copy_photos_to_snowflake_success(mock_cursor, sample_jsonl_file_path, monkeypatch):
    """Test successful copy operation to Snowflake"""
    # Mock environment variables
    monkeypatch.setenv('SNOWFLAKE_DATABASE', 'test_db')
    monkeypatch.setenv('SNOWFLAKE_SCHEMA_BRONZE', 'bronze')
    
    # Test the function
    copy_photos_to_snowflake(mock_cursor, sample_jsonl_file_path)
    
    # Verify the correct SQL commands were executed
    expected_calls = [
        f"USE SCHEMA test_db.bronze;",
        "REMOVE @%RAW_PHOTO_RESPONSE PATTERN='.*';",
        f"PUT file://{sample_jsonl_file_path} @%RAW_PHOTO_RESPONSE OVERWRITE = TRUE",
        """
            COPY INTO RAW_PHOTO_RESPONSE
            FROM @%RAW_PHOTO_RESPONSE
            FILE_FORMAT = (TYPE = 'JSON')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'CONTINUE'
        """
    ]
    
    # Check that execute was called the expected number of times
    assert mock_cursor.execute.call_count == 4
    
    # Verify the calls were made with expected SQL
    actual_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert actual_calls[0] == expected_calls[0]
    assert actual_calls[1] == expected_calls[1]
    assert actual_calls[2] == expected_calls[2]
    # For the multi-line SQL, just check it contains key parts
    assert "COPY INTO RAW_PHOTO_RESPONSE" in actual_calls[3]
    assert "FILE_FORMAT = (TYPE = 'JSON')" in actual_calls[3]

def test_copy_photos_to_snowflake_file_cleanup(mock_cursor, sample_jsonl_file_path, monkeypatch):
    """Test that the JSONL file is cleaned up after processing"""
    # Mock environment variables
    monkeypatch.setenv('SNOWFLAKE_DATABASE', 'test_db')
    monkeypatch.setenv('SNOWFLAKE_SCHEMA_BRONZE', 'bronze')
    
    # Verify file exists before processing
    assert os.path.exists(sample_jsonl_file_path)
    
    # Test the function
    copy_photos_to_snowflake(mock_cursor, sample_jsonl_file_path)
    
    # Verify file was cleaned up
    assert not os.path.exists(sample_jsonl_file_path)

def test_copy_photos_to_snowflake_exception_handling(mock_cursor, sample_jsonl_file_path, monkeypatch):
    """Test that file cleanup happens even when SQL execution fails"""
    # Mock environment variables
    monkeypatch.setenv('SNOWFLAKE_DATABASE', 'test_db')
    monkeypatch.setenv('SNOWFLAKE_SCHEMA_BRONZE', 'bronze')
    
    # Make the PUT command fail
    def side_effect(sql):
        if "PUT file://" in sql:
            raise Exception("Snowflake connection error")
        return None
    
    mock_cursor.execute.side_effect = side_effect
    
    # Verify file exists before processing
    assert os.path.exists(sample_jsonl_file_path)
    
    # Test the function - should raise exception but still clean up file
    with pytest.raises(Exception, match="Snowflake connection error"):
        copy_photos_to_snowflake(mock_cursor, sample_jsonl_file_path)
    
    # Verify file was still cleaned up despite the exception
    assert not os.path.exists(sample_jsonl_file_path)

def test_copy_photos_to_snowflake_nonexistent_file(mock_cursor, monkeypatch):
    """Test handling of non-existent file path"""
    # Mock environment variables
    monkeypatch.setenv('SNOWFLAKE_DATABASE', 'test_db')
    monkeypatch.setenv('SNOWFLAKE_SCHEMA_BRONZE', 'bronze')
    
    nonexistent_file = "/tmp/nonexistent_file.jsonl"
    
    # Test the function with non-existent file
    copy_photos_to_snowflake(mock_cursor, nonexistent_file)
    
    # Verify SQL commands were still executed
    assert mock_cursor.execute.call_count == 4
    
    # Verify the PUT command was attempted with the non-existent file
    put_call = mock_cursor.execute.call_args_list[2][0][0]
    assert f"PUT file://{nonexistent_file}" in put_call

@patch('src.utils.snowflake.os.getenv')
def test_get_snowflake_connection_missing_env_vars(mock_getenv):
    """Test connection when environment variables are missing"""
    # Mock getenv to return None for all calls
    mock_getenv.return_value = None
    
    with patch('src.utils.snowflake.load_dotenv'):
        with patch('src.utils.snowflake.snowflake.connector.connect') as mock_connect:
            get_snowflake_connection()
            
            # Verify connection was called with None values
            mock_connect.assert_called_once_with(
                account=None,
                password=None,
                user=None,
                role=None,
                warehouse=None,
                database=None,
                schema=None
            )
