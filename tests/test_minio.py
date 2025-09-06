import pytest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime, timedelta

from src.utils.minio import (
    get_minio_client,
    upload_json_to_minio,
    extract_json_as_jsonl_from_minio,
    get_recent_minio_files
)

@pytest.fixture
def mock_minio_client():
    with patch('minio.Minio') as mock_minio:
        client = MagicMock()
        mock_minio.return_value = client
        yield client

@pytest.fixture
def sample_json_data():
    return {
        "photos": [
            {"id": 1, "img_src": "http://example.com/photo1.jpg"},
            {"id": 2, "img_src": "http://example.com/photo2.jpg"}
        ]
    }

def test_get_minio_client(monkeypatch):
    """Test Minio client initialization with environment variables"""
    # Mock environment variables
    env_vars = {
        'MINIO_EXTERNAL_URL': 'localhost:9000',
        'MINIO_ROOT_USER': 'testuser',
        'MINIO_ROOT_PASSWORD': 'testpass'
    }
    for key, value in env_vars.items():
        monkeypatch.setenv(key, value)
    
    # Mock load_dotenv to do nothing
    with patch('src.utils.minio.load_dotenv'):
        with patch('src.utils.minio.Minio') as mock_minio:
            get_minio_client()
            mock_minio.assert_called_once_with(
                'localhost:9000',
                access_key='testuser',
                secret_key='testpass',
                secure=False
            )

def test_upload_json_to_minio(mock_minio_client, sample_json_data, monkeypatch):
    """Test uploading JSON data to MinIO"""
    # Mock environment variable
    monkeypatch.setenv('MINIO_BUCKET', 'test-bucket')
    
    # Set up mock client behavior
    mock_minio_client.bucket_exists.return_value = False
    
    # Test uploading data
    upload_json_to_minio(mock_minio_client, 'test/path.json', sample_json_data)
    
    # Verify bucket was created if it didn't exist
    mock_minio_client.make_bucket.assert_called_once_with('test-bucket')
    
    # Verify put_object was called with correct parameters
    mock_minio_client.put_object.assert_called_once()
    call_args = mock_minio_client.put_object.call_args[1]
    assert call_args['bucket_name'] == 'test-bucket'
    assert call_args['object_name'] == 'test/path.json'
    assert call_args['content_type'] == 'application/json'

def test_extract_json_as_jsonl_from_minio(mock_minio_client, sample_json_data, monkeypatch, tmp_path):
    """Test extracting JSON from MinIO and converting to JSONL"""
    monkeypatch.setenv('MINIO_BUCKET_NAME', 'test-bucket')
    test_file = tmp_path / "test.json"
    
    # Write sample data to temp file
    with open(test_file, 'w') as f:
        json.dump(sample_json_data, f)
    
    def mock_fget_object(bucket, src, dst):
        with open(test_file, 'rb') as src_file:
            with open(dst, 'wb') as dst_file:
                dst_file.write(src_file.read())
    
    mock_minio_client.fget_object.side_effect = mock_fget_object
    
    # Test the function
    result_path = extract_json_as_jsonl_from_minio(mock_minio_client, 'test/path.json')
    
    # Verify the JSONL file was created correctly
    with open(result_path, 'r') as f:
        jsonl_content = f.read().strip()
        loaded_data = json.loads(jsonl_content)
        assert loaded_data == sample_json_data

@patch('src.utils.minio.setup_logger')
def test_get_recent_minio_files(mock_logger, mock_minio_client, monkeypatch):
    """Test retrieving recent files from MinIO"""
    # Mock environment variable
    monkeypatch.setenv('MINIO_BUCKET_NAME', 'test-bucket')
    
    # Set up logger mock
    logger_instance = MagicMock()
    mock_logger.return_value = logger_instance
    
    # Create mock objects with different timestamps
    now = datetime.now()
    recent_file = MagicMock()
    recent_file.last_modified = now - timedelta(minutes=30)
    recent_file.object_name = 'photos/recent.json'
    
    old_file = MagicMock()
    old_file.last_modified = now - timedelta(minutes=120)
    old_file.object_name = 'photos/old.json'
    
    # Mock list_objects to return our test files
    mock_minio_client.list_objects.return_value = [recent_file, old_file]
    
    # Test the function
    recent_files = get_recent_minio_files(mock_minio_client, minutes_ago=60)
    
    # Verify only recent files are returned
    assert len(recent_files) == 1
    assert recent_files[0] == 'photos/recent.json'

@patch('src.utils.minio.setup_logger')
def test_get_recent_minio_files_error_handling(mock_logger, mock_minio_client, monkeypatch):
    """Test error handling in get_recent_minio_files"""
    # Mock environment variable
    monkeypatch.setenv('MINIO_BUCKET_NAME', 'test-bucket')
    
    # Set up logger mock
    logger_instance = MagicMock()
    mock_logger.return_value = logger_instance
    
    # Mock list_objects to raise an exception
    mock_minio_client.list_objects.side_effect = Exception("Connection error")
    
    # Test the function
    recent_files = get_recent_minio_files(mock_minio_client)
    
    # Verify empty list is returned on error
    assert recent_files == []
    # Verify error was logged
    logger_instance.error.assert_called_once()
