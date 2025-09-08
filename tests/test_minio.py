import pytest
from unittest.mock import patch, MagicMock
import json
from datetime import datetime, timedelta
from src.utils.minio import (
    get_minio_client,
    upload_json_to_minio,
    extract_json_as_jsonl_from_minio,
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

def test_upload_json_to_minio_new_bucket(mock_minio_client, sample_json_data, monkeypatch):
    # Mock environment variable
    monkeypatch.setenv('MINIO_BUCKET', 'test-bucket')
    
    # Set up mock client behavior - bucket doesn't exist
    mock_minio_client.bucket_exists.return_value = False
    
    # Test uploading data
    upload_json_to_minio(mock_minio_client, 'test/path.json', sample_json_data)
    
    # Verify bucket was created
    mock_minio_client.make_bucket.assert_called_once_with('test-bucket')
    
    # Verify put_object was called with correct parameters
    mock_minio_client.put_object.assert_called_once()
    call_args = mock_minio_client.put_object.call_args[1]
    assert call_args['bucket_name'] == 'test-bucket'
    assert call_args['object_name'] == 'test/path.json'
    assert call_args['content_type'] == 'application/json'

def test_upload_json_to_minio_existing_bucket(mock_minio_client, sample_json_data, monkeypatch):
    monkeypatch.setenv('MINIO_BUCKET', 'existing-bucket')
    
    # Set up mock client behavior - bucket exists
    mock_minio_client.bucket_exists.return_value = True
    
    upload_json_to_minio(mock_minio_client, 'photos/test.json', sample_json_data)
    
    # Verify bucket was NOT created since it already exists
    mock_minio_client.make_bucket.assert_not_called()
    
    # Verify put_object was still called
    mock_minio_client.put_object.assert_called_once()

def test_upload_json_to_minio_large_data(mock_minio_client, monkeypatch):
    monkeypatch.setenv('MINIO_BUCKET', 'test-bucket')
    mock_minio_client.bucket_exists.return_value = True
    
    # Create large dataset
    large_data = {"photos": [{"id": i, "img_src": f"photo{i}.jpg"} for i in range(1000)]}
    
    upload_json_to_minio(mock_minio_client, 'bulk/photos.json', large_data)
    
    # Verify the data size was calculated correctly
    call_args = mock_minio_client.put_object.call_args[1]
    assert call_args['length'] > 10000  # Should be a large file

def test_extract_json_as_jsonl_from_minio(mock_minio_client, sample_json_data, monkeypatch, tmp_path):
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
    
    # Verify original JSON file was cleaned up
    assert not result_path.endswith('.json')
    assert result_path.endswith('.jsonl')

def test_extract_json_as_jsonl_from_minio_error(mock_minio_client, monkeypatch):
    monkeypatch.setenv('MINIO_BUCKET_NAME', 'test-bucket')
    
    # Mock fget_object to raise an exception
    mock_minio_client.fget_object.side_effect = Exception("MinIO connection error")
    
    with pytest.raises(Exception, match="MinIO connection error"):
        extract_json_as_jsonl_from_minio(mock_minio_client, 'test/path.json')
