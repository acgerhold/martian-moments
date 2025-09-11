from unittest.mock import patch, MagicMock, mock_open
import pytest
import json
import os
import tempfile
from io import BytesIO
from src.utils.minio import get_minio_client, upload_json_to_minio, extract_json_as_jsonl_from_minio

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def mock_minio_client():
    return MagicMock()

@pytest.fixture
def sample_final_json():
    return {
        "filename": "mars_rover_photos_batch_sol_100_to_100_2025-09-10T12:00:00.json",
        "sol_start": 100,
        "sol_end": 100,
        "photo_count": 2,
        "photos": [
            {"id": 1, "img_src": "http://example.com/photo1.jpg"},
            {"id": 2, "img_src": "http://example.com/photo2.jpg"}
        ],
        "ingestion_date": "2025-09-10T12:00:00"
    }

# ====== GET_MINIO_CLIENT TESTS ======

@patch.dict(os.environ, {
    'MINIO_EXTERNAL_URL': 'localhost:9000',
    'MINIO_ROOT_USER': 'testuser',
    'MINIO_ROOT_PASSWORD': 'testpass'
})
def test_get_minio_client_success():
    """Test successful MinIO client creation"""
    with patch('src.utils.minio.Minio') as mock_minio:
        mock_client_instance = MagicMock()
        mock_minio.return_value = mock_client_instance
        
        result = get_minio_client()
        
        mock_minio.assert_called_once_with(
            'localhost:9000',
            access_key='testuser',
            secret_key='testpass',
            secure=False
        )
        assert result == mock_client_instance

@patch.dict(os.environ, {}, clear=True)
def test_get_minio_client_missing_env_vars():
    """Test MinIO client creation with missing environment variables"""
    with patch('src.utils.minio.Minio') as mock_minio:
        mock_client_instance = MagicMock()
        mock_minio.return_value = mock_client_instance
        
        result = get_minio_client()
        
        mock_minio.assert_called_once_with(
            None,  # MINIO_EXTERNAL_URL not set
            access_key=None,  # MINIO_ROOT_USER not set
            secret_key=None,  # MINIO_ROOT_PASSWORD not set
            secure=False
        )
        assert result == mock_client_instance

# ====== UPLOAD_JSON_TO_MINIO TESTS ======

def test_upload_json_to_minio_new_bucket(mock_minio_client, sample_final_json, mock_logger):
    """Test uploading JSON to MinIO when bucket doesn't exist"""
    mock_minio_client.bucket_exists.return_value = False
    
    with patch('src.utils.minio.MINIO_BUCKET', 'test-bucket'):
        upload_json_to_minio(mock_minio_client, sample_final_json, mock_logger)
        
        # Verify bucket creation
        mock_minio_client.bucket_exists.assert_called_once_with('test-bucket')
        mock_minio_client.make_bucket.assert_called_once_with('test-bucket')
        
        # Verify file upload
        mock_minio_client.put_object.assert_called_once()
        call_args = mock_minio_client.put_object.call_args
        
        assert call_args[1]['bucket_name'] == 'test-bucket'
        assert call_args[1]['object_name'] == 'photos/mars_rover_photos_batch_sol_100_to_100_2025-09-10T12:00:00.json'
        assert call_args[1]['content_type'] == 'application/json'
        assert isinstance(call_args[1]['data'], BytesIO)
        
        # Verify logging
        assert mock_logger.info.call_count == 2
        log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Attempting upload to MinIO" in msg for msg in log_calls)
        assert any("Uploaded to MinIO" in msg and "Photos Count: 2" in msg for msg in log_calls)

def test_upload_json_to_minio_existing_bucket(mock_minio_client, sample_final_json, mock_logger):
    """Test uploading JSON to MinIO when bucket already exists"""
    mock_minio_client.bucket_exists.return_value = True
    
    with patch('src.utils.minio.MINIO_BUCKET', 'existing-bucket'):
        upload_json_to_minio(mock_minio_client, sample_final_json, mock_logger)
        
        # Verify bucket existence check but no creation
        mock_minio_client.bucket_exists.assert_called_once_with('existing-bucket')
        mock_minio_client.make_bucket.assert_not_called()
        
        # Verify file upload still happens
        mock_minio_client.put_object.assert_called_once()

def test_upload_json_to_minio_empty_photos(mock_minio_client, mock_logger):
    """Test uploading JSON with no photos"""
    empty_json = {
        "filename": "empty_batch_2025-09-10T12:00:00.json",
        "sol_start": 100,
        "sol_end": 100,
        "photo_count": 0,
        "photos": [],
        "ingestion_date": "2025-09-10T12:00:00"
    }
    
    mock_minio_client.bucket_exists.return_value = True
    
    with patch('src.utils.minio.MINIO_BUCKET', 'test-bucket'):
        upload_json_to_minio(mock_minio_client, empty_json, mock_logger)
        
        mock_minio_client.put_object.assert_called_once()
        
        # Verify logging shows 0 photos
        log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Photos Count: 0" in msg for msg in log_calls)

# ====== EXTRACT_JSON_AS_JSONL_FROM_MINIO TESTS ======

def test_extract_json_as_jsonl_from_minio_success(mock_minio_client, sample_final_json, mock_logger):
    """Test successful extraction and conversion from MinIO"""
    minio_filepath = "test-bucket/photos/test_file.json"
    
    with patch('tempfile.gettempdir', return_value='/tmp'), \
         patch('src.utils.minio.MINIO_BUCKET', 'test-bucket'), \
         patch('builtins.open', mock_open(read_data=json.dumps(sample_final_json))), \
         patch('os.remove') as mock_remove:
        
        result = extract_json_as_jsonl_from_minio(mock_minio_client, minio_filepath, mock_logger)
        
        # Verify MinIO download
        mock_minio_client.fget_object.assert_called_once_with(
            'test-bucket', 
            'photos/test_file.json', 
            '/tmp/test_file.json'
        )
        
        # Verify result path
        assert result == '/tmp/test_file.jsonl'
        
        # Verify temp file cleanup
        mock_remove.assert_called_once_with('/tmp/test_file.json')
        
        # Verify logging
        assert mock_logger.info.call_count == 3
        log_calls = [call[0][0] for call in mock_logger.info.call_args_list]
        assert any("Attempting extract from MinIO" in msg for msg in log_calls)
        assert any("Extracted from MinIO" in msg for msg in log_calls)

def test_extract_json_as_jsonl_from_minio_filepath_parsing(mock_minio_client, mock_logger):
    """Test filepath parsing with different input formats"""
    test_cases = [
        ("test-bucket/photos/file.json", "photos/file.json"),
        ("photos/another_file.json", "photos/another_file.json"),
        ("simple_file.json", "simple_file.json")
    ]
    
    for input_path, expected_minio_path in test_cases:
        mock_minio_client.reset_mock()
        mock_logger.reset_mock()
        
        with patch('tempfile.gettempdir', return_value='/tmp'), \
             patch('src.utils.minio.MINIO_BUCKET', 'test-bucket'), \
             patch('builtins.open', mock_open(read_data='{"test": "data"}')), \
             patch('os.remove'):
            
            result = extract_json_as_jsonl_from_minio(mock_minio_client, input_path, mock_logger)
            
            # Verify correct MinIO path is used
            mock_minio_client.fget_object.assert_called_once()
            call_args = mock_minio_client.fget_object.call_args[0]
            assert call_args[1] == expected_minio_path

def test_extract_json_as_jsonl_from_minio_file_operations(mock_minio_client, mock_logger):
    """Test file read/write operations during extraction"""
    minio_filepath = "test-bucket/photos/data.json"
    test_data = {"photos": [{"id": 1}], "photo_count": 1}
    
    with patch('tempfile.gettempdir', return_value='/tmp'), \
         patch('src.utils.minio.MINIO_BUCKET', 'test-bucket'), \
         patch('os.remove') as mock_remove:
        
        # Mock file operations
        mock_file_handles = {}
        
        def mock_open_func(filename, mode='r'):
            mock_handle = MagicMock()
            mock_file_handles[filename] = mock_handle
            
            if mode == 'r':
                mock_handle.__enter__.return_value.read.return_value = json.dumps(test_data)
                mock_handle.__enter__.return_value.load = MagicMock(return_value=test_data)
            elif mode == 'w':
                mock_handle.__enter__.return_value.write = MagicMock()
            
            return mock_handle
        
        with patch('builtins.open', side_effect=mock_open_func):
            result = extract_json_as_jsonl_from_minio(mock_minio_client, minio_filepath, mock_logger)
            
            # Verify correct files were opened
            assert '/tmp/data.json' in mock_file_handles  # Read operation
            assert '/tmp/data.jsonl' in mock_file_handles  # Write operation
            
            # Verify result path
            assert result == '/tmp/data.jsonl'
            
            # Verify temp JSON file was removed
            mock_remove.assert_called_once_with('/tmp/data.json')

def test_extract_json_as_jsonl_from_minio_complex_data(mock_minio_client, mock_logger):
    """Test extraction with complex nested JSON data"""
    complex_data = {
        "filename": "complex_batch.json",
        "photos": [
            {
                "id": 123,
                "img_src": "http://example.com/photo123.jpg",
                "camera": {"name": "MASTCAM", "full_name": "Mast Camera"},
                "rover": {"name": "Curiosity", "status": "active"}
            }
        ],
        "metadata": {"processing_time": 1.5, "api_version": "v1.0"}
    }
    
    minio_filepath = "test-bucket/photos/complex.json"
    
    with patch('tempfile.gettempdir', return_value='/tmp'), \
         patch('src.utils.minio.MINIO_BUCKET', 'test-bucket'), \
         patch('builtins.open', mock_open(read_data=json.dumps(complex_data))), \
         patch('os.remove'):
        
        result = extract_json_as_jsonl_from_minio(mock_minio_client, minio_filepath, mock_logger)
        
        assert result == '/tmp/complex.jsonl'
        mock_minio_client.fget_object.assert_called_once_with(
            'test-bucket', 
            'photos/complex.json', 
            '/tmp/complex.json'
        )
