from unittest.mock import patch, MagicMock
import pytest
import requests
from src.ingestion import extract_photos_from_nasa, extract_manifest_from_nasa

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def sample_photos_response():
    return {
        "photos": [
            {"id": 1, "img_src": "http://example.com/photo1.jpg", "camera": {"name": "FHAZ"}},
            {"id": 2, "img_src": "http://example.com/photo2.jpg", "camera": {"name": "RHAZ"}}
        ]
    }

@pytest.fixture
def sample_manifest_response():
    return {
        "photo_manifest": {
            "name": "Perseverance",
            "landing_date": "2021-02-18",
            "launch_date": "2020-07-30",
            "status": "active",
            "max_sol": 500,
            "max_date": "2022-06-26",
            "total_photos": 150000
        }
    }

# PHOTOS TESTS
def test_extract_photos_from_nasa_success(mock_logger, sample_photos_response):
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = sample_photos_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("Perseverance", 1, mock_logger)
        
        assert result == sample_photos_response
        mock_get.assert_called_once()
        assert "rovers/Perseverance/photos" in mock_get.call_args[0][0]
        assert "sol=1" in mock_get.call_args[0][0]
        mock_logger.info.assert_called_with("Processing photos request for rover: Perseverance on sol: 1")

def test_extract_photos_from_nasa_different_rovers(mock_logger, sample_photos_response):
    rovers = ["Curiosity", "Opportunity", "Spirit"]
    
    for rover in rovers:
        with patch('src.ingestion.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = sample_photos_response
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = extract_photos_from_nasa(rover, 100, mock_logger)
            
            assert result == sample_photos_response
            assert f"rovers/{rover}/photos" in mock_get.call_args[0][0]
            assert "sol=100" in mock_get.call_args[0][0]

def test_extract_photos_from_nasa_http_error(mock_logger):
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("InvalidRover", 1, mock_logger)
        
        assert result == {"photos": []}
        mock_logger.error.assert_called()
        error_call = mock_logger.error.call_args[0][0]
        assert "Error processing photos request" in error_call
        assert "InvalidRover" in error_call

def test_extract_photos_from_nasa_timeout(mock_logger):
    with patch('src.ingestion.requests.get') as mock_get:
        mock_get.side_effect = requests.Timeout("Request timed out")
        
        result = extract_photos_from_nasa("Perseverance", 1, mock_logger)
        
        assert result == {"photos": []}
        mock_logger.error.assert_called()

def test_extract_photos_from_nasa_json_error(mock_logger):
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("Perseverance", 1, mock_logger)
        
        assert result == {"photos": []}
        mock_logger.error.assert_called()

def test_extract_photos_from_nasa_empty_response(mock_logger):
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {"photos": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("Perseverance", 999, mock_logger)
        
        assert result == {"photos": []}

# MANIFEST TESTS
def test_extract_manifest_from_nasa_success(mock_logger, sample_manifest_response):
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = sample_manifest_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = extract_manifest_from_nasa("Perseverance", mock_logger)
        
        assert result == sample_manifest_response
        mock_get.assert_called_once()
        assert "manifests/Perseverance" in mock_get.call_args[0][0]
        mock_logger.info.assert_called_with("Processing manifest request for rover: Perseverance")

def test_extract_manifest_from_nasa_error(mock_logger):
    with patch('src.ingestion.requests.get') as mock_get:
        mock_get.side_effect = requests.ConnectionError("Connection failed")
        
        result = extract_manifest_from_nasa("Perseverance", mock_logger)
        
        assert result == {"manifest": []}
        mock_logger.error.assert_called()
        error_call = mock_logger.error.call_args[0][0]
        assert "Error processing manifest request" in error_call

@patch('src.ingestion.NASA_KEY', 'test_api_key_12345')
def test_extract_photos_nasa_key_handling(mock_logger):
    """Test NASA API key environment variable handling"""
    
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {"photos": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        extract_photos_from_nasa("Perseverance", 1, mock_logger)
        
        # Verify API key was included in the request
        call_url = mock_get.call_args[0][0]
        assert "api_key=test_api_key_12345" in call_url
