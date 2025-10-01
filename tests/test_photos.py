from unittest.mock import patch, MagicMock
import pytest
import json
from src.ingestion.photos import extract_photos_from_nasa, create_final_photos_json, generate_tasks_for_photos_batch

@pytest.fixture
def mock_logger():
    return MagicMock()

@pytest.fixture
def sample_photos_response():
    return {
        "photos": [
            {
                "id": 12345,
                "sol": 100,
                "camera": {
                    "id": 20,
                    "name": "FHAZ",
                    "rover_id": 5,
                    "full_name": "Front Hazard Avoidance Camera"
                },
                "img_src": "http://mars.nasa.gov/msl-raw-images/proj/msl/redops/ods/surface/sol/00100/opgs/edr/fcam/FLB_00100_0123456789_EDR.JPG",
                "earth_date": "2012-11-27",
                "rover": {
                    "id": 5,
                    "name": "Curiosity",
                    "landing_date": "2012-08-05",
                    "launch_date": "2011-11-26",
                    "status": "active"
                }
            }
        ]
    }

@pytest.fixture
def sample_ingestion_schedule():
    return [
        {"rover_name": "Curiosity", "sol_start": 100, "sol_end": 150},
        {"rover_name": "Perseverance", "sol_start": 99, "sol_end": 149}
    ]

# ====== EXTRACT_PHOTOS_FROM_NASA TESTS ======

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_success(mock_get, mock_logger, sample_photos_response):
    """Test successful photo extraction"""
    mock_response = MagicMock()
    mock_response.json.return_value = sample_photos_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_photos_from_nasa("Curiosity", 100, mock_logger)
    
    assert result == sample_photos_response
    mock_logger.info.assert_any_call("Processing Photos Request - Rover: Curiosity, Sol: 100")
    mock_logger.info.assert_any_call("Successful Photos Request - Rover: Curiosity, Sol: 100, Photo Count: 1")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_request_timeout(mock_get, mock_logger):
    """Test photo extraction with request timeout"""
    mock_get.side_effect = Exception("Timeout error")
    
    result = extract_photos_from_nasa("Curiosity", 100, mock_logger)
    
    assert result == {"photos": []}
    mock_logger.error.assert_called_once_with("Unsuccessful Photos Request -  Rover: Curiosity, Sol: 100, Error: Timeout error")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_empty_response(mock_get, mock_logger):
    """Test photo extraction with empty photos array"""
    mock_response = MagicMock()
    mock_response.json.return_value = {"photos": []}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_photos_from_nasa("Spirit", 200, mock_logger)
    
    assert result == {"photos": []}
    mock_logger.info.assert_any_call("Successful Photos Request - Rover: Spirit, Sol: 200, Photo Count: 0")

# ====== CREATE_FINAL_PHOTOS_JSON TESTS ======

def test_create_final_photos_json_success(mock_logger, sample_photos_response):
    """Test successful creation of final photos JSON with new signature"""
    all_rover_photo_results = [sample_photos_response, {"photos": [{"id": 67890, "sol": 101}]}]
    sol_range = [100, 101]
    
    with patch('src.ingestion.photos.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-15T15:30:00"
        
        result = create_final_photos_json(all_rover_photo_results, sol_range, mock_logger)
        
        assert result["filename"] == "mars_rover_photos_batch_sol_100_to_101_2025-09-15T15:30:00.json"
        assert result["sol_start"] == 100
        assert result["sol_end"] == 101
        assert result["photo_count"] == 2
        assert result["ingestion_date"] == "2025-09-15T15:30:00"
        assert len(result["photos"]) == 2
        
        mock_logger.info.assert_any_call("Creating Photos Batch File")
        mock_logger.info.assert_any_call("Created Photos Batch - File: mars_rover_photos_batch_sol_100_to_101_2025-09-15T15:30:00.json, Photo Count: 2")

def test_create_final_photos_json_single_sol(mock_logger, sample_photos_response):
    """Test creation of final photos JSON with single sol"""
    all_rover_photo_results = [sample_photos_response]
    sol_range = [100]
    
    with patch('src.ingestion.photos.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-15T15:30:00"
        
        result = create_final_photos_json(all_rover_photo_results, sol_range, mock_logger)
        
        assert result["sol_start"] == 100
        assert result["sol_end"] == 100
        assert result["photo_count"] == 1

def test_create_final_photos_json_empty_results(mock_logger):
    """Test creation of final photos JSON with empty photo results"""
    all_rover_photo_results = [{"photos": []}, {"photos": []}]
    sol_range = [100, 101]
    
    with patch('src.ingestion.photos.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-15T15:30:00"
        
        result = create_final_photos_json(all_rover_photo_results, sol_range, mock_logger)
        
        assert result["photo_count"] == 0
        assert result["photos"] == []