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
            },
            {
                "id": 12346,
                "sol": 100,
                "camera": {
                    "id": 21,
                    "name": "RHAZ",
                    "rover_id": 5,
                    "full_name": "Rear Hazard Avoidance Camera"
                },
                "img_src": "http://mars.nasa.gov/msl-raw-images/proj/msl/redops/ods/surface/sol/00100/opgs/edr/rcam/RLB_00100_0123456790_EDR.JPG",
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
    mock_logger.info.assert_any_call("Processing photos request for rover: Curiosity on sol: 100")
    mock_logger.info.assert_any_call("Fetched 2 photos for Curiosity on sol 100")
    
    # Verify the request URL was constructed correctly
    expected_url = f"https://api.nasa.gov/mars-photos/api/v1/rovers/Curiosity/photos?sol=100&api_key="
    actual_call = mock_get.call_args[0][0]
    assert actual_call.startswith("https://api.nasa.gov/mars-photos/api/v1/rovers/Curiosity/photos?sol=100&api_key=")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_request_timeout(mock_get, mock_logger):
    """Test photo extraction with request timeout"""
    mock_get.side_effect = Exception("Timeout error")
    
    result = extract_photos_from_nasa("Curiosity", 100, mock_logger)
    
    assert result == {"photos": []}
    mock_logger.error.assert_called_once_with("Error processing photos request for rover: Curiosity on sol: 100: Timeout error")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_http_error(mock_get, mock_logger):
    """Test photo extraction with HTTP error"""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("HTTP 404 Error")
    mock_get.return_value = mock_response
    
    result = extract_photos_from_nasa("Curiosity", 100, mock_logger)
    
    assert result == {"photos": []}
    mock_logger.error.assert_called_once_with("Error processing photos request for rover: Curiosity on sol: 100: HTTP 404 Error")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_empty_response(mock_get, mock_logger):
    """Test photo extraction with empty photos array"""
    mock_response = MagicMock()
    mock_response.json.return_value = {"photos": []}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_photos_from_nasa("Curiosity", 100, mock_logger)
    
    assert result == {"photos": []}
    mock_logger.info.assert_any_call("Fetched 0 photos for Curiosity on sol 100")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_different_rovers(mock_get, mock_logger, sample_photos_response):
    """Test photo extraction for different rovers"""
    mock_response = MagicMock()
    mock_response.json.return_value = sample_photos_response
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # Test Perseverance
    result = extract_photos_from_nasa("Perseverance", 50, mock_logger)
    actual_call = mock_get.call_args[0][0]
    assert "Perseverance" in actual_call
    assert "sol=50" in actual_call
    
    # Test Opportunity
    result = extract_photos_from_nasa("Opportunity", 200, mock_logger)
    actual_call = mock_get.call_args[0][0]
    assert "Opportunity" in actual_call
    assert "sol=200" in actual_call

# ====== CREATE_FINAL_PHOTOS_JSON TESTS ======

def test_create_final_photos_json_success(mock_logger, sample_photos_response):
    """Test successful creation of final photos JSON"""
    batch = [100]
    all_rover_photo_results = [sample_photos_response]
    
    with patch('src.ingestion.photos.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_photos_json(batch, all_rover_photo_results, mock_logger)
        
        assert result["filename"] == "mars_rover_photos_batch_sol_100_to_100_2025-09-13T15:30:00.json"
        assert result["sol_start"] == 100
        assert result["sol_end"] == 100
        assert result["photo_count"] == 2
        assert result["ingestion_date"] == "2025-09-13T15:30:00"
        assert len(result["photos"]) == 2
        assert result["photos"][0]["id"] == 12345
        assert result["photos"][1]["id"] == 12346
        
        mock_logger.info.assert_any_call("Creating photos final .json")
        mock_logger.info.assert_any_call("Created file - Name: mars_rover_photos_batch_sol_100_to_100_2025-09-13T15:30:00.json, Date: 2025-09-13T15:30:00, Photo Count: 2")

def test_create_final_photos_json_multiple_sols(mock_logger, sample_photos_response):
    """Test creation of final photos JSON with multiple sols"""
    batch = [98, 99, 100, 101, 102]
    all_rover_photo_results = [
        sample_photos_response,
        {"photos": [{"id": 99999, "sol": 99}]},
        {"photos": []}  # Empty result
    ]
    
    with patch('src.ingestion.photos.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_photos_json(batch, all_rover_photo_results, mock_logger)
        
        assert result["filename"] == "mars_rover_photos_batch_sol_98_to_102_2025-09-13T15:30:00.json"
        assert result["sol_start"] == 98
        assert result["sol_end"] == 102
        assert result["photo_count"] == 3  # 2 from sample + 1 from second result
        assert len(result["photos"]) == 3

def test_create_final_photos_json_empty_results(mock_logger):
    """Test creation of final photos JSON with empty photo results"""
    batch = [100]
    all_rover_photo_results = [
        {"photos": []},
        {"photos": []}
    ]
    
    with patch('src.ingestion.photos.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_photos_json(batch, all_rover_photo_results, mock_logger)
        
        assert result["photo_count"] == 0
        assert result["photos"] == []

def test_create_final_photos_json_missing_photos_key(mock_logger):
    """Test handling of results missing 'photos' key"""
    batch = [100]
    all_rover_photo_results = [
        {"images": []},  # Wrong key
        {},  # Missing key entirely
        {"photos": [{"id": 12345}]}  # Valid result
    ]
    
    with patch('src.ingestion.photos.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-13T15:30:00"
        
        result = create_final_photos_json(batch, all_rover_photo_results, mock_logger)
        
        assert result["photo_count"] == 1  # Only the valid result
        assert len(result["photos"]) == 1

# ====== GENERATE_TASKS_FOR_PHOTOS_BATCH TESTS ======

def test_generate_tasks_for_photos_batch_single_rover_single_sol(mock_logger):
    """Test task generation for single rover and single sol"""
    rovers = ["Curiosity"]
    sol_batch = [100]
    
    result = generate_tasks_for_photos_batch(rovers, sol_batch, mock_logger)
    
    assert len(result) == 1
    assert result[0] == {"rover": "Curiosity", "sol": 100}
    mock_logger.info.assert_any_call("Generating tasks for photos DAG run")
    mock_logger.info.assert_any_call("1 tasks scheduled for this DAG run")

def test_generate_tasks_for_photos_batch_multiple_rovers_multiple_sols(mock_logger):
    """Test task generation for multiple rovers and multiple sols"""
    rovers = ["Curiosity", "Perseverance"]
    sol_batch = [100, 101]
    
    result = generate_tasks_for_photos_batch(rovers, sol_batch, mock_logger)
    
    assert len(result) == 4  # 2 rovers * 2 sols
    expected_tasks = [
        {"rover": "Curiosity", "sol": 100},
        {"rover": "Curiosity", "sol": 101},
        {"rover": "Perseverance", "sol": 100},
        {"rover": "Perseverance", "sol": 101}
    ]
    assert result == expected_tasks
    mock_logger.info.assert_any_call("4 tasks scheduled for this DAG run")

def test_generate_tasks_for_photos_batch_empty_inputs(mock_logger):
    """Test task generation with empty inputs"""
    rovers = []
    sol_batch = []
    
    result = generate_tasks_for_photos_batch(rovers, sol_batch, mock_logger)
    
    assert len(result) == 0
    mock_logger.info.assert_any_call("0 tasks scheduled for this DAG run")

def test_generate_tasks_for_photos_batch_empty_rovers(mock_logger):
    """Test task generation with empty rovers but valid sols"""
    rovers = []
    sol_batch = [100, 101]
    
    result = generate_tasks_for_photos_batch(rovers, sol_batch, mock_logger)
    
    assert len(result) == 0
    mock_logger.info.assert_any_call("0 tasks scheduled for this DAG run")

def test_generate_tasks_for_photos_batch_empty_sols(mock_logger):
    """Test task generation with valid rovers but empty sols"""
    rovers = ["Curiosity", "Perseverance"] 
    sol_batch = []
    
    result = generate_tasks_for_photos_batch(rovers, sol_batch, mock_logger)
    
    assert len(result) == 0
    mock_logger.info.assert_any_call("0 tasks scheduled for this DAG run")