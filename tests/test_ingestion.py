from unittest.mock import patch, MagicMock
import pytest
import requests
from src.ingestion import extract_photos_from_nasa, create_final_batch_json, generate_tasks_for_batch

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

# ====== EXTRACT_PHOTOS_FROM_NASA TESTS ======

def test_extract_photos_from_nasa_success(mock_logger, sample_photos_response):
    """Test successful photo extraction from NASA API"""
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = sample_photos_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("Curiosity", 100, mock_logger)
        
        assert result == sample_photos_response
        mock_get.assert_called_once()
        call_url = mock_get.call_args[0][0]
        assert "rovers/Curiosity/photos" in call_url
        assert "sol=100" in call_url
        mock_logger.info.assert_any_call("Processing photos request for rover: Curiosity on sol: 100")

def test_extract_photos_from_nasa_api_error(mock_logger):
    """Test photo extraction when API returns error"""
    with patch('src.ingestion.requests.get') as mock_get:
        mock_get.side_effect = requests.ConnectionError("Network error")
        
        result = extract_photos_from_nasa("Perseverance", 50, mock_logger)
        
        assert result == {"photos": []}
        mock_logger.error.assert_called_once()
        error_msg = mock_logger.error.call_args[0][0]
        assert "Error processing photos request" in error_msg
        assert "Perseverance" in error_msg
        assert "sol: 50" in error_msg

def test_extract_photos_from_nasa_http_error(mock_logger):
    """Test photo extraction when API returns HTTP error"""
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("Spirit", 999, mock_logger)
        
        assert result == {"photos": []}
        mock_logger.error.assert_called_once()

def test_extract_photos_from_nasa_empty_response(mock_logger):
    """Test photo extraction when API returns no photos"""
    with patch('src.ingestion.requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.json.return_value = {"photos": []}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = extract_photos_from_nasa("Opportunity", 2000, mock_logger)
        
        assert result == {"photos": []}
        mock_logger.info.assert_any_call("Fetched 0 photos for Opportunity on sol 2000")

# ====== CREATE_FINAL_BATCH_JSON TESTS ======

def test_create_final_batch_json_single_sol(mock_logger):
    """Test batch JSON creation with single sol"""
    batch = [100]
    all_rover_photos_results = [
        {"photos": [{"id": 1, "img_src": "http://example.com/photo1.jpg"}]},
        {"photos": [{"id": 2, "img_src": "http://example.com/photo2.jpg"}]}
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-10T12:00:00"
        
        result = create_final_batch_json(batch, all_rover_photos_results, mock_logger)
        
        assert result["sol_start"] == 100
        assert result["sol_end"] == 100
        assert result["photo_count"] == 2
        assert len(result["photos"]) == 2
        assert result["ingestion_date"] == "2025-09-10T12:00:00"
        assert "mars_rover_photos_batch_sol_100_to_100_" in result["filename"]
        mock_logger.info.assert_any_call("Creating final .json")

def test_create_final_batch_json_multiple_sols(mock_logger):
    """Test batch JSON creation with multiple sols"""
    batch = [95, 96, 97]
    all_rover_photos_results = [
        {"photos": [{"id": 1, "img_src": "http://example.com/photo1.jpg"}]},
        {"photos": [{"id": 2, "img_src": "http://example.com/photo2.jpg"}]},
        {"photos": []}  # Empty photos for one result
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-10T15:30:45"
        
        result = create_final_batch_json(batch, all_rover_photos_results, mock_logger)
        
        assert result["sol_start"] == 95
        assert result["sol_end"] == 97
        assert result["photo_count"] == 2
        assert len(result["photos"]) == 2
        assert result["ingestion_date"] == "2025-09-10T15:30:45"
        assert "mars_rover_photos_batch_sol_95_to_97_" in result["filename"]

def test_create_final_batch_json_empty_results(mock_logger):
    """Test batch JSON creation when all results are empty"""
    batch = [500]
    all_rover_photos_results = [
        {"photos": []},
        {"photos": []}
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-10T08:15:30"
        
        result = create_final_batch_json(batch, all_rover_photos_results, mock_logger)
        
        assert result["sol_start"] == 500
        assert result["sol_end"] == 500
        assert result["photo_count"] == 0
        assert len(result["photos"]) == 0
        assert result["ingestion_date"] == "2025-09-10T08:15:30"
        assert "mars_rover_photos_batch_sol_500_to_500_" in result["filename"]

def test_create_final_batch_json_data_structure(mock_logger):
    """Test batch JSON structure and data types"""
    batch = [200, 201]
    test_photos = [
        {"id": 123, "img_src": "http://test.com/photo123.jpg", "camera": {"name": "MASTCAM"}},
        {"id": 124, "img_src": "http://test.com/photo124.jpg", "camera": {"name": "NAVCAM"}}
    ]
    all_rover_photos_results = [{"photos": test_photos}]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-10T18:45:22"
        
        result = create_final_batch_json(batch, all_rover_photos_results, mock_logger)
        
        # Verify all required keys exist
        required_keys = ["filename", "sol_start", "sol_end", "photo_count", "photos", "ingestion_date"]
        for key in required_keys:
            assert key in result
        
        # Verify data types
        assert isinstance(result["filename"], str)
        assert isinstance(result["sol_start"], int)
        assert isinstance(result["sol_end"], int)
        assert isinstance(result["photo_count"], int)
        assert isinstance(result["photos"], list)
        assert isinstance(result["ingestion_date"], str)
        
        # Verify photo data is preserved exactly
        assert result["photos"] == test_photos
        assert result["photo_count"] == len(test_photos)

# ====== GENERATE_TASKS_FOR_BATCH TESTS ======

def test_generate_tasks_for_batch_single_sol(mock_logger):
    """Test task generation for single sol"""
    batch = [100]
    
    with patch('src.ingestion.MARS_ROVERS', ['Curiosity', 'Perseverance']):
        result = generate_tasks_for_batch(batch, mock_logger)
        
        expected_tasks = [
            {"rover": "Curiosity", "sol": 100},
            {"rover": "Perseverance", "sol": 100}
        ]
        
        assert result == expected_tasks
        assert len(result) == 2
        mock_logger.info.assert_any_call("Generating tasks for DAG run")
        mock_logger.info.assert_any_call("2 tasks scheduled for this DAG run")

def test_generate_tasks_for_batch_multiple_sols(mock_logger):
    """Test task generation for multiple sols"""
    batch = [50, 51]
    
    with patch('src.ingestion.MARS_ROVERS', ['Spirit', 'Opportunity']):
        result = generate_tasks_for_batch(batch, mock_logger)
        
        expected_tasks = [
            {"rover": "Spirit", "sol": 50},
            {"rover": "Spirit", "sol": 51},
            {"rover": "Opportunity", "sol": 50},
            {"rover": "Opportunity", "sol": 51}
        ]
        
        assert result == expected_tasks
        assert len(result) == 4
        mock_logger.info.assert_any_call("4 tasks scheduled for this DAG run")

def test_generate_tasks_for_batch_empty_batch(mock_logger):
    """Test task generation for empty batch"""
    batch = []
    
    with patch('src.ingestion.MARS_ROVERS', ['Curiosity']):
        result = generate_tasks_for_batch(batch, mock_logger)
        
        assert result == []
        assert len(result) == 0
        mock_logger.info.assert_any_call("0 tasks scheduled for this DAG run")

def test_generate_tasks_for_batch_large_batch(mock_logger):
    """Test task generation for large batch"""
    batch = list(range(1, 6))  # [1, 2, 3, 4, 5]
    
    with patch('src.ingestion.MARS_ROVERS', ['Curiosity', 'Perseverance', 'Spirit']):
        result = generate_tasks_for_batch(batch, mock_logger)
        
        # Should have 3 rovers * 5 sols = 15 tasks
        assert len(result) == 15
        
        # Verify all combinations exist
        for rover in ['Curiosity', 'Perseverance', 'Spirit']:
            for sol in range(1, 6):
                assert {"rover": rover, "sol": sol} in result
        
        mock_logger.info.assert_any_call("15 tasks scheduled for this DAG run")
