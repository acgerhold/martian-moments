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
    mock_logger.info.assert_any_call("Processing photos request for rover: Curiosity on sol: 100")
    mock_logger.info.assert_any_call("Fetched 1 photos for Curiosity on sol 100")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_request_timeout(mock_get, mock_logger):
    """Test photo extraction with request timeout"""
    mock_get.side_effect = Exception("Timeout error")
    
    result = extract_photos_from_nasa("Curiosity", 100, mock_logger)
    
    assert result == {"photos": []}
    mock_logger.error.assert_called_once_with("Error processing photos request for rover: Curiosity on sol: 100: Timeout error")

@patch('src.ingestion.photos.requests.get')
def test_extract_photos_from_nasa_empty_response(mock_get, mock_logger):
    """Test photo extraction with empty photos array"""
    mock_response = MagicMock()
    mock_response.json.return_value = {"photos": []}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    result = extract_photos_from_nasa("Spirit", 200, mock_logger)
    
    assert result == {"photos": []}
    mock_logger.info.assert_any_call("Fetched 0 photos for Spirit on sol 200")

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
        
        mock_logger.info.assert_any_call("Creating photos final .json")
        mock_logger.info.assert_any_call("Created file - Name: mars_rover_photos_batch_sol_100_to_101_2025-09-15T15:30:00.json, Date: 2025-09-15T15:30:00, Photo Count: 2")

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

# ====== GENERATE_TASKS_FOR_PHOTOS_BATCH TESTS ======

def test_generate_tasks_for_photos_batch_new_format(mock_logger, sample_ingestion_schedule):
    """Test task generation with new ingestion schedule format"""
    result = generate_tasks_for_photos_batch(sample_ingestion_schedule, mock_logger)
    
    expected_tasks = [
        {"rover": "Curiosity", "sol": 100},
        {"rover": "Curiosity", "sol": 101},
        {"rover": "Curiosity", "sol": 102},
        {"rover": "Curiosity", "sol": 103},
        {"rover": "Curiosity", "sol": 104},
        {"rover": "Curiosity", "sol": 105},
        {"rover": "Curiosity", "sol": 106},
        {"rover": "Curiosity", "sol": 107},
        {"rover": "Curiosity", "sol": 108},
        {"rover": "Curiosity", "sol": 109},
        {"rover": "Curiosity", "sol": 110},
        {"rover": "Curiosity", "sol": 111},
        {"rover": "Curiosity", "sol": 112},
        {"rover": "Curiosity", "sol": 113},
        {"rover": "Curiosity", "sol": 114},
        {"rover": "Curiosity", "sol": 115},
        {"rover": "Curiosity", "sol": 116},
        {"rover": "Curiosity", "sol": 117},
        {"rover": "Curiosity", "sol": 118},
        {"rover": "Curiosity", "sol": 119},
        {"rover": "Curiosity", "sol": 120},
        {"rover": "Curiosity", "sol": 121},
        {"rover": "Curiosity", "sol": 122},
        {"rover": "Curiosity", "sol": 123},
        {"rover": "Curiosity", "sol": 124},
        {"rover": "Curiosity", "sol": 125},
        {"rover": "Curiosity", "sol": 126},
        {"rover": "Curiosity", "sol": 127},
        {"rover": "Curiosity", "sol": 128},
        {"rover": "Curiosity", "sol": 129},
        {"rover": "Curiosity", "sol": 130},
        {"rover": "Curiosity", "sol": 131},
        {"rover": "Curiosity", "sol": 132},
        {"rover": "Curiosity", "sol": 133},
        {"rover": "Curiosity", "sol": 134},
        {"rover": "Curiosity", "sol": 135},
        {"rover": "Curiosity", "sol": 136},
        {"rover": "Curiosity", "sol": 137},
        {"rover": "Curiosity", "sol": 138},
        {"rover": "Curiosity", "sol": 139},
        {"rover": "Curiosity", "sol": 140},
        {"rover": "Curiosity", "sol": 141},
        {"rover": "Curiosity", "sol": 142},
        {"rover": "Curiosity", "sol": 143},
        {"rover": "Curiosity", "sol": 144},
        {"rover": "Curiosity", "sol": 145},
        {"rover": "Curiosity", "sol": 146},
        {"rover": "Curiosity", "sol": 147},
        {"rover": "Curiosity", "sol": 148},
        {"rover": "Curiosity", "sol": 149},
        {"rover": "Perseverance", "sol": 99},
        {"rover": "Perseverance", "sol": 100},
        {"rover": "Perseverance", "sol": 101},
        {"rover": "Perseverance", "sol": 102},
        {"rover": "Perseverance", "sol": 103},
        {"rover": "Perseverance", "sol": 104},
        {"rover": "Perseverance", "sol": 105},
        {"rover": "Perseverance", "sol": 106},
        {"rover": "Perseverance", "sol": 107},
        {"rover": "Perseverance", "sol": 108},
        {"rover": "Perseverance", "sol": 109},
        {"rover": "Perseverance", "sol": 110},
        {"rover": "Perseverance", "sol": 111},
        {"rover": "Perseverance", "sol": 112},
        {"rover": "Perseverance", "sol": 113},
        {"rover": "Perseverance", "sol": 114},
        {"rover": "Perseverance", "sol": 115},
        {"rover": "Perseverance", "sol": 116},
        {"rover": "Perseverance", "sol": 117},
        {"rover": "Perseverance", "sol": 118},
        {"rover": "Perseverance", "sol": 119},
        {"rover": "Perseverance", "sol": 120},
        {"rover": "Perseverance", "sol": 121},
        {"rover": "Perseverance", "sol": 122},
        {"rover": "Perseverance", "sol": 123},
        {"rover": "Perseverance", "sol": 124},
        {"rover": "Perseverance", "sol": 125},
        {"rover": "Perseverance", "sol": 126},
        {"rover": "Perseverance", "sol": 127},
        {"rover": "Perseverance", "sol": 128},
        {"rover": "Perseverance", "sol": 129},
        {"rover": "Perseverance", "sol": 130},
        {"rover": "Perseverance", "sol": 131},
        {"rover": "Perseverance", "sol": 132},
        {"rover": "Perseverance", "sol": 133},
        {"rover": "Perseverance", "sol": 134},
        {"rover": "Perseverance", "sol": 135},
        {"rover": "Perseverance", "sol": 136},
        {"rover": "Perseverance", "sol": 137},
        {"rover": "Perseverance", "sol": 138},
        {"rover": "Perseverance", "sol": 139},
        {"rover": "Perseverance", "sol": 140},
        {"rover": "Perseverance", "sol": 141},
        {"rover": "Perseverance", "sol": 142},
        {"rover": "Perseverance", "sol": 143},
        {"rover": "Perseverance", "sol": 144},
        {"rover": "Perseverance", "sol": 145},
        {"rover": "Perseverance", "sol": 146},
        {"rover": "Perseverance", "sol": 147},
        {"rover": "Perseverance", "sol": 148}
    ]
    # sol_start will be max(100, 99) = 100
    # sol_end will be max(150, 149) = 150  
    # sol_range will be range(100, 150) = [100, 101, ..., 149]
    expected_sol_range = list(range(100, 150))
    
    assert result["tasks"] == expected_tasks
    assert result["sol_range"] == expected_sol_range
    mock_logger.info.assert_any_call("Generating tasks for photos DAG run")
    mock_logger.info.assert_any_call(f"{len(expected_tasks)} tasks scheduled for this DAG run")

def test_generate_tasks_for_photos_batch_single_rover(mock_logger):
    """Test task generation with single rover"""
    ingestion_schedule = [{"rover_name": "Curiosity", "sol_start": 100, "sol_end": 103}]
    
    result = generate_tasks_for_photos_batch(ingestion_schedule, mock_logger)
    
    expected_tasks = [
        {"rover": "Curiosity", "sol": 100},
        {"rover": "Curiosity", "sol": 101},
        {"rover": "Curiosity", "sol": 102}
    ]
    expected_sol_range = [100, 101, 102]
    
    assert result["tasks"] == expected_tasks
    assert result["sol_range"] == expected_sol_range
    assert len(result["tasks"]) == 3

def test_generate_tasks_for_photos_batch_overlapping_sols(mock_logger):
    """Test task generation with overlapping sol ranges"""
    ingestion_schedule = [
        {"rover_name": "Curiosity", "sol_start": 100, "sol_end": 102},
        {"rover_name": "Perseverance", "sol_start": 101, "sol_end": 103}
    ]
    
    result = generate_tasks_for_photos_batch(ingestion_schedule, mock_logger)
    
    # Should create tasks for each rover's range independently
    expected_tasks = [
        {"rover": "Curiosity", "sol": 100},
        {"rover": "Curiosity", "sol": 101},
        {"rover": "Perseverance", "sol": 101},
        {"rover": "Perseverance", "sol": 102}
    ]
    # sol_start will be max(100, 101) = 101
    # sol_end will be max(102, 103) = 103
    # sol_range will be range(101, 103) = [101, 102]
    expected_sol_range = [101, 102]
    
    assert result["tasks"] == expected_tasks
    assert result["sol_range"] == expected_sol_range

def test_generate_tasks_for_photos_batch_empty_schedule(mock_logger):
    """Test task generation with empty ingestion schedule"""
    ingestion_schedule = []
    
    result = generate_tasks_for_photos_batch(ingestion_schedule, mock_logger)
    
    assert result["tasks"] == []
    assert result["sol_range"] == []
    mock_logger.info.assert_any_call("0 tasks scheduled for this DAG run")

def test_generate_tasks_for_photos_batch_same_start_end_sol(mock_logger):
    """Test task generation when start and end sol are the same"""
    ingestion_schedule = [{"rover_name": "Spirit", "sol_start": 100, "sol_end": 100}]
    
    result = generate_tasks_for_photos_batch(ingestion_schedule, mock_logger)
    
    # range(100, 100) produces empty range, so no tasks
    assert result["tasks"] == []
    assert result["sol_range"] == []