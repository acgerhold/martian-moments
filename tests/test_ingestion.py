from unittest.mock import patch, MagicMock
import pytest
import requests
from src.ingestion import extract_photos_from_nasa, extract_manifest_from_nasa, create_final_json, create_final_batch_json

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

# BATCH FILE TESTS
def test_create_final_batch_json_single_sol(): 
    sols = [100]
    all_rover_results = [
        [
            [
                {"id": 1, "img_src": "http://example.com/photo1.jpg", "sol": 100},
                {"id": 2, "img_src": "http://example.com/photo2.jpg", "sol": 100}
            ]
        ]
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T10:30:00"
        
        result = create_final_batch_json(sols, all_rover_results)
        
        assert result["sol_start"] == 100
        assert result["sol_end"] == 100
        assert result["photo_count"] == 2
        assert len(result["photos"]) == 2
        assert result["ingestion_date"] == "2025-09-08T10:30:00"
        assert "mars_rover_photos_batch_sol_100_" in result["filename"]
        assert result["photos"][0]["id"] == 1
        assert result["photos"][1]["id"] == 2

def test_create_final_batch_json_multiple_sols():
    sols = [98, 99, 100]
    all_rover_results = [
        [
            [
                {"id": 1, "img_src": "http://example.com/photo1.jpg", "sol": 98},
                {"id": 2, "img_src": "http://example.com/photo2.jpg", "sol": 99}
            ]
        ],
        [
            [
                {"id": 3, "img_src": "http://example.com/photo3.jpg", "sol": 100}
            ]
        ]
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T15:45:30"
        
        result = create_final_batch_json(sols, all_rover_results)
        
        assert result["sol_start"] == 98
        assert result["sol_end"] == 100
        assert result["photo_count"] == 3
        assert len(result["photos"]) == 3
        assert result["ingestion_date"] == "2025-09-08T15:45:30"
        assert "mars_rover_photos_batch_sol_98_to_100_" in result["filename"]

def test_create_final_batch_json_empty_results():
    sols = [500]
    all_rover_results = [
        [
            []  # Empty photo array
        ]
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T12:00:00"
        
        result = create_final_batch_json(sols, all_rover_results)
        
        assert result["sol_start"] == 500
        assert result["sol_end"] == 500
        assert result["photo_count"] == 0
        assert len(result["photos"]) == 0
        assert result["ingestion_date"] == "2025-09-08T12:00:00"
        assert "mars_rover_photos_batch_sol_500_" in result["filename"]

def test_create_final_batch_json_mixed_empty_and_filled():
    sols = [10, 11, 12]
    all_rover_results = [
        [
            []  # Empty
        ],
        [
            [
                {"id": 1, "img_src": "http://example.com/photo1.jpg", "sol": 11}
            ]
        ],
        [
            []  # Empty
        ],
        [
            [
                {"id": 2, "img_src": "http://example.com/photo2.jpg", "sol": 12},
                {"id": 3, "img_src": "http://example.com/photo3.jpg", "sol": 12}
            ]
        ]
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T08:15:45"
        
        result = create_final_batch_json(sols, all_rover_results)
        
        assert result["sol_start"] == 10
        assert result["sol_end"] == 12
        assert result["photo_count"] == 3
        assert len(result["photos"]) == 3
        assert result["ingestion_date"] == "2025-09-08T08:15:45"
        assert "mars_rover_photos_batch_sol_10_to_12_" in result["filename"]

def test_create_final_batch_json_large_dataset():
    sols = list(range(1, 11))  # sols 1-10
    # Create large dataset
    large_photo_set = [{"id": i, "img_src": f"http://example.com/photo{i}.jpg", "sol": i % 10 + 1} for i in range(1000)]
    
    all_rover_results = [
        [large_photo_set]
    ]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T20:00:00"
        
        result = create_final_batch_json(sols, all_rover_results)
        
        assert result["sol_start"] == 1
        assert result["sol_end"] == 10
        assert result["photo_count"] == 1000
        assert len(result["photos"]) == 1000
        assert result["ingestion_date"] == "2025-09-08T20:00:00"
        assert "mars_rover_photos_batch_sol_1_to_10_" in result["filename"]

def test_create_final_batch_json_filename_format():
    # Test single sol filename
    sols_single = [42]
    all_rover_results_single = [[[]]]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-12-25T12:30:45"
        
        result_single = create_final_batch_json(sols_single, all_rover_results_single)
        expected_single = "mars_rover_photos_batch_sol_42_2025-12-25T12:30:45.json"
        assert result_single["filename"] == expected_single
        
        # Test range filename
        sols_range = [1, 5, 3, 2, 4]  # Unordered to test min/max
        all_rover_results_range = [[[]]]
        
        result_range = create_final_batch_json(sols_range, all_rover_results_range)
        expected_range = "mars_rover_photos_batch_sol_1_to_5_2025-12-25T12:30:45.json"
        assert result_range["filename"] == expected_range

def test_create_final_batch_json_data_structure():
    sols = [150]
    test_photos = [
        {"id": 123, "img_src": "http://test.com/photo123.jpg", "camera": {"name": "NAVCAM"}},
        {"id": 124, "img_src": "http://test.com/photo124.jpg", "camera": {"name": "FHAZ"}}
    ]
    all_rover_results = [[test_photos]]
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T14:22:33"
        
        result = create_final_batch_json(sols, all_rover_results)
        
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
        
        # Verify photo data is preserved
        assert result["photos"] == test_photos

# SINGLE ROVER/SOL JSON TESTS
def test_create_final_json_success():
    rover = "Perseverance"
    sol = 150
    photos_result = {
        "photos": [
            {"id": 123, "img_src": "http://example.com/photo123.jpg", "camera": {"name": "FHAZ"}},
            {"id": 124, "img_src": "http://example.com/photo124.jpg", "camera": {"name": "RHAZ"}}
        ]
    }
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T14:30:15"
        
        result = create_final_json(rover, sol, photos_result)
        
        assert result["filename"] == "perseverance_photos_sol_150_2025-09-08T14:30:15.json"
        assert result["sol_start"] == 150
        assert result["sol_end"] == 150
        assert result["photo_count"] == 2
        assert len(result["photos"]) == 2
        assert result["ingestion_date"] == "2025-09-08T14:30:15"
        assert result["photos"] == photos_result["photos"]

def test_create_final_json_different_rovers():
    rovers = ["Curiosity", "Opportunity", "Spirit", "Perseverance"]
    sol = 100
    photos_result = {
        "photos": [
            {"id": 1, "img_src": "http://example.com/photo1.jpg"}
        ]
    }
    
    for rover in rovers:
        with patch('src.ingestion.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "2025-09-08T10:00:00"
            
            result = create_final_json(rover, sol, photos_result)
            
            expected_filename = f"{rover.lower()}_photos_sol_100_2025-09-08T10:00:00.json"
            assert result["filename"] == expected_filename
            assert result["sol_start"] == 100
            assert result["sol_end"] == 100

def test_create_final_json_empty_photos():
    rover = "Curiosity"
    sol = 999
    photos_result = {"photos": []}
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T16:45:30"
        
        result = create_final_json(rover, sol, photos_result)
        
        assert result["filename"] == "curiosity_photos_sol_999_2025-09-08T16:45:30.json"
        assert result["sol_start"] == 999
        assert result["sol_end"] == 999
        assert result["photo_count"] == 0
        assert len(result["photos"]) == 0
        assert result["ingestion_date"] == "2025-09-08T16:45:30"

def test_create_final_json_large_photo_count():
    rover = "Perseverance"
    sol = 50
    # Create large photo dataset
    large_photos = [{"id": i, "img_src": f"http://example.com/photo{i}.jpg"} for i in range(500)]
    photos_result = {"photos": large_photos}
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T22:15:00"
        
        result = create_final_json(rover, sol, photos_result)
        
        assert result["filename"] == "perseverance_photos_sol_50_2025-09-08T22:15:00.json"
        assert result["photo_count"] == 500
        assert len(result["photos"]) == 500
        assert result["photos"] == large_photos

def test_create_final_json_different_sols():
    rover = "Opportunity"
    sols_to_test = [0, 1, 100, 500, 1000, 2500]
    photos_result = {
        "photos": [
            {"id": 1, "img_src": "http://example.com/photo1.jpg"}
        ]
    }
    
    for sol in sols_to_test:
        with patch('src.ingestion.datetime') as mock_datetime:
            mock_datetime.now.return_value.strftime.return_value = "2025-09-08T12:00:00"
            
            result = create_final_json(rover, sol, photos_result)
            
            expected_filename = f"opportunity_photos_sol_{sol}_2025-09-08T12:00:00.json"
            assert result["filename"] == expected_filename
            assert result["sol_start"] == sol
            assert result["sol_end"] == sol

def test_create_final_json_filename_format():
    rover = "Spirit"
    sol = 75
    photos_result = {"photos": []}
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-12-31T23:59:59"
        
        result = create_final_json(rover, sol, photos_result)
        
        expected_filename = "spirit_photos_sol_75_2025-12-31T23:59:59.json"
        assert result["filename"] == expected_filename
        
        # Verify filename components
        assert rover.lower() in result["filename"]
        assert f"sol_{sol}" in result["filename"]
        assert "2025-12-31T23:59:59" in result["filename"]
        assert result["filename"].endswith(".json")

def test_create_final_json_data_structure():
    rover = "Perseverance"
    sol = 200
    test_photos = [
        {"id": 456, "img_src": "http://test.com/photo456.jpg", "camera": {"name": "MASTCAM"}},
        {"id": 457, "img_src": "http://test.com/photo457.jpg", "camera": {"name": "NAVCAM"}}
    ]
    photos_result = {"photos": test_photos}
    
    with patch('src.ingestion.datetime') as mock_datetime:
        mock_datetime.now.return_value.strftime.return_value = "2025-09-08T18:30:45"
        
        result = create_final_json(rover, sol, photos_result)
        
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

def test_create_final_json_none_or_falsy_photos_result():
    rover = "Curiosity"
    sol = 42
    
    # Test with None
    result_none = create_final_json(rover, sol, None)
    assert result_none is None
    
    # Test with empty dict
    result_empty = create_final_json(rover, sol, {})
    assert result_empty is None
    
    # Test with False
    result_false = create_final_json(rover, sol, False)
    assert result_false is None
