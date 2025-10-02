"""Pytest configuration and fixtures."""
import pytest
import os
from pathlib import Path
import tempfile
from database.duckdb_client import DuckDBClient
from api.police_uk import PoliceUKClient
from api.ordnance_survey import OrdnanceSurveyClient


@pytest.fixture
def temp_db():
    """Create a temporary DuckDB database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    
    db_client = DuckDBClient(db_path)
    db_client.connect()
    db_client.initialize_schema()
    
    yield db_client
    
    db_client.close()
    # Clean up
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def sample_boundary_coords():
    """Sample boundary coordinates for testing."""
    return [
        {"latitude": "52.6394", "longitude": "-1.1458"},
        {"latitude": "52.6389", "longitude": "-1.1457"},
        {"latitude": "52.6383", "longitude": "-1.1455"},
        {"latitude": "52.6394", "longitude": "-1.1458"},  # Close the polygon
    ]


@pytest.fixture
def sample_events():
    """Sample police events for testing."""
    return [
        {
            "title": "Bike register event",
            "description": "Register your bike for free",
            "address": "Town Hall Square, Leicester",
            "type": "other",
            "start_date": "2024-09-09T10:00:00",
            "end_date": "2024-09-09T12:00:00",
            "contact_details": {
                "email": "test@police.uk",
                "telephone": "101"
            }
        },
        {
            "title": "Student Consultation",
            "description": "Talk to local PCSO",
            "address": "The Tannery, Bath Lane, Leicester",
            "type": "other",
            "start_date": "2024-09-11T15:00:00",
            "end_date": "2024-09-11T16:00:00",
            "contact_details": {}
        }
    ]


@pytest.fixture
def mock_os_api_key():
    """Mock OS Names API key for testing."""
    return "test_api_key_12345"
