"""Tests for the admin status endpoint."""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, AsyncMock, patch
from main import app


@pytest.fixture
def mock_db_client():
    """Mock database client."""
    mock = Mock()
    mock.get_database_stats.return_value = {
        "neighbourhoods": 4656,
        "forces": 44,
        "storage_mb": 125.5,
        "last_updated": "2025-10-02T17:00:00"
    }
    return mock


@pytest.fixture
def mock_sync_state():
    """Mock sync state."""
    async def get_state():
        return {
            "status": "idle",
            "error": None,
            "last_sync": {
                "completed_at": "2025-10-02T14:00:00",
                "duration_seconds": 3245,
                "neighbourhoods_synced": 4512,
                "neighbourhoods_failed": 144,
                "neighbourhoods_no_boundary": 0,
                "total_neighbourhoods": 4656,
                "forces_processed": 43,
                "forces_failed": 1,
                "success_rate": 96.9
            }
        }
    
    mock = Mock()
    mock.get_state = get_state
    return mock


def test_status_endpoint_structure(mock_db_client, mock_sync_state):
    """Test that status endpoint returns correct structure."""
    with patch('main.db_client', mock_db_client):
        with patch('database.sync_state.sync_state', mock_sync_state):
            client = TestClient(app)
            response = client.get("/admin/status")
            
            assert response.status_code == 200
            data = response.json()
            
            # Check top-level structure
            assert "status" in data
            assert "database" in data
            assert "sync" in data
            assert "cache" in data
            assert "scheduler" in data
            
            assert data["status"] == "operational"


def test_status_endpoint_database_info(mock_db_client, mock_sync_state):
    """Test that database information is included."""
    with patch('main.db_client', mock_db_client):
        with patch('database.sync_state.sync_state', mock_sync_state):
            client = TestClient(app)
            response = client.get("/admin/status")
            
            data = response.json()
            db_info = data["database"]
            
            assert db_info["neighbourhoods"] == 4656
            assert db_info["forces"] == 44
            assert db_info["storage_mb"] == 125.5
            assert db_info["last_updated"] == "2025-10-02T17:00:00"


def test_status_endpoint_sync_info(mock_db_client, mock_sync_state):
    """Test that sync information is included."""
    with patch('main.db_client', mock_db_client):
        with patch('database.sync_state.sync_state', mock_sync_state):
            client = TestClient(app)
            response = client.get("/admin/status")
            
            data = response.json()
            sync_info = data["sync"]
            
            assert sync_info["status"] == "idle"
            assert "last_sync" in sync_info
            assert sync_info["last_sync"]["neighbourhoods_synced"] == 4512
            assert sync_info["last_sync"]["success_rate"] == 96.9


def test_status_endpoint_running_sync(mock_db_client):
    """Test status endpoint when sync is running."""
    async def get_running_state():
        return {
            "status": "running",
            "error": None,
            "progress": {
                "current_force": "metropolitan",
                "current_force_name": "Metropolitan Police Service",
                "forces_processed": 15,
                "total_forces": 44,
                "neighbourhoods_processed": 1850,
                "total_neighbourhoods": 4656,
                "neighbourhoods_synced": 1800,
                "neighbourhoods_failed": 50,
                "neighbourhoods_no_boundary": 0,
                "percentage": 39.7
            },
            "timing": {
                "started_at": "2025-10-02T17:30:00",
                "elapsed_seconds": 1080
            }
        }
    
    mock_sync = Mock()
    mock_sync.get_state = get_running_state
    
    with patch('main.db_client', mock_db_client):
        with patch('database.sync_state.sync_state', mock_sync):
            client = TestClient(app)
            response = client.get("/admin/status")
            
            data = response.json()
            sync_info = data["sync"]
            
            assert sync_info["status"] == "running"
            assert "progress" in sync_info
            assert sync_info["progress"]["current_force"] == "metropolitan"
            assert sync_info["progress"]["percentage"] == 39.7
            assert "timing" in sync_info
            assert sync_info["timing"]["elapsed_seconds"] == 1080


def test_status_endpoint_failed_sync(mock_db_client):
    """Test status endpoint when sync has failed."""
    async def get_failed_state():
        return {
            "status": "failed",
            "error": "API connection timeout"
        }
    
    mock_sync = Mock()
    mock_sync.get_state = get_failed_state
    
    with patch('main.db_client', mock_db_client):
        with patch('database.sync_state.sync_state', mock_sync):
            client = TestClient(app)
            response = client.get("/admin/status")
            
            data = response.json()
            sync_info = data["sync"]
            
            assert sync_info["status"] == "failed"
            assert sync_info["error"] == "API connection timeout"


def test_status_endpoint_cache_info(mock_db_client, mock_sync_state):
    """Test that cache information is included."""
    with patch('main.db_client', mock_db_client):
        with patch('database.sync_state.sync_state', mock_sync_state):
            client = TestClient(app)
            response = client.get("/admin/status")
            
            data = response.json()
            cache_info = data["cache"]
            
            assert "calendar_feeds" in cache_info
            assert "postcode_lookups" in cache_info
            assert "size" in cache_info["calendar_feeds"]
            assert "max_size" in cache_info["calendar_feeds"]
            assert "ttl_hours" in cache_info["calendar_feeds"]


def test_status_endpoint_scheduler_info(mock_db_client, mock_sync_state):
    """Test that scheduler information is included."""
    with patch('main.db_client', mock_db_client):
        with patch('database.sync_state.sync_state', mock_sync_state):
            with patch('main.executor', Mock()):
                client = TestClient(app)
                response = client.get("/admin/status")
                
                data = response.json()
                scheduler_info = data["scheduler"]
                
                assert "active" in scheduler_info
                assert "next_sync" in scheduler_info
                assert scheduler_info["next_sync"] == "Manual trigger via /admin/sync endpoint"


def test_status_endpoint_no_database(mock_sync_state):
    """Test status endpoint when database is not available."""
    with patch('main.db_client', None):
        with patch('database.sync_state.sync_state', mock_sync_state):
            client = TestClient(app)
            response = client.get("/admin/status")
            
            data = response.json()
            db_info = data["database"]
            
            # Should return default values
            assert db_info["neighbourhoods"] == 0
            assert db_info["forces"] == 0
            assert db_info["storage_mb"] == 0.0
            assert db_info["last_updated"] is None
