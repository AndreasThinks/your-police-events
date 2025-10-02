"""Tests for location service."""
import pytest
from services.location import LocationService
from api.ordnance_survey import OrdnanceSurveyClient
from database.duckdb_client import DuckDBClient


@pytest.mark.asyncio
async def test_find_neighbourhood_by_postcode(temp_db, sample_boundary_coords, mocker):
    """Test finding neighbourhood by postcode."""
    # Insert test neighbourhood
    temp_db.insert_neighbourhood(
        force_id="leicestershire",
        neighbourhood_id="NC04",
        name="City Centre",
        boundary_coords=sample_boundary_coords
    )
    
    # Mock OS API client
    mock_os_client = mocker.Mock(spec=OrdnanceSurveyClient)
    mock_os_client.find_postcode = mocker.AsyncMock(return_value={
        "name": "LE1 5WW",
        "geometry_x": 458700,
        "geometry_y": 305800,
        "postcode_district": "LE1",
        "populated_place": "Leicester",
        "county": "Leicestershire",
        "country": "England"
    })
    
    service = LocationService(mock_os_client, temp_db)
    
    # Find neighbourhood
    result = await service.find_neighbourhood_by_postcode("LE1 5WW")
    
    assert result is not None
    force_id, neighbourhood_id, name = result
    assert force_id == "leicestershire"
    assert neighbourhood_id == "NC04"


@pytest.mark.asyncio
async def test_postcode_caching(temp_db, sample_boundary_coords, mocker):
    """Test that postcode lookups are cached."""
    temp_db.insert_neighbourhood(
        force_id="test-force",
        neighbourhood_id="TEST01",
        name="Test Area",
        boundary_coords=sample_boundary_coords
    )
    
    mock_os_client = mocker.Mock(spec=OrdnanceSurveyClient)
    mock_os_client.find_postcode = mocker.AsyncMock(return_value={
        "name": "SW1A 1AA",
        "geometry_x": 529090,
        "geometry_y": 179645,
        "postcode_district": "SW1A",
        "populated_place": "Westminster",
        "county": "Greater London",
        "country": "England"
    })
    
    service = LocationService(mock_os_client, temp_db)
    
    # First call
    await service.find_neighbourhood_by_postcode("SW1A 1AA")
    
    # Second call - should use cache
    await service.find_neighbourhood_by_postcode("SW1A 1AA")
    
    # OS API should only be called once
    assert mock_os_client.find_postcode.call_count == 1


@pytest.mark.asyncio
async def test_postcode_not_found(mocker):
    """Test handling of postcode not found."""
    mock_os_client = mocker.Mock(spec=OrdnanceSurveyClient)
    mock_os_client.find_postcode = mocker.AsyncMock(return_value=None)
    
    mock_db = mocker.Mock(spec=DuckDBClient)
    
    service = LocationService(mock_os_client, mock_db)
    
    result = await service.find_neighbourhood_by_postcode("INVALID")
    
    assert result is None


def test_find_neighbourhood_by_coords(temp_db, sample_boundary_coords):
    """Test finding neighbourhood by coordinates."""
    temp_db.insert_neighbourhood(
        force_id="test-force",
        neighbourhood_id="TEST01",
        name="Test Area",
        boundary_coords=sample_boundary_coords
    )
    
    mock_os_client = None  # Not needed for this test
    service = LocationService(mock_os_client, temp_db)
    
    result = service.find_neighbourhood_by_coords(-1.1457, 52.6390)
    
    assert result is not None
    force_id, neighbourhood_id, name = result
    assert force_id == "test-force"
    assert neighbourhood_id == "TEST01"
