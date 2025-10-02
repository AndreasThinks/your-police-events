"""Tests for DuckDB client and spatial operations."""
import pytest
from database.duckdb_client import DuckDBClient


def test_database_initialization(temp_db):
    """Test database connection and schema initialization."""
    assert temp_db.conn is not None
    
    # Check that table exists
    result = temp_db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='neighbourhoods'"
    ).fetchone()
    assert result is not None


def test_insert_neighbourhood(temp_db, sample_boundary_coords):
    """Test inserting a neighbourhood with boundary."""
    temp_db.insert_neighbourhood(
        force_id="test-force",
        neighbourhood_id="TEST01",
        name="Test Neighbourhood",
        boundary_coords=sample_boundary_coords
    )
    
    count = temp_db.get_neighbourhood_count()
    assert count == 1


def test_find_neighbourhood_by_coords(temp_db, sample_boundary_coords):
    """Test finding neighbourhood by coordinates."""
    # Insert test neighbourhood
    temp_db.insert_neighbourhood(
        force_id="leicestershire",
        neighbourhood_id="NC04",
        name="City Centre",
        boundary_coords=sample_boundary_coords
    )
    
    # Test point inside the polygon
    result = temp_db.find_neighbourhood_by_coords(-1.1457, 52.6390)
    
    assert result is not None
    force_id, neighbourhood_id, name = result
    assert force_id == "leicestershire"
    assert neighbourhood_id == "NC04"
    assert name == "City Centre"


def test_find_neighbourhood_outside_boundary(temp_db, sample_boundary_coords):
    """Test that points outside boundaries return None."""
    temp_db.insert_neighbourhood(
        force_id="test-force",
        neighbourhood_id="TEST01",
        name="Test Neighbourhood",
        boundary_coords=sample_boundary_coords
    )
    
    # Test point far outside the polygon
    result = temp_db.find_neighbourhood_by_coords(0.0, 51.5)
    assert result is None


def test_transform_bng_to_wgs84(temp_db):
    """Test BNG to WGS84 coordinate transformation."""
    # Test with known coordinates (Leicester city centre approx)
    easting = 458700
    northing = 305800
    
    lng, lat = temp_db.transform_bng_to_wgs84(easting, northing)
    
    # Check coordinates are in reasonable range for UK
    assert -2.0 < lng < -1.0
    assert 52.0 < lat < 53.0


def test_insert_or_replace_neighbourhood(temp_db, sample_boundary_coords):
    """Test that inserting same neighbourhood twice updates it."""
    # Insert first time
    temp_db.insert_neighbourhood(
        force_id="test-force",
        neighbourhood_id="TEST01",
        name="Original Name",
        boundary_coords=sample_boundary_coords
    )
    
    # Insert again with different name
    temp_db.insert_neighbourhood(
        force_id="test-force",
        neighbourhood_id="TEST01",
        name="Updated Name",
        boundary_coords=sample_boundary_coords
    )
    
    # Should still only have one neighbourhood
    count = temp_db.get_neighbourhood_count()
    assert count == 1
    
    # Name should be updated
    result = temp_db.find_neighbourhood_by_coords(-1.1457, 52.6390)
    assert result is not None
    _, _, name = result
    assert name == "Updated Name"


def test_clear_all_neighbourhoods(temp_db, sample_boundary_coords):
    """Test clearing all neighbourhoods from database."""
    # Insert some neighbourhoods
    temp_db.insert_neighbourhood(
        force_id="test-force-1",
        neighbourhood_id="TEST01",
        name="Test 1",
        boundary_coords=sample_boundary_coords
    )
    temp_db.insert_neighbourhood(
        force_id="test-force-2",
        neighbourhood_id="TEST02",
        name="Test 2",
        boundary_coords=sample_boundary_coords
    )
    
    assert temp_db.get_neighbourhood_count() == 2
    
    temp_db.clear_all_neighbourhoods()
    
    assert temp_db.get_neighbourhood_count() == 0
