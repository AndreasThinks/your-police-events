"""Tests for URL storage in database."""
import pytest
from database.duckdb_client import DuckDBClient


def test_store_full_url_in_database(temp_db, sample_boundary_coords):
    """Test that we can store and retrieve full URLs."""
    # Insert neighbourhood with full URL
    full_url = "https://www.met.police.uk/area/your-area/met/Westminster/Pimlico-North"
    neighbourhood_id = "E05013802N"
    
    temp_db.insert_neighbourhood(
        force_id="metropolitan",
        neighbourhood_id=neighbourhood_id,
        name="Pimlico North",
        boundary_coords=sample_boundary_coords,
        force_url_slug=full_url,  # Store full URL
        neighbourhood_url_slug=neighbourhood_id
    )
    
    # Retrieve it
    result = temp_db.find_neighbourhood_by_coords(-1.1457, 52.6390)
    
    assert result is not None
    force_id, retrieved_neighbourhood_id, name, force_url_slug, neighbourhood_url_slug = result
    
    # Verify we got the full URL back
    assert force_url_slug == full_url
    assert neighbourhood_url_slug == neighbourhood_id
    assert force_id == "metropolitan"
    assert retrieved_neighbourhood_id == neighbourhood_id
    assert name == "Pimlico North"


def test_url_can_be_none(temp_db, sample_boundary_coords):
    """Test that URL fields can be None if not provided."""
    temp_db.insert_neighbourhood(
        force_id="test-force",
        neighbourhood_id="TEST01",
        name="Test Area",
        boundary_coords=sample_boundary_coords,
        force_url_slug=None,  # No URL provided
        neighbourhood_url_slug=None
    )
    
    result = temp_db.find_neighbourhood_by_coords(-1.1457, 52.6390)
    
    assert result is not None
    force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug = result
    
    # Should handle None gracefully
    assert force_url_slug is None
    assert neighbourhood_url_slug is None


def test_different_force_url_formats(temp_db, sample_boundary_coords):
    """Test that different URL formats are stored correctly."""
    test_cases = [
        {
            "force_id": "metropolitan",
            "url": "https://www.met.police.uk/area/your-area/met/Westminster/St-James's",
            "neighbourhood_id": "E05013806N"
        },
        {
            "force_id": "city-of-london",
            "url": "https://www.cityoflondon.police.uk/area/your-area/city-of-london/Aldgate",
            "neighbourhood_id": "COL01"
        },
        {
            "force_id": "leicestershire",
            "url": "https://www.leics.police.uk/area/your-area/leicestershire/City-Centre",
            "neighbourhood_id": "NC04"
        }
    ]
    
    # Create slightly different boundaries for each
    for i, test_case in enumerate(test_cases):
        # Offset the boundary slightly for each test case
        offset = i * 0.01
        coords = [
            {"latitude": str(52.63 + offset), "longitude": "-1.16"},
            {"latitude": str(52.63 + offset), "longitude": "-1.13"},
            {"latitude": str(52.65 + offset), "longitude": "-1.13"},
            {"latitude": str(52.65 + offset), "longitude": "-1.16"},
            {"latitude": str(52.63 + offset), "longitude": "-1.16"},
        ]
        
        temp_db.insert_neighbourhood(
            force_id=test_case["force_id"],
            neighbourhood_id=test_case["neighbourhood_id"],
            name=f"Test {i}",
            boundary_coords=coords,
            force_url_slug=test_case["url"],
            neighbourhood_url_slug=test_case["neighbourhood_id"]
        )
    
    # Verify all were stored correctly
    count = temp_db.get_neighbourhood_count()
    assert count == len(test_cases)
    
    # Check each one
    for i, test_case in enumerate(test_cases):
        offset = i * 0.01
        result = temp_db.find_neighbourhood_by_coords(-1.145, 52.64 + offset)
        
        assert result is not None
        force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug = result
        
        assert force_url_slug == test_case["url"]
        assert neighbourhood_url_slug == test_case["neighbourhood_id"]
        assert force_id == test_case["force_id"]


def test_url_with_special_characters(temp_db, sample_boundary_coords):
    """Test that URLs with special characters are stored correctly."""
    # URL with apostrophe and hyphens
    url_with_special_chars = "https://www.met.police.uk/area/your-area/met/Westminster/St-James's"
    
    temp_db.insert_neighbourhood(
        force_id="metropolitan",
        neighbourhood_id="E05013806N",
        name="St James's",
        boundary_coords=sample_boundary_coords,
        force_url_slug=url_with_special_chars,
        neighbourhood_url_slug="E05013806N"
    )
    
    result = temp_db.find_neighbourhood_by_coords(-1.1457, 52.6390)
    
    assert result is not None
    _, _, _, force_url_slug, _ = result
    
    # Verify special characters are preserved
    assert force_url_slug == url_with_special_chars
    assert "'" in force_url_slug  # Apostrophe preserved
    assert "-" in force_url_slug  # Hyphens preserved
