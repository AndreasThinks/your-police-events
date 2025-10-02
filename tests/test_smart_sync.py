"""Tests for smart sync system including strategy and recovery."""
import pytest
from datetime import datetime, timedelta
from database.duckdb_client import DuckDBClient
from database.sync_strategy import determine_sync_strategy, SyncStrategy


@pytest.fixture
def test_db():
    """Create a test database."""
    db = DuckDBClient(":memory:")
    db.connect()
    db.initialize_schema()
    yield db
    db.close()


def test_empty_database_strategy(test_db):
    """Test strategy for empty database."""
    strategy = determine_sync_strategy(test_db)
    
    assert strategy.sync_type == "full"
    assert strategy.delay_minutes == 2
    assert "empty" in strategy.reason.lower()


def test_no_metadata_strategy(test_db):
    """Test strategy when no metadata exists (legacy database)."""
    # Add some neighbourhoods but no metadata
    test_db.conn.execute("""
        INSERT INTO neighbourhoods (force_id, neighbourhood_id, name, boundary)
        VALUES ('test', 'test1', 'Test Area', ST_GeomFromText('POINT(0 0)'))
    """)
    
    strategy = determine_sync_strategy(test_db)
    
    assert strategy.sync_type == "skip"
    assert "legacy" in strategy.reason.lower()


def test_stale_lock_detection(test_db):
    """Test detection of stale sync lock (crashed mid-sync)."""
    # Add neighbourhoods
    test_db.conn.execute("""
        INSERT INTO neighbourhoods (force_id, neighbourhood_id, name, boundary)
        VALUES ('test', 'test1', 'Test Area', ST_GeomFromText('POINT(0 0)'))
    """)
    
    # Create stale lock (3 hours old)
    stale_time = datetime.now() - timedelta(hours=3)
    test_db.save_sync_metadata({
        'last_sync_started': stale_time,
        'sync_status': 'running',
        'total_forces': 44,
        'forces_synced': 10,
        'forces_failed': 0,
        'total_neighbourhoods': 1000,
        'neighbourhoods_synced': 500,
        'success_rate': 50.0,
        'error_message': None,
        'sync_duration_seconds': None
    })
    
    # Mark some forces as failed
    test_db.update_force_status('sussex', 'Sussex Police', {
        'last_sync_started': stale_time,
        'sync_status': 'failed',
        'neighbourhoods_expected': 273,
        'neighbourhoods_synced': 0,
        'error_message': 'Timeout'
    })
    
    strategy = determine_sync_strategy(test_db)
    
    assert strategy.sync_type == "recovery"
    assert strategy.delay_minutes == 5
    assert "stale lock" in strategy.reason.lower()
    assert 'sussex' in strategy.force_ids


def test_failed_sync_recovery(test_db):
    """Test recovery strategy after failed sync."""
    # Add neighbourhoods
    test_db.conn.execute("""
        INSERT INTO neighbourhoods (force_id, neighbourhood_id, name, boundary)
        VALUES ('test', 'test1', 'Test Area', ST_GeomFromText('POINT(0 0)'))
    """)
    
    # Create failed sync metadata
    test_db.save_sync_metadata({
        'last_sync_started': datetime.now() - timedelta(hours=1),
        'last_sync_completed': datetime.now() - timedelta(minutes=30),
        'sync_status': 'failed',
        'total_forces': 44,
        'forces_synced': 40,
        'forces_failed': 4,
        'total_neighbourhoods': 4656,
        'neighbourhoods_synced': 4200,
        'success_rate': 90.2,
        'error_message': 'Some forces failed',
        'sync_duration_seconds': 1800
    })
    
    # Mark failed forces
    for force_id in ['sussex', 'devon-and-cornwall']:
        test_db.update_force_status(force_id, f'{force_id} Police', {
            'last_sync_started': datetime.now() - timedelta(hours=1),
            'last_sync_completed': datetime.now() - timedelta(minutes=30),
            'sync_status': 'failed',
            'neighbourhoods_expected': 200,
            'neighbourhoods_synced': 0,
            'error_message': 'API timeout'
        })
    
    strategy = determine_sync_strategy(test_db)
    
    assert strategy.sync_type == "recovery"
    assert strategy.delay_minutes == 5
    assert "failed" in strategy.reason.lower()
    assert len(strategy.force_ids) == 2
    assert 'sussex' in strategy.force_ids
    assert 'devon-and-cornwall' in strategy.force_ids


def test_stale_data_strategy(test_db):
    """Test strategy for stale data (>8 days old)."""
    # Add neighbourhoods
    test_db.conn.execute("""
        INSERT INTO neighbourhoods (force_id, neighbourhood_id, name, boundary)
        VALUES ('test', 'test1', 'Test Area', ST_GeomFromText('POINT(0 0)'))
    """)
    
    # Create old sync metadata (9 days ago)
    old_time = datetime.now() - timedelta(days=9)
    test_db.save_sync_metadata({
        'last_sync_started': old_time,
        'last_sync_completed': old_time + timedelta(hours=2),
        'sync_status': 'completed',
        'total_forces': 44,
        'forces_synced': 44,
        'forces_failed': 0,
        'total_neighbourhoods': 4656,
        'neighbourhoods_synced': 4656,
        'success_rate': 100.0,
        'error_message': None,
        'sync_duration_seconds': 7200
    })
    
    strategy = determine_sync_strategy(test_db)
    
    assert strategy.sync_type == "full"
    assert strategy.delay_minutes == 2
    assert "stale" in strategy.reason.lower() or "days old" in strategy.reason.lower()


def test_fresh_data_strategy(test_db):
    """Test strategy for fresh data (<6 days old)."""
    # Add neighbourhoods
    test_db.conn.execute("""
        INSERT INTO neighbourhoods (force_id, neighbourhood_id, name, boundary)
        VALUES ('test', 'test1', 'Test Area', ST_GeomFromText('POINT(0 0)'))
    """)
    
    # Create recent sync metadata (2 days ago)
    recent_time = datetime.now() - timedelta(days=2)
    test_db.save_sync_metadata({
        'last_sync_started': recent_time,
        'last_sync_completed': recent_time + timedelta(hours=2),
        'sync_status': 'completed',
        'total_forces': 44,
        'forces_synced': 44,
        'forces_failed': 0,
        'total_neighbourhoods': 4656,
        'neighbourhoods_synced': 4656,
        'success_rate': 100.0,
        'error_message': None,
        'sync_duration_seconds': 7200
    })
    
    strategy = determine_sync_strategy(test_db)
    
    assert strategy.sync_type == "skip"
    assert strategy.delay_minutes is None
    assert "fresh" in strategy.reason.lower()


def test_approaching_stale_strategy(test_db):
    """Test strategy for data approaching staleness (6-8 days old)."""
    # Add neighbourhoods
    test_db.conn.execute("""
        INSERT INTO neighbourhoods (force_id, neighbourhood_id, name, boundary)
        VALUES ('test', 'test1', 'Test Area', ST_GeomFromText('POINT(0 0)'))
    """)
    
    # Create sync metadata (7 days ago)
    time_7days = datetime.now() - timedelta(days=7)
    test_db.save_sync_metadata({
        'last_sync_started': time_7days,
        'last_sync_completed': time_7days + timedelta(hours=2),
        'sync_status': 'completed',
        'total_forces': 44,
        'forces_synced': 44,
        'forces_failed': 0,
        'total_neighbourhoods': 4656,
        'neighbourhoods_synced': 4656,
        'success_rate': 100.0,
        'error_message': None,
        'sync_duration_seconds': 7200
    })
    
    strategy = determine_sync_strategy(test_db)
    
    assert strategy.sync_type == "skip"
    assert "weekly" in strategy.reason.lower()


def test_metadata_persistence(test_db):
    """Test that sync metadata is correctly saved and retrieved."""
    metadata = {
        'last_sync_started': datetime.now(),
        'last_sync_completed': datetime.now() + timedelta(hours=2),
        'sync_status': 'completed',
        'total_forces': 44,
        'forces_synced': 44,
        'forces_failed': 0,
        'total_neighbourhoods': 4656,
        'neighbourhoods_synced': 4500,
        'success_rate': 96.7,
        'error_message': None,
        'sync_duration_seconds': 7200
    }
    
    test_db.save_sync_metadata(metadata)
    retrieved = test_db.get_sync_metadata()
    
    assert retrieved is not None
    assert retrieved['sync_status'] == 'completed'
    assert retrieved['total_forces'] == 44
    assert retrieved['neighbourhoods_synced'] == 4500
    assert abs(retrieved['success_rate'] - 96.7) < 0.01  # Float comparison with tolerance


def test_force_status_tracking(test_db):
    """Test per-force status tracking."""
    test_db.update_force_status('metropolitan', 'Metropolitan Police', {
        'last_sync_started': datetime.now(),
        'last_sync_completed': datetime.now() + timedelta(minutes=30),
        'sync_status': 'success',
        'neighbourhoods_expected': 679,
        'neighbourhoods_synced': 679,
        'error_message': None
    })
    
    status = test_db.get_force_status('metropolitan')
    
    assert status is not None
    assert status['sync_status'] == 'success'
    assert status['neighbourhoods_synced'] == 679


def test_failed_forces_retrieval(test_db):
    """Test retrieval of failed forces."""
    # Add some force statuses
    test_db.update_force_status('sussex', 'Sussex Police', {
        'last_sync_started': datetime.now(),
        'last_sync_completed': datetime.now(),
        'sync_status': 'failed',
        'neighbourhoods_expected': 273,
        'neighbourhoods_synced': 0,
        'error_message': 'Timeout'
    })
    
    test_db.update_force_status('metropolitan', 'Metropolitan Police', {
        'last_sync_started': datetime.now(),
        'last_sync_completed': datetime.now(),
        'sync_status': 'success',
        'neighbourhoods_expected': 679,
        'neighbourhoods_synced': 679,
        'error_message': None
    })
    
    test_db.update_force_status('devon-and-cornwall', 'Devon & Cornwall Police', {
        'last_sync_started': datetime.now(),
        'last_sync_completed': datetime.now(),
        'sync_status': 'partial',
        'neighbourhoods_expected': 200,
        'neighbourhoods_synced': 150,
        'error_message': '50 failed'
    })
    
    failed = test_db.get_failed_forces()
    
    assert len(failed) == 2
    assert 'sussex' in failed
    assert 'devon-and-cornwall' in failed
    assert 'metropolitan' not in failed
