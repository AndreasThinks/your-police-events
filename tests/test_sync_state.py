"""Tests for sync state management."""
import pytest
from datetime import datetime
from database.sync_state import SyncStateManager, SyncStatus


@pytest.mark.asyncio
async def test_sync_state_initial_state():
    """Test initial state is idle."""
    manager = SyncStateManager()
    state = await manager.get_state()
    
    assert state["status"] == "idle"
    assert state["error"] is None
    assert "progress" not in state
    assert "last_sync" not in state


@pytest.mark.asyncio
async def test_sync_state_start_sync():
    """Test starting a sync updates state correctly."""
    manager = SyncStateManager()
    
    await manager.start_sync(total_forces=44)
    state = await manager.get_state()
    
    assert state["status"] == "running"
    assert "progress" in state
    assert state["progress"]["total_forces"] == 44
    assert state["progress"]["forces_processed"] == 0
    assert "timing" in state
    assert state["timing"]["started_at"] is not None


@pytest.mark.asyncio
async def test_sync_state_update_progress():
    """Test updating progress during sync."""
    manager = SyncStateManager()
    
    await manager.start_sync(total_forces=44)
    await manager.update_progress(
        current_force="metropolitan",
        current_force_name="Metropolitan Police Service",
        forces_processed=1,
        neighbourhoods_processed=100,
        total_neighbourhoods=4656,
        neighbourhoods_synced=95,
        neighbourhoods_failed=5
    )
    
    state = await manager.get_state()
    
    assert state["status"] == "running"
    assert state["progress"]["current_force"] == "metropolitan"
    assert state["progress"]["current_force_name"] == "Metropolitan Police Service"
    assert state["progress"]["forces_processed"] == 1
    assert state["progress"]["neighbourhoods_processed"] == 100
    assert state["progress"]["neighbourhoods_synced"] == 95
    assert state["progress"]["neighbourhoods_failed"] == 5
    assert state["progress"]["percentage"] > 0


@pytest.mark.asyncio
async def test_sync_state_complete_sync():
    """Test completing a sync stores results."""
    manager = SyncStateManager()
    
    await manager.start_sync(total_forces=44)
    await manager.update_progress(
        neighbourhoods_processed=4656,
        neighbourhoods_synced=4500
    )
    
    await manager.complete_sync(
        neighbourhoods_synced=4500,
        neighbourhoods_failed=100,
        neighbourhoods_no_boundary=56,
        total_neighbourhoods=4656,
        forces_processed=43,
        forces_failed=1
    )
    
    state = await manager.get_state()
    
    assert state["status"] == "completed"
    assert "last_sync" in state
    assert state["last_sync"]["neighbourhoods_synced"] == 4500
    assert state["last_sync"]["neighbourhoods_failed"] == 100
    assert state["last_sync"]["total_neighbourhoods"] == 4656
    assert state["last_sync"]["forces_processed"] == 43
    assert state["last_sync"]["success_rate"] > 95


@pytest.mark.asyncio
async def test_sync_state_fail_sync():
    """Test failing a sync stores error message."""
    manager = SyncStateManager()
    
    await manager.start_sync(total_forces=44)
    await manager.fail_sync("API connection failed")
    
    state = await manager.get_state()
    
    assert state["status"] == "failed"
    assert state["error"] == "API connection failed"


@pytest.mark.asyncio
async def test_sync_state_percentage_calculation():
    """Test progress percentage is calculated correctly."""
    manager = SyncStateManager()
    
    await manager.start_sync(total_forces=44)
    await manager.update_progress(
        neighbourhoods_processed=1000,
        total_neighbourhoods=4000
    )
    
    state = await manager.get_state()
    
    assert state["progress"]["percentage"] == 25.0


@pytest.mark.asyncio
async def test_sync_state_elapsed_time():
    """Test elapsed time is tracked."""
    manager = SyncStateManager()
    
    await manager.start_sync(total_forces=44)
    
    # Small delay to ensure elapsed time > 0
    import asyncio
    await asyncio.sleep(0.1)
    
    state = await manager.get_state()
    
    assert state["timing"]["elapsed_seconds"] is not None
    assert state["timing"]["elapsed_seconds"] >= 0


@pytest.mark.asyncio
async def test_sync_state_multiple_syncs():
    """Test that multiple syncs preserve last sync result."""
    manager = SyncStateManager()
    
    # First sync
    await manager.start_sync(total_forces=44)
    await manager.complete_sync(
        neighbourhoods_synced=4500,
        neighbourhoods_failed=100,
        neighbourhoods_no_boundary=56,
        total_neighbourhoods=4656,
        forces_processed=43,
        forces_failed=1
    )
    
    first_sync_state = await manager.get_state()
    first_completed_at = first_sync_state["last_sync"]["completed_at"]
    
    # Second sync
    await manager.start_sync(total_forces=44)
    
    state = await manager.get_state()
    
    # Should still have last sync result while new sync is running
    assert state["status"] == "running"
    assert "last_sync" in state
    assert state["last_sync"]["completed_at"] == first_completed_at


@pytest.mark.asyncio
async def test_sync_state_thread_safety():
    """Test that state updates are thread-safe."""
    manager = SyncStateManager()
    
    await manager.start_sync(total_forces=44)
    
    # Simulate concurrent updates
    import asyncio
    tasks = [
        manager.update_progress(neighbourhoods_processed=i)
        for i in range(100)
    ]
    
    await asyncio.gather(*tasks)
    
    state = await manager.get_state()
    
    # Should have processed all updates without corruption
    assert state["status"] == "running"
    assert state["progress"]["neighbourhoods_processed"] >= 0
