"""Sync state management for tracking neighbourhood sync progress."""
import asyncio
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict
from enum import Enum


class SyncStatus(Enum):
    """Sync status states."""
    IDLE = "idle"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class SyncProgress:
    """Current sync progress information."""
    current_force: Optional[str] = None
    current_force_name: Optional[str] = None
    forces_processed: int = 0
    total_forces: int = 0
    neighbourhoods_processed: int = 0
    total_neighbourhoods: int = 0
    neighbourhoods_synced: int = 0
    neighbourhoods_failed: int = 0
    neighbourhoods_no_boundary: int = 0
    
    @property
    def percentage(self) -> float:
        """Calculate overall progress percentage."""
        if self.total_neighbourhoods == 0:
            return 0.0
        return (self.neighbourhoods_processed / self.total_neighbourhoods) * 100


@dataclass
class SyncTiming:
    """Sync timing information."""
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    @property
    def elapsed_seconds(self) -> Optional[int]:
        """Calculate elapsed time in seconds."""
        if not self.started_at:
            return None
        end_time = self.completed_at or datetime.now()
        return int((end_time - self.started_at).total_seconds())
    
    @property
    def estimated_completion(self) -> Optional[datetime]:
        """Estimate completion time based on current progress."""
        # This would need progress info to calculate
        return None


@dataclass
class LastSyncResult:
    """Results from the last completed sync."""
    completed_at: Optional[datetime] = None
    duration_seconds: Optional[int] = None
    neighbourhoods_synced: int = 0
    neighbourhoods_failed: int = 0
    neighbourhoods_no_boundary: int = 0
    total_neighbourhoods: int = 0
    forces_processed: int = 0
    forces_failed: int = 0
    next_sync_at: Optional[datetime] = None
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage."""
        if self.total_neighbourhoods == 0:
            return 0.0
        return (self.neighbourhoods_synced / self.total_neighbourhoods) * 100


class SyncStateManager:
    """Thread-safe manager for sync state."""
    
    def __init__(self):
        self._lock = asyncio.Lock()
        self._status = SyncStatus.IDLE
        self._progress = SyncProgress()
        self._timing = SyncTiming()
        self._last_result: Optional[LastSyncResult] = None
        self._error_message: Optional[str] = None
    
    async def start_sync(self, total_forces: int):
        """Mark sync as started."""
        async with self._lock:
            self._status = SyncStatus.RUNNING
            self._progress = SyncProgress(total_forces=total_forces)
            self._timing = SyncTiming(started_at=datetime.now())
            self._error_message = None
    
    async def update_progress(
        self,
        current_force: Optional[str] = None,
        current_force_name: Optional[str] = None,
        forces_processed: Optional[int] = None,
        neighbourhoods_processed: Optional[int] = None,
        total_neighbourhoods: Optional[int] = None,
        neighbourhoods_synced: Optional[int] = None,
        neighbourhoods_failed: Optional[int] = None,
        neighbourhoods_no_boundary: Optional[int] = None
    ):
        """Update sync progress."""
        async with self._lock:
            if current_force is not None:
                self._progress.current_force = current_force
            if current_force_name is not None:
                self._progress.current_force_name = current_force_name
            if forces_processed is not None:
                self._progress.forces_processed = forces_processed
            if neighbourhoods_processed is not None:
                self._progress.neighbourhoods_processed = neighbourhoods_processed
            if total_neighbourhoods is not None:
                self._progress.total_neighbourhoods = total_neighbourhoods
            if neighbourhoods_synced is not None:
                self._progress.neighbourhoods_synced = neighbourhoods_synced
            if neighbourhoods_failed is not None:
                self._progress.neighbourhoods_failed = neighbourhoods_failed
            if neighbourhoods_no_boundary is not None:
                self._progress.neighbourhoods_no_boundary = neighbourhoods_no_boundary
    
    async def complete_sync(
        self,
        neighbourhoods_synced: int,
        neighbourhoods_failed: int,
        neighbourhoods_no_boundary: int,
        total_neighbourhoods: int,
        forces_processed: int,
        forces_failed: int
    ):
        """Mark sync as completed and store results."""
        async with self._lock:
            self._status = SyncStatus.COMPLETED
            self._timing.completed_at = datetime.now()
            
            # Store last sync result
            self._last_result = LastSyncResult(
                completed_at=self._timing.completed_at,
                duration_seconds=self._timing.elapsed_seconds,
                neighbourhoods_synced=neighbourhoods_synced,
                neighbourhoods_failed=neighbourhoods_failed,
                neighbourhoods_no_boundary=neighbourhoods_no_boundary,
                total_neighbourhoods=total_neighbourhoods,
                forces_processed=forces_processed,
                forces_failed=forces_failed
            )
            
            # Reset progress
            self._progress = SyncProgress()
    
    async def fail_sync(self, error_message: str):
        """Mark sync as failed."""
        async with self._lock:
            self._status = SyncStatus.FAILED
            self._timing.completed_at = datetime.now()
            self._error_message = error_message
    
    async def set_next_sync(self, next_sync_at: datetime):
        """Set the next scheduled sync time."""
        async with self._lock:
            if self._last_result:
                self._last_result.next_sync_at = next_sync_at
            else:
                # Create a minimal last result if none exists
                self._last_result = LastSyncResult(next_sync_at=next_sync_at)
    
    async def get_next_sync(self) -> Optional[datetime]:
        """Get the next scheduled sync time."""
        async with self._lock:
            if self._last_result:
                return self._last_result.next_sync_at
            return None
    
    async def get_state(self) -> Dict[str, Any]:
        """Get current sync state as dictionary."""
        async with self._lock:
            state = {
                "status": self._status.value,
                "error": self._error_message
            }
            
            # Add progress if sync is running
            if self._status == SyncStatus.RUNNING:
                state["progress"] = {
                    "current_force": self._progress.current_force,
                    "current_force_name": self._progress.current_force_name,
                    "forces_processed": self._progress.forces_processed,
                    "total_forces": self._progress.total_forces,
                    "neighbourhoods_processed": self._progress.neighbourhoods_processed,
                    "total_neighbourhoods": self._progress.total_neighbourhoods,
                    "neighbourhoods_synced": self._progress.neighbourhoods_synced,
                    "neighbourhoods_failed": self._progress.neighbourhoods_failed,
                    "neighbourhoods_no_boundary": self._progress.neighbourhoods_no_boundary,
                    "percentage": round(self._progress.percentage, 1)
                }
                
                state["timing"] = {
                    "started_at": self._timing.started_at.isoformat() if self._timing.started_at else None,
                    "elapsed_seconds": self._timing.elapsed_seconds
                }
            
            # Add last sync result if available
            if self._last_result:
                state["last_sync"] = {
                    "completed_at": self._last_result.completed_at.isoformat() if self._last_result.completed_at else None,
                    "duration_seconds": self._last_result.duration_seconds,
                    "neighbourhoods_synced": self._last_result.neighbourhoods_synced,
                    "neighbourhoods_failed": self._last_result.neighbourhoods_failed,
                    "neighbourhoods_no_boundary": self._last_result.neighbourhoods_no_boundary,
                    "total_neighbourhoods": self._last_result.total_neighbourhoods,
                    "forces_processed": self._last_result.forces_processed,
                    "forces_failed": self._last_result.forces_failed,
                    "success_rate": round(self._last_result.success_rate, 1),
                    "next_sync_at": self._last_result.next_sync_at.isoformat() if self._last_result.next_sync_at else None
                }
            
            return state


# Global sync state manager instance
sync_state = SyncStateManager()
