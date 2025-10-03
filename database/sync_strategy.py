"""Smart sync strategy determination for startup."""
import logging
from datetime import datetime, timedelta
from typing import Tuple, Optional, List
from database.duckdb_client import DuckDBClient

logger = logging.getLogger(__name__)


class SyncStrategy:
    """Represents a sync strategy decision."""
    
    def __init__(
        self,
        sync_type: str,
        delay_minutes: Optional[int],
        force_ids: Optional[List[str]] = None,
        reason: str = ""
    ):
        self.sync_type = sync_type  # 'full', 'recovery', 'skip'
        self.delay_minutes = delay_minutes
        self.force_ids = force_ids or []
        self.reason = reason
    
    def __repr__(self):
        return f"SyncStrategy(type={self.sync_type}, delay={self.delay_minutes}min, reason='{self.reason}')"


def determine_sync_strategy(db_client: DuckDBClient) -> SyncStrategy:
    """
    Determine what type of sync to run on startup based on database state.
    
    Args:
        db_client: Connected DuckDB client
        
    Returns:
        SyncStrategy object with sync type, delay, and optional force IDs
    """
    neighbourhood_count = db_client.get_neighbourhood_count()
    metadata = db_client.get_sync_metadata()
    
    # Case 1: Empty database - need full initial sync
    if neighbourhood_count == 0:
        logger.info("Database is empty - scheduling full initial sync")
        return SyncStrategy(
            sync_type="full",
            delay_minutes=2,
            reason="Database is empty"
        )
    
    # Case 2: No metadata yet (legacy database) - assume it's okay
    if not metadata:
        logger.info("No sync metadata found - assuming database is valid")
        return SyncStrategy(
            sync_type="skip",
            delay_minutes=None,
            reason="No metadata (legacy database)"
        )
    
    # Case 3: Detect incomplete sync (started but never completed)
    if metadata['last_sync_started'] and not metadata['last_sync_completed']:
        logger.warning("Incomplete sync detected - sync started but never completed")
        failed_forces = db_client.get_failed_forces()
        
        if failed_forces:
            logger.info(f"Will recover {len(failed_forces)} forces: {failed_forces}")
            return SyncStrategy(
                sync_type="recovery",
                delay_minutes=5,
                force_ids=failed_forces,
                reason="Incomplete sync detected (crash during sync)"
            )
        else:
            # No specific forces identified - do full sync as fallback
            logger.warning("Incomplete sync detected but no failed forces tracked - doing full sync")
            return SyncStrategy(
                sync_type="full",
                delay_minutes=5,
                reason="Incomplete sync detected (no force tracking - full sync fallback)"
            )
    
    # Case 4: Detect corrupted state (completion before start - impossible)
    if (metadata['last_sync_completed'] and metadata['last_sync_started'] and
        metadata['last_sync_completed'] < metadata['last_sync_started']):
        logger.warning("Corrupted sync state detected")
        return SyncStrategy(
            sync_type="full",
            delay_minutes=5,
            reason="Corrupted sync state"
        )
    
    # Case 5: Detect stale lock (sync crashed mid-run)
    if metadata['sync_status'] == 'running':
        if metadata['last_sync_started']:
            hours_since = (datetime.now() - metadata['last_sync_started']).total_seconds() / 3600
            if hours_since > 2:
                failed_forces = db_client.get_failed_forces()
                logger.warning(
                    f"Stale lock detected ({hours_since:.1f}h old) - "
                    f"scheduling recovery sync for {len(failed_forces)} forces"
                )
                return SyncStrategy(
                    sync_type="recovery",
                    delay_minutes=5,
                    force_ids=failed_forces,
                    reason=f"Stale lock detected ({hours_since:.1f}h old)"
                )
    
    # Case 6: Last sync failed - recover failed forces
    if metadata['sync_status'] == 'failed':
        failed_forces = db_client.get_failed_forces()
        if failed_forces:
            logger.info(
                f"Last sync failed - scheduling recovery sync for {len(failed_forces)} forces"
            )
            return SyncStrategy(
                sync_type="recovery",
                delay_minutes=5,
                force_ids=failed_forces,
                reason="Recovering from failed sync"
            )
        else:
            # Failed but no specific forces marked - do full sync
            logger.info("Last sync failed but no failed forces tracked - scheduling full sync")
            return SyncStrategy(
                sync_type="full",
                delay_minutes=5,
                reason="Last sync failed (no force tracking)"
            )
    
    # Case 7: Check data freshness
    if metadata['last_sync_completed']:
        days_old = (datetime.now() - metadata['last_sync_completed']).total_seconds() / 86400
        
        if days_old > 8:
            logger.info(f"Data is {days_old:.1f} days old - scheduling full sync")
            return SyncStrategy(
                sync_type="full",
                delay_minutes=2,
                reason=f"Data is {days_old:.1f} days old (stale)"
            )
        elif days_old > 6:
            logger.info(f"Data is {days_old:.1f} days old - weekly sync will handle it")
            return SyncStrategy(
                sync_type="skip",
                delay_minutes=None,
                reason=f"Data is {days_old:.1f} days old (weekly sync scheduled)"
            )
    
    # Case 8: Data is fresh
    logger.info("Data is fresh - no sync needed")
    return SyncStrategy(
        sync_type="skip",
        delay_minutes=None,
        reason="Data is fresh"
    )
