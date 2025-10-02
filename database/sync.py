"""Weekly data synchronization job for neighbourhood boundaries."""
import asyncio
import logging
from typing import List, Dict, Any
from collections import defaultdict
from api.police_uk import PoliceUKClient
from database.duckdb_client import DuckDBClient
from database.sync_state import sync_state

logger = logging.getLogger(__name__)


async def sync_all_neighbourhoods(db_client: DuckDBClient):
    """
    Sync all police force neighbourhoods and their boundaries to the database.
    
    This is a long-running operation that fetches data for all forces and
    neighbourhoods in the UK. Uses retry logic to handle temporary failures.
    
    Args:
        db_client: Connected DuckDB client
    """
    police_client = PoliceUKClient(timeout=60.0, max_retries=3)
    
    # Track statistics
    stats = {
        "total_forces": 0,
        "forces_processed": 0,
        "forces_failed": 0,
        "total_neighbourhoods": 0,
        "neighbourhoods_synced": 0,
        "neighbourhoods_no_boundary": 0,
        "neighbourhoods_failed": 0,
        "failed_forces": [],
        "failed_neighbourhoods": []
    }
    
    try:
        logger.info("Starting neighbourhood sync with retry logic...")
        
        # Get all forces
        forces = await police_client.get_forces()
        stats["total_forces"] = len(forces)
        logger.info(f"Found {len(forces)} police forces")
        
        if not forces:
            logger.error("Failed to fetch forces list - aborting sync")
            await sync_state.fail_sync("Failed to fetch forces list")
            return
        
        # Mark sync as started
        await sync_state.start_sync(total_forces=len(forces))
        
        for force in forces:
            force_id = force['id']
            force_name = force['name']
            
            # Get neighbourhoods for this force
            neighbourhoods = await police_client.get_neighbourhoods(force_id)
            
            if not neighbourhoods:
                logger.warning(f"No neighbourhoods returned for {force_name} ({force_id})")
                stats["forces_failed"] += 1
                stats["failed_forces"].append({
                    "force_id": force_id,
                    "force_name": force_name,
                    "reason": "No neighbourhoods returned"
                })
                continue
            
            stats["forces_processed"] += 1
            logger.info(
                f"Processing {len(neighbourhoods)} neighbourhoods for {force_name} "
                f"({stats['forces_processed']}/{stats['total_forces']})"
            )
            
            # Update sync state
            await sync_state.update_progress(
                current_force=force_id,
                current_force_name=force_name,
                forces_processed=stats["forces_processed"],
                total_neighbourhoods=stats["total_neighbourhoods"] + len(neighbourhoods)
            )
            
            force_success = 0
            force_no_boundary = 0
            force_failed = 0
            
            for neighbourhood in neighbourhoods:
                neighbourhood_id = neighbourhood['id']
                neighbourhood_name = neighbourhood['name']
                stats["total_neighbourhoods"] += 1
                
                # Get boundary for this neighbourhood
                boundary = await police_client.get_neighbourhood_boundary(
                    force_id, neighbourhood_id
                )
                
                if boundary and len(boundary) > 0:
                    try:
                        # Insert into database
                        db_client.insert_neighbourhood(
                            force_id=force_id,
                            neighbourhood_id=neighbourhood_id,
                            name=neighbourhood_name,
                            boundary_coords=boundary
                        )
                        stats["neighbourhoods_synced"] += 1
                        force_success += 1
                    except Exception as e:
                        logger.error(
                            f"Database error for {force_id}/{neighbourhood_id}: {e}"
                        )
                        stats["neighbourhoods_failed"] += 1
                        force_failed += 1
                        stats["failed_neighbourhoods"].append({
                            "force_id": force_id,
                            "neighbourhood_id": neighbourhood_id,
                            "name": neighbourhood_name,
                            "reason": f"Database error: {e}"
                        })
                elif boundary is not None and len(boundary) == 0:
                    # Empty boundary (legitimate - some neighbourhoods don't have boundaries)
                    logger.debug(
                        f"No boundary data for {force_id}/{neighbourhood_id} ({neighbourhood_name})"
                    )
                    stats["neighbourhoods_no_boundary"] += 1
                    force_no_boundary += 1
                else:
                    # Failed to fetch boundary after retries
                    logger.warning(
                        f"Failed to fetch boundary for {force_id}/{neighbourhood_id} "
                        f"({neighbourhood_name}) after retries"
                    )
                    stats["neighbourhoods_failed"] += 1
                    force_failed += 1
                    stats["failed_neighbourhoods"].append({
                        "force_id": force_id,
                        "neighbourhood_id": neighbourhood_id,
                        "name": neighbourhood_name,
                        "reason": "Failed to fetch boundary after retries"
                    })
                
                # Update progress periodically
                if stats["neighbourhoods_processed"] % 10 == 0:
                    await sync_state.update_progress(
                        neighbourhoods_processed=stats["neighbourhoods_processed"],
                        neighbourhoods_synced=stats["neighbourhoods_synced"],
                        neighbourhoods_failed=stats["neighbourhoods_failed"],
                        neighbourhoods_no_boundary=stats["neighbourhoods_no_boundary"]
                    )
                
                # Small delay to be respectful to the API
                await asyncio.sleep(0.1)
            
            logger.info(
                f"  {force_name}: {force_success} synced, "
                f"{force_no_boundary} no boundary, {force_failed} failed"
            )
        
        # Final summary
        logger.info("="*70)
        logger.info("SYNC COMPLETE - SUMMARY")
        logger.info("="*70)
        logger.info(f"Forces: {stats['forces_processed']}/{stats['total_forces']} processed")
        if stats['forces_failed'] > 0:
            logger.warning(f"  {stats['forces_failed']} forces failed")
        
        logger.info(
            f"Neighbourhoods: {stats['neighbourhoods_synced']}/{stats['total_neighbourhoods']} synced"
        )
        if stats['neighbourhoods_no_boundary'] > 0:
            logger.info(f"  {stats['neighbourhoods_no_boundary']} had no boundary data")
        if stats['neighbourhoods_failed'] > 0:
            logger.warning(f"  {stats['neighbourhoods_failed']} failed")
        
        success_rate = (
            stats['neighbourhoods_synced'] / stats['total_neighbourhoods'] * 100
            if stats['total_neighbourhoods'] > 0 else 0
        )
        logger.info(f"Success rate: {success_rate:.1f}%")
        
        # Log failed forces
        if stats['failed_forces']:
            logger.warning(f"\nFailed forces ({len(stats['failed_forces'])}):")
            for failed in stats['failed_forces']:
                logger.warning(f"  - {failed['force_id']}: {failed['reason']}")
        
        # Log sample of failed neighbourhoods
        if stats['failed_neighbourhoods']:
            logger.warning(
                f"\nFailed neighbourhoods ({len(stats['failed_neighbourhoods'])} total), "
                f"showing first 10:"
            )
            for failed in stats['failed_neighbourhoods'][:10]:
                logger.warning(
                    f"  - {failed['force_id']}/{failed['neighbourhood_id']}: "
                    f"{failed['name']} - {failed['reason']}"
                )
        
        logger.info("="*70)
        
        # Mark sync as completed
        await sync_state.complete_sync(
            neighbourhoods_synced=stats["neighbourhoods_synced"],
            neighbourhoods_failed=stats["neighbourhoods_failed"],
            neighbourhoods_no_boundary=stats["neighbourhoods_no_boundary"],
            total_neighbourhoods=stats["total_neighbourhoods"],
            forces_processed=stats["forces_processed"],
            forces_failed=stats["forces_failed"]
        )
        
    except Exception as e:
        logger.error(f"Sync failed with exception: {e}")
        await sync_state.fail_sync(str(e))
        raise
    finally:
        await police_client.close()


async def run_sync_async(db_client: DuckDBClient):
    """
    Run the sync operation asynchronously.
    
    Args:
        db_client: Connected DuckDB client
    """
    await sync_all_neighbourhoods(db_client)


def run_sync(db_client: DuckDBClient):
    """
    Run the sync operation (wrapper for use with APScheduler).
    
    Args:
        db_client: Connected DuckDB client
    """
    import asyncio
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(sync_all_neighbourhoods(db_client))
    finally:
        loop.close()
