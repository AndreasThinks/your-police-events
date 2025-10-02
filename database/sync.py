"""Weekly data synchronization job for neighbourhood boundaries."""
import asyncio
import logging
from typing import List
from api.police_uk import PoliceUKClient
from database.duckdb_client import DuckDBClient

logger = logging.getLogger(__name__)


async def sync_all_neighbourhoods(db_client: DuckDBClient):
    """
    Sync all police force neighbourhoods and their boundaries to the database.
    
    This is a long-running operation that fetches data for all forces and
    neighbourhoods in the UK.
    
    Args:
        db_client: Connected DuckDB client
    """
    police_client = PoliceUKClient()
    
    try:
        logger.info("Starting neighbourhood sync...")
        
        # Get all forces
        forces = await police_client.get_forces()
        logger.info(f"Found {len(forces)} police forces")
        
        total_neighbourhoods = 0
        successful_syncs = 0
        
        for force in forces:
            force_id = force['id']
            force_name = force['name']
            
            try:
                # Get neighbourhoods for this force
                neighbourhoods = await police_client.get_neighbourhoods(force_id)
                logger.info(
                    f"Processing {len(neighbourhoods)} neighbourhoods for {force_name}"
                )
                
                for neighbourhood in neighbourhoods:
                    neighbourhood_id = neighbourhood['id']
                    neighbourhood_name = neighbourhood['name']
                    total_neighbourhoods += 1
                    
                    try:
                        # Get boundary for this neighbourhood
                        boundary = await police_client.get_neighbourhood_boundary(
                            force_id, neighbourhood_id
                        )
                        
                        if boundary:
                            # Insert into database
                            db_client.insert_neighbourhood(
                                force_id=force_id,
                                neighbourhood_id=neighbourhood_id,
                                name=neighbourhood_name,
                                boundary_coords=boundary
                            )
                            successful_syncs += 1
                        else:
                            logger.warning(
                                f"No boundary data for {force_id}/{neighbourhood_id}"
                            )
                        
                        # Small delay to avoid overwhelming the API
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(
                            f"Error syncing {force_id}/{neighbourhood_id}: {e}"
                        )
                        continue
                
            except Exception as e:
                logger.error(f"Error processing force {force_id}: {e}")
                continue
        
        logger.info(
            f"Sync complete: {successful_syncs}/{total_neighbourhoods} "
            f"neighbourhoods synced successfully"
        )
        
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
