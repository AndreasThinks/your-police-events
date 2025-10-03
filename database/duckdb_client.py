"""DuckDB client for spatial queries and neighbourhood storage."""
import duckdb
from typing import Optional, Tuple, List, Dict, Any
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class DuckDBClient:
    """Client for DuckDB spatial database operations."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        # Ensure data directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = None
    
    def connect(self):
        """Connect to DuckDB and initialize spatial extension."""
        self.conn = duckdb.connect(self.db_path)
        
        # Install and load spatial extension
        self.conn.execute("INSTALL spatial;")
        self.conn.execute("LOAD spatial;")
        
        logger.info(f"Connected to DuckDB at {self.db_path}")
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            logger.info("Closed DuckDB connection")
    
    def initialize_schema(self):
        """Create the neighbourhoods table if it doesn't exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS neighbourhoods (
                force_id VARCHAR,
                neighbourhood_id VARCHAR,
                name VARCHAR,
                force_url_slug VARCHAR,
                neighbourhood_url_slug VARCHAR,
                boundary GEOMETRY,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (force_id, neighbourhood_id)
            );
        """)
        
        # Create spatial index
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_boundary 
            ON neighbourhoods USING RTREE (boundary);
        """)
        
        # Create sync metadata table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS sync_metadata (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_sync_started TIMESTAMP,
                last_sync_completed TIMESTAMP,
                sync_status VARCHAR,
                total_forces INTEGER,
                forces_synced INTEGER,
                forces_failed INTEGER,
                total_neighbourhoods INTEGER,
                neighbourhoods_synced INTEGER,
                success_rate FLOAT,
                error_message VARCHAR,
                sync_duration_seconds INTEGER
            );
        """)
        
        # Create force sync status table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS force_sync_status (
                force_id VARCHAR PRIMARY KEY,
                force_name VARCHAR,
                last_sync_started TIMESTAMP,
                last_sync_completed TIMESTAMP,
                sync_status VARCHAR,
                neighbourhoods_expected INTEGER,
                neighbourhoods_synced INTEGER,
                error_message VARCHAR
            );
        """)
        
        logger.info("Database schema initialized")
    
    def insert_neighbourhood(
        self,
        force_id: str,
        neighbourhood_id: str,
        name: str,
        boundary_coords: List[Dict[str, str]],
        force_url_slug: Optional[str] = None,
        neighbourhood_url_slug: Optional[str] = None
    ):
        """
        Insert or update a neighbourhood with its boundary polygon.
        
        Args:
            force_id: Police force identifier
            neighbourhood_id: Neighbourhood identifier
            name: Neighbourhood name
            boundary_coords: List of dicts with 'latitude' and 'longitude' keys
            force_url_slug: URL-friendly force slug (optional)
            neighbourhood_url_slug: URL-friendly neighbourhood slug (optional)
        """
        if not boundary_coords:
            logger.warning(
                f"No boundary coordinates for {force_id}/{neighbourhood_id}"
            )
            return
        
        # Convert boundary coordinates to WKT POLYGON format
        # Police UK API returns lat/lng (WGS84)
        try:
            coords_str = ", ".join([
                f"{coord['longitude']} {coord['latitude']}"
                for coord in boundary_coords
            ])
        except (KeyError, TypeError) as e:
            logger.error(
                f"Invalid coordinate format for {force_id}/{neighbourhood_id}: {e}"
            )
            return
        
        # Close the polygon by adding first point at the end if needed
        first_coord = boundary_coords[0]
        last_coord = boundary_coords[-1]
        if (first_coord['latitude'] != last_coord['latitude'] or 
            first_coord['longitude'] != last_coord['longitude']):
            coords_str += f", {first_coord['longitude']} {first_coord['latitude']}"
        
        wkt = f"POLYGON(({coords_str}))"
        
        try:
            # First, validate the geometry to prevent segfaults
            validation_result = self.conn.execute("""
                SELECT ST_IsValid(ST_GeomFromText(?)) as is_valid
            """, [wkt]).fetchone()
            
            if not validation_result or not validation_result[0]:
                logger.warning(
                    f"Invalid geometry for {force_id}/{neighbourhood_id}. "
                    f"Attempting to fix with ST_MakeValid..."
                )
                
                # Try to fix the geometry
                try:
                    self.conn.execute("""
                        INSERT OR REPLACE INTO neighbourhoods 
                        (force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug, boundary, updated_at)
                        VALUES (?, ?, ?, ?, ?, ST_MakeValid(ST_GeomFromText(?)), CURRENT_TIMESTAMP)
                    """, [force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug, wkt])
                    logger.info(f"Fixed and inserted neighbourhood {force_id}/{neighbourhood_id}")
                    return
                except Exception as fix_error:
                    logger.error(
                        f"Could not fix geometry for {force_id}/{neighbourhood_id}: {fix_error}. Skipping."
                    )
                    return
            
            # Geometry is valid, insert normally
            self.conn.execute("""
                INSERT OR REPLACE INTO neighbourhoods 
                (force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug, boundary, updated_at)
                VALUES (?, ?, ?, ?, ?, ST_GeomFromText(?), CURRENT_TIMESTAMP)
            """, [force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug, wkt])
            
            logger.debug(f"Inserted neighbourhood {force_id}/{neighbourhood_id}")
            
        except Exception as e:
            logger.error(
                f"Error inserting neighbourhood {force_id}/{neighbourhood_id}: {type(e).__name__}: {e}. "
                f"This neighbourhood will be skipped."
            )
            # Don't raise - just skip this neighbourhood and continue
            return
    
    def find_neighbourhood_by_coords(
        self, longitude: float, latitude: float
    ) -> Optional[Tuple[str, str, str, str, str]]:
        """
        Find which neighbourhood contains the given coordinates.
        
        Args:
            longitude: Longitude in WGS84
            latitude: Latitude in WGS84
            
        Returns:
            Tuple of (force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug) or None if not found
        """
        try:
            result = self.conn.execute("""
                SELECT force_id, neighbourhood_id, name, force_url_slug, neighbourhood_url_slug
                FROM neighbourhoods
                WHERE ST_Contains(boundary, ST_Point(?, ?))
                LIMIT 1
            """, [longitude, latitude]).fetchone()
            
            if result:
                return result
            
            logger.info(f"No neighbourhood found for coords ({longitude}, {latitude})")
            return None
            
        except Exception as e:
            logger.error(f"Error finding neighbourhood by coords: {e}")
            raise
    
    def transform_bng_to_wgs84(
        self, easting: float, northing: float
    ) -> Tuple[float, float]:
        """
        Transform British National Grid coordinates to WGS84 (lng/lat).
        
        Args:
            easting: BNG easting (GEOMETRY_X)
            northing: BNG northing (GEOMETRY_Y)
            
        Returns:
            Tuple of (longitude, latitude) in WGS84
        """
        try:
            result = self.conn.execute("""
                SELECT 
                    ST_X(ST_Transform(ST_Point(?, ?), 'EPSG:27700', 'EPSG:4326')) as x,
                    ST_Y(ST_Transform(ST_Point(?, ?), 'EPSG:27700', 'EPSG:4326')) as y
            """, [easting, northing, easting, northing]).fetchone()
            
            # ST_Transform returns (lat, lng) but we need (lng, lat)
            # ST_X gives latitude, ST_Y gives longitude (swapped!)
            return (result[1], result[0])  # Return (longitude, latitude)
            
        except Exception as e:
            logger.error(f"Error transforming coordinates: {e}")
            raise
    
    def get_neighbourhood_count(self) -> int:
        """Get the total number of neighbourhoods in the database."""
        result = self.conn.execute(
            "SELECT COUNT(*) FROM neighbourhoods"
        ).fetchone()
        return result[0] if result else 0
    
    def clear_all_neighbourhoods(self):
        """Clear all neighbourhoods from the database."""
        self.conn.execute("DELETE FROM neighbourhoods")
        logger.info("Cleared all neighbourhoods from database")
    
    def get_database_stats(self) -> dict:
        """Get database statistics including size and counts."""
        import os
        
        stats = {
            "neighbourhoods": self.get_neighbourhood_count(),
            "forces": 0,
            "storage_mb": 0.0,
            "last_updated": None
        }
        
        # Get number of unique forces
        try:
            result = self.conn.execute(
                "SELECT COUNT(DISTINCT force_id) FROM neighbourhoods"
            ).fetchone()
            stats["forces"] = result[0] if result else 0
        except Exception as e:
            logger.error(f"Error getting force count: {e}")
        
        # Get last updated timestamp
        try:
            result = self.conn.execute(
                "SELECT MAX(updated_at) FROM neighbourhoods"
            ).fetchone()
            if result and result[0]:
                stats["last_updated"] = result[0].isoformat()
        except Exception as e:
            logger.error(f"Error getting last updated: {e}")
        
        # Get database file size
        try:
            if os.path.exists(self.db_path):
                size_bytes = os.path.getsize(self.db_path)
                stats["storage_mb"] = round(size_bytes / (1024 * 1024), 2)
        except Exception as e:
            logger.error(f"Error getting database size: {e}")
        
        return stats
    
    def save_sync_metadata(self, metadata: Dict[str, Any]):
        """Save sync metadata to database."""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO sync_metadata (
                    id, last_sync_started, last_sync_completed, sync_status,
                    total_forces, forces_synced, forces_failed,
                    total_neighbourhoods, neighbourhoods_synced,
                    success_rate, error_message, sync_duration_seconds
                ) VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                metadata.get('last_sync_started'),
                metadata.get('last_sync_completed'),
                metadata.get('sync_status'),
                metadata.get('total_forces'),
                metadata.get('forces_synced'),
                metadata.get('forces_failed'),
                metadata.get('total_neighbourhoods'),
                metadata.get('neighbourhoods_synced'),
                metadata.get('success_rate'),
                metadata.get('error_message'),
                metadata.get('sync_duration_seconds')
            ])
            logger.debug("Saved sync metadata")
        except Exception as e:
            logger.error(f"Error saving sync metadata: {e}")
            raise
    
    def get_sync_metadata(self) -> Optional[Dict[str, Any]]:
        """Retrieve last sync metadata."""
        try:
            result = self.conn.execute("""
                SELECT last_sync_started, last_sync_completed, sync_status,
                       total_forces, forces_synced, forces_failed,
                       total_neighbourhoods, neighbourhoods_synced,
                       success_rate, error_message, sync_duration_seconds
                FROM sync_metadata
                WHERE id = 1
            """).fetchone()
            
            if not result:
                return None
            
            return {
                'last_sync_started': result[0],
                'last_sync_completed': result[1],
                'sync_status': result[2],
                'total_forces': result[3],
                'forces_synced': result[4],
                'forces_failed': result[5],
                'total_neighbourhoods': result[6],
                'neighbourhoods_synced': result[7],
                'success_rate': result[8],
                'error_message': result[9],
                'sync_duration_seconds': result[10]
            }
        except Exception as e:
            logger.error(f"Error getting sync metadata: {e}")
            return None
    
    def update_force_status(self, force_id: str, force_name: str, status: Dict[str, Any]):
        """Update sync status for a specific force."""
        try:
            self.conn.execute("""
                INSERT OR REPLACE INTO force_sync_status (
                    force_id, force_name, last_sync_started, last_sync_completed,
                    sync_status, neighbourhoods_expected, neighbourhoods_synced,
                    error_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                force_id,
                force_name,
                status.get('last_sync_started'),
                status.get('last_sync_completed'),
                status.get('sync_status'),
                status.get('neighbourhoods_expected'),
                status.get('neighbourhoods_synced'),
                status.get('error_message')
            ])
            logger.debug(f"Updated force status for {force_id}")
        except Exception as e:
            logger.error(f"Error updating force status: {e}")
            raise
    
    def get_failed_forces(self) -> List[str]:
        """
        Get list of forces that failed in last sync or are stuck in running state.
        Includes forces with status 'failed', 'partial', or 'running' for > 2 hours.
        """
        try:
            from datetime import datetime, timedelta
            
            # Get forces that explicitly failed or partially succeeded
            results = self.conn.execute("""
                SELECT force_id
                FROM force_sync_status
                WHERE sync_status IN ('failed', 'partial')
                ORDER BY force_id
            """).fetchall()
            
            failed_forces = [row[0] for row in results]
            
            # Also get forces stuck in 'running' state for > 2 hours (stale locks)
            two_hours_ago = datetime.now() - timedelta(hours=2)
            stale_results = self.conn.execute("""
                SELECT force_id
                FROM force_sync_status
                WHERE sync_status = 'running'
                  AND last_sync_started < ?
                  AND (last_sync_completed IS NULL OR last_sync_completed < last_sync_started)
                ORDER BY force_id
            """, [two_hours_ago]).fetchall()
            
            stale_forces = [row[0] for row in stale_results]
            
            if stale_forces:
                logger.info(f"Found {len(stale_forces)} forces stuck in 'running' state: {stale_forces}")
            
            # Combine and deduplicate
            all_failed = list(set(failed_forces + stale_forces))
            return sorted(all_failed)
            
        except Exception as e:
            logger.error(f"Error getting failed forces: {e}")
            return []
    
    def get_force_status(self, force_id: str) -> Optional[Dict[str, Any]]:
        """Get sync status for a specific force."""
        try:
            result = self.conn.execute("""
                SELECT force_name, last_sync_started, last_sync_completed,
                       sync_status, neighbourhoods_expected, neighbourhoods_synced,
                       error_message
                FROM force_sync_status
                WHERE force_id = ?
            """, [force_id]).fetchone()
            
            if not result:
                return None
            
            return {
                'force_name': result[0],
                'last_sync_started': result[1],
                'last_sync_completed': result[2],
                'sync_status': result[3],
                'neighbourhoods_expected': result[4],
                'neighbourhoods_synced': result[5],
                'error_message': result[6]
            }
        except Exception as e:
            logger.error(f"Error getting force status: {e}")
            return None
