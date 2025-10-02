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
        
        logger.info("Database schema initialized")
    
    def insert_neighbourhood(
        self,
        force_id: str,
        neighbourhood_id: str,
        name: str,
        boundary_coords: List[Dict[str, str]]
    ):
        """
        Insert or update a neighbourhood with its boundary polygon.
        
        Args:
            force_id: Police force identifier
            neighbourhood_id: Neighbourhood identifier
            name: Neighbourhood name
            boundary_coords: List of dicts with 'latitude' and 'longitude' keys
        """
        if not boundary_coords:
            logger.warning(
                f"No boundary coordinates for {force_id}/{neighbourhood_id}"
            )
            return
        
        # Convert boundary coordinates to WKT POLYGON format
        # Police UK API returns lat/lng (WGS84)
        coords_str = ", ".join([
            f"{coord['longitude']} {coord['latitude']}"
            for coord in boundary_coords
        ])
        
        # Close the polygon by adding first point at the end if needed
        first_coord = boundary_coords[0]
        last_coord = boundary_coords[-1]
        if (first_coord['latitude'] != last_coord['latitude'] or 
            first_coord['longitude'] != last_coord['longitude']):
            coords_str += f", {first_coord['longitude']} {first_coord['latitude']}"
        
        wkt = f"POLYGON(({coords_str}))"
        
        try:
            # Use INSERT OR REPLACE to update if exists
            self.conn.execute("""
                INSERT OR REPLACE INTO neighbourhoods 
                (force_id, neighbourhood_id, name, boundary, updated_at)
                VALUES (?, ?, ?, ST_GeomFromText(?), CURRENT_TIMESTAMP)
            """, [force_id, neighbourhood_id, name, wkt])
            
            logger.debug(f"Inserted neighbourhood {force_id}/{neighbourhood_id}")
        except Exception as e:
            logger.error(
                f"Error inserting neighbourhood {force_id}/{neighbourhood_id}: {e}"
            )
            raise
    
    def find_neighbourhood_by_coords(
        self, longitude: float, latitude: float
    ) -> Optional[Tuple[str, str, str]]:
        """
        Find which neighbourhood contains the given coordinates.
        
        Args:
            longitude: Longitude in WGS84
            latitude: Latitude in WGS84
            
        Returns:
            Tuple of (force_id, neighbourhood_id, name) or None if not found
        """
        try:
            result = self.conn.execute("""
                SELECT force_id, neighbourhood_id, name
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
                    ST_X(ST_Transform(ST_Point(?, ?), 'EPSG:27700', 'EPSG:4326')) as lng,
                    ST_Y(ST_Transform(ST_Point(?, ?), 'EPSG:27700', 'EPSG:4326')) as lat
            """, [easting, northing, easting, northing]).fetchone()
            
            # Return (longitude, latitude)
            return (result[0], result[1])
            
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
