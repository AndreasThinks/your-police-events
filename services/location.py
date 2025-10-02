"""Location service for postcode lookup and neighbourhood finding."""
import logging
from typing import Optional, Tuple
from api.ordnance_survey import OrdnanceSurveyClient
from database.duckdb_client import DuckDBClient

logger = logging.getLogger(__name__)


class LocationService:
    """Service for converting postcodes to neighbourhoods."""
    
    def __init__(self, os_client: OrdnanceSurveyClient, db_client: DuckDBClient):
        self.os_client = os_client
        self.db_client = db_client
    
    async def find_neighbourhood_by_postcode(
        self, postcode: str
    ) -> Optional[Tuple[str, str, str]]:
        """
        Find the police neighbourhood for a given postcode.
        
        Args:
            postcode: UK postcode (e.g., "SW1A 1AA")
            
        Returns:
            Tuple of (force_id, neighbourhood_id, neighbourhood_name) or None
        """
        try:
            # Step 1: Get BNG coordinates from OS Names API
            postcode_data = await self.os_client.find_postcode(postcode)
            
            if not postcode_data:
                logger.warning(f"Postcode not found: {postcode}")
                return None
            
            geometry_x = postcode_data['geometry_x']
            geometry_y = postcode_data['geometry_y']
            
            if not geometry_x or not geometry_y:
                logger.error(f"No coordinates for postcode: {postcode}")
                return None
            
            logger.info(
                f"Found BNG coordinates for {postcode}: "
                f"({geometry_x}, {geometry_y})"
            )
            
            # Step 2: Transform BNG to WGS84
            longitude, latitude = self.db_client.transform_bng_to_wgs84(
                geometry_x, geometry_y
            )
            
            logger.info(
                f"Transformed to WGS84: ({longitude}, {latitude})"
            )
            
            # Step 3: Find neighbourhood containing these coordinates
            neighbourhood = self.db_client.find_neighbourhood_by_coords(
                longitude, latitude
            )
            
            if neighbourhood:
                force_id, neighbourhood_id, name = neighbourhood
                logger.info(
                    f"Found neighbourhood: {name} ({force_id}/{neighbourhood_id})"
                )
                return neighbourhood
            else:
                logger.warning(
                    f"No neighbourhood found for postcode {postcode} "
                    f"at coords ({longitude}, {latitude})"
                )
                return None
                
        except Exception as e:
            logger.error(f"Error finding neighbourhood for postcode {postcode}: {e}")
            raise
    
    def find_neighbourhood_by_coords(
        self, longitude: float, latitude: float
    ) -> Optional[Tuple[str, str, str]]:
        """
        Find the police neighbourhood for given WGS84 coordinates.
        
        Args:
            longitude: Longitude in WGS84
            latitude: Latitude in WGS84
            
        Returns:
            Tuple of (force_id, neighbourhood_id, neighbourhood_name) or None
        """
        try:
            neighbourhood = self.db_client.find_neighbourhood_by_coords(
                longitude, latitude
            )
            
            if neighbourhood:
                force_id, neighbourhood_id, name = neighbourhood
                logger.info(
                    f"Found neighbourhood: {name} ({force_id}/{neighbourhood_id})"
                )
            else:
                logger.warning(
                    f"No neighbourhood found at coords ({longitude}, {latitude})"
                )
            
            return neighbourhood
            
        except Exception as e:
            logger.error(
                f"Error finding neighbourhood for coords ({longitude}, {latitude}): {e}"
            )
            raise
