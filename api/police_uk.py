"""Client for the Police UK API."""
import httpx
from typing import List, Dict, Any
import logging

logger = logging.getLogger(__name__)

BASE_URL = "https://data.police.uk/api"


class PoliceUKClient:
    """Client for interacting with the Police UK API."""
    
    def __init__(self):
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
    
    async def get_forces(self) -> List[Dict[str, str]]:
        """
        Get list of all police forces.
        
        Returns:
            List of dicts with 'id' and 'name' keys
        """
        try:
            response = await self.client.get(f"{BASE_URL}/forces")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching forces: {e}")
            raise
    
    async def get_neighbourhoods(self, force_id: str) -> List[Dict[str, str]]:
        """
        Get list of neighbourhoods for a specific force.
        
        Args:
            force_id: Police force identifier
            
        Returns:
            List of dicts with 'id' and 'name' keys
        """
        try:
            response = await self.client.get(f"{BASE_URL}/{force_id}/neighbourhoods")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching neighbourhoods for {force_id}: {e}")
            raise
    
    async def get_neighbourhood_boundary(
        self, force_id: str, neighbourhood_id: str
    ) -> List[Dict[str, str]]:
        """
        Get boundary coordinates for a neighbourhood.
        
        Args:
            force_id: Police force identifier
            neighbourhood_id: Neighbourhood identifier
            
        Returns:
            List of dicts with 'latitude' and 'longitude' keys
        """
        try:
            response = await self.client.get(
                f"{BASE_URL}/{force_id}/{neighbourhood_id}/boundary"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(
                f"Error fetching boundary for {force_id}/{neighbourhood_id}: {e}"
            )
            raise
    
    async def get_neighbourhood_events(
        self, force_id: str, neighbourhood_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get events for a neighbourhood.
        
        Args:
            force_id: Police force identifier
            neighbourhood_id: Neighbourhood identifier
            
        Returns:
            List of event dicts with details like title, description, dates, etc.
        """
        try:
            response = await self.client.get(
                f"{BASE_URL}/{force_id}/{neighbourhood_id}/events"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(
                f"Error fetching events for {force_id}/{neighbourhood_id}: {e}"
            )
            # Return empty list if no events or error
            return []
