"""Client for the Ordnance Survey Names API."""
import httpx
from typing import Optional, Dict, Any
import logging
import os

logger = logging.getLogger(__name__)

BASE_URL = "https://api.os.uk/search/names/v1"


class OrdnanceSurveyClient:
    """Client for interacting with the OS Names API."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = httpx.AsyncClient(timeout=30.0)
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
    
    async def find_postcode(self, postcode: str) -> Optional[Dict[str, Any]]:
        """
        Find coordinates for a postcode using OS Names API.
        
        Args:
            postcode: UK postcode (e.g., "SW1A 1AA")
            
        Returns:
            Dict with postcode info including GEOMETRY_X, GEOMETRY_Y (BNG coordinates)
            or None if not found
        """
        # Clean postcode (remove spaces for API call)
        clean_postcode = postcode.replace(" ", "").upper()
        
        try:
            response = await self.client.get(
                f"{BASE_URL}/find",
                params={
                    "query": clean_postcode,
                    "key": self.api_key,
                    "fq": "LOCAL_TYPE:Postcode",
                    "maxresults": 1
                }
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("results") and len(data["results"]) > 0:
                result = data["results"][0]
                return {
                    "name": result.get("NAME1"),
                    "geometry_x": result.get("GEOMETRY_X"),
                    "geometry_y": result.get("GEOMETRY_Y"),
                    "postcode_district": result.get("POSTCODE_DISTRICT"),
                    "populated_place": result.get("POPULATED_PLACE"),
                    "county": result.get("COUNTY_UNITARY"),
                    "country": result.get("COUNTRY")
                }
            
            logger.warning(f"No results found for postcode: {postcode}")
            return None
            
        except Exception as e:
            logger.error(f"Error finding postcode {postcode}: {e}")
            raise
