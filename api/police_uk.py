"""Client for the Police UK API."""
import httpx
from typing import List, Dict, Any, Optional
import logging
import asyncio

logger = logging.getLogger(__name__)

BASE_URL = "https://data.police.uk/api"


class PoliceUKClient:
    """Client for interacting with the Police UK API."""
    
    def __init__(self, timeout: float = 60.0, max_retries: int = 3):
        """
        Initialize the Police UK API client.
        
        Args:
            timeout: Request timeout in seconds (default: 60s)
            max_retries: Maximum number of retry attempts for failed requests (default: 3)
        """
        self.client = httpx.AsyncClient(timeout=timeout)
        self.max_retries = max_retries
    
    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
    
    async def _make_request_with_retry(
        self, 
        url: str, 
        operation_name: str
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Make an HTTP request with retry logic.
        
        Args:
            url: The URL to request
            operation_name: Description of the operation for logging
            
        Returns:
            JSON response as list of dicts, or None if all retries failed
        """
        last_error = None
        
        for attempt in range(self.max_retries):
            try:
                response = await self.client.get(url)
                
                # Check for temporary server errors that should be retried
                if response.status_code in [502, 503, 504]:
                    if attempt < self.max_retries - 1:
                        delay = 0.5 * (2 ** attempt)  # Exponential backoff: 0.5s, 1s, 2s
                        logger.warning(
                            f"{operation_name}: HTTP {response.status_code}, "
                            f"retrying in {delay}s (attempt {attempt + 1}/{self.max_retries})"
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        logger.error(
                            f"{operation_name}: HTTP {response.status_code} after "
                            f"{self.max_retries} attempts"
                        )
                        return None
                
                # Raise for other HTTP errors
                response.raise_for_status()
                return response.json()
                
            except httpx.TimeoutException as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = 0.5 * (2 ** attempt)
                    logger.warning(
                        f"{operation_name}: Timeout, retrying in {delay}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"{operation_name}: Timeout after {self.max_retries} attempts")
                    return None
                    
            except httpx.HTTPStatusError as e:
                # Don't retry 4xx errors (client errors)
                if 400 <= e.response.status_code < 500:
                    logger.error(f"{operation_name}: HTTP {e.response.status_code} - {e}")
                    return None
                
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = 0.5 * (2 ** attempt)
                    logger.warning(
                        f"{operation_name}: HTTP error, retrying in {delay}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"{operation_name}: Failed after {self.max_retries} attempts - {e}")
                    return None
                    
            except Exception as e:
                last_error = e
                if attempt < self.max_retries - 1:
                    delay = 0.5 * (2 ** attempt)
                    logger.warning(
                        f"{operation_name}: Error {type(e).__name__}, retrying in {delay}s "
                        f"(attempt {attempt + 1}/{self.max_retries})"
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(
                        f"{operation_name}: Failed after {self.max_retries} attempts - "
                        f"{type(e).__name__}: {e}"
                    )
                    return None
        
        return None
    
    async def get_forces(self) -> List[Dict[str, str]]:
        """
        Get list of all police forces.
        
        Returns:
            List of dicts with 'id' and 'name' keys, or empty list on failure
        """
        result = await self._make_request_with_retry(
            f"{BASE_URL}/forces",
            "Fetching forces"
        )
        return result if result is not None else []
    
    async def get_neighbourhoods(self, force_id: str) -> List[Dict[str, str]]:
        """
        Get list of neighbourhoods for a specific force.
        
        Args:
            force_id: Police force identifier
            
        Returns:
            List of dicts with 'id' and 'name' keys, or empty list on failure
        """
        result = await self._make_request_with_retry(
            f"{BASE_URL}/{force_id}/neighbourhoods",
            f"Fetching neighbourhoods for {force_id}"
        )
        return result if result is not None else []
    
    async def get_neighbourhood_details(
        self, force_id: str, neighbourhood_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a neighbourhood including URL slugs.
        
        Args:
            force_id: Police force identifier
            neighbourhood_id: Neighbourhood identifier
            
        Returns:
            Dict with neighbourhood details including 'url_force' field, or None on failure
        """
        try:
            response = await self.client.get(f"{BASE_URL}/{force_id}/{neighbourhood_id}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching details for {force_id}/{neighbourhood_id}: {e}")
            return None
    
    async def get_neighbourhood_boundary(
        self, force_id: str, neighbourhood_id: str
    ) -> List[Dict[str, str]]:
        """
        Get boundary coordinates for a neighbourhood.
        
        Args:
            force_id: Police force identifier
            neighbourhood_id: Neighbourhood identifier
            
        Returns:
            List of dicts with 'latitude' and 'longitude' keys, or empty list on failure
        """
        result = await self._make_request_with_retry(
            f"{BASE_URL}/{force_id}/{neighbourhood_id}/boundary",
            f"Fetching boundary for {force_id}/{neighbourhood_id}"
        )
        return result if result is not None else []
    
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
        result = await self._make_request_with_retry(
            f"{BASE_URL}/{force_id}/{neighbourhood_id}/events",
            f"Fetching events for {force_id}/{neighbourhood_id}"
        )
        return result if result is not None else []
