"""Main FastAPI application for local police events calendar service."""
import os
import logging
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
from cachetools import TTLCache

from api.police_uk import PoliceUKClient
from api.ordnance_survey import OrdnanceSurveyClient
from database.duckdb_client import DuckDBClient
from database.sync import run_sync, run_sync_async
from services.location import LocationService
from services.calendar import CalendarService

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global instances
db_client: Optional[DuckDBClient] = None
os_client: Optional[OrdnanceSurveyClient] = None
police_client: Optional[PoliceUKClient] = None
location_service: Optional[LocationService] = None
calendar_service: Optional[CalendarService] = None
scheduler: Optional[BackgroundScheduler] = None

# Cache for calendar feeds (key: "force_id:neighbourhood_id", value: ics bytes)
cache_ttl_hours = int(os.getenv("CACHE_TTL_HOURS", "3"))
calendar_cache = TTLCache(maxsize=1000, ttl=cache_ttl_hours * 3600)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for startup and shutdown."""
    global db_client, os_client, police_client, location_service, calendar_service, scheduler
    
    logger.info("Starting up application...")
    
    # Initialize database
    db_path = os.getenv("DATABASE_PATH", "./data/police_events.duckdb")
    db_client = DuckDBClient(db_path)
    db_client.connect()
    db_client.initialize_schema()
    
    # Check if we need initial sync
    neighbourhood_count = db_client.get_neighbourhood_count()
    logger.info(f"Database has {neighbourhood_count} neighbourhoods")
    
    if neighbourhood_count == 0:
        logger.info("No neighbourhoods in database, running initial sync...")
        await run_sync_async(db_client)
    
    # Initialize API clients
    os_api_key = os.getenv("OS_NAMES_API_KEY")
    if not os_api_key or os_api_key == "your_api_key_here":
        logger.warning("OS_NAMES_API_KEY not set! Postcode lookup will not work.")
    
    os_client = OrdnanceSurveyClient(os_api_key)
    police_client = PoliceUKClient()
    
    # Initialize services
    location_service = LocationService(os_client, db_client)
    calendar_service = CalendarService(police_client)
    
    # Set up weekly sync scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=lambda: run_sync(db_client),
        trigger="interval",
        weeks=1,
        id="weekly_sync",
        name="Weekly neighbourhood boundary sync"
    )
    scheduler.start()
    logger.info("Scheduled weekly sync job")
    
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    if scheduler:
        scheduler.shutdown()
    
    if os_client:
        await os_client.close()
    
    if police_client:
        await police_client.close()
    
    if db_client:
        db_client.close()
    
    logger.info("Application shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Local Police Events Calendar",
    description="Subscribe to your local police neighbourhood events as a calendar feed",
    version="1.0.0",
    lifespan=lifespan
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


# Pydantic models
class PostcodeLookupRequest(BaseModel):
    postcode: str


class PostcodeLookupResponse(BaseModel):
    force_id: str
    neighbourhood_id: str
    neighbourhood_name: str
    calendar_url: str


@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main HTML page."""
    with open("static/index.html", "r") as f:
        return f.read()


@app.get("/health")
async def health_check():
    """Health check endpoint for Railway."""
    neighbourhood_count = db_client.get_neighbourhood_count()
    return {
        "status": "healthy",
        "neighbourhoods": neighbourhood_count
    }


@app.post("/lookup", response_model=PostcodeLookupResponse)
async def lookup_postcode(request: PostcodeLookupRequest, http_request: Request):
    """
    Look up a postcode and return the calendar URL for that neighbourhood.
    """
    postcode = request.postcode.strip()
    
    if not postcode:
        raise HTTPException(status_code=400, detail="Postcode is required")
    
    try:
        # Find neighbourhood for postcode
        result = await location_service.find_neighbourhood_by_postcode(postcode)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"No police neighbourhood found for postcode {postcode}"
            )
        
        force_id, neighbourhood_id, neighbourhood_name = result
        
        # Generate calendar URL
        base_url = str(http_request.base_url).rstrip('/')
        calendar_url = f"{base_url}/calendar/{force_id}/{neighbourhood_id}.ics"
        
        return PostcodeLookupResponse(
            force_id=force_id,
            neighbourhood_id=neighbourhood_id,
            neighbourhood_name=neighbourhood_name,
            calendar_url=calendar_url
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error looking up postcode {postcode}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/calendar/{force_id}/{neighbourhood_id}.ics")
async def get_calendar(force_id: str, neighbourhood_id: str):
    """
    Get the iCalendar feed for a specific neighbourhood.
    """
    cache_key = f"{force_id}:{neighbourhood_id}"
    
    # Check cache
    if cache_key in calendar_cache:
        logger.info(f"Serving cached calendar for {cache_key}")
        ics_data = calendar_cache[cache_key]
    else:
        try:
            # Generate calendar
            logger.info(f"Generating calendar for {cache_key}")
            ics_data = await calendar_service.generate_ics_feed(force_id, neighbourhood_id)
            
            # Cache it
            calendar_cache[cache_key] = ics_data
            
        except Exception as e:
            logger.error(f"Error generating calendar for {cache_key}: {e}")
            raise HTTPException(status_code=500, detail="Error generating calendar")
    
    # Return as iCalendar file
    return Response(
        content=ics_data,
        media_type="text/calendar",
        headers={
            "Content-Disposition": f"attachment; filename={force_id}_{neighbourhood_id}.ics"
        }
    )


@app.post("/admin/sync")
async def trigger_sync():
    """
    Manually trigger a neighbourhood sync (admin endpoint).
    """
    try:
        logger.info("Manual sync triggered")
        run_sync(db_client)
        neighbourhood_count = db_client.get_neighbourhood_count()
        return {
            "status": "success",
            "message": f"Sync completed. {neighbourhood_count} neighbourhoods in database."
        }
    except Exception as e:
        logger.error(f"Error during manual sync: {e}")
        raise HTTPException(status_code=500, detail="Sync failed")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
