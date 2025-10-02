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
from middleware.rate_limit import limiter, setup_rate_limiting
from middleware.monitoring import setup_monitoring
from utils.error_messages import (
    validate_uk_postcode,
    get_postcode_not_found_message,
    get_neighbourhood_not_found_message,
    get_api_error_message,
    suggest_postcode_corrections
)

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
    
    # Only run initial sync if explicitly enabled (to avoid deployment timeouts)
    initial_sync = os.getenv("INITIAL_SYNC", "false").lower() == "true"
    
    if neighbourhood_count == 0 and initial_sync:
        logger.info("No neighbourhoods in database, running initial sync...")
        await run_sync_async(db_client)
    elif neighbourhood_count == 0:
        logger.warning(
            "Database is empty but INITIAL_SYNC is not enabled. "
            "Use POST /admin/sync to populate the database."
        )
    
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

# Set up middleware
setup_rate_limiting(app)
setup_monitoring()

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")


# Pydantic models
class PostcodeLookupRequest(BaseModel):
    postcode: str


class EventPreview(BaseModel):
    title: str
    start_date: str
    end_date: str
    address: str
    description: str


class PostcodeLookupResponse(BaseModel):
    force_id: str
    neighbourhood_id: str
    neighbourhood_name: str
    calendar_url: str
    event_count: int
    preview_events: list[EventPreview]


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
@limiter.limit("10/minute")
async def lookup_postcode(request: PostcodeLookupRequest, http_request: Request):
    """
    Look up a postcode and return the calendar URL for that neighbourhood.
    Rate limited to 10 requests per minute per IP.
    """
    postcode = request.postcode.strip()
    
    if not postcode:
        raise HTTPException(status_code=400, detail="Postcode is required")
    
    # Validate postcode format
    is_valid, error_msg = validate_uk_postcode(postcode)
    if not is_valid:
        suggestions = suggest_postcode_corrections(postcode)
        detail = error_msg
        if suggestions:
            detail += f"\n\nDid you mean: {', '.join(suggestions)}?"
        raise HTTPException(status_code=400, detail=detail)
    
    try:
        # Find neighbourhood for postcode
        result = await location_service.find_neighbourhood_by_postcode(postcode)
        
        if not result:
            # Provide helpful error message
            detail = get_neighbourhood_not_found_message(postcode)
            raise HTTPException(status_code=404, detail=detail)
        
        force_id, neighbourhood_id, neighbourhood_name = result
        
        # Generate calendar URL
        base_url = str(http_request.base_url).rstrip('/')
        calendar_url = f"{base_url}/calendar/{force_id}/{neighbourhood_id}.ics"
        
        # Fetch events for preview
        try:
            events = await police_client.get_neighbourhood_events(force_id, neighbourhood_id)
            
            # Get next 3 upcoming events
            from datetime import datetime
            now = datetime.now()
            upcoming_events = []
            
            for event in events:
                try:
                    start_date = datetime.fromisoformat(event['start_date'].replace('Z', '+00:00'))
                    if start_date >= now:
                        upcoming_events.append(EventPreview(
                            title=event.get('title', 'Untitled Event'),
                            start_date=event.get('start_date', ''),
                            end_date=event.get('end_date', ''),
                            address=event.get('address', ''),
                            description=event.get('description', '')
                        ))
                except (ValueError, KeyError):
                    continue
            
            # Sort by start date and take first 3
            upcoming_events.sort(key=lambda e: e.start_date)
            preview_events = upcoming_events[:3]
            event_count = len(upcoming_events)
            
        except Exception as e:
            logger.warning(f"Could not fetch events for preview: {e}")
            preview_events = []
            event_count = 0
        
        return PostcodeLookupResponse(
            force_id=force_id,
            neighbourhood_id=neighbourhood_id,
            neighbourhood_name=neighbourhood_name,
            calendar_url=calendar_url,
            event_count=event_count,
            preview_events=preview_events
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error looking up postcode {postcode}: {e}")
        detail = get_api_error_message()
        raise HTTPException(status_code=500, detail=detail)


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


@app.get("/admin/status")
async def get_status():
    """
    Get system status and statistics.
    """
    neighbourhood_count = db_client.get_neighbourhood_count() if db_client else 0
    
    # Get cache statistics
    cache_size = len(calendar_cache)
    cache_max = calendar_cache.maxsize
    
    # Get postcode cache size if available
    postcode_cache_size = len(location_service._postcode_cache) if location_service else 0
    
    return {
        "status": "operational",
        "database": {
            "neighbourhoods": neighbourhood_count,
            "path": os.getenv("DATABASE_PATH", "./data/police_events.duckdb")
        },
        "cache": {
            "calendar_feeds": {
                "size": cache_size,
                "max_size": cache_max,
                "ttl_hours": cache_ttl_hours
            },
            "postcode_lookups": {
                "size": postcode_cache_size
            }
        },
        "scheduler": {
            "active": scheduler is not None and scheduler.running if scheduler else False,
            "next_sync": "Weekly (every 7 days)"
        }
    }


@app.post("/admin/sync")
@limiter.limit("1/hour")
async def trigger_sync(request: Request):
    """
    Manually trigger a neighbourhood sync (admin endpoint).
    Rate limited to 1 request per hour.
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
