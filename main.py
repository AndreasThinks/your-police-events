"""Main FastAPI application for local police events calendar service."""
import os
import sys
import json
import logging
from contextlib import asynccontextmanager
from datetime import timedelta, datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from concurrent.futures import ProcessPoolExecutor
from dotenv import load_dotenv
from cachetools import TTLCache
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from api.police_uk import PoliceUKClient
from api.ordnance_survey import OrdnanceSurveyClient
from database.duckdb_client import DuckDBClient
from database.sync import run_sync, run_sync_async, run_sync_with_recovery
from database.sync_strategy import determine_sync_strategy
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


# Structured JSON logging formatter for Railway
class StructuredFormatter(logging.Formatter):
    """
    Custom formatter that outputs logs in JSON format for Railway.
    Railway automatically parses JSON logs and provides better filtering.
    """
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "message": record.getMessage(),
            "level": record.levelname.lower(),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "logger": record.name,
        }
        
        # Add exception info if present
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        
        # Add any extra fields from the log record
        if hasattr(record, "extra_fields"):
            log_data.update(record.extra_fields)
        
        result = json.dumps(log_data)
        
        # Force flush to ensure logs appear in Railway immediately
        sys.stdout.flush()
        
        return result


# Configure structured logging
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(StructuredFormatter())

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    handlers=[handler]
)

logger = logging.getLogger(__name__)

# Global instances
db_client: Optional[DuckDBClient] = None
os_client: Optional[OrdnanceSurveyClient] = None
police_client: Optional[PoliceUKClient] = None
location_service: Optional[LocationService] = None
calendar_service: Optional[CalendarService] = None
executor: Optional[ProcessPoolExecutor] = None
scheduler: Optional[AsyncIOScheduler] = None

# Cache for calendar feeds (key: "force_id:neighbourhood_id", value: ics bytes)
cache_ttl_hours = int(os.getenv("CACHE_TTL_HOURS", "3"))
calendar_cache = TTLCache(maxsize=1000, ttl=cache_ttl_hours * 3600)

# Sync configuration
sync_interval_days = int(os.getenv("SYNC_INTERVAL_DAYS", "7"))


async def scheduled_sync_job():
    """Background job to sync neighbourhood data periodically."""
    from database.sync_state import sync_state
    
    try:
        logger.info(f"Starting scheduled sync (runs every {sync_interval_days} days)")
        await run_sync_async(db_client)
        
        # Calculate and set next sync time
        next_sync = datetime.now() + timedelta(days=sync_interval_days)
        await sync_state.set_next_sync(next_sync)
        
        logger.info(f"Scheduled sync completed. Next sync at: {next_sync.isoformat()}")
    except Exception as e:
        logger.error(f"Error in scheduled sync: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for startup and shutdown."""
    global db_client, os_client, police_client, location_service, calendar_service, executor, scheduler
    
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
    
    # Set up process pool executor for sync jobs
    # Using max_workers=1 to ensure only one sync runs at a time
    executor = ProcessPoolExecutor(max_workers=1)
    
    logger.info("Process pool executor initialized for sync jobs")
    
    # Initialize scheduler for automatic syncs
    from database.sync_state import sync_state
    
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        scheduled_sync_job,
        IntervalTrigger(days=sync_interval_days),
        id='neighbourhood_sync',
        name='Sync neighbourhood data',
        replace_existing=True
    )
    scheduler.start()
    logger.info(f"Scheduler started - will sync every {sync_interval_days} days")
    
    # Set initial next sync time
    next_sync = datetime.now() + timedelta(days=sync_interval_days)
    await sync_state.set_next_sync(next_sync)
    logger.info(f"Next scheduled sync: {next_sync.isoformat()}")
    
    # Determine smart sync strategy based on database state
    if not initial_sync:
        strategy = determine_sync_strategy(db_client)
        logger.info(f"Sync strategy: {strategy}")
        
        if strategy.sync_type == "full":
            logger.info(f"Will run full sync in background: {strategy.reason}")
            
        elif strategy.sync_type == "recovery":
            logger.info(
                f"Recovery sync needed for {len(strategy.force_ids)} forces: {strategy.reason}"
            )
            
        else:  # skip
            logger.info(f"No startup sync needed: {strategy.reason}")
    
    logger.info("Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application...")
    
    if scheduler:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler shut down")
    
    if executor:
        executor.shutdown(wait=True)
        logger.info("Process pool executor shut down")
    
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


class CoordinateLookupRequest(BaseModel):
    latitude: float
    longitude: float


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
async def lookup_postcode(postcode_request: PostcodeLookupRequest, request: Request):
    """
    Look up a postcode and return the calendar URL for that neighbourhood.
    Rate limited to 10 requests per minute per IP.
    """
    postcode = postcode_request.postcode.strip()
    
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
        base_url = str(request.base_url).rstrip('/')
        calendar_url = f"{base_url}/calendar/{force_id}/{neighbourhood_id}.ics"
        
        # Fetch events for preview - wrap in try/except to ensure we always return valid response
        preview_events = []
        event_count = 0
        
        try:
            events = await police_client.get_neighbourhood_events(force_id, neighbourhood_id)
            
            if events:
                # Get next 3 upcoming events
                from datetime import datetime
                now = datetime.now()
                upcoming_events = []
                
                for event in events:
                    try:
                        start_date_str = event.get('start_date', '')
                        if not start_date_str:
                            continue
                        
                        # Parse date and check if it's in the future
                        start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                        if start_date >= now:
                            upcoming_events.append(EventPreview(
                                title=event.get('title', 'Untitled Event'),
                                start_date=start_date_str,
                                end_date=event.get('end_date', ''),
                                address=event.get('address', ''),
                                description=event.get('description', '')[:200] if event.get('description') else ''
                            ))
                    except (ValueError, KeyError, TypeError) as parse_error:
                        logger.debug(f"Skipping event due to parsing error: {parse_error}")
                        continue
                
                # Sort by start date and take first 3
                if upcoming_events:
                    upcoming_events.sort(key=lambda e: e.start_date)
                    preview_events = upcoming_events[:3]
                    event_count = len(upcoming_events)
                    
        except Exception as e:
            # Log but don't fail the request
            logger.warning(f"Could not fetch events for preview: {e}")
        
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


@app.post("/lookup-coords", response_model=PostcodeLookupResponse)
@limiter.limit("10/minute")
async def lookup_coordinates(coords_request: CoordinateLookupRequest, request: Request):
    """
    Look up coordinates and return the calendar URL for that neighbourhood.
    Rate limited to 10 requests per minute per IP.
    """
    latitude = coords_request.latitude
    longitude = coords_request.longitude
    
    # Validate coordinates are in UK range
    if not (49.0 <= latitude <= 61.0 and -8.0 <= longitude <= 2.0):
        raise HTTPException(
            status_code=400, 
            detail="Coordinates are outside UK boundaries"
        )
    
    try:
        # Find neighbourhood by coordinates
        result = location_service.find_neighbourhood_by_coords(longitude, latitude)
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail="No police neighbourhood found at these coordinates. The area may not have neighbourhood policing data available."
            )
        
        force_id, neighbourhood_id, neighbourhood_name = result
        
        # Generate calendar URL
        base_url = str(request.base_url).rstrip('/')
        calendar_url = f"{base_url}/calendar/{force_id}/{neighbourhood_id}.ics"
        
        # Fetch events for preview
        preview_events = []
        event_count = 0
        
        try:
            events = await police_client.get_neighbourhood_events(force_id, neighbourhood_id)
            
            if events:
                from datetime import datetime
                now = datetime.now()
                upcoming_events = []
                
                for event in events:
                    try:
                        start_date_str = event.get('start_date', '')
                        if not start_date_str:
                            continue
                        
                        start_date = datetime.fromisoformat(start_date_str.replace('Z', '+00:00'))
                        if start_date >= now:
                            upcoming_events.append(EventPreview(
                                title=event.get('title', 'Untitled Event'),
                                start_date=start_date_str,
                                end_date=event.get('end_date', ''),
                                address=event.get('address', ''),
                                description=event.get('description', '')[:200] if event.get('description') else ''
                            ))
                    except (ValueError, KeyError, TypeError) as parse_error:
                        logger.debug(f"Skipping event due to parsing error: {parse_error}")
                        continue
                
                if upcoming_events:
                    upcoming_events.sort(key=lambda e: e.start_date)
                    preview_events = upcoming_events[:3]
                    event_count = len(upcoming_events)
                    
        except Exception as e:
            logger.warning(f"Could not fetch events for preview: {e}")
        
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
        logger.error(f"Error looking up coordinates ({latitude}, {longitude}): {e}")
        raise HTTPException(status_code=500, detail="Error processing coordinates")


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
    Get comprehensive system status including sync progress and database statistics.
    """
    from database.sync_state import sync_state
    
    # Get database statistics
    db_stats = db_client.get_database_stats() if db_client else {
        "neighbourhoods": 0,
        "forces": 0,
        "storage_mb": 0.0,
        "last_updated": None
    }
    
    # Get cache statistics
    cache_size = len(calendar_cache)
    cache_max = calendar_cache.maxsize
    postcode_cache_size = len(location_service._postcode_cache) if location_service else 0
    
    # Get sync state
    sync_info = await sync_state.get_state()
    
    return {
        "status": "operational",
        "database": db_stats,
        "sync": sync_info,
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
            "active": executor is not None,
            "next_sync": "Manual trigger via /admin/sync endpoint"
        }
    }


@app.get("/api/sync-status")
async def get_sync_status():
    """
    Get sync status for display on frontend (last updated, next update).
    Returns timestamps that can be used to calculate relative times.
    """
    from database.sync_state import sync_state
    
    sync_info = await sync_state.get_state()
    
    last_sync = None
    next_sync = None
    
    if sync_info.get("last_sync"):
        last_sync = sync_info["last_sync"].get("completed_at")
        next_sync = sync_info["last_sync"].get("next_sync_at")
    
    return {
        "last_updated": last_sync,
        "next_update": next_sync
    }


@app.post("/admin/sync")
@limiter.limit("1/hour")
async def trigger_sync(request: Request):
    """
    Manually trigger a neighbourhood sync (admin endpoint).
    Rate limited to 1 request per hour.
    """
    from database.sync_state import sync_state
    
    try:
        logger.info("Manual sync triggered")
        await run_sync_async(db_client)
        
        # Set next sync time after manual sync
        next_sync = datetime.now() + timedelta(days=sync_interval_days)
        await sync_state.set_next_sync(next_sync)
        
        neighbourhood_count = db_client.get_neighbourhood_count()
        return {
            "status": "success",
            "message": f"Sync completed. {neighbourhood_count} neighbourhoods in database.",
            "next_sync": next_sync.isoformat()
        }
    except Exception as e:
        logger.error(f"Error during manual sync: {e}")
        raise HTTPException(status_code=500, detail="Sync failed")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
