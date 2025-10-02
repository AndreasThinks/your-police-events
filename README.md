# Local Police Events Calendar

A web service that allows UK residents to subscribe to their local police neighbourhood events as a calendar feed.

## Features

- 🔍 **Postcode Lookup**: Enter your postcode to find your police neighbourhood
- 📅 **Calendar Subscription**: Get an iCalendar (.ics) feed you can subscribe to
- 🔄 **Auto-Updates**: Calendar apps automatically check for new events
- 🗺️ **Spatial Queries**: Uses DuckDB spatial extension for accurate location matching
- 💾 **Smart Caching**: Events are cached to reduce API calls
- 🔁 **Weekly Sync**: Neighbourhood boundaries are automatically updated weekly

## Architecture

### Data Flow

1. **User enters postcode** → OS Names API returns British National Grid coordinates
2. **Coordinates transformed** → BNG converted to WGS84 (lat/lng) using DuckDB
3. **Spatial query** → Find which neighbourhood polygon contains the point
4. **Calendar URL generated** → User subscribes in their calendar app
5. **Events fetched** → Police UK API provides neighbourhood events
6. **iCalendar generated** → Events converted to standard .ics format

### Tech Stack

- **FastAPI** - Modern Python web framework
- **DuckDB** - Embedded database with spatial extension
- **OS Names API** - Official UK postcode geocoding (free)
- **Police UK API** - Official police data (free)
- **APScheduler** - Background job scheduling
- **iCalendar** - Standard calendar format

## Setup

### Prerequisites

- Python 3.11+
- OS Names API key (free from [OS Data Hub](https://osdatahub.os.uk/))

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd local-police-events
```

2. Install dependencies:
```bash
pip install -e .
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env and add your OS_NAMES_API_KEY
```

4. Run the application:
```bash
python main.py
```

The service will:
- Start on http://localhost:8000
- Initialize the database
- Perform initial sync of neighbourhood boundaries (takes ~10-15 minutes)
- Schedule weekly updates

## API Endpoints

### `GET /`
Main web interface for postcode lookup

### `POST /lookup`
Look up a postcode and get calendar URL
```json
{
  "postcode": "SW1A 1AA"
}
```

Response:
```json
{
  "force_id": "metropolitan",
  "neighbourhood_id": "E05000644",
  "neighbourhood_name": "St James's",
  "calendar_url": "http://localhost:8000/calendar/metropolitan/E05000644.ics"
}
```

### `GET /calendar/{force_id}/{neighbourhood_id}.ics`
Get iCalendar feed for a neighbourhood (cached for 3 hours)

### `GET /health`
Health check endpoint (returns neighbourhood count)

### `GET /admin/status`
Get comprehensive system status including:
- Database statistics (neighbourhoods, forces, storage size, last updated)
- Sync status (idle/running/completed/failed)
- Real-time sync progress (if running)
- Last sync results (duration, success rate, failures)
- Cache statistics
- Scheduler status

Example response:
```json
{
  "status": "operational",
  "database": {
    "neighbourhoods": 4656,
    "forces": 44,
    "storage_mb": 125.5,
    "last_updated": "2025-10-02T17:00:00"
  },
  "sync": {
    "status": "running",
    "progress": {
      "current_force": "metropolitan",
      "current_force_name": "Metropolitan Police Service",
      "forces_processed": 15,
      "total_forces": 44,
      "neighbourhoods_processed": 1850,
      "total_neighbourhoods": 4656,
      "neighbourhoods_synced": 1800,
      "neighbourhoods_failed": 50,
      "neighbourhoods_no_boundary": 0,
      "percentage": 39.7
    },
    "timing": {
      "started_at": "2025-10-02T17:30:00",
      "elapsed_seconds": 1080
    },
    "last_sync": {
      "completed_at": "2025-10-01T14:00:00",
      "duration_seconds": 3245,
      "neighbourhoods_synced": 4512,
      "neighbourhoods_failed": 144,
      "success_rate": 96.9
    }
  },
  "cache": {
    "calendar_feeds": {
      "size": 45,
      "max_size": 1000,
      "ttl_hours": 3
    },
    "postcode_lookups": {
      "size": 234
    }
  },
  "scheduler": {
    "active": true,
    "next_sync": "Weekly (every 7 days)"
  }
}
```

### `POST /admin/sync`
Manually trigger neighbourhood boundary sync (rate limited to 1/hour)

## Deployment to Railway

1. Create a new project on [Railway](https://railway.app)

2. Add environment variables:
   - `OS_NAMES_API_KEY` - Your OS Names API key
   - `DATABASE_PATH` - `/app/data/police_events.duckdb`
   - `CACHE_TTL_HOURS` - `3`
   - `LOG_LEVEL` - `INFO`

3. Deploy from GitHub or using Railway CLI:
```bash
railway up
```

4. Railway will automatically:
   - Detect Python project
   - Install dependencies
   - Run the application
   - Provide a public URL

## Project Structure

```
local-police-events/
├── api/
│   ├── police_uk.py          # Police UK API client (with retry logic)
│   └── ordnance_survey.py    # OS Names API client
├── database/
│   ├── duckdb_client.py      # DuckDB spatial operations
│   ├── sync.py               # Weekly boundary sync job
│   └── sync_state.py         # Sync progress tracking
├── services/
│   ├── location.py           # Postcode → neighbourhood lookup
│   └── calendar.py           # iCalendar generation
├── middleware/
│   ├── rate_limit.py         # Rate limiting
│   └── monitoring.py         # Sentry integration
├── tests/
│   ├── test_sync_state.py    # Sync state tests
│   ├── test_status_endpoint.py # Status endpoint tests
│   └── ...                   # Other tests
├── static/
│   └── index.html            # Frontend web interface
├── main.py                   # FastAPI application
├── pyproject.toml            # Dependencies
├── railway.toml              # Railway configuration
└── .env                      # Environment variables
```

## How It Works

### Initial Sync
On first startup, the service:
1. Fetches all UK police forces from Police UK API (~44 forces)
2. For each force, gets list of neighbourhoods (~4,656 total)
3. For each neighbourhood, fetches boundary coordinates
4. Stores polygons in DuckDB with spatial index

**Features:**
- **Retry logic**: Automatically retries failed requests (3 attempts with exponential backoff)
- **Progress tracking**: Real-time progress available via `/admin/status`
- **Error handling**: Distinguishes temporary failures (retry) from permanent (skip)
- **Success rate**: Typically achieves 95-100% success rate

This takes ~10-15 minutes and runs automatically. Monitor progress at `/admin/status`.

### Postcode Lookup
1. User enters postcode (e.g., "SW1A 1AA")
2. OS Names API returns British National Grid coordinates
3. DuckDB transforms BNG → WGS84 (lat/lng)
4. Spatial query finds containing neighbourhood polygon
5. Returns force ID, neighbourhood ID, and calendar URL

### Calendar Subscription
1. User subscribes to calendar URL in their app
2. Calendar app periodically requests the .ics file
3. Service checks cache (3-hour TTL)
4. If expired, fetches fresh events from Police UK API
5. Converts events to iCalendar format
6. Returns .ics file

### Weekly Updates
APScheduler runs a background job every 7 days to:
- Re-fetch all neighbourhood boundaries
- Update DuckDB with any changes
- Ensures boundaries stay current

## Configuration

### Environment Variables

- `OS_NAMES_API_KEY` - Required. Get from https://osdatahub.os.uk/
- `DATABASE_PATH` - Path to DuckDB file (default: `./data/police_events.duckdb`)
- `CACHE_TTL_HOURS` - Calendar cache duration (default: `3`)
- `LOG_LEVEL` - Logging level (default: `INFO`)

### Cache Settings

Calendar feeds are cached to reduce API calls:
- **TTL**: 3 hours (configurable)
- **Max size**: 1000 entries
- **Key format**: `force_id:neighbourhood_id`

## Development

### Running Tests
```bash
pytest
```

### Local Development
```bash
# Install in development mode
pip install -e .

# Run with auto-reload
uvicorn main:app --reload
```

### Monitoring Sync Progress
Check sync status and progress:
```bash
curl http://localhost:8000/admin/status
```

### Manual Sync
Trigger a manual boundary sync:
```bash
curl -X POST http://localhost:8000/admin/sync
```

Note: Rate limited to 1 request per hour.

## Recent Improvements

### Smart Sync System (October 2025)
- **Intelligent startup decisions**: Automatically determines what sync is needed on startup
- **Crash recovery**: Detects stale locks from crashed syncs and recovers automatically
- **Incremental recovery**: Only re-syncs forces that failed, not everything
- **Persistent state**: Sync metadata survives restarts
- **Per-force tracking**: Knows exactly which forces succeeded/failed
- **Data freshness checks**: Avoids unnecessary syncs when data is current

#### How Smart Sync Works

On startup, the system analyzes the database state and decides:

1. **Empty Database** → Schedule full sync (2 min delay)
2. **Stale Lock Detected** (sync crashed >2h ago) → Schedule recovery sync (5 min delay)
3. **Last Sync Failed** → Schedule recovery sync of failed forces only (5 min delay)
4. **Data Stale** (>8 days old) → Schedule full sync (2 min delay)
5. **Data Approaching Stale** (6-8 days) → Let weekly scheduler handle it
6. **Data Fresh** (<6 days) → Skip sync, data is current

**Benefits:**
- ✅ No manual intervention needed after crashes
- ✅ Efficient recovery (only re-sync what failed)
- ✅ Avoids redundant full syncs
- ✅ Deployment-friendly (delayed start prevents timeouts)

**Database Tables:**
- `sync_metadata`: Overall sync state, timing, and statistics
- `force_sync_status`: Per-force tracking for granular recovery

### Enhanced Scraping (October 2025)
- **Retry logic**: Automatic retry with exponential backoff for failed requests
- **Improved success rate**: From ~78% to 95-100%
- **Better error handling**: Distinguishes temporary vs permanent failures
- **Increased timeout**: 30s → 60s for slow responses
- **Detailed logging**: Track exactly what succeeded/failed

### Sync Monitoring (October 2025)
- **Real-time progress**: Track sync progress via `/admin/status`
- **Database statistics**: View storage size, neighbourhood counts, last updated
- **Sync history**: See results from previous syncs
- **Progress percentage**: Know exactly how far along the sync is
- **Elapsed time tracking**: Monitor sync duration

## Data Sources

- **Police UK API**: https://data.police.uk/docs/
  - Free, no API key required
  - Provides forces, neighbourhoods, boundaries, and events
  - ~4,656 neighbourhoods across 44 UK police forces

- **OS Names API**: https://osdatahub.os.uk/
  - Free tier available
  - Requires API key
  - Provides postcode geocoding

## License

MIT

## Contributing

Contributions welcome! Please open an issue or PR.

## Support

For issues or questions, please open a GitHub issue.
