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

### `POST /admin/sync`
Manually trigger neighbourhood boundary sync

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
│   ├── police_uk.py          # Police UK API client
│   └── ordnance_survey.py    # OS Names API client
├── database/
│   ├── duckdb_client.py      # DuckDB spatial operations
│   └── sync.py               # Weekly boundary sync job
├── services/
│   ├── location.py           # Postcode → neighbourhood lookup
│   └── calendar.py           # iCalendar generation
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
1. Fetches all UK police forces from Police UK API
2. For each force, gets list of neighbourhoods
3. For each neighbourhood, fetches boundary coordinates
4. Stores polygons in DuckDB with spatial index

This takes ~10-15 minutes and runs automatically.

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

### Manual Sync
Trigger a manual boundary sync:
```bash
curl -X POST http://localhost:8000/admin/sync
```

## Data Sources

- **Police UK API**: https://data.police.uk/docs/
  - Free, no API key required
  - Provides forces, neighbourhoods, boundaries, and events

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
