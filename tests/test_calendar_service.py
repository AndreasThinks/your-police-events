"""Tests for calendar service and iCalendar generation."""
import pytest
from services.calendar import CalendarService
from api.police_uk import PoliceUKClient
from icalendar import Calendar


@pytest.mark.asyncio
async def test_generate_ics_feed(sample_events, mocker):
    """Test generating iCalendar feed from events."""
    # Mock the Police UK API client
    mock_client = mocker.Mock(spec=PoliceUKClient)
    mock_client.get_neighbourhood_events = mocker.AsyncMock(return_value=sample_events)
    
    service = CalendarService(mock_client)
    
    # Generate calendar
    ics_data = await service.generate_ics_feed("leicestershire", "NC04")
    
    # Verify it's valid iCalendar data
    assert ics_data is not None
    assert isinstance(ics_data, bytes)
    
    # Parse the calendar
    cal = Calendar.from_ical(ics_data)
    
    # Check calendar properties
    assert cal.get('prodid') == '-//Local Police Events//EN'
    assert cal.get('version') == '2.0'
    
    # Check events were added
    events = [component for component in cal.walk() if component.name == "VEVENT"]
    assert len(events) == 2
    
    # Check first event details
    event1 = events[0]
    assert "Bike register event" in str(event1.get('summary'))
    assert event1.get('location') == "Town Hall Square, Leicester"


@pytest.mark.asyncio
async def test_generate_ics_feed_empty_events(mocker):
    """Test generating calendar with no events."""
    mock_client = mocker.Mock(spec=PoliceUKClient)
    mock_client.get_neighbourhood_events = mocker.AsyncMock(return_value=[])
    
    service = CalendarService(mock_client)
    
    ics_data = await service.generate_ics_feed("test-force", "TEST01")
    
    # Should still generate valid calendar, just with no events
    assert ics_data is not None
    cal = Calendar.from_ical(ics_data)
    events = [component for component in cal.walk() if component.name == "VEVENT"]
    assert len(events) == 0


@pytest.mark.asyncio
async def test_calendar_includes_contact_details(sample_events, mocker):
    """Test that contact details are included in calendar events."""
    mock_client = mocker.Mock(spec=PoliceUKClient)
    mock_client.get_neighbourhood_events = mocker.AsyncMock(return_value=sample_events)
    
    service = CalendarService(mock_client)
    ics_data = await service.generate_ics_feed("leicestershire", "NC04")
    
    cal = Calendar.from_ical(ics_data)
    events = [component for component in cal.walk() if component.name == "VEVENT"]
    
    # First event has contact details
    event1 = events[0]
    contact = str(event1.get('contact', ''))
    assert 'test@police.uk' in contact or 'Email' in contact
