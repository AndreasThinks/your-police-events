"""Calendar service for generating iCalendar feeds from police events."""
import logging
from typing import List, Dict, Any
from datetime import datetime
from icalendar import Calendar, Event
from api.police_uk import PoliceUKClient

logger = logging.getLogger(__name__)


class CalendarService:
    """Service for generating iCalendar feeds from police events."""
    
    def __init__(self, police_client: PoliceUKClient):
        self.police_client = police_client
    
    async def generate_ics_feed(
        self, force_id: str, neighbourhood_id: str
    ) -> bytes:
        """
        Generate an iCalendar (.ics) feed for a neighbourhood's events.
        
        Args:
            force_id: Police force identifier
            neighbourhood_id: Neighbourhood identifier
            
        Returns:
            iCalendar data as bytes
        """
        try:
            # Fetch events from Police UK API
            events = await self.police_client.get_neighbourhood_events(
                force_id, neighbourhood_id
            )
            
            # Create calendar
            cal = Calendar()
            cal.add('prodid', '-//Local Police Events//EN')
            cal.add('version', '2.0')
            cal.add('calscale', 'GREGORIAN')
            cal.add('method', 'PUBLISH')
            cal.add('x-wr-calname', f'Police Events - {force_id}/{neighbourhood_id}')
            cal.add('x-wr-timezone', 'Europe/London')
            cal.add('x-wr-caldesc', 
                   f'Neighbourhood police events for {force_id}/{neighbourhood_id}')
            
            # Add each event
            for event_data in events:
                event = Event()
                
                # Required fields
                event.add('summary', event_data.get('title', 'Police Event'))
                event.add('uid', f"{force_id}-{neighbourhood_id}-{event_data.get('title', '')}-{event_data.get('start_date', '')}")
                
                # Description
                description = event_data.get('description', '')
                if description:
                    event.add('description', description)
                
                # Location
                address = event_data.get('address', '')
                if address:
                    event.add('location', address)
                
                # Start and end times
                start_date = event_data.get('start_date')
                end_date = event_data.get('end_date')
                
                if start_date:
                    try:
                        # Parse ISO format datetime
                        start_dt = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
                        event.add('dtstart', start_dt)
                    except Exception as e:
                        logger.warning(f"Error parsing start date {start_date}: {e}")
                
                if end_date:
                    try:
                        end_dt = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
                        event.add('dtend', end_dt)
                    except Exception as e:
                        logger.warning(f"Error parsing end date {end_date}: {e}")
                
                # Contact details (if available)
                contact_details = event_data.get('contact_details', {})
                if contact_details:
                    contact_info = []
                    if contact_details.get('email'):
                        contact_info.append(f"Email: {contact_details['email']}")
                    if contact_details.get('telephone'):
                        contact_info.append(f"Tel: {contact_details['telephone']}")
                    if contact_details.get('web'):
                        contact_info.append(f"Web: {contact_details['web']}")
                    
                    if contact_info:
                        event.add('contact', ', '.join(contact_info))
                
                # Event type
                event_type = event_data.get('type', 'other')
                event.add('categories', [event_type])
                
                # Add timestamp
                event.add('dtstamp', datetime.utcnow())
                
                cal.add_component(event)
            
            logger.info(
                f"Generated calendar with {len(events)} events for "
                f"{force_id}/{neighbourhood_id}"
            )
            
            # Return as bytes
            return cal.to_ical()
            
        except Exception as e:
            logger.error(
                f"Error generating calendar for {force_id}/{neighbourhood_id}: {e}"
            )
            raise
