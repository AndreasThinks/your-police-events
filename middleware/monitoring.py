"""Error monitoring and performance tracking with Sentry."""
import logging
import os
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

logger = logging.getLogger(__name__)


def setup_monitoring():
    """
    Initialize Sentry for error monitoring and performance tracking.
    Only initializes if SENTRY_DSN environment variable is set.
    """
    sentry_dsn = os.getenv("SENTRY_DSN")
    
    if not sentry_dsn:
        logger.info("SENTRY_DSN not set, skipping error monitoring setup")
        return
    
    # Configure Sentry
    sentry_sdk.init(
        dsn=sentry_dsn,
        # Set traces_sample_rate to 1.0 to capture 100% of transactions for performance monitoring
        # In production, you might want to lower this to reduce costs
        traces_sample_rate=0.1,  # 10% of requests
        # Set profiles_sample_rate to 1.0 to profile 100% of sampled transactions
        profiles_sample_rate=0.1,  # 10% of sampled transactions
        integrations=[
            FastApiIntegration(
                transaction_style="endpoint",  # Group by endpoint
            ),
            LoggingIntegration(
                level=logging.INFO,  # Capture info and above as breadcrumbs
                event_level=logging.ERROR  # Send errors as events
            ),
        ],
        environment=os.getenv("RAILWAY_ENVIRONMENT", "development"),
        # Add custom tags
        before_send=add_custom_context,
    )
    
    logger.info("Sentry monitoring initialized")


def add_custom_context(event, hint):
    """
    Add custom context to Sentry events.
    
    Args:
        event: Sentry event dict
        hint: Additional context
        
    Returns:
        Modified event dict
    """
    # Add custom tags if available
    if "request" in event:
        request = event["request"]
        if "data" in request:
            # Add postcode to context if present
            data = request.get("data", {})
            if isinstance(data, dict) and "postcode" in data:
                event.setdefault("tags", {})["postcode"] = data["postcode"]
    
    return event


def capture_exception(error: Exception, context: dict = None):
    """
    Manually capture an exception with optional context.
    
    Args:
        error: Exception to capture
        context: Optional dict of additional context
    """
    if context:
        with sentry_sdk.push_scope() as scope:
            for key, value in context.items():
                scope.set_context(key, value)
            sentry_sdk.capture_exception(error)
    else:
        sentry_sdk.capture_exception(error)
