"""Rate limiting middleware using SlowAPI."""
import logging
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from fastapi import Request

logger = logging.getLogger(__name__)


def get_identifier(request: Request) -> str:
    """
    Get identifier for rate limiting.
    Uses IP address + user agent for better tracking.
    """
    ip = get_remote_address(request)
    user_agent = request.headers.get("user-agent", "unknown")
    # Use first 50 chars of user agent to avoid huge keys
    return f"{ip}:{user_agent[:50]}"


# Create limiter instance
limiter = Limiter(
    key_func=get_identifier,
    default_limits=["100/hour"],  # Global default
    storage_uri="memory://",
)


def setup_rate_limiting(app):
    """
    Set up rate limiting for the FastAPI app.
    
    Args:
        app: FastAPI application instance
    """
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    logger.info("Rate limiting configured")
