"""Utility functions for generating user-friendly error messages."""
import re
from typing import Optional, Tuple


def validate_uk_postcode(postcode: str) -> Tuple[bool, Optional[str]]:
    """
    Validate UK postcode format.
    
    Args:
        postcode: Postcode string to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Remove spaces and convert to uppercase
    clean = postcode.replace(" ", "").upper()
    
    # UK postcode regex pattern
    # Format: AA9A 9AA, A9A 9AA, A9 9AA, A99 9AA, AA9 9AA, AA99 9AA
    pattern = r'^[A-Z]{1,2}[0-9][A-Z0-9]?[0-9][A-Z]{2}$'
    
    if not clean:
        return False, "Postcode cannot be empty"
    
    if len(clean) < 5 or len(clean) > 8:
        return False, "Postcode must be between 5 and 8 characters"
    
    if not re.match(pattern, clean):
        return False, "Invalid UK postcode format. Example: SW1A 1AA"
    
    return True, None


def get_postcode_not_found_message(postcode: str) -> str:
    """
    Generate helpful message when postcode is not found.
    
    Args:
        postcode: The postcode that wasn't found
        
    Returns:
        User-friendly error message
    """
    return (
        f"We couldn't find the postcode '{postcode}'. "
        "Please check:\n"
        "• The postcode is spelled correctly\n"
        "• You're using a UK postcode\n"
        "• The postcode is complete (e.g., SW1A 1AA, not just SW1A)"
    )


def get_neighbourhood_not_found_message(postcode: str) -> str:
    """
    Generate message when postcode is valid but no neighbourhood found.
    
    Args:
        postcode: The valid postcode
        
    Returns:
        User-friendly error message
    """
    return (
        f"We found the postcode '{postcode}', but couldn't match it to a "
        "police neighbourhood. This might happen if:\n"
        "• The area doesn't have neighbourhood policing data yet\n"
        "• The postcode is in a remote or newly developed area\n"
        "• There's a temporary issue with the boundary data\n\n"
        "Try a nearby postcode or contact your local police force directly."
    )


def get_api_error_message() -> str:
    """
    Generate message for API errors.
    
    Returns:
        User-friendly error message
    """
    return (
        "We're having trouble looking up that postcode right now. "
        "This is usually temporary. Please try again in a few moments."
    )


def get_rate_limit_message(retry_after: int) -> str:
    """
    Generate message for rate limit errors.
    
    Args:
        retry_after: Seconds until user can retry
        
    Returns:
        User-friendly error message
    """
    minutes = retry_after // 60
    seconds = retry_after % 60
    
    if minutes > 0:
        time_str = f"{minutes} minute{'s' if minutes != 1 else ''}"
        if seconds > 0:
            time_str += f" and {seconds} second{'s' if seconds != 1 else ''}"
    else:
        time_str = f"{seconds} second{'s' if seconds != 1 else ''}"
    
    return (
        f"You've made too many requests. "
        f"Please wait {time_str} before trying again."
    )


def suggest_postcode_corrections(postcode: str) -> list[str]:
    """
    Suggest possible corrections for invalid postcodes.
    
    Args:
        postcode: Invalid postcode
        
    Returns:
        List of suggested corrections
    """
    suggestions = []
    clean = postcode.replace(" ", "").upper()
    
    # Common mistakes
    # 1. Missing space - add space before last 3 chars
    if len(clean) >= 5:
        suggested = f"{clean[:-3]} {clean[-3:]}"
        suggestions.append(suggested)
    
    # 2. Extra characters
    if len(clean) > 7:
        # Try removing last char
        shortened = clean[:-1]
        if len(shortened) >= 5:
            suggested = f"{shortened[:-3]} {shortened[-3:]}"
            suggestions.append(suggested)
    
    # 3. Common letter/number confusions
    replacements = {
        'O': '0', '0': 'O',
        'I': '1', '1': 'I',
        'S': '5', '5': 'S',
    }
    
    for old, new in replacements.items():
        if old in clean:
            corrected = clean.replace(old, new, 1)
            if len(corrected) >= 5:
                suggested = f"{corrected[:-3]} {corrected[-3:]}"
                if suggested not in suggestions:
                    suggestions.append(suggested)
    
    return suggestions[:3]  # Return max 3 suggestions
