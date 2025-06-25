import re
from datetime import datetime, timedelta


def replace_dates_with_two_following_days(url, extra_days=2, length_of_stay=2):
    """
    Replace the $DATES placeholder in the URL with two following days. In format: 15.04.2025-17.04.2025
    """
    # Get today's date
    today = datetime.today()

    # Calculate the next two days
    next_day = today + timedelta(days=extra_days)
    day_after_next = today + timedelta(days=extra_days + length_of_stay)

    # Format the dates in the required format
    formatted_dates = f"{next_day.strftime('%d.%m.%Y')}-{day_after_next.strftime('%d.%m.%Y')}"

    # Replace the placeholder in the URL
    updated_url = url.replace("$DATES", formatted_dates)

    return updated_url

def generate_date_pairs(start_date, end_date, stay_length=1) -> list[tuple[datetime, datetime]]:
    """
    Generate all possible date pairs within a range with a fixed stay length.
    Returns list of (check_in, check_out) tuples.
    """
    date_pairs = []
    current_date = start_date
    
    while current_date <= end_date - timedelta(days=stay_length):
        check_in = current_date
        check_out = current_date + timedelta(days=stay_length)
        date_pairs.append((check_in, check_out))
        current_date += timedelta(days=1)
    
    return date_pairs

# Function to validate date range format
def validate_date_range(date_range: str) -> bool:
    """Validate that date range is in format dd.mm.yyyy-dd.mm.yyyy"""
    pattern = r"^\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4}$"
    if not re.match(pattern, date_range):
        return False
    
    try:
        start_str, end_str = date_range.split("-")
        start_date = datetime.strptime(start_str, "%d.%m.%Y")
        end_date = datetime.strptime(end_str, "%d.%m.%Y")
        
        # Check that end date is after start date
        if end_date <= start_date:
            return False
            
        return True
    except ValueError:
        return False
    

