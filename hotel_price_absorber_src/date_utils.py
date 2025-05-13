from datetime import datetime, timedelta
import re


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


def replace_dates_with_placeholder(links):
    # Regular expression to match the dates pattern in the URL
    pattern = r'(dates=)(\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4})'
    
    # Replace the matched pattern with dates=$DATES
    modified_links = [re.sub(pattern, r'\1$DATES', link) for link in links]
    
    return modified_links