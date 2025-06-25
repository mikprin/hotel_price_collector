import re
from datetime import datetime, timedelta


def replace_dates_with_placeholder(links):
    # Regular expression to match the dates pattern in the URL
    pattern = r'(dates=)(\d{2}\.\d{2}\.\d{4}-\d{2}\.\d{2}\.\d{4})'
    
    # Replace the matched pattern with dates=$DATES
    modified_links = [re.sub(pattern, r'\1$DATES', link) for link in links]
    
    return modified_links



def format_date_for_url(date_obj):
    """Format a datetime object to dd.mm.yyyy as required by Ostrovok URLs."""
    return date_obj.strftime('%d.%m.%Y')
