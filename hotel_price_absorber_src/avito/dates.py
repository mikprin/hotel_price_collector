import re


def extract_dates_from_url(url: str) -> tuple[str, str]:
    """Extract check-in and check-out dates from URL and convert to DD-MM-YYYY format."""
    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)
    
    check_in = query_params.get('checkIn', [''])[0]
    check_out = query_params.get('checkOut', [''])[0]
    
    # Convert from YYYY-MM-DD to DD-MM-YYYY if dates exist
    if check_in and re.match(r'\d{4}-\d{2}-\d{2}', check_in):
        year, month, day = check_in.split('-')
        check_in = f"{day}-{month}-{year}"
        
    if check_out and re.match(r'\d{4}-\d{2}-\d{2}', check_out):
        year, month, day = check_out.split('-')
        check_out = f"{day}-{month}-{year}"
    
    return check_in, check_out

def replace_dates_with_placeholder(links):
    # Regular expression to match the checkIn and checkOut dates pattern in the URL
    pattern = r'(checkIn=)(\d{4}-\d{2}-\d{2})(&checkOut=)(\d{4}-\d{2}-\d{2})'
    
    # Replace the matched pattern with checkIn=$CHECKIN&checkOut=$CHECKOUT
    modified_links = [re.sub(pattern, r'\1$CHECKIN\3$CHECKOUT', link) for link in links]
    
    return modified_links


def format_date_for_url(date_obj):
    """Format a datetime object to yyyy-mm-dd as required by Avito URLs."""
    return date_obj.strftime('%Y-%m-%d')