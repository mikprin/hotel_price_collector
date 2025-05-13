# Collect price for following dates range:

from hotel_price_absorber_src.date_utils import replace_dates_with_placeholder



from hotel_price_absorber_src.date_utils import replace_dates_with_placeholder
from hotel_price_absorber_src.ostrovok.scraper import get_price_from_simple_url
import os
import polars as pl
import time
import random
from datetime import datetime, timedelta
import argparse

def parse_date_range(date_range_str):
    """
    Parse a date range string in Russian format and return start and end dates.
    
    Examples:
    - Даты с 20 по 30 мая
    - С 1 по 10 июня
    - с 3 по 10 июля
    - С 1 по 15 августа
    """
    # Dictionary for Russian month names
    month_map = {
        'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4,
        'мая': 5, 'июня': 6, 'июля': 7, 'августа': 8,
        'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
    }
    
    # Lowercase and clean up the string
    date_range_str = date_range_str.lower().strip()
    
    # Extract the start day, end day, and month
    for month_name, month_num in month_map.items():
        if month_name in date_range_str:
            # Get the current year
            current_year = datetime.now().year
            
            # Extract start and end days
            parts = date_range_str.split(' ')
            
            # Find the day numbers
            day_numbers = []
            for part in parts:
                if part.isdigit():
                    day_numbers.append(int(part))
            
            # Need at least two numbers for start and end days
            if len(day_numbers) >= 2:
                start_day, end_day = day_numbers[0], day_numbers[1]
                
                # Create datetime objects
                start_date = datetime(current_year, month_num, start_day)
                end_date = datetime(current_year, month_num, end_day)
                
                return start_date, end_date
    
    # If parsing fails, raise an error
    raise ValueError(f"Could not parse date range: {date_range_str}")

def generate_date_pairs(start_date, end_date, stay_length=1):
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

def format_date_for_url(date_obj):
    """Format a datetime object to dd.mm.yyyy as required by Ostrovok URLs."""
    return date_obj.strftime('%d.%m.%Y')

def collect_hotel_prices_for_date_ranges(raw_links, date_ranges, stay_length=1):
    """
    Collect prices for a list of hotel URLs across multiple date ranges.
    
    Args:
        raw_links: List of hotel URLs (raw format)
        date_ranges: List of date range strings in Russian format
        stay_length: Length of stay in days
    
    Returns:
        List of price data objects
    """
    all_prices = []
    
    # Process each link to add $DATES placeholder
    
    # processed_links = [replace_dates_with_placeholder(link) for link in raw_links]
    processed_links = replace_dates_with_placeholder(raw_links)
    
    # print(f"Processed {len(processed_links)} hotel links with date placeholders
    # Process each date range
    for date_range_str in date_ranges:
        try:
            print(f"\nProcessing date range: {date_range_str}")
            start_date, end_date = parse_date_range(date_range_str)
            print(f"Parsed as: {start_date.strftime('%d %B %Y')} to {end_date.strftime('%d %B %Y')}")
            
            # Generate all date pairs within this range
            date_pairs = generate_date_pairs(start_date, end_date, stay_length)
            print(f"Generated {len(date_pairs)} check-in dates to process")
            
            # Process each hotel for each date pair
            for i, link in enumerate(processed_links):
                print(f"\nHotel {i+1}/{len(processed_links)}")
                
                for check_in, check_out in date_pairs:
                    # Format dates for URL
                    formatted_dates = f"{format_date_for_url(check_in)}-{format_date_for_url(check_out)}"
                    print(f"Link: {link}")
                    # Replace the placeholder in the URL
                    updated_link = link.replace("$DATES", formatted_dates)
                    
                    try:
                        print(f"Checking prices for {check_in.strftime('%d.%m.%Y')}-{check_out.strftime('%d.%m.%Y')}")
                        
                        # Get price data
                        price_data = get_price_from_simple_url(updated_link)
                        
                        # Add date information to price data
                        price_dict = price_data.model_dump()
                        price_dict['check_in_date'] = check_in.strftime('%Y-%m-%d')
                        price_dict['check_out_date'] = check_out.strftime('%Y-%m-%d')
                        price_dict['date_range'] = date_range_str
                        
                        all_prices.append(price_dict)
                        print(f"✓ {price_data.hotel_name}: {price_data.hotel_price} {price_data.hotel_currency}")
                        
                        # Add a small random delay to avoid triggering anti-scraping measures
                        time.sleep(random.uniform(1, 3))
                        
                    except Exception as e:
                        print(f"✗ Error processing {updated_link}: {e}")
            
        except ValueError as e:
            print(f"Error with date range '{date_range_str}': {e}")
    
    return all_prices

def save_to_csv(prices, filename="hotel_prices_by_date.csv"):
    """
    Convert price data to a Polars DataFrame and save to csv.
    """
    if not prices:
        print("No prices to save.")
        return None
    
    # Convert list of price dictionaries to DataFrame
    df = pl.DataFrame(prices)
    
    # Save DataFrame to CSV file
    try:
        df.write_csv(filename)
        print(f"Prices saved to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
    return df

def main():
    parser = argparse.ArgumentParser(description='Collect hotel prices for date ranges')
    parser.add_argument('--links', type=str, required=True, help='File with raw hotel links (one per line)')
    parser.add_argument('--dates', type=str, required=True, help='File with date ranges (one per line)')
    parser.add_argument('--stay', type=int, default=1, help='Length of stay in days (default: 1)')
    parser.add_argument('--output', type=str, default="hotel_prices_by_date.csv", help='Output CSV file')
    args = parser.parse_args()
    
    # Read raw links from file
    with open(args.links, 'r', encoding='utf-8') as f:
        raw_links = [line.strip() for line in f.readlines() if line.strip()]
    
    # Read date ranges from file
    with open(args.dates, 'r', encoding='utf-8') as f:
        date_ranges = [line.strip() for line in f.readlines() if line.strip()]
    
    print(f"Loaded {len(raw_links)} hotel links and {len(date_ranges)} date ranges")
    
    # Create output directory if needed
    os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else '.', exist_ok=True)
    
    # Collect prices and save to CSV
    prices = collect_hotel_prices_for_date_ranges(raw_links, date_ranges, args.stay)
    df = save_to_csv(prices, args.output)
    
    # Print summary
    if df is not None:
        print("\nSummary of Collected Data:")
        print(f"Total price points collected: {len(df)}")
        print(f"Hotels checked: {df['hotel_name'].n_unique()}")
        print(f"Date ranges processed: {df['date_range'].n_unique()}")
        
        # Show price statistics if available
        if 'hotel_price' in df.columns:
            print("\nPrice Statistics:")
            stats = df.select(
                pl.col("hotel_price").min().alias("Min Price"),
                pl.col("hotel_price").mean().alias("Avg Price"),
                pl.col("hotel_price").max().alias("Max Price")
            )
            print(stats)

if __name__ == "__main__":
    main()