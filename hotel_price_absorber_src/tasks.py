import os
import random
import time
from datetime import datetime, timedelta

import polars as pl

from hotel_price_absorber_src.database.redis import PriceRange
from hotel_price_absorber_src.database.sqlite import HotelPriceDB
from hotel_price_absorber_src.database.user_database import HotelGroup, UserDataStorage
from hotel_price_absorber_src.date_utils import format_date_for_url, generate_date_pairs, replace_dates_with_placeholder
from hotel_price_absorber_src.logger import general_logger as logger
from hotel_price_absorber_src.ostrovok.scraper import get_price_from_simple_url
from hotel_price_absorber_src.schema import OstrovokHotelPrice


def get_price_range_for_group(range: PriceRange) -> bool:
    """
    Get price range for a specific hotel group.
    """
    # Create a new instance of UserDataStorage
    user_data_storage = UserDataStorage()
    price_db = HotelPriceDB()

    # Get the hotel group from the database
    group = user_data_storage.get_group(range.group_name)

    if not group:
        logger.error(f"Group {range.group_name} not found.")
        return False

    # Create a list to store the prices
    prices = []

    # Cast datetime from date strings
    start_date = datetime.strptime(range.start_date, "%d.%m.%Y")
    end_date = datetime.strptime(range.end_date, "%d.%m.%Y")
    # Generate date pairs for tend_datehe given range
    date_pairs = generate_date_pairs(start_date, end_date, range.days_of_stay)

    # Iterate through the hotels in the group
    for hotel in group.hotels:
        
        for date_pair in date_pairs:
            # Generate the URL for the hotel with the date range

            # Format the dates for the URL
            formatted_start_date = format_date_for_url(date_pair[0])
            formatted_end_date = format_date_for_url(date_pair[1])

            # Replace the $DATES placeholder with actual dates
            url = replace_dates_with_placeholder([hotel.url])[0]
            url = url.replace("$DATES", f"{formatted_start_date}-{formatted_end_date}")

            # Collect prices for the hotel
            
            try:
                price = get_price_from_simple_url(url)
                price.group_name = range.group_name
            
                if hotel.name is not None:
                    price.hotel_name = hotel.name
                
                if range.run_id is not None:
                    price.run_id = range.run_id

                prices.append(price)
                lst_row_id = price_db.save(price)
            
                if lst_row_id:
                    logger.info(f"Price {price} saved with ID: {lst_row_id}")
                else:
                    logger.error(f"Failed to save price {price}")
            except Exception as e:
                logger.error(f"Error collecting price for hotel {hotel.name} for {formatted_start_date}:\n{e}")

            # Simulate a random delay between requests
            time.sleep(random.uniform(0.5, 5.0))
            
    # Convert the list of prices to a Polars DataFrame
    df = pl.DataFrame(prices)

    logger.info(f"Collected prices for group {group.group_name}: {len(prices)} hotels")

    # price_db.save_batch(prices)

    # Save the DataFrame to a CSV file
    dir = os.getenv("DB_PATH", "/database")
    file_path = os.path.join(dir, "hotel_prices_example", f"{group.group_name}_prices.csv")
    os.makedirs(dir, exist_ok=True)
    df.write_csv(file_path)
    logger.info(f"Prices for group {group.group_name} saved to CSV: {file_path}")
    return True