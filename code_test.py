import os
import random
import time
from datetime import datetime, timedelta

import polars as pl
from dotenv import load_dotenv

from hotel_price_absorber_src.ostrovok.scraper import get_price_from_simple_url
from hotel_price_absorber_src.schema import OstrovokHotelPrice

load_dotenv()

os.environ["DB_PATH"] = "./database"


TEST_GROUP = "Test group"

from hotel_price_absorber_src.database.sqlite import HotelPriceDB


db = HotelPriceDB()

MEASURMENT_GROUP = "Test measurment"

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

def add_date_placeholder(url):
    """
    Add $DATES placeholder to the URL if not present.
    """
    if "$DATES" not in url:
        if url.endswith("/"):
            url += "?dates=$DATES&guests=2"
        else:
            url += "&dates=$DATES&guests=2"
    return url


# links = [
#     "https://ostrovok.ru/hotel/russia/st._petersburg/mid10006831/maximusvertical_aparthotel/?dates=$DATES&guests=2",
#     "https://ostrovok.ru/hotel/russia/st._petersburg/mid10675845/simple_stunningaparts_apartments/?dates=$DATES&guests=2",
#     "https://ostrovok.ru/hotel/russia/st._petersburg/mid9721358/wei_by_vertical_hotel/?dates=$DATES&guests=2",
# ]
links = [
    "https://ostrovok.ru/hotel/russia/st._petersburg/mid10006831/maximusvertical_aparthotel/?dates=01.12.2025-05.12.2025&guests=2"
]

prices = list()


def collect_hotel_prices(links) -> list[OstrovokHotelPrice]:
    """
    Collect prices for a list of hotel URLs and return a Polars DataFrame.
    """
    prices = []
    
    for i, link in enumerate(links):
        try:
            print(f"Processing hotel {i+1}/{len(links)}: {link}")
            updated_link = replace_dates_with_two_following_days(link, extra_days=4, length_of_stay=5)
            print(f"Updated link: {updated_link}")

            # Get price data
            price_data = get_price_from_simple_url(updated_link)
            price_data.group_name = MEASURMENT_GROUP
            prices.append(price_data)
            print(f"Collected price data for {price_data.hotel_name}: {price_data.hotel_price} {price_data.hotel_currency}")
            # Add a small random delay to avoid triggering anti-scraping measures
            time.sleep(random.uniform(1, 3))
            
        except Exception as e:
            print(f"Error processing {link}: {e}")
    
    return prices

def save_to_csv(prices, filename="hotel_prices.csv"):
    """
    Convert price objects to a Polars DataFrame and save to csv.
    """
    if not prices:
        print("No prices to save.")
        return None
    
    # Convert list of OstrovokHotelPrice objects to DataFrame
    df = pl.DataFrame([price.model_dump() for price in prices])
    
    # Save DataFrame to Excel file
    try:
        df.write_csv(filename)
        print(f"Prices saved to {filename}")
    except Exception as e:
        print(f"Error saving to CSV: {e}")
    return df

# Run the collection and save to Excel
if __name__ == "__main__":
    print(f"Starting price collection for {len(links)} hotels...")
    prices = collect_hotel_prices(links)
    # Mkdir if not exist
    dir_path = "hotel_prices_example"
    
    # Today in format DD-MM-YYYY
    today = datetime.today().strftime('%d_%m_%Y')
    
    path_to_save = os.path.join(dir_path, f"./ostrovok_hotel_prices_{today}.csv")
    os.makedirs(dir_path, exist_ok=True)
    df = save_to_csv(prices, path_to_save)
    
    db.save_batch(prices)
    
    # Print the results as table for console view
    if df is not None:
        print("\nFull Results Table:")
        print(df)

    # Save it to database    
    
    # print(replace_dates_with_two_following_days(links[0]))
    
    # price = get_price_from_simple_url(replace_dates_with_two_following_days(links[-1]))
    # print(price)