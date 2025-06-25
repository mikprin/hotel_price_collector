import re
import time
from datetime import datetime, timedelta

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from hotel_price_absorber_src.engine.chorome import get_chrome_driver
from hotel_price_absorber_src.logger import general_logger as logger


from hotel_price_absorber_src.schema import HotelPrice

from hotel_price_absorber_src.avito.dates import extract_dates_from_url
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException

import urllib.parse

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


def extract_price_and_currency(price_text: str) -> tuple[float, str]:
    """Extract price as float and currency from price text."""
    # Remove non-breaking spaces and regular spaces
    # print(f"Extracting price from text: {price_text}")
    clean_text = price_text.replace('\xa0', '').replace(' ', '')
    
    # Try to extract numbers and currency
    match = re.search(r'(\d+[.,]?\d*)([$€£¥₽])', clean_text)
    if match:
        price_str, currency = match.groups()
        # Replace comma with dot for float conversion if needed
        price_str = price_str.replace(',', '.')
        return float(price_str), currency
    
    # Fallback: extract just the numbers
    numbers = re.findall(r'\d+', price_text)
    if numbers:
        # Join all digit sequences (handles spaces in numbers like "4 900")
        price = float(''.join(numbers))
        
        # Try to extract currency
        currencies = re.findall(r'[$€£¥₽]', price_text)
        currency = currencies[0] if currencies else None
        
        return price, currency
    
    return 0.0, None

def find_daily_price(price_elements):
    """
    Find the per-day price from a list of elements containing prices.
    Strategy: 
    1. Look for elements with currency symbol and lower values (likely daily rate)
    2. Check for elements near "за сутки" text
    """
    daily_price = None
    prices = []
    
    # First, collect all prices
    for element in price_elements:
        text = element.text.strip()
        if re.search(r'[$€£¥₽]', text):
            # Skip if it contains words like "deposit" or "security"
            if any(word in text.lower() for word in ['залог', 'deposit', 'security']):
                continue
            
            # Extract price and save both the element and the price
            price_tuple = extract_price_and_currency(text)
            if price_tuple[0] > 0:
                prices.append((element, price_tuple[0], price_tuple[1], text))
    
    # If we have prices, the lowest one is likely the daily rate
    # (assuming the total is higher and security deposits are filtered out)
    if prices:
        # Sort by price ascending
        prices.sort(key=lambda x: x[1])
        # Return the lowest price that's not zero
        return prices[0][1], prices[0][2], prices[0][3]
    
    return 0.0, None, None



def avito_get_price_from_avito_url(url: str, group_name: str, hotel_name: str | None = None) -> HotelPrice:
    """
    Get price from an Avito URL.
    
    Args:
        url (str): The URL of the Avito listing.
        
    Returns:
        OstrovokHotelPrice: An object containing the price and other details.
    """
    logger.debug(f"Extracting price from URL: {url}")
    
    driver = get_chrome_driver()
    driver.get(url)
    check_in_date, check_out_date = extract_dates_from_url(url)
    
    # Get current timestamp
    timestamp = int(datetime.now().timestamp())
    
    # Wait for the page to load
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    
    # Get hotel name from title if Not defined in call
    if hotel_name is None:
        try:
            title_element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.TAG_NAME, "h1"))
            )
            hotel_name = title_element.text.strip()
        except (TimeoutException, NoSuchElementException):
            try:
                hotel_name = driver.title.split('|')[0].strip()
            except:
                hotel_name = None
    

    # Get room details
    try:
        room_details = driver.find_elements(By.XPATH, "//*[contains(text(), 'м²')]")
        if room_details:
            for detail in room_details:
                text = detail.text.strip()
                if 'м²' in text and 'кроват' in text.lower():
                    room_name = text
                    break
            else:
                room_name = room_details[0].text.strip()
        else:
            room_name = None
    except:
        room_name = None
    
    # Find all elements with price information
    price_elements = []
    price_elements.extend(driver.find_elements(By.XPATH, "//*[contains(text(), '₽')]"))
    price_elements.extend(driver.find_elements(By.XPATH, "//*[contains(text(), '$')]"))
    price_elements.extend(driver.find_elements(By.XPATH, "//*[contains(text(), '€')]"))
    price_elements.extend(driver.find_elements(By.XPATH, "//*[contains(text(), '£')]"))
    
    # Debug logging
    for element in price_elements:
        text = element.text.strip()
        logger.info(f"Price element text: {text}")
    
    # Find per day price element
    hotel_price, hotel_currency, price_text = find_daily_price(price_elements)
    
    # If price is still 0, try another approach - look for CSS selectors commonly used for prices
    if hotel_price == 0:
        try:
            # Try various common price selector patterns
            price_selectors = [
                "span.price", "div.price", ".price-value", 
                "[data-price]", "[itemprop='price']"
            ]
            
            for selector in price_selectors:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    for element in elements:
                        text = element.text.strip()
                        if re.search(r'[$€£¥₽]', text):
                            price, currency = extract_price_and_currency(text)
                            if price > 0:
                                hotel_price = price
                                hotel_currency = currency
                                price_text = text
                                break
        except:
            pass
    
    # Create the HotelPrice object
    return HotelPrice(
        hotel_url=url,
        hotel_price=hotel_price,
        measurment_taken_at=timestamp,
        check_in_date=check_in_date,
        check_out_date=check_out_date,
        hotel_name=hotel_name,
        hotel_currency=hotel_currency,
        room_name=room_name,
        comments=f"Scraped from Avito.ru at {datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}",
        group_name=group_name
    )
    
    
        