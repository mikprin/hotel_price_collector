from selenium.webdriver.common.by import By
import re
from hotel_price_absorber_src.engine.chorome import get_chrome_driver
from hotel_price_absorber_src.schema import OstrovokHotelPrice

from hotel_price_absorber_src.logger import general_logger as logger


def get_price_from_simple_url(url):
    """
    Extracts hotel price information from an Ostrovok.ru URL.
    
    Args:
        url (str): The full URL to the hotel page on Ostrovok.ru
        
    Returns:
        OstrovokHotelPrice: Object containing hotel name, URL, room info, and price
    """
    logger.debug(f"Extracting price from URL: {url}")
    
    driver = get_chrome_driver()
    
    try:
        driver.get(url)
        # Wait for page to load completely
        driver.implicitly_wait(2)
        
        # Get hotel name from page title or specific element
        hotel_name = driver.title.split(" in ")[0].strip()
        if " reviews" in hotel_name:
            hotel_name = hotel_name.split(" reviews")[0].strip()
        
        # Try to find room containers
        room_containers = driver.find_elements(By.CSS_SELECTOR, ".room-card, .room-option, div[data-component='RoomCard']")
        
        if not room_containers:
            # Alternative selector if the first doesn't work
            room_containers = driver.find_elements(By.XPATH, "//div[contains(@class, 'room') or contains(@class, 'option')]")
        
        best_room = None
        lowest_price = float('inf')
        
        # If we found room containers
        if room_containers and len(room_containers) > 0:
            for container in room_containers:
                try:
                    # Extract room name
                    room_name_elem = container.find_elements(By.XPATH, ".//h3 | .//div[contains(@class, 'title') or contains(@class, 'name')]")
                    room_name = room_name_elem[0].text if room_name_elem else "Standard Room"
                    
                    # Find price within this container
                    price_elem = container.find_elements(By.XPATH, ".//*[contains(text(), '₽') and not(contains(text(), 'Prepayment'))]")
                    
                    if price_elem:
                        price_text = price_elem[0].text
                        match = re.search(r'([\d\s,.]+)\s*₽', price_text)
                        
                        if match:
                            # Convert price string to float
                            price_str = match.group(1).replace(' ', '').replace('.', '')
                            price_value = float(price_str)
                            
                            # Track lowest price option
                            if price_value < lowest_price:
                                lowest_price = price_value
                                best_room = {
                                    "room_name": room_name,
                                    "price": price_value
                                }
                except Exception:
                    continue
        
        # Fallback: If we couldn't get structured data, try to find any price
        if best_room is None:
            price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '₽') and not(contains(text(), 'Prepayment'))]")
            
            for elem in price_elements:
                try:
                    price_text = elem.text
                    match = re.search(r'([\d\s,.]+)\s*₽', price_text)
                    
                    if match:
                        price_str = match.group(1).replace(' ', '').replace(',', '.')
                        price_value = float(price_str)
                        
                        if price_value < lowest_price:
                            lowest_price = price_value
                            best_room = {
                                "room_name": "Standard Room",
                                "price": price_value
                            }
                except Exception:
                    continue
        
        # Create and return the result object
        if best_room and lowest_price != float('inf'):
            return OstrovokHotelPrice(
                hotel_name=hotel_name,
                hotel_url=url,
                room_name=best_room["room_name"],
                hotel_price=best_room["price"],
                hotel_currency="₽"
            )
        else:
            # If no price found, return object with None/0 values
            logger.error(f"No price found for {hotel_name} from {url}")
            return OstrovokHotelPrice(
                hotel_name=hotel_name,
                hotel_url=url,
                room_name=None,
                hotel_price=0.0,
                hotel_currency="₽"
            )
            
    except Exception as e:
        logger.error(f"Error extracting price data from {url}: {e}")
        # Return minimal object in case of error
        return OstrovokHotelPrice(
            hotel_name=url.split("/")[-2].replace("_", " ").title() if "/mid" in url else "Unknown Hotel",
            hotel_url=url,
            room_name=None,
            hotel_price=0.0,
            hotel_currency=None
        )
        
    finally:
        driver.quit()