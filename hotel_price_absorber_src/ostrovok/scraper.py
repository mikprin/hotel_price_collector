import re
import time

from selenium.webdriver.common.by import By

from hotel_price_absorber_src.engine.chorome import get_chrome_driver
from hotel_price_absorber_src.logger import general_logger as logger
from hotel_price_absorber_src.schema import OstrovokHotelPrice


def get_price_from_simple_url(url) -> OstrovokHotelPrice:
    """
    Extracts hotel price information from an Ostrovok.ru URL.
    
    Args:
        url (str): The full URL to the hotel page on Ostrovok.ru
        Example: https://ostrovok.ru/hotel/russia/st._petersburg/mid9992800/apartpage_marata/?q=2042&dates=05.04.2025-07.04.2025&guests=2
        
    Returns:
        OstrovokHotelPrice: Object containing hotel name, URL, room info, and price
        
    Note: 
        - Handles case when "There are no rooms available for the selected dates" is shown
        - Prevents scraping prices from recommended hotels when target hotel has no availability
    """
    logger.debug(f"Extracting price from URL: {url}")
    
    # Extract dates from URL and convert format
    check_in_date = None
    check_out_date = None
    if "dates=" in url:
        dates_range = url.split("dates=")[-1].split("&")[0]
        try:
            date_parts = dates_range.split('-')
            if len(date_parts) == 2:
                # Convert from DD.MM.YYYY to DD-MM-YYYY
                check_in_date = date_parts[0].replace('.', '-')
                check_out_date = date_parts[1].replace('.', '-')
        except Exception as e:
            logger.debug(f"Error parsing dates: {e}")
    
    # Get current timestamp for measurement
    measurement_taken_at = int(time.time())
    
    driver = get_chrome_driver()
    
    try:
        driver.get(url)
        # Wait for page to load completely
        driver.implicitly_wait(2)
        
        # Get hotel name - using partial class matching
        try:
            # Using partial class matching to handle class hash changes
            hotel_name_elem = driver.find_element(By.CSS_SELECTOR, "h1[class*='DesktopHeader_name']")
            hotel_name = hotel_name_elem.text
        except:
            # Fallback to title if specific element not found
            hotel_name = driver.title.split(" in ")[0].strip()
            if " reviews" in hotel_name:
                hotel_name = hotel_name.split(" reviews")[0].strip()
        
        # Get hotel address - not used in new schema, but keeping for local reference
        try:
            address_elem = driver.find_element(By.CSS_SELECTOR, "span[class*='DesktopHeader_address']")
            address = address_elem.text
        except:
            address = None
        
        # CHECK FOR NO AVAILABILITY MESSAGE FIRST
        # This prevents scraping prices from recommended hotels when the target hotel has no rooms
        try:
            # Check for various "no rooms available" messages in different languages
            no_availability_texts = [
                "There are no rooms available for the selected dates",
                "На выбранные даты нет номеров",
                "No rooms available",
                # "Нет номеров",
                # "Sold out",
                # "Распродано"
            ]
            
            page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
            
            for no_avail_text in no_availability_texts:
                if no_avail_text.lower() in page_text:
                    logger.debug(f"No rooms available message found: '{no_avail_text}'")
                    return OstrovokHotelPrice(
                        hotel_name=hotel_name,
                        hotel_url=url,
                        room_name=None,
                        hotel_price=0.0,
                        hotel_currency="₽",
                        check_in_date=check_in_date,
                        check_out_date=check_out_date,
                        comments="No rooms available for selected dates",
                        measurment_taken_at=measurement_taken_at
                    )
                    
        except Exception as e:
            logger.debug(f"Error checking for no availability message: {e}")
        
        # PRIMARY METHOD: Look for headline price with resilient selectors
        try:
            # Try multiple selectors for the headline price
            headline_price_selectors = [
                "p[class*='Price_priceTitle']",
                "p[class*='priceTitle']",
                "div[class*='Header'] p[class*='price']",
                "div[class*='price'] p",
                "p[class*='Price']"
            ]
            
            headline_price_elem = None
            for selector in headline_price_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    for elem in elements:
                        text = elem.text
                        if "₽" in text:
                            headline_price_elem = elem
                            break
                    if headline_price_elem:
                        break
                except:
                    continue
            
            if not headline_price_elem:
                raise Exception("Could not find headline price element")
                
            price_text = headline_price_elem.text
            
            # Extract the numeric value and remove "from" if present
            price_text = price_text.replace("from", "").strip()
            match = re.search(r'([\d\s,.]+)\s*₽', price_text)
            
            if match:
                price_str = match.group(1).replace(' ', '').replace(',', '')
                lowest_price = float(price_str)
                best_room = {
                    "room_name": "Standard Room",  # Default since we're getting the headline price
                    "price": lowest_price
                }
                logger.debug(f"Found headline price: {lowest_price} ₽")
            else:
                raise Exception("Price format in headline price not recognized")
                
        except Exception as e:
            logger.debug(f"Could not extract headline price: {e}, falling back to room search")
            
            # BACKUP METHOD 1: Try to find room containers with robust selectors
            best_room = None
            lowest_price = float('inf')
            
            # Try multiple possible selectors for room containers - all using partial matching
            room_container_selectors = [
                "div[data-component='RoomRow']", 
                "div[data-component='RoomCard']",
                "div[class*='Room_room']",
                "div[class*='RoomCard']",
                "div[class*='room-card']", 
                "div[class*='room-option']"
            ]
            
            room_containers = []
            for selector in room_container_selectors:
                try:
                    containers = driver.find_elements(By.CSS_SELECTOR, selector)
                    if containers and len(containers) > 0:
                        room_containers = containers
                        logger.debug(f"Found {len(containers)} room containers using selector: {selector}")
                        break
                except:
                    continue
            
            # If we found room containers
            if room_containers and len(room_containers) > 0:
                for container in room_containers:
                    try:
                        # Extract room name - try different possible selectors with partial matching
                        room_name_selectors = [
                            "h3", 
                            "div[class*='title']", 
                            "div[class*='name']", 
                            "div[class*='RoomName']"
                        ]
                        
                        room_name = "Standard Room"  # Default
                        for name_selector in room_name_selectors:
                            try:
                                room_name_elems = container.find_elements(By.CSS_SELECTOR, name_selector)
                                if room_name_elems:
                                    room_name = room_name_elems[0].text
                                    if room_name:  # Make sure we got actual text
                                        break
                            except:
                                continue
                        
                        # Find price within this container using partial class matching
                        price_selectors = [
                            "*[class*='Price'] *[class*='price']",
                            "*[class*='price']", 
                            "*[class*='Price']",
                            "*[class*='cost']",
                            "*[class*='amount']"
                        ]
                        
                        for price_selector in price_selectors:
                            try:
                                price_elems = container.find_elements(By.CSS_SELECTOR, price_selector)
                                for elem in price_elems:
                                    price_text = elem.text
                                    if "₽" in price_text and "Prepayment" not in price_text:
                                        match = re.search(r'([\d\s,.]+)\s*₽', price_text)
                                        
                                        if match:
                                            price_str = match.group(1).replace(' ', '').replace(',', '')
                                            try:
                                                price_value = float(price_str)
                                                
                                                if price_value < lowest_price:
                                                    lowest_price = price_value
                                                    best_room = {
                                                        "room_name": room_name,
                                                        "price": price_value
                                                    }
                                                    logger.debug(f"Found room price: {price_value} ₽ for {room_name}")
                                            except ValueError:
                                                logger.debug(f"Could not convert price '{price_str}' to float")
                            except Exception as _:
                                continue
                    except Exception as room_error:
                        logger.debug(f"Error processing room: {room_error}")
                        continue
            
            # BACKUP METHOD 2: If structured approach failed, find any price on the page
            if best_room is None:
                try:
                    # First try CSS selectors with partial matching
                    generic_price_selectors = [
                        "*[class*='price']",
                        "*[class*='Price']",
                        "*[class*='cost']",
                        "*[class*='amount']"
                    ]
                    
                    price_elements = []
                    for selector in generic_price_selectors:
                        elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        price_elements.extend([e for e in elements if "₽" in e.text and "Prepayment" not in e.text])
                    
                    # If that fails, try XPath
                    if not price_elements:
                        price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '₽') and not(contains(text(), 'Prepayment'))]")
                    
                    logger.debug(f"Fallback: Found {len(price_elements)} elements containing '₽'")
                    
                    for elem in price_elements:
                        try:
                            price_text = elem.text
                            match = re.search(r'([\d\s,.]+)\s*₽', price_text)
                            
                            if match:
                                price_str = match.group(1).replace(' ', '').replace(',', '')
                                try:
                                    price_value = float(price_str)
                                    
                                    if price_value < lowest_price:
                                        lowest_price = price_value
                                        best_room = {
                                            "room_name": "Standard Room",
                                            "price": price_value
                                        }
                                        logger.debug(f"Found generic price: {price_value} ₽")
                                except ValueError:
                                    logger.error(f"Could not convert price '{price_str}' to float")
                        except Exception as price_error:
                            logger.error(f"Error processing price element: {price_error}")
                            continue
                except Exception as fallback_error:
                    logger.error(f"Error in fallback price search: {fallback_error}")
        
        # Create and return the result object with the new schema
        if best_room and lowest_price != float('inf'):
            return OstrovokHotelPrice(
                hotel_name=hotel_name,
                hotel_url=url,
                room_name=best_room["room_name"],
                hotel_price=best_room["price"],
                hotel_currency="₽",
                check_in_date=check_in_date,
                check_out_date=check_out_date,
                measurment_taken_at=measurement_taken_at
            )
        else:   
            # If no price found, return object with None/0 values
            logger.error(f"No price found for {hotel_name} from {url}")
            return OstrovokHotelPrice(
                hotel_name=hotel_name,
                hotel_url=url,
                room_name=None,
                hotel_price=0.0,
                hotel_currency="₽",
                check_in_date=check_in_date,
                check_out_date=check_out_date,
                comments="No price found",
                measurment_taken_at=measurement_taken_at
            )
            
    except Exception as e:
        logger.error(f"Error extracting price data from {url}: {e}")
        # Take screenshot for debugging
        try:
            driver.save_screenshot(f"error_screenshot_{time.strftime('%Y%m%d_%H%M%S')}.png")
            logger.debug("Screenshot saved for debugging")
        except Exception as screenshot_error:
            logger.error(f"Error saving screenshot: {screenshot_error}")
            
        # Return minimal object in case of error with the new schema
        return OstrovokHotelPrice(
            hotel_name=url.split("/")[-2].replace("_", " ").title() if "/mid" in url else "Unknown Hotel",
            hotel_url=url,
            room_name=None,
            hotel_price=0.0,
            hotel_currency=None,
            check_in_date=check_in_date,
            check_out_date=check_out_date,
            comments=f"Error: {str(e)}",
            measurment_taken_at=measurement_taken_at
        )
        
    finally:
        driver.quit()