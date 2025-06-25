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

def check_room_availability(driver) -> dict:
    """
    Check if the room is available for the selected dates.
    Returns dict with availability status and reason.
    """
    try:
        # Strategy 1: Look for "от" text near the price container
        price_container = driver.find_element(
            By.CSS_SELECTOR, 
            '[data-marker="item-view/item-price-container"]'
        )
        
        # Check if there's "от" text in the price container
        container_text = price_container.text.strip()
        logger.info(f"Price container text: {container_text}")
        
        if 'от' in container_text:
            return {
                'available': False,
                'reason': 'Room not available for selected dates (shows starting price)',
                'display_text': container_text
            }
        
        # Strategy 2: More specific check - look for span with "от" before price
        try:
            # Look for spans containing "от" near the price element
            ot_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'от') and contains(@class, 'hQ3Iv')]")
            
            if ot_elements:
                # Get the full price display text
                try:
                    price_value_element = driver.find_element(By.ID, "bx_item-price-value")
                    full_text = price_value_element.text.strip()
                    logger.info(f"Found 'от' element, full price text: {full_text}")
                    
                    return {
                        'available': False,
                        'reason': 'Room not available for selected dates (от prefix found)',
                        'display_text': full_text
                    }
                except:
                    return {
                        'available': False,
                        'reason': 'Room not available for selected dates (от prefix found)',
                        'display_text': 'от [price] ₽ за сутки'
                    }
        except:
            pass
        
        # Strategy 3: JavaScript check as fallback
        try:
            script = """
            let priceContainer = document.querySelector('[data-marker="item-view/item-price-container"]');
            if (priceContainer) {
                let text = priceContainer.textContent;
                if (text.includes('от')) {
                    return {
                        available: false,
                        text: text.trim()
                    };
                }
            }
            
            // Also check for specific span with "от"
            let otSpans = document.querySelectorAll('span');
            for (let span of otSpans) {
                if (span.textContent.trim() === 'от') {
                    let parent = span.closest('[data-marker="item-view/item-price-container"]');
                    if (parent) {
                        return {
                            available: false,
                            text: parent.textContent.trim()
                        };
                    }
                }
            }
            
            return {available: true};
            """
            
            result = driver.execute_script(script)
            if result and not result.get('available', True):
                return {
                    'available': False,
                    'reason': 'Room not available for selected dates (JavaScript detection)',
                    'display_text': result.get('text', 'от [price] ₽ за сутки')
                }
                
        except Exception as e:
            logger.warning(f"JavaScript availability check failed: {str(e)}")
        
    except Exception as e:
        logger.warning(f"Availability check failed: {str(e)}")
    
    # If no unavailability indicators found, assume available
    return {
        'available': True,
        'reason': 'No unavailability indicators found',
        'display_text': None
    }

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

def extract_avito_price_targeted(driver) -> tuple[float, str, str]:
    """
    Extract price using targeted selectors based on Avito's HTML structure.
    Returns: (price, currency, full_price_text)
    """
    logger.info("Starting targeted price extraction for Avito")
    
    # First check if room is unavailable (has "от" text)
    availability_status = check_room_availability(driver)
    if not availability_status['available']:
        logger.info(f"Room not available: {availability_status['reason']}")
        return 0.0, '₽', availability_status['display_text']
    
    # Strategy 1: Use the specific data-marker for item price
    try:
        price_element = driver.find_element(
            By.CSS_SELECTOR, 
            '[data-marker="item-view/item-price"]'
        )
        
        # Try to get price from content attribute first (most reliable)
        price_content = price_element.get_attribute('content')
        if price_content:
            logger.info(f"Found price in content attribute: {price_content}")
            
            # Get currency from nearby elements
            currency = None
            try:
                currency_element = driver.find_element(
                    By.CSS_SELECTOR, 
                    '[itemprop="priceCurrency"]'
                )
                currency_content = currency_element.get_attribute('content')
                if currency_content == 'RUB':
                    currency = '₽'
                else:
                    currency = currency_content
            except:
                # Fallback to text search for currency
                parent = price_element.find_element(By.XPATH, "./..")
                if '₽' in parent.text:
                    currency = '₽'
            
            return float(price_content), currency, f"{price_content} {currency}"
            
        # If no content attribute, get from text
        price_text = price_element.text.strip()
        logger.info(f"Found price in text: {price_text}")
        
        if price_text:
            # Extract numbers and currency from text
            numbers = re.findall(r'\d+', price_text.replace('\xa0', '').replace(' ', ''))
            if numbers:
                price = float(''.join(numbers))
                currency = '₽' if '₽' in price_text else None
                return price, currency, price_text
                
    except NoSuchElementException:
        logger.warning("Could not find price using data-marker strategy")
    
    # Strategy 2: Use the price container
    try:
        price_container = driver.find_element(
            By.CSS_SELECTOR, 
            '[data-marker="item-view/item-price-container"]'
        )
        
        # Look for itemprop="price" within the container
        price_elements = price_container.find_elements(By.CSS_SELECTOR, '[itemprop="price"]')
        
        for price_element in price_elements:
            # Try content attribute first
            content = price_element.get_attribute('content')
            if content and content.isdigit():
                logger.info(f"Found price in container content: {content}")
                return float(content), '₽', f"{content} ₽"
            
            # Try text content
            text = price_element.text.strip()
            if text:
                numbers = re.findall(r'\d+', text.replace('\xa0', '').replace(' ', ''))
                if numbers:
                    price = float(''.join(numbers))
                    logger.info(f"Found price in container text: {price}")
                    return price, '₽', text
                    
    except NoSuchElementException:
        logger.warning("Could not find price using price container strategy")
    
    # Strategy 3: Use the specific ID
    try:
        price_element = driver.find_element(By.ID, "bx_item-price-value")
        
        # Look for spans with itemprop="price" inside
        price_spans = price_element.find_elements(By.CSS_SELECTOR, '[itemprop="price"]')
        
        for span in price_spans:
            content = span.get_attribute('content')
            if content and content.isdigit():
                logger.info(f"Found price by ID content: {content}")
                return float(content), '₽', f"{content} ₽"
                
            text = span.text.strip()
            if text:
                numbers = re.findall(r'\d+', text.replace('\xa0', '').replace(' ', ''))
                if numbers:
                    price = float(''.join(numbers))
                    logger.info(f"Found price by ID text: {price}")
                    return price, '₽', text
                    
    except NoSuchElementException:
        logger.warning("Could not find price using ID strategy")
    
    # Strategy 4: JavaScript extraction as fallback
    try:
        script = """
        // Try to find price using multiple methods
        let priceValue = null;
        let currency = '₽';
        
        // Method 1: data-marker
        let priceElement = document.querySelector('[data-marker="item-view/item-price"]');
        if (priceElement) {
            let content = priceElement.getAttribute('content');
            if (content) {
                priceValue = content;
            } else {
                let text = priceElement.textContent;
                let numbers = text.replace(/\\s/g, '').match(/\\d+/g);
                if (numbers) {
                    priceValue = numbers.join('');
                }
            }
        }
        
        // Method 2: itemprop="price"
        if (!priceValue) {
            let priceElements = document.querySelectorAll('[itemprop="price"]');
            for (let el of priceElements) {
                let content = el.getAttribute('content');
                if (content && content.match(/^\\d+$/)) {
                    priceValue = content;
                    break;
                } else {
                    let text = el.textContent;
                    let numbers = text.replace(/\\s/g, '').match(/\\d+/g);
                    if (numbers && numbers.length > 0) {
                        priceValue = numbers.join('');
                        break;
                    }
                }
            }
        }
        
        return {price: priceValue, currency: currency};
        """
        
        result = driver.execute_script(script)
        if result and result.get('price'):
            price = float(result['price'])
            currency = result.get('currency', '₽')
            logger.info(f"Found price using JavaScript: {price}")
            return price, currency, f"{price} {currency}"
            
    except Exception as e:
        logger.warning(f"JavaScript extraction failed: {str(e)}")
    
    # Strategy 5: Fallback to original method but more targeted
    logger.warning("Using fallback price extraction method")
    return extract_price_fallback(driver)

def extract_price_fallback(driver) -> tuple[float, str, str]:
    """Fallback method using the original approach but improved"""
    try:
        # Look for elements containing currency but exclude deposits
        price_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '₽') and not(contains(text(), 'Залог')) and not(contains(text(), 'залог'))]")
        
        prices = []
        for element in price_elements:
            text = element.text.strip()
            logger.info(f"Fallback found text: {text}")
            
            # Skip empty text or text that's just currency
            if not text or text == '₽':
                continue
                
            # Extract numbers
            numbers = re.findall(r'\d+', text.replace('\xa0', '').replace(' ', ''))
            if numbers:
                price = float(''.join(numbers))
                prices.append((price, text))
        
        if prices:
            # Sort and take smallest (likely the per-night rate)
            prices.sort(key=lambda x: x[0])
            price, text = prices[0]
            logger.info(f"Fallback extracted price: {price}")
            return price, '₽', text
            
    except Exception as e:
        logger.error(f"Fallback extraction failed: {str(e)}")
    
    return 0.0, None, None

def avito_get_price_from_avito_url(url: str, group_name: str, hotel_name: str | None = None) -> HotelPrice:
    """
    Get price from an Avito URL using improved targeting.
    
    Args:
        url (str): The URL of the Avito listing.
        group_name (str): Group name for tracking
        hotel_name (str, optional): Hotel name if known
        
    Returns:
        HotelPrice: An object containing the price and other details.
    """
    logger.debug(f"Extracting price from URL: {url}")
    
    driver = get_chrome_driver()
    
    try:
        driver.get(url)
        check_in_date, check_out_date = extract_dates_from_url(url)
        
        # Get current timestamp
        timestamp = int(datetime.now().timestamp())
        
        # Wait for the page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait specifically for price container to load
        try:
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, '[data-marker="item-view/item-price-container"]'))
            )
        except TimeoutException:
            logger.warning("Price container not found, proceeding anyway")
        
        # Get hotel name from title if not defined in call
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
        room_name = None
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
        except Exception as e:
            logger.warning(f"Could not extract room details: {str(e)}")
        
        # Extract price using improved targeting
        hotel_price, hotel_currency, price_text = extract_avito_price_targeted(driver)
        
        # Create appropriate comment based on availability
        if hotel_price == 0.0 and price_text and 'от' in price_text:
            comment = f"Room not available for selected dates ({check_in_date} to {check_out_date}). Showing starting price: {price_text}. Scraped at {datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}"
        else:
            comment = f"Scraped from Avito.ru at {datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')}. Price text: {price_text}"
        
        logger.info(f"Final extracted price: {hotel_price} {hotel_currency}")
        
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
            comments=comment,
            group_name=group_name
        )
        
    except Exception as e:
        logger.error(f"Error in avito_get_price_from_avito_url: {str(e)}")
        raise e
    finally:
        driver.quit()