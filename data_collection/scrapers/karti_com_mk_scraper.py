import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
import selenium
from selenium import webdriver
from selenium.webdriver.common.by import By

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False
    selenium.webdriver.common.by
    import By

from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import os
import glob
import hashlib
import logging


class KartiEventsScraper:
    def __init__(self, debug=True):
        self.base_url = "https://karti.com.mk"
        self.events_url = "https://karti.com.mk"
        self.driver = None
        self.wait = None
        self.debug = debug
        self.raw_data_dir = "../raw_data"
        self.processed_data_dir = "../processed_data"

        logging.basicConfig(
            level=logging.INFO if debug else logging.WARNING,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)

    def clean_old_files(self):
        old_raw_files = glob.glob(os.path.join(self.raw_data_dir, "karti_events_raw_*.csv"))
        old_processed_files = glob.glob(os.path.join(self.processed_data_dir, "karti_events_*.csv"))

        for file_path in old_raw_files + old_processed_files:
            os.remove(file_path)
            self.logger.info(f"Removed file: {file_path}")

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            self.driver.set_page_load_timeout(30)
            self.wait = WebDriverWait(self.driver, 10)
            self.logger.info("Chrome driver initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing driver: {e}")
            raise

    def close_driver(self):
        if self.driver:
            self.driver.quit()
            self.logger.info("Driver closed")

    def generate_event_id(self, title: str, date: str = "") -> str:
        clean_title = re.sub(r'[^\w\s]', '', title.lower())
        combined = f"{clean_title}_{date}".strip('_')
        return hashlib.md5(combined.encode()).hexdigest()

    def parse_date(self, date_text: str) -> str:
        if not date_text:
            return ""

        date_text = date_text.strip()

        mk_months = {
            'Јануари': '01', 'Февруари': '02', 'Март': '03', 'Април': '04',
            'Мај': '05', 'Јуни': '06', 'Јули': '07', 'Август': '08',
            'Септември': '09', 'Октомври': '10', 'Ноември': '11', 'Декември': '12',
            'јануари': '01', 'февруари': '02', 'март': '03', 'април': '04',
            'мај': '05', 'јуни': '06', 'јули': '07', 'август': '08',
            'септември': '09', 'октомври': '10', 'ноември': '11', 'декември': '12'
        }

        for mk_month, num_month in mk_months.items():
            if mk_month in date_text:
                parts = date_text.replace(mk_month, num_month).split()
                if len(parts) >= 3:
                    day = parts[0].zfill(2)
                    month = num_month
                    year = parts[2]
                    return f"{year}-{month}-{day}"

        return date_text

    def parse_price(self, price_text: str) -> Dict:
        result = {
            'price_text': price_text,
            'price_min': None,
            'price_max': None,
            'currency': 'MKD',
            'is_free': False
        }

        if not price_text: return result

        price_text = price_text.strip()

        if any(word in price_text.lower() for word in ['бесплатно', 'free', 'бесплатен']):
            result['is_free'] = True
            return result

        numbers = re.findall(r'\d+', price_text)
        if numbers:
            if len(numbers) == 1:
                result['price_min'] = int(numbers[0])
                result['price_max'] = int(numbers[0])
            elif len(numbers) >= 2:
                result['price_min'] = int(numbers[0])
                result['price_max'] = int(numbers[-1])

        if 'EUR' in price_text.upper() or '€' in price_text:
            result['currency'] = 'EUR'
        elif 'USD' in price_text.upper() or '$' in price_text:
            result['currency'] = 'USD'

        return result

    def extract_event_from_card(self, event_card) -> Dict:
        event_data = {
            'event_id': '',
            'url': '',
            'title': '',
            'date_start': '',
            'date_end': '',
            'time_start': '',
            'location': '',
            'venue': '',
            'ticket_url': '',
            'ticket_price_text': '',
            'price_min': None,
            'price_max': None,
            'currency': 'MKD',
            'ticket_free': False,
            'description': '',
            'category': '',
            'organizer': '',
            'image_url': '',
            'scraped_at': datetime.now().isoformat()
        }

        try:
            href = event_card.get_attribute('href')
            if href:
                if not href.startswith('http'):
                    href = self.base_url + '/' + href.lstrip('/')
                event_data['url'] = href
                event_data['ticket_url'] = href

            title_element = event_card.find_element(By.CSS_SELECTOR, ".k-event-list-event-title")
            if title_element:
                event_data['title'] = title_element.text.strip()

            date_element = event_card.find_element(By.CSS_SELECTOR, ".k-events-event-date")
            if date_element:
                date_text = date_element.text.strip()
                event_data['date_start'] = self.parse_date(date_text)

                if '-' in date_text and not date_text.startswith('http'):
                    parts = date_text.split('-')
                    if len(parts) >= 2:
                        end_part = parts[1].strip()
                        event_data['date_end'] = self.parse_date(end_part)

            venue_element = event_card.find_element(By.CSS_SELECTOR, ".k-events-venue-details")
            if venue_element:
                venue_text = venue_element.text.strip()
                event_data['venue'] = venue_text
                event_data['location'] = venue_text

            try:
                price_element = event_card.find_element(By.CSS_SELECTOR, ".cost")
                if price_element:
                    price_text = price_element.text.strip()
                    event_data['ticket_price_text'] = price_text

                    price_info = self.parse_price(price_text)
                    event_data.update(price_info)
                    event_data['ticket_free'] = price_info['is_free']
            except NoSuchElementException:
                pass

            try:
                img_element = event_card.find_element(By.CSS_SELECTOR, ".k-events-event-image img")
                if img_element:
                    img_src = img_element.get_attribute('src')
                    if img_src:
                        if not img_src.startswith('http'):
                            img_src = self.base_url + '/' + img_src.lstrip('/')
                        event_data['image_url'] = img_src
            except NoSuchElementException:
                pass

            class_attr = event_card.get_attribute('class')
            if class_attr:
                if 'concerts' in class_attr:
                    event_data['category'] = 'Концерт'
                elif 'festivals' in class_attr:
                    event_data['category'] = 'Фестивал'
                elif 'theater' in class_attr:
                    event_data['category'] = 'Театар'
                elif 'sport_events' in class_attr:
                    event_data['category'] = 'Спорт'
                elif 'philharmonic' in class_attr:
                    event_data['category'] = 'Филхармонија'
                elif 'mob' in class_attr:
                    event_data['category'] = 'Опера/Балет'
                else:
                    event_data['category'] = 'Настан'

            if event_data['title']:
                event_data['event_id'] = self.generate_event_id(
                    event_data['title'],
                    event_data['date_start']
                )

                event_data['description'] = f"{event_data['category']}: {event_data['title']}"

            return event_data

        except Exception as e:
            self.logger.error(f"Error extracting from card: {e}")
            return event_data

    def parse_description_details(self, description: str) -> Dict:
        details = {
            'parsed_price': '',
            'parsed_time': '',
            'parsed_venue': '',
            'parsed_date': '',
            'additional_prices': [],
            'contact_info': '',
            'event_type': ''
        }

        if not description:
            return details

        price_patterns = [
            r'(\d+[-–]\d+)\s*(мкд|eur|usd|денари)',
            r'(\d+)\s*(eur|usd)\s*/\s*(\d+)\s*(мкд|денари)',
            r'(\d+)\s*(мкд|eur|usd|денари)',
            r'(\d+[-–]\d+)\s*(eur|usd)',
            r'билети:\s*(\d+[-–]\d+)\s*(мкд|eur)',
            r'цена:\s*(\d+[-–]\d+)\s*(мкд|eur)',
            r'карти:\s*(\d+[-–]\d+)\s*(мкд|eur)'
        ]

        prices_found = []
        for pattern in price_patterns:
            matches = re.findall(pattern, description, re.IGNORECASE)
            for match in matches:
                if isinstance(match, tuple):
                    price_text = ' '.join(match).strip()
                else:
                    price_text = match.strip()
                if price_text not in prices_found:
                    prices_found.append(price_text)

        if prices_found:
            details['parsed_price'] = ' | '.join(prices_found)
            details['additional_prices'] = prices_found

        time_patterns = [
            r'(\d{1,2}:\d{2})',
            r'во\s*(\d{1,2}:\d{2})',
            r'почеток\s*(\d{1,2}:\d{2})',
            r'старт\s*(\d{1,2}:\d{2})'
        ]

        for pattern in time_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                details['parsed_time'] = match.group(1)
                break

        venue_patterns = [
            r'(\d{1,2}:\d{2})\s+([^А-Ш]+?)(?=[А-Ш]|$)',
            r'мкд\s+\d{1,2}:\d{2}\s+([^А-Ш\n]+)',
            r'eur\s+\d{1,2}:\d{2}\s+([^А-Ш\n]+)'
        ]

        for pattern in venue_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                venue = match.group(-1).strip()
                if len(venue) > 5 and len(venue) < 100:
                    details['parsed_venue'] = venue
                    break

        contact_patterns = [
            r'тел\.?\s*:?\s*(\d{2,3}[-\s]*\d{3}[-\s]*\d{3,4})',
            r'телефон\s*:?\s*(\d{2,3}[-\s]*\d{3}[-\s]*\d{3,4})',
            r'контакт\s*:?\s*(\d{2,3}[-\s]*\d{3}[-\s]*\d{3,4})',
            r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
            r'(www\.[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
            r'(facebook\.com/[a-zA-Z0-9._-]+)',
            r'(instagram\.com/[a-zA-Z0-9._-]+)'
        ]

        contacts = []
        for pattern in contact_patterns:
            matches = re.findall(pattern, description, re.IGNORECASE)
            contacts.extend(matches)

        if contacts:
            details['contact_info'] = ' | '.join(contacts)

        event_types = {
            'концерт': ['концерт', 'настап', 'музика', 'пејач', 'бенд'],
            'фестивал': ['фестивал', 'festival'],
            'театар': ['театар', 'претстава', 'drama', 'comedy'],
            'спорт': ['натпревар', 'фудбал', 'кошарка', 'спорт'],
            'конференција': ['форум', 'конференција', 'семинар', 'work'],
            'забава': ['парти', 'журка', 'party', 'dance']
        }

        for event_type, keywords in event_types.items():
            for keyword in keywords:
                if keyword.lower() in description.lower():
                    details['event_type'] = event_type
                    break
            if details['event_type']:
                break

        return details

    def scrape_event_details(self, event_url: str) -> Dict:
        details = {
            'description_full': '',
            'organizer': '',
            'contact_info': '',
            'additional_info': '',
            'event_details': '',
            'age_restriction': '',
            'dress_code': '',
            'ticket_info': '',
            'parsed_details': {}
        }

        if not event_url:
            return details

        try:
            self.logger.info(f"Processing: {event_url}")
            self.driver.get(event_url)
            time.sleep(3)

            description_selectors = [
                "body",
                "main",
                ".container",
                ".content",
                ".event-description",
                ".event-content",
                ".description",
                ".event-details",
                "[class*='description']",
                "[class*='content']",
                ".entry-content",
                "article",
                ".text"
            ]

            best_description = ""
            for selector in description_selectors:
                try:
                    desc_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if desc_element:
                        desc_text = desc_element.text.strip()
                        desc_text = re.sub(r'\s+', ' ', desc_text)

                        if len(desc_text) > len(best_description):
                            best_description = desc_text

                except NoSuchElementException:
                    continue

            if best_description and len(best_description) > 20:
                details['description_full'] = best_description
                self.logger.info(f"Description found: {best_description[:100]}...")

                parsed = self.parse_description_details(best_description)
                details['parsed_details'] = parsed

                if parsed['parsed_price']:
                    self.logger.info(f"Parsed prices: {parsed['parsed_price']}")
                if parsed['parsed_time']:
                    self.logger.info(f"Parsed time: {parsed['parsed_time']}")
                if parsed['parsed_venue']:
                    self.logger.info(f"Parsed venue: {parsed['parsed_venue']}")
                if parsed['contact_info']:
                    self.logger.info(f"Contact: {parsed['contact_info']}")

            organizer_selectors = [
                ".organizer",
                ".event-organizer",
                ".promoter",
                ".seller",
                ".vendor",
                "[class*='organizer']",
                "[class*='promoter']",
                ".event-info .organizer",
                ".meta .organizer",
                ".author"
            ]

            for selector in organizer_selectors:
                try:
                    org_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if org_element and org_element.text.strip():
                        org_text = org_element.text.strip()
                        if len(org_text) < 100:
                            details['organizer'] = org_text
                            self.logger.info(f"Organizer: {details['organizer']}")
                            break
                except NoSuchElementException:
                    continue

            try:
                page_source = self.driver.page_source
                if page_source and len(page_source) > 1000:
                    if HAS_BS4:
                        try:
                            soup = BeautifulSoup(page_source, 'html.parser')
                            clean_text = soup.get_text()
                            clean_text = re.sub(r'\s+', ' ', clean_text).strip()

                            if len(clean_text) > len(details.get('description_full', '')):
                                details['additional_info'] = clean_text[:5000]
                        except Exception as e:
                            self.logger.debug(f"BeautifulSoup error: {e}")
                    else:
                        clean_text = re.sub(r'<[^>]+>', '', page_source)
                        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                        details['additional_info'] = clean_text[:3000]

            except Exception as e:
                self.logger.debug(f"Cannot get additional info: {e}")

            ticket_selectors = [
                ".ticket-info",
                ".ticket-details",
                ".price-info",
                "[class*='ticket']",
                "[class*='price']",
                ".buy-ticket",
                ".purchase"
            ]

            ticket_info = []
            for selector in ticket_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in elements:
                        text = element.text.strip()
                        if text and len(text) < 500 and text not in ticket_info:
                            ticket_info.append(text)
                except NoSuchElementException:
                    continue

            if ticket_info:
                details['ticket_info'] = ' | '.join(ticket_info)
                self.logger.info(f"Ticket info: {details['ticket_info'][:100]}...")

        except Exception as e:
            self.logger.error(f"Error scraping details: {e}")

        return details

    def find_event_cards(self) -> List:
        card_selectors = [
            "a.k_event_link",
            ".k_event_link",
            "[class*='event-card']",
            "[class*='event-item']",
            ".event-container a",
            ".events-list a"
        ]

        for selector in card_selectors:
            try:
                cards = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if cards:
                    self.logger.info(f"Using selector '{selector}' - found {len(cards)} cards")
                    return cards
            except Exception as e:
                self.logger.debug(f"Selector '{selector}' failed: {e}")
                continue

        self.logger.warning("Cannot find event cards")
        return []

    def scroll_and_load_more(self):
        try:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            load_more_selectors = [
                "#show_more_events",
                ".load-more",
                ".show-more",
                "[class*='load-more']",
                "[class*='show-more']"
            ]

            for selector in load_more_selectors:
                try:
                    button = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if button.is_displayed() and button.is_enabled():
                        self.logger.info(f"Clicking Load More button")
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(3)
                        return True
                except NoSuchElementException:
                    continue

            return False

        except Exception as e:
            self.logger.error(f"Error during scroll/load more: {e}")
            return False

    def scrape_events(self, max_load_attempts: int = 3) -> List[Dict]:

        try:
            self.driver.get(self.events_url)
            time.sleep(5)

            all_events = []

            for attempt in range(max_load_attempts):

                event_cards = self.find_event_cards()

                if not event_cards:
                    break

                self.logger.info(f"Found {len(event_cards)} event cards")

                current_events = []
                for i, card in enumerate(event_cards):
                    try:
                        if self.debug and i < 3:
                            self.logger.info(f"\nCard {i + 1}")

                        event_data = self.extract_event_from_card(card)

                        if event_data['title'] and event_data['event_id']:
                            current_events.append(event_data)
                            if self.debug and i < 3:
                                self.logger.info(f"Title: {event_data['title']}")
                                self.logger.info(f"Date: {event_data['date_start']}")
                                self.logger.info(f"Venue: {event_data['venue']}")
                                self.logger.info(f"Price: {event_data['ticket_price_text']}")
                        else:
                            if self.debug and i < 3:
                                self.logger.warning(f"Card {i + 1}: No valid data")

                    except Exception as e:
                        self.logger.error(f"Error processing card {i + 1}: {e}")
                        continue

                self.logger.info(f"Attempt {attempt + 1}: Collected {len(current_events)} valid events")
                all_events.extend(current_events)

                if attempt < max_load_attempts - 1:
                    if not self.scroll_and_load_more():
                        self.logger.info("No more events to load")
                        break

            unique_events = self.remove_duplicates(all_events)
            self.logger.info(f"After removing duplicates: {len(unique_events)} unique events")

            detailed_events = self.scrape_detailed_data(unique_events)

            return detailed_events

        except Exception as e:
            self.logger.error(f"Critical error during scraping: {e}")
            return []

    def remove_duplicates(self, events: List[Dict]) -> List[Dict]:
        unique_events = []
        seen_ids = set()

        for event in events:
            event_id = event.get('event_id', '')
            if event_id and event_id not in seen_ids:
                unique_events.append(event)
                seen_ids.add(event_id)
            elif self.debug:
                self.logger.debug(f"Duplicate removed: {event.get('title', 'No title')}")

        return unique_events

    def scrape_detailed_data(self, events: List[Dict]) -> List[Dict]:
        if not events:
            return []

        self.logger.info(f"\nPhase 2: Detailed data for {len(events)} events")

        detailed_events = []
        for i, event in enumerate(events):
            self.logger.info(f"\n{i + 1}/{len(events)} - {event['title']}")

            if event.get('url'):
                try:
                    details = self.scrape_event_details(event['url'])

                    if details['description_full']:
                        event['description'] = details['description_full']
                    if details['organizer']:
                        event['organizer'] = details['organizer']
                    if details['contact_info']:
                        event['contact_info'] = details['contact_info']
                    if details['ticket_info']:
                        event['ticket_info'] = details['ticket_info']
                    if details['additional_info']:
                        event['additional_info'] = details['additional_info']

                    parsed = details.get('parsed_details', {})
                    if parsed:
                        if parsed.get('parsed_price'):
                            event['parsed_price'] = parsed['parsed_price']
                        if parsed.get('parsed_time'):
                            event['parsed_time'] = parsed['parsed_time']
                        if parsed.get('parsed_venue'):
                            event['parsed_venue'] = parsed['parsed_venue']
                        if parsed.get('contact_info'):
                            event['parsed_contact'] = parsed['contact_info']
                        if parsed.get('event_type'):
                            event['parsed_event_type'] = parsed['event_type']
                        if parsed.get('additional_prices'):
                            event['all_prices'] = ', '.join(parsed['additional_prices'])

                    event.update({k: v for k, v in details.items() if v and k != 'parsed_details'})

                except Exception as e:
                    self.logger.error(f"Error during detailed scraping: {e}")
            else:
                self.logger.info("Skipping (no valid link)")

            detailed_events.append(event)

        self.logger.info(f"\nPhase 2 complete: {len(detailed_events)} events with details")
        return detailed_events

    def save_to_csv(self, events: List[Dict], filename_suffix: str = "") -> str:
        if not events:
            self.logger.warning("No events to save")
            return ""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if filename_suffix:
            filename = f"karti_events_{filename_suffix}_{timestamp}.csv"
        else:
            filename = f"karti_events_{timestamp}.csv"

        filepath = os.path.join(self.processed_data_dir, filename)

        try:
            df = pd.DataFrame(events)
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            self.logger.info(f"Saved {len(events)} events to: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"Error saving: {e}")
            return ""

    def print_summary(self, events: List[Dict]):
        if not events:
            self.logger.info("No events to display")
            return

        self.logger.info(f"\nSUMMARY")
        self.logger.info(f"Total events: {len(events)}")

        with_description = sum(1 for e in events if e.get('description') and len(e['description']) > 50)
        with_organizer = sum(1 for e in events if e.get('organizer'))
        with_price = sum(1 for e in events if e.get('ticket_price_text'))
        with_parsed_price = sum(1 for e in events if e.get('parsed_price'))
        with_parsed_time = sum(1 for e in events if e.get('parsed_time'))
        with_venue = sum(1 for e in events if e.get('venue'))
        with_contact = sum(1 for e in events if e.get('contact_info') or e.get('parsed_contact'))
        free_events = sum(1 for e in events if e.get('ticket_free'))

        self.logger.info(f"With description: {with_description}")
        self.logger.info(f"With organizer: {with_organizer}")
        self.logger.info(f"With price: {with_price}")
        self.logger.info(f"With parsed prices: {with_parsed_price}")
        self.logger.info(f"With parsed time: {with_parsed_time}")
        self.logger.info(f"With venue: {with_venue}")
        self.logger.info(f"With contact: {with_contact}")
        self.logger.info(f"Free events: {free_events}")

        categories = {}
        for event in events:
            cat = event.get('category', 'Unknown')
            categories[cat] = categories.get(cat, 0) + 1

        self.logger.info(f"\nCategories:")
        for cat, count in categories.items():
            self.logger.info(f"  {cat}: {count}")

        parsed_types = {}
        for event in events:
            p_type = event.get('parsed_event_type', 'Unknown')
            parsed_types[p_type] = parsed_types.get(p_type, 0) + 1

        if any(t != 'Unknown' for t in parsed_types.keys()):
            self.logger.info(f"\nParsed types:")
            for p_type, count in parsed_types.items():
                if p_type != 'Unknown':
                    self.logger.info(f"  {p_type}: {count}")

        self.logger.info(f"\nFirst 3 events:")
        for i, event in enumerate(events[:3]):
            self.logger.info(f"{i + 1}. {event.get('title', 'No title')}")
            self.logger.info(f"   Date: {event.get('date_start', 'No date')}")
            self.logger.info(f"   Venue: {event.get('venue', 'No venue')}")
            self.logger.info(f"   Original price: {event.get('ticket_price_text', 'No price')}")
            if event.get('parsed_price'):
                self.logger.info(f"   Parsed prices: {event.get('parsed_price')}")
            if event.get('parsed_time'):
                self.logger.info(f"   Time: {event.get('parsed_time')}")
            if event.get('parsed_event_type'):
                self.logger.info(f"   Type: {event.get('parsed_event_type')}")
            self.logger.info(f"   URL: {event.get('url', 'No URL')}")

    def run_full_scrape(self, max_load_attempts: int = 3, save_results: bool = True) -> List[Dict]:
        try:
            self.logger.info("Starting scraping process")

            self.setup_driver()

            events = self.scrape_events(max_load_attempts)

            if save_results and events:
                self.save_to_csv(events, "final")

            self.print_summary(events)

            self.logger.info("Scraping completed")
            return events

        except Exception as e:
            self.logger.error(f"Critical error: {e}")
            return []
        finally:
            self.close_driver()


def main():
    print("Karti.com.mk Events Scraper")
    print("=" * 50)

    scraper = KartiEventsScraper(debug=True)

    try:
        scraper.clean_old_files()

        events = scraper.run_full_scrape(max_load_attempts=3, save_results=True)

        if events:
            print(f"\nSuccessfully scraped {len(events)} events")

            print("\nExamples:")
            for i, event in enumerate(events[:5]):
                print(f"{i + 1}. {event.get('title', 'No title')}")
                print(f"   Date: {event.get('date_start', 'No date')}")
                print(f"   Venue: {event.get('venue', 'Unknown')}")
                print(f"   Price: {event.get('ticket_price_text', 'No price')}")
                print(f"   Category: {event.get('category', 'No category')}")
                print(f"   URL: {event.get('url', 'No URL')}")
                print(f"   Description: {event.get('description', 'No description')[:100]}...")
                print()
        else:
            print("No events scraped. Check configuration.")

    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
    except Exception as e:
        print(f"\nUnexpected error: {e}")
    finally:
        print("\nEnd of program")


def scrape_single_event(url: str) -> Dict:
    scraper = KartiEventsScraper(debug=True)
    try:
        scraper.setup_driver()
        details = scraper.scrape_event_details(url)
        return details
    finally:
        scraper.close_driver()


def test_selectors():
    scraper = KartiEventsScraper(debug=True)
    try:
        scraper.setup_driver()
        scraper.driver.get("https://karti.com.mk")
        time.sleep(5)

        print("Testing selectors...")

        test_selectors = [
            "a.k_event_link",
            ".k_event_link",
            ".k-event-list-event-title",
            ".k-events-event-date",
            ".k-events-venue-details",
            ".cost"
        ]

        for selector in test_selectors:
            try:
                elements = scraper.driver.find_elements(By.CSS_SELECTOR, selector)
                print(f"'{selector}': {len(elements)} elements")
                if elements and len(elements) > 0:
                    print(f"   Example: {elements[0].text[:50]}...")
            except Exception as e:
                print(f"'{selector}': Error - {e}")

        cards = scraper.driver.find_elements(By.CSS_SELECTOR, "a.k_event_link")
        if cards:
            print(f"\nDetailed analysis of first card:")
            card = cards[0]
            print(f"   Tag: {card.tag_name}")
            print(f"   Class: {card.get_attribute('class')}")
            print(f"   Href: {card.get_attribute('href')}")
            print(f"   HTML: {card.get_attribute('outerHTML')[:300]}...")

            sub_elements = [
                ".k-event-list-event-title",
                ".k-events-event-date",
                ".k-events-venue-details",
                ".cost",
                "h2",
                "img"
            ]

            for sub_sel in sub_elements:
                try:
                    sub_elem = card.find_element(By.CSS_SELECTOR, sub_sel)
                    print(f"   {sub_sel}: '{sub_elem.text[:30]}'")
                except:
                    print(f"   {sub_sel}: Not found")

    finally:
        scraper.close_driver()


if __name__ == "__main__":
    main()