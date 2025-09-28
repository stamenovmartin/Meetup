#!/usr/bin/env python3

import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import os
import glob
import hashlib
import logging


class ITEventsScraper:

    def __init__(self, debug=True):
        self.base_url = "https://it.mk"
        self.it_nastan_url = "https://it.mk/tag/it-nastan/"
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
        old_raw_files = glob.glob(os.path.join(self.raw_data_dir, "it_events_raw_*.csv"))
        old_processed_files = glob.glob(os.path.join(self.processed_data_dir, "it_events_*.csv"))

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
        chrome_options.add_argument("--disable-images")
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

    def debug_page_structure(self):
        if not self.debug:
            return

        try:
            self.logger.info("Analyzing page structure...")

            all_articles = self.driver.find_elements(By.TAG_NAME, "article")
            self.logger.info(f"Total <article> elements: {len(all_articles)}")

            selectors_to_test = [
                "article.post.category-it-nastan",
                "article.post",
                "article",
                ".post",
                "[class*='post']",
                "[class*='event']",
                "[class*='nastan']"
            ]

            for selector in selectors_to_test:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                self.logger.info(f"'{selector}': {len(elements)} elements")

            for i, article in enumerate(all_articles[:2]):
                self.logger.info(f"\n--- Article {i + 1} ---")
                self.logger.info(f"Classes: {article.get_attribute('class')}")

                title_elements = article.find_elements(By.CSS_SELECTOR,
                                                       "h1, h2, h3, .entry-title, .post-title, [class*='title']")
                for j, title_el in enumerate(title_elements):
                    text = title_el.text.strip()
                    if text:
                        self.logger.info(
                            f"  Title {j + 1}: '{text}' (tag: {title_el.tag_name}, class: {title_el.get_attribute('class')})")

                links = article.find_elements(By.TAG_NAME, "a")
                for j, link in enumerate(links[:3]):
                    href = link.get_attribute('href')
                    text = link.text.strip()
                    if href and 'it-nastan' in href:
                        self.logger.info(f"  Link {j + 1}: '{text}' -> {href}")

                self.logger.info(f"  HTML preview: {article.get_attribute('outerHTML')[:200]}...")

        except Exception as e:
            self.logger.error(f"Error during debug: {e}")

    def extract_title_robust(self, item) -> str:
        title_strategies = [
            lambda: item.find_element(By.CSS_SELECTOR, ".entry-title").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, ".post-title").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "h1").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "h2").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "h3").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "[class*='title']").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "a").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "a").get_attribute("title"),
            lambda: item.find_element(By.CSS_SELECTOR, ".entry-header").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "header").text.strip(),
        ]

        for i, strategy in enumerate(title_strategies):
            try:
                title = strategy()
                if title and len(title.strip()) > 3:
                    if self.debug:
                        self.logger.debug(f"  Title found with strategy {i + 1}: '{title}'")
                    return title.strip()
            except (NoSuchElementException, Exception):
                continue

        try:
            full_text = item.text.strip()
            if full_text and len(full_text) > 3:
                first_line = full_text.split('\n')[0].strip()
                if len(first_line) > 3:
                    self.logger.debug(f"  Using first line as title: '{first_line}'")
                    return first_line
        except:
            pass

        return ""

    def extract_url_robust(self, item) -> str:
        url_strategies = [
            lambda: item.find_element(By.CSS_SELECTOR, 'a[href*="/it-nastan/"]').get_attribute('href'),
            lambda: item.find_element(By.CSS_SELECTOR, 'a[href*="it-nastan"]').get_attribute('href'),
            lambda: item.find_element(By.CSS_SELECTOR, 'a').get_attribute('href'),
        ]

        for strategy in url_strategies:
            try:
                url = strategy()
                if url and 'it-nastan' in url:
                    return url
            except (NoSuchElementException, Exception):
                continue
        return ""

    def extract_date_robust(self, item) -> str:
        date_strategies = [
            lambda: item.find_element(By.CSS_SELECTOR, ".entry-date").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, ".post-date").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, ".date").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "[class*='date']").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "time").text.strip(),
            lambda: item.find_element(By.CSS_SELECTOR, "time").get_attribute('datetime'),
        ]

        for strategy in date_strategies:
            try:
                date = strategy()
                if date and len(date.strip()) > 0:
                    return date.strip()
            except (NoSuchElementException, Exception):
                continue
        return ""

    def scrape_event_details(self, event_url: str) -> Dict:
        details = {
            'description_full': '',
            'organizer': '',
            'category': '',
            'duration': '',
            'location_full': '',
            'image_url': '',
            'contact_info': '',
            'ticket_price_text': '',
            'ticket_free': True,
            'ticket_price_numeric': None
        }

        if not event_url or event_url == self.base_url:
            return details

        try:
            self.logger.info(f"  Processing: {event_url}")
            self.driver.get(event_url)
            time.sleep(3)

            organizer_selectors = [".author", ".organizer", ".post-author", "[class*='author']", ".entry-meta"]
            for selector in organizer_selectors:
                try:
                    org_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if org_element and org_element.text.strip():
                        details['organizer'] = org_element.text.strip()
                        self.logger.info(f"    Organizer: {details['organizer']}")
                        break
                except:
                    continue

            description_selectors = [
                ".entry-content",
                ".post-content",
                ".content",
                "[class*='content']",
                "article .text",
                ".description"
            ]
            for selector in description_selectors:
                try:
                    desc_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    if desc_element and desc_element.text.strip():
                        desc_text = desc_element.text.strip()
                        desc_text = re.sub(r'\s+', ' ', desc_text)
                        if len(desc_text) > 50:
                            details['description_full'] = desc_text
                            self.logger.info(f"    Description: {desc_text[:100]}...")
                            break
                except:
                    continue

            image_selectors = [
                ".post-thumbnail img",
                ".featured-image img",
                "article img",
                ".entry-content img"
            ]
            for selector in image_selectors:
                try:
                    img_element = self.driver.find_element(By.CSS_SELECTOR, selector)
                    img_src = img_element.get_attribute('src')
                    if img_src and img_src.startswith('http'):
                        details['image_url'] = img_src
                        self.logger.info(f"    Image: {img_src}")
                        break
                except:
                    continue

            price_info = self.extract_ticket_price_info()
            details.update(price_info)

        except Exception as e:
            self.logger.error(f"    Error scraping details: {e}")

        return details

    def extract_ticket_price_info(self) -> Dict:
        price_info = {
            'ticket_price_text': '',
            'ticket_free': True,
            'ticket_price_numeric': None
        }

        try:
            page_text = self.driver.find_element(By.TAG_NAME, "body").text.lower()

            free_keywords = [
                'бесплатно', 'free', 'без надомест', 'бесплатен влез',
                'no cost', 'без плаќање', 'бесплатна регистрација'
            ]

            for keyword in free_keywords:
                if keyword in page_text:
                    price_info['ticket_price_text'] = 'Бесплатно'
                    price_info['ticket_free'] = True
                    self.logger.info(f"    Price: Free (found: '{keyword}')")
                    return price_info

            price_selectors = [
                ".price", ".ticket-price", ".cost", "[class*='price']",
                ".entry-price", ".event-price", "[class*='cost']",
                ".registration-fee", ".admission", "[class*='fee']"
            ]

            for selector in price_selectors:
                try:
                    price_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    for element in price_elements:
                        price_text = element.text.strip()
                        if price_text and self.contains_price_info(price_text):
                            extracted_price = self.parse_price_text(price_text)
                            if extracted_price:
                                price_info.update(extracted_price)
                                self.logger.info(f"    Price: {price_info['ticket_price_text']}")
                                return price_info
                except:
                    continue

            price_patterns = [
                r'(?:цена|price|cost|билет)[\s:]*(\d+(?:[,.]?\d+)?)\s*(?:ден|mkd|евра?|eur|€|\$)',
                r'(\d+(?:[,.]?\d+)?)\s*(?:ден|mkd|евра?|eur|€|\$)(?:\s*(?:за билет|per ticket|цена))?',
                r'(?:регистрација|registration)[\s:]*(\d+(?:[,.]?\d+)?)\s*(?:ден|mkd|евра?|eur|€|\$)',
                r'(?:влез|entrance|admission)[\s:]*(\d+(?:[,.]?\d+)?)\s*(?:ден|mkd|евра?|eur|€|\$)'
            ]

            for pattern in price_patterns:
                matches = re.findall(pattern, page_text, re.IGNORECASE)
                if matches:
                    price_num = matches[0].replace(',', '.')
                    try:
                        price_numeric = float(price_num)
                        currency = self.detect_currency_from_context(page_text, price_num)

                        price_info['ticket_price_text'] = f"{price_num} {currency}"
                        price_info['ticket_free'] = False
                        price_info['ticket_price_numeric'] = price_numeric

                        self.logger.info(f"    Price: {price_info['ticket_price_text']} (regex)")
                        return price_info
                    except ValueError:
                        continue

            self.logger.info(f"    Price: No information found (default: free)")

        except Exception as e:
            self.logger.error(f"    Error extracting price: {e}")

        return price_info

    def contains_price_info(self, text: str) -> bool:
        price_indicators = [
            'ден', 'mkd', 'евра', 'eur', '€', '$', 'цена', 'price',
            'cost', 'билет', 'ticket', 'регистрација', 'registration',
            'влез', 'entrance', 'admission', 'fee'
        ]

        text_lower = text.lower()
        has_number = re.search(r'\d+', text)
        has_price_word = any(word in text_lower for word in price_indicators)

        return has_number and has_price_word

    def parse_price_text(self, price_text: str) -> Dict:
        try:
            number_match = re.search(r'(\d+(?:[,.]?\d+)?)', price_text)
            if not number_match:
                return None

            price_num = number_match.group(1).replace(',', '.')
            price_numeric = float(price_num)

            currency = self.detect_currency_from_context(price_text, price_num)

            return {
                'ticket_price_text': f"{price_num} {currency}",
                'ticket_free': False,
                'ticket_price_numeric': price_numeric
            }

        except (ValueError, AttributeError):
            return None

    def detect_currency_from_context(self, text: str, price_num: str) -> str:
        text_lower = text.lower()

        if any(curr in text_lower for curr in ['eur', '€', 'евра', 'евро']):
            return 'EUR'
        elif any(curr in text_lower for curr in ['$', 'usd', 'dollar']):
            return 'USD'
        elif any(curr in text_lower for curr in ['ден', 'mkd', 'денар']):
            return 'MKD'
        else:
            price_val = float(price_num.replace(',', '.'))
            if price_val > 100:
                return 'MKD'
            else:
                return 'EUR'

    def find_event_containers(self) -> List:
        container_selectors = [
            "article.post.category-it-nastan",
            "article.post",
            "article[class*='nastan']",
            "article[class*='event']",
            ".post.category-it-nastan",
            ".post",
            "[class*='event-item']",
            "[class*='post-item']"
        ]

        for selector in container_selectors:
            try:
                containers = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if containers:
                    valid_containers = []
                    for container in containers:
                        links = container.find_elements(By.TAG_NAME, "a")
                        for link in links:
                            href = link.get_attribute('href')
                            if href and 'it-nastan' in href:
                                valid_containers.append(container)
                                break

                    if valid_containers:
                        self.logger.info(
                            f"Using selector '{selector}' - found {len(valid_containers)} containers")
                        return valid_containers

            except Exception as e:
                self.logger.debug(f"Selector '{selector}' failed: {e}")
                continue

        self.logger.warning("Using alternative approach - searching direct links")
        return self.find_events_by_links()

    def find_events_by_links(self) -> List:
        try:
            event_links = self.driver.find_elements(By.CSS_SELECTOR, 'a[href*="it-nastan"]')

            containers = []
            seen_parents = set()

            for link in event_links:
                try:
                    parent = link
                    for _ in range(5):
                        parent = parent.find_element(By.XPATH, "..")
                        tag = parent.tag_name.lower()
                        if tag in ['article', 'div'] and parent not in seen_parents:
                            containers.append(parent)
                            seen_parents.add(parent)
                            break
                except:
                    continue

            self.logger.info(f"Found {len(containers)} containers via links")
            return containers

        except Exception as e:
            self.logger.error(f"Error in alternative approach: {e}")
            return []

    def extract_basic_event_data(self, event_items, source_name: str) -> List[Dict]:
        events = []

        self.logger.info(f"{source_name}: Processing {len(event_items)} elements...")

        for i, item in enumerate(event_items):
            try:
                if self.debug and i < 3:
                    self.logger.info(f"\n--- Element {i + 1} ---")
                    self.logger.info(f"Tag: {item.tag_name}, Class: {item.get_attribute('class')}")

                event_data = {
                    'event_id': '',
                    'url': '',
                    'title': '',
                    'date_start': '',
                    'time_start': '',
                    'location': '',
                    'ticket_url': '',
                    'ticket_price_text': '',
                    'ticket_free': True,
                    'description': '',
                    'category': 'IT Event',
                    'organizer': '',
                    'duration': '',
                    'image_url': '',
                    'scraped_at': datetime.now().isoformat()
                }

                url = self.extract_url_robust(item)
                if url:
                    event_data['url'] = url
                    event_data['ticket_url'] = url

                title = self.extract_title_robust(item)
                if title:
                    event_data['title'] = title

                date = self.extract_date_robust(item)
                if date:
                    event_data['date_start'] = date

                if event_data['title']:
                    event_data['event_id'] = self.generate_event_id(
                        event_data['title'],
                        event_data['date_start']
                    )
                    event_data['description'] = f"IT настан: {event_data['title']}"

                    events.append(event_data)
                    self.logger.info(f"   {len(events)}. {event_data['title']}")

                    if self.debug:
                        self.logger.info(f"      URL: {event_data['url']}")
                        self.logger.info(f"      Date: {event_data['date_start']}")
                else:
                    if self.debug:
                        self.logger.warning(f"   Element {i + 1}: No valid title")

            except Exception as e:
                self.logger.error(f"   Error processing element {i + 1}: {e}")
                continue

        self.logger.info(f"   {source_name}: Collected {len(events)} valid events")
        return events

    def scrape_events(self, max_pages: int = 50) -> List[Dict]:
        self.logger.info("Starting event scraping from it.mk/tag/it-nastan/...")

        try:
            self.driver.get(self.it_nastan_url)
            self.logger.info(f"Loaded page: {self.it_nastan_url}")
            time.sleep(5)

            self.debug_page_structure()

            all_events = []
            page_num = 1

            while page_num <= max_pages:
                self.logger.info(f"\nPage {page_num}/{max_pages}")

                event_items = self.find_event_containers()

                if not event_items:
                    self.logger.warning(f"No events on page {page_num}")
                    break

                page_events = self.extract_basic_event_data(event_items, f"Page {page_num}")
                all_events.extend(page_events)

                try:
                    next_selectors = [
                        "a.nextpostslink",
                        ".next-page",
                        ".pagination .next",
                        "a[rel='next']",
                        ".nav-next a"
                    ]

                    next_link = None
                    for selector in next_selectors:
                        try:
                            next_link = self.driver.find_element(By.CSS_SELECTOR, selector)
                            if next_link.is_displayed() and next_link.is_enabled():
                                break
                            next_link = None
                        except:
                            continue

                    if next_link:
                        next_href = next_link.get_attribute('href')
                        self.logger.info(f"   Going to next page: {next_href}")
                        self.driver.get(next_href)
                        time.sleep(5)
                        page_num += 1
                    else:
                        self.logger.info("   No next page")
                        break

                except Exception as e:
                    self.logger.error(f"   Navigation error: {e}")
                    break

            self.logger.info(f"\nPhase 1 complete: Collected {len(all_events)} events from {page_num} pages")

            unique_events = self.remove_duplicates(all_events)
            self.logger.info(f"After removing duplicates: {len(unique_events)} unique events")

            detailed_events = self.scrape_detailed_data(unique_events)

            return detailed_events

        except Exception as e:
            self.logger.error(f"Critical error during scraping: {e}")
            return []

    def remove_duplicates(self, events: List[Dict]) -> List[Dict]:
        unique_events = []
        seen_events = set()

        for event in events:
            key_parts = [
                event.get('title', '').lower().strip(),
                event.get('date_start', '').strip(),
                event.get('url', '').strip()
            ]
            event_key = '|'.join(key_parts)

            if event_key not in seen_events and event.get('title'):
                unique_events.append(event)
                seen_events.add(event_key)
            else:
                self.logger.debug(f"Duplicate removed: {event.get('title', 'No title')}")

        return unique_events

    def scrape_detailed_data(self, events: List[Dict]) -> List[Dict]:
        if not events:
            return []

        self.logger.info(f"\nPhase 2: Detailed data for {len(events)} events")

        detailed_events = []
        for i, event in enumerate(events):
            self.logger.info(f"\n{i + 1}/{len(events)} - {event['title']}")

            if event.get('url') and event['url'] != self.base_url:
                try:
                    details = self.scrape_event_details(event['url'])

                    if details['description_full']:
                        event['description'] = details['description_full']
                    if details['organizer']:
                        event['organizer'] = details['organizer']
                    if details['image_url']:
                        event['image_url'] = details['image_url']

                    event.update({k: v for k, v in details.items() if v})

                except Exception as e:
                    self.logger.error(f"    Error during detailed scraping: {e}")
            else:
                self.logger.info("    Skipping (no valid link)")

            detailed_events.append(event)

        self.logger.info(f"\nPhase 2 complete: {len(detailed_events)} events with details")
        return detailed_events

    def save_to_csv(self, events: List[Dict], filename_suffix: str = "") -> str:
        if not events:
            self.logger.warning("No events to save")
            return ""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        if filename_suffix:
            filename = f"it_events_{filename_suffix}_{timestamp}.csv"
        else:
            filename = f"it_events_{timestamp}.csv"

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
        with_date = sum(1 for e in events if e.get('date_start'))
        with_image = sum(1 for e in events if e.get('image_url'))

        self.logger.info(f"With description: {with_description}")
        self.logger.info(f"With organizer: {with_organizer}")
        self.logger.info(f"With date: {with_date}")
        self.logger.info(f"With image: {with_image}")

        self.logger.info(f"\nFirst 3 events:")
        for i, event in enumerate(events[:3]):
            self.logger.info(f"{i + 1}. {event.get('title', 'No title')}")
            self.logger.info(f"   Date: {event.get('date_start', 'No date')}")
            self.logger.info(f"   URL: {event.get('url', 'No URL')}")

    def run_full_scrape(self, max_pages: int = 50, save_results: bool = True) -> List[Dict]:
        try:
            self.logger.info("Starting scraping process")

            self.setup_driver()

            events = self.scrape_events(max_pages)

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
    print("IT Events Scraper - Enhanced Version")
    print("=" * 50)

    scraper = ITEventsScraper(debug=True)

    try:
        scraper.clean_old_files()

        events = scraper.run_full_scrape(max_pages=50, save_results=True)

        if events:
            print(f"\nSuccessfully scraped {len(events)} events")

            print("\nExamples:")
            for i, event in enumerate(events[:3]):
                print(f"{i + 1}. {event.get('title', 'No title')}")
                print(f"   Date: {event.get('date_start', 'No date')}")
                print(f"   Organizer: {event.get('organizer', 'Unknown')}")
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


if __name__ == "__main__":
    main()