import time
import re
import json
from datetime import datetime, timedelta
from typing import List, Dict, Set
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os
import glob
import hashlib


class AllEventsScraper:

    def __init__(self):
        self.base_url = "https://allevents.in"
        self.skopje_url = "https://allevents.in/skopje/all"
        self.driver = None
        self.raw_data_dir = "../raw_data"
        self.processed_data_dir = "../processed_data"

        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)

        self.category_keywords = {
            'Music': [
                'music', 'concert', 'музика', 'концерт', 'band', 'бенд', 'dj', 'диџеј',
                'jazz', 'џез', 'rock', 'рок', 'pop', 'поп', 'classical', 'класична',
                'symphony', 'симфонија', 'orchestra', 'оркестар', 'piano', 'пијано',
                'guitar', 'гитара', 'violin', 'виолина', 'opera', 'опера', 'singing',
                'пеење', 'choir', 'хор', 'festival', 'фестивал', 'live', 'acoustic',
                'electronic', 'techno', 'house', 'hip-hop', 'rap', 'folk', 'фолк'
            ],
            'Sports': [
                'sport', 'спорт', 'football', 'фудбал', 'basketball', 'кошарка',
                'tennis', 'тенис', 'volleyball', 'одбојка', 'handball', 'ракомет',
                'marathon', 'маратон', 'running', 'трчање', 'fitness', 'фитнес',
                'gym', 'теретана', 'yoga', 'јога', 'swimming', 'пливање', 'match',
                'натпревар', 'championship', 'шампионат', 'tournament', 'турнир',
                'race', 'трка', 'cycling', 'велосипедизам', 'boxing', 'бокс', 'run','marathon','маратон'
            ],
            'Theater': [
                'theater', 'театар', 'theatre', 'drama', 'драма', 'play', 'претстава',
                'performance', 'изведба', 'acting', 'глума', 'stage', 'сцена',
                'comedy', 'комедија', 'tragedy', 'трагедија', 'монодрама', 'monodrama',
                'кабаре', 'cabaret', 'мјузикл', 'musical'
            ],
            'Art': [
                'art', 'уметност', 'exhibition', 'изложба', 'gallery', 'галерија',
                'museum', 'музеј', 'painting', 'сликарство', 'sculpture', 'скулптура',
                'photography', 'фотографија', 'contemporary', 'современа', 'modern',
                'модерна', 'installation', 'инсталација', 'visual', 'визуелна',
                'artist', 'уметник', 'artwork', 'дела'
            ],
            'Education': [
                'workshop', 'работилница', 'seminar', 'семинар', 'training', 'обука',
                'course', 'курс', 'lecture', 'предавање', 'conference', 'конференција',
                'masterclass', 'мастерклас', 'tutorial', 'туторијал', 'learning',
                'учење', 'education', 'едукација', 'academy', 'академија', 'school',
                'училиште', 'university', 'универзитет', 'study', 'студија'
            ],
            'Cinema': [
                'film', 'филм', 'movie', 'cinema', 'кино', 'screening', 'проекција',
                'premiere', 'премиера', 'documentary', 'документарец', 'animation',
                'анимација', 'short film', 'краток филм', 'festival', 'фестивал',
                'director', 'режисер', 'actor', 'актер'
            ],
            'Food': [
                'food', 'храна', 'restaurant', 'ресторан', 'cuisine', 'кујна',
                'tasting', 'дегустација', 'wine', 'вино', 'beer', 'пиво', 'cooking',
                'готвење', 'chef', 'готвач', 'dinner', 'вечера', 'lunch', 'ручек',
                'brunch', 'бранч', 'cocktail', 'коктел', 'gastro', 'гастро',
                'culinary', 'кулинарски', 'pizza', 'пица', 'burger', 'бургер'
            ],
            'Business': [
                'business', 'бизнис', 'networking', 'мрежење', 'startup', 'стартап',
                'entrepreneur', 'претприемач', 'innovation', 'иновација', 'tech',
                'технологија', 'it', 'marketing', 'маркетинг', 'sales', 'продажба',
                'management', 'менаџмент', 'leadership', 'лидерство', 'career',
                'кариера', 'job', 'работа', 'recruitment', 'вработување'
            ],
            'Party': [
                'party', 'забава', 'nightlife', 'ноќен живот',
                'dancing', 'танцување', 'celebration', 'прослава', 'birthday',
                'роденден', 'wedding', 'свадба', 'gathering'
            ],
            'Kids': [
                'kids', 'деца', 'children', 'family', 'семејство', 'puppet', 'кукли',
                'animation', 'анимација', 'playground', 'игралиште', 'baby', 'бебе',
                'toddler', 'малечок', 'teen', 'тинејџер', 'youth', 'младина',
                'school', 'училиште', 'kindergarten', 'градинка'
            ]
        }

    def clean_old_files(self):
        old_raw_files = glob.glob(os.path.join(self.raw_data_dir, "allevents_raw_*.csv"))
        old_processed_files = glob.glob(os.path.join(self.processed_data_dir, "allevents_events_*.csv"))

        for file_path in old_raw_files + old_processed_files:
            os.remove(file_path)

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.driver.set_page_load_timeout(30)

    def close_driver(self):
        if self.driver:
            self.driver.quit()

    def generate_event_id(self, title: str, date: str) -> str:
        clean_title = re.sub(r'[^\w\s]', '', title.lower())
        combined = f"{clean_title}_{date}"
        return hashlib.md5(combined.encode()).hexdigest()

    def detect_category_from_text(self, text: str) -> str:
        if not text:
            return 'event'

        text_lower = text.lower()
        category_scores = {}

        for category, keywords in self.category_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    score += len(keyword) / 5
            category_scores[category] = score

        if category_scores:
            best_category = max(category_scores, key=category_scores.get)
            if category_scores[best_category] > 0:
                return best_category

        return 'event'

    def extract_json_ld_data(self, page_source: str) -> Dict:
        json_ld_data = {}
        try:
            json_ld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
            matches = re.findall(json_ld_pattern, page_source, re.DOTALL | re.IGNORECASE)

            for match in matches:
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, dict):
                        if '@type' in data:
                            event_type = data.get('@type', '')
                            if 'Event' in str(event_type):
                                json_ld_data['type'] = event_type
                                json_ld_data['name'] = data.get('name', '')
                                json_ld_data['description'] = data.get('description', '')
                                json_ld_data['category'] = data.get('category', '')

                                if 'location' in data:
                                    location = data['location']
                                    if isinstance(location, dict):
                                        json_ld_data['location'] = location.get('name', '')

                                if 'organizer' in data:
                                    organizer = data['organizer']
                                    if isinstance(organizer, dict):
                                        json_ld_data['organizer'] = organizer.get('name', '')
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            print(f"    JSON-LD extraction error: {e}")

        return json_ld_data

    def detect_category_from_page(self) -> str:
        try:
            current_url = self.driver.current_url
            url_lower = current_url.lower()

            url_patterns = {
                'music': ['/music/', '/concert/', '/festival/', '/band/', '/dj/'],
                'sports': ['/sport/', '/football/', '/basketball/', '/tennis/', '/gym/'],
                'theater': ['/theater/', '/theatre/', '/drama/', '/play/'],
                'art': ['/art/', '/exhibition/', '/gallery/', '/museum/'],
                'film': ['/film/', '/movie/', '/cinema/', '/screening/'],
                'education': ['/workshop/', '/seminar/', '/training/', '/course/'],
                'food': ['/food/', '/restaurant/', '/cuisine/', '/dining/'],
                'party': ['/party/', '/club/', '/nightlife/', '/celebration/'],
                'kids': ['/kids/', '/children/', '/family/']
            }

            for category, patterns in url_patterns.items():
                for pattern in patterns:
                    if pattern in url_lower:
                        print(f"    Category from URL: {category}")
                        return category

            try:
                meta_keywords = self.driver.find_element(By.CSS_SELECTOR, "meta[name='keywords']")
                keywords_content = meta_keywords.get_attribute('content')
                if keywords_content:
                    detected_cat = self.detect_category_from_text(keywords_content)
                    if detected_cat != 'event':
                        print(f"    Category from meta keywords: {detected_cat}")
                        return detected_cat
            except:
                pass

            page_source = self.driver.page_source
            json_ld_data = self.extract_json_ld_data(page_source)
            if json_ld_data.get('category'):
                detected_cat = self.detect_category_from_text(json_ld_data['category'])
                if detected_cat != 'event':
                    print(f"    Category from JSON-LD: {detected_cat}")
                    return detected_cat

            try:
                breadcrumbs = self.driver.find_elements(By.CSS_SELECTOR, ".breadcrumb, [class*='breadcrumb']")
                for breadcrumb in breadcrumbs:
                    text = breadcrumb.text
                    detected_cat = self.detect_category_from_text(text)
                    if detected_cat != 'event':
                        print(f"    Category from breadcrumbs: {detected_cat}")
                        return detected_cat
            except:
                pass

            try:
                title_elem = self.driver.find_element(By.CSS_SELECTOR, "h1, .event-title, .eps-heading-1")
                title = title_elem.text if title_elem else ""

                desc_elem = self.driver.find_element(By.CSS_SELECTOR, ".event-description, .event-description-html")
                description = desc_elem.text if desc_elem else ""

                combined_text = f"{title} {description}"
                detected_cat = self.detect_category_from_text(combined_text)
                if detected_cat != 'event':
                    print(f"    Category from content: {detected_cat}")
                    return detected_cat
            except:
                pass

            try:
                tags = self.driver.find_elements(By.CSS_SELECTOR,
                                                 ".tag, .label, .category, [class*='tag'], [class*='category']")
                all_tags_text = " ".join([tag.text for tag in tags])
                detected_cat = self.detect_category_from_text(all_tags_text)
                if detected_cat != 'event':
                    print(f"    Category from tags: {detected_cat}")
                    return detected_cat
            except:
                pass

        except Exception as e:
            print(f"    Category detection error: {e}")

        return 'event'

    def scrape_event_details(self, event_url: str) -> Dict:
        details = {
            'description_full': '',
            'organizer': '',
            'category': '',
            'duration': '',
            'location_full': ''
        }

        if not event_url or event_url == self.base_url:
            return details

        try:
            print(f"  Processing: {event_url}")
            self.driver.get(event_url)
            time.sleep(4)

            page_source = self.driver.page_source
            json_ld_data = self.extract_json_ld_data(page_source)

            try:
                if json_ld_data.get('organizer'):
                    details['organizer'] = json_ld_data['organizer']
                else:
                    org_element = self.driver.find_element(By.CSS_SELECTOR,
                                                           ".eps-org-name, .organizer, [class*='organizer']")
                    if org_element and org_element.text.strip():
                        details['organizer'] = org_element.text.strip()

                if details['organizer']:
                    print(f"    Organizer: {details['organizer']}")
            except:
                print(f"    Organizer not found")

            try:
                time_element = self.driver.find_element(By.CSS_SELECTOR,
                                                        ".event-time-label, .event-time, [class*='time']")
                if time_element and time_element.text.strip():
                    time_text = time_element.text.strip()
                    time_match = re.search(r'(\d{1,2}:\d{2}\s*[ap]m)\s*to\s*(\d{1,2}:\d{2}\s*[ap]m)', time_text)
                    if time_match:
                        start_time = time_match.group(1)
                        end_time = time_match.group(2)
                        details['duration'] = f"{start_time} - {end_time}"
                        print(f"    Duration: {details['duration']}")
            except:
                try:
                    duration_elements = self.driver.find_elements(By.XPATH,
                                                                  "//i[@class='icon icon-duration']/following-sibling::span")
                    for elem in duration_elements:
                        if elem.text.strip():
                            details['duration'] = elem.text.strip()
                            print(f"    Duration: {details['duration']}")
                            break
                except:
                    print(f"    Duration not found")

            try:
                if json_ld_data.get('location'):
                    details['location_full'] = json_ld_data['location']
                else:
                    location_elements = self.driver.find_elements(By.CSS_SELECTOR,
                                                                  ".event-location, .venue, [class*='location'], [class*='venue']")
                    full_location = ""
                    for elem in location_elements:
                        if elem.text.strip():
                            if full_location:
                                full_location += ", " + elem.text.strip()
                            else:
                                full_location = elem.text.strip()

                    if full_location:
                        details['location_full'] = full_location

                if details['location_full']:
                    print(f"    Location: {details['location_full'][:50]}...")
            except:
                print(f"    Location not found")

            try:
                detected_category = self.detect_category_from_page()
                details['category'] = detected_category
                print(f"    Category: {details['category']}")
            except Exception as e:
                print(f"    Category detection error: {e}")
                details['category'] = 'event'

            try:
                if json_ld_data.get('description'):
                    details['description_full'] = json_ld_data['description']
                else:
                    description_element = self.driver.find_element(By.CSS_SELECTOR,
                                                                   ".event-description-html, .event-description, .description, [class*='description']")
                    if description_element and description_element.text.strip():
                        desc_text = description_element.text.strip()
                        desc_text = re.sub(r'\s+', ' ', desc_text)
                        details['description_full'] = desc_text

                if details['description_full']:
                    print(f"    Description: {details['description_full'][:50]}...")
            except:
                print(f"    Description not found")

        except Exception as e:
            print(f"    Error during scraping: {e}")

        return details

    def detect_category_from_card(self, item) -> str:
        try:
            card_text = item.text

            try:
                tags = item.find_elements(By.CSS_SELECTOR,
                                          ".tag, .label, .category, [class*='tag'], [class*='category']")
                if tags:
                    tags_text = " ".join([tag.text for tag in tags])
                    category = self.detect_category_from_text(tags_text)
                    if category != 'event':
                        return category
            except:
                pass

            category = self.detect_category_from_text(card_text)
            return category

        except:
            return 'event'

    def scrape_events(self) -> List[Dict]:
        print("Scraping events from AllEvents Skopje...")

        self.driver.get(self.skopje_url)
        time.sleep(8)
        self.load_all_events(max_clicks=15, wait_time=4)

        print(f"\nProcessing events from main page...")

        event_selectors_to_try = [
            ".event-card",
            ".eps-event-card",
            "[class*='event-card']",
            ".event-item",
            "[data-eid]"
        ]

        event_items = []
        for selector in event_selectors_to_try:
            event_items = self.driver.find_elements(By.CSS_SELECTOR, selector)
            if event_items:
                print(f"   Using selector: {selector} - found {len(event_items)} items")
                break

        if not event_items:
            print("   No events on page")
            return []

        all_events = self.extract_basic_event_data(event_items, "Main page")

        print(f"\nPhase 1 complete: Collected {len(all_events)} events")

        unique_events = []
        seen_events = set()
        for event in all_events:
            event_key = f"{event['title'].lower()}_{event['date_start']}"
            if event_key not in seen_events and event['title']:
                unique_events.append(event)
                seen_events.add(event_key)

        print(f"After removing duplicates: {len(unique_events)} unique events")

        print("\nPhase 2: Collecting detailed data...")
        detailed_events = []

        for i, event in enumerate(unique_events):
            print(f"\n{i + 1}/{len(unique_events)} - {event['title']}")

            if event['url'] and event['url'] != self.base_url:
                try:
                    event_details = self.scrape_event_details(event['url'])

                    if event_details['description_full']:
                        event['description'] = event_details['description_full']
                    if event_details['organizer']:
                        event['organizer'] = event_details['organizer']
                    if event_details['duration']:
                        event['duration'] = event_details['duration']
                    if event_details['location_full']:
                        event['location'] = event_details['location_full']
                    if event_details['category'] and event_details['category'] != 'event':
                        event['category'] = event_details['category']

                except Exception as e:
                    print(f"    Error during detailed scraping: {e}")
            else:
                print(f"    Skipping (no valid link)")

            detailed_events.append(event)

        print(f"\nPhase 2 complete: {len(detailed_events)} events with details")

        category_stats = {}
        for event in detailed_events:
            cat = event.get('category', 'event')
            category_stats[cat] = category_stats.get(cat, 0) + 1

        print("\nCategory statistics:")
        for cat, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"   {cat}: {count} events")

        return detailed_events

    def load_all_events(self, max_clicks=10, wait_time=5):
        clicks = 0
        while clicks < max_clicks:
            try:
                load_more_button = self.driver.find_element(By.ID, "show_more_events")
                if load_more_button.is_displayed() and load_more_button.is_enabled():
                    print(f"Clicking 'View More' ({clicks + 1}/{max_clicks})...")
                    self.driver.execute_script("arguments[0].click();", load_more_button)
                    time.sleep(wait_time)
                    clicks += 1
                else:
                    print("'View More' button not available.")
                    break
            except Exception as e:
                print("No more 'View More' button or error:", e)
                break
        print(f"Finished clicking 'View More' {clicks} times.")

    def extract_basic_event_data(self, event_items, source_name: str) -> List[Dict]:
        events = []

        for i, item in enumerate(event_items):
            try:
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
                    'category': 'event',
                    'organizer': '',
                    'duration': ''
                }

                try:
                    links = item.find_elements(By.CSS_SELECTOR, 'a[href*="/skopje/"]')
                    if links:
                        href = links[0].get_attribute('href')
                        if href:
                            event_data['url'] = href
                            event_data['ticket_url'] = href
                except:
                    pass

                try:
                    title_selectors = [
                        ".eps-heading-1",
                        ".event-title",
                        "h1", "h2", "h3",
                        "[class*='title']"
                    ]

                    for selector in title_selectors:
                        title_elements = item.find_elements(By.CSS_SELECTOR, selector)
                        if title_elements and title_elements[0].text.strip():
                            event_data['title'] = title_elements[0].text.strip()
                            break

                    if not event_data['title']:
                        links = item.find_elements(By.CSS_SELECTOR, 'a')
                        for link in links:
                            if link.text.strip() and len(link.text.strip()) > 5:
                                event_data['title'] = link.text.strip()
                                break
                except:
                    pass

                try:
                    event_data['category'] = self.detect_category_from_card(item)
                except:
                    event_data['category'] = 'event'

                try:
                    date_selectors = [
                        "[class*='date']",
                        "[class*='time']",
                        ".event-date",
                        ".eps-heading-2"
                    ]

                    for selector in date_selectors:
                        date_elements = item.find_elements(By.CSS_SELECTOR, selector)
                        for elem in date_elements:
                            text = elem.text.strip()
                            date_match = re.search(r'(\d{1,2})\s+(\w+)[,\s]+(\d{4})', text)
                            if date_match:
                                day, month_name, year = date_match.groups()
                                month_mapping = {
                                    'jan': 'Јануари', 'feb': 'Февруари', 'mar': 'Март', 'apr': 'Април',
                                    'may': 'Мај', 'jun': 'Јуни', 'jul': 'Јули', 'aug': 'Август',
                                    'sep': 'Септември', 'oct': 'Октомври', 'nov': 'Ноември', 'dec': 'Декември'
                                }
                                month_short = month_name.lower()[:3]
                                if month_short in month_mapping:
                                    event_data['date_start'] = f"{int(day)} {month_mapping[month_short]}"
                                    break

                            time_match = re.search(r'(\d{1,2}:\d{2}\s*[ap]m)', text)
                            if time_match:
                                event_data['time_start'] = time_match.group(1)

                        if event_data['date_start']:
                            break
                except:
                    pass

                try:
                    location_selectors = [
                        ".event-location",
                        "[class*='location']",
                        "[class*='venue']"
                    ]

                    for selector in location_selectors:
                        location_elements = item.find_elements(By.CSS_SELECTOR, selector)
                        if location_elements and location_elements[0].text.strip():
                            event_data['location'] = location_elements[0].text.strip()
                            break
                except:
                    pass

                if event_data['title']:
                    event_data['event_id'] = self.generate_event_id(event_data['title'], event_data['date_start'])
                    event_data['description'] = f"Настан: {event_data['title']}"

                    events.append(event_data)
                    print(f"   {len(events)}. {event_data['title']} [{event_data['category']}]")

            except Exception as e:
                continue

        print(f"   {source_name}: {len(events)} events")
        return events

    def save_data(self, events: List[Dict]):
        if not events:
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        df_raw = pd.DataFrame(events)
        raw_path = f"{self.raw_data_dir}/allevents_raw_{timestamp}.csv"
        df_raw.to_csv(raw_path, index=False, encoding='utf-8')

        processed_events = []
        for event in events:
            processed_event = {
                'event_id': event.get('event_id', ''),
                'url': event.get('url', ''),
                'title': event.get('title', ''),
                'date_start': event.get('date_start', ''),
                'time_start': event.get('time_start', ''),
                'location': event.get('location', ''),
                'ticket_url': event.get('ticket_url', ''),
                'ticket_price_text': event.get('ticket_price_text', ''),
                'ticket_free': event.get('ticket_free', True),
                'description': event.get('description', ''),
                'category': event.get('category', 'event'),
                'categories': [event.get('category', 'event')],
                'organizer': event.get('organizer', ''),
                'duration': event.get('duration', '')
            }
            processed_events.append(processed_event)

        df_processed = pd.DataFrame(processed_events)
        processed_path = f"{self.processed_data_dir}/allevents_events_{timestamp}.csv"
        df_processed.to_csv(processed_path, index=False, encoding='utf-8')

        print(f"Saved:")
        print(f"   Raw data: {raw_path}")
        print(f"   Processed data: {processed_path}")

    def run_scraping(self) -> Dict:
        self.clean_old_files()
        self.setup_driver()

        events = self.scrape_events()

        if events:
            self.save_data(events)

        self.close_driver()

        return {
            'events': events,
            'total_events': len(events),
            'scraping_timestamp': datetime.now().isoformat()
        }


def main():
    scraper = AllEventsScraper()
    results = scraper.run_scraping()

    if results['total_events'] > 0:
        print(f"Found {results['total_events']} events!")
    else:
        print("No results")


if __name__ == "__main__":
    main()