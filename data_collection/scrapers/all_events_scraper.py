#!/usr/bin/env python3

import time
import re
import json
from datetime import datetime, timedelta
from typing import List, Dict, Set
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

        # Категории и клучни зборови за детекција
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
        """Генерира уникатен event_id"""
        clean_title = re.sub(r'[^\w\s]', '', title.lower())
        combined = f"{clean_title}_{date}"
        return hashlib.md5(combined.encode()).hexdigest()

    def detect_category_from_text(self, text: str) -> str:
        """Детектира категорија врз основа на текст"""
        if not text:
            return 'event'

        text_lower = text.lower()
        category_scores = {}

        # Пресметај score за секоја категорија
        for category, keywords in self.category_keywords.items():
            score = 0
            for keyword in keywords:
                if keyword.lower() in text_lower:
                    # Повисок score за подолги клучни зборови (поспецифични)
                    score += len(keyword) / 5
            category_scores[category] = score

        # Најди категорија со највисок score
        if category_scores:
            best_category = max(category_scores, key=category_scores.get)
            if category_scores[best_category] > 0:
                return best_category

        return 'event'

    def extract_json_ld_data(self, page_source: str) -> Dict:
        """Извлекува structured data (JSON-LD) од страницата"""
        json_ld_data = {}
        try:
            # Барај JSON-LD script тагови
            json_ld_pattern = r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>'
            matches = re.findall(json_ld_pattern, page_source, re.DOTALL | re.IGNORECASE)

            for match in matches:
                try:
                    data = json.loads(match.strip())
                    if isinstance(data, dict):
                        # Извлечи корисни информации
                        if '@type' in data:
                            event_type = data.get('@type', '')
                            if 'Event' in str(event_type):
                                json_ld_data['type'] = event_type
                                json_ld_data['name'] = data.get('name', '')
                                json_ld_data['description'] = data.get('description', '')
                                json_ld_data['category'] = data.get('category', '')

                                # Локација
                                if 'location' in data:
                                    location = data['location']
                                    if isinstance(location, dict):
                                        json_ld_data['location'] = location.get('name', '')

                                # Организатор
                                if 'organizer' in data:
                                    organizer = data['organizer']
                                    if isinstance(organizer, dict):
                                        json_ld_data['organizer'] = organizer.get('name', '')
                except json.JSONDecodeError:
                    continue
        except Exception as e:
            print(f"    ⚠️ Грешка при извлекување JSON-LD: {e}")

        return json_ld_data

    def detect_category_from_page(self) -> str:
        """Детектира категорија од целата страница"""
        try:
            # 1. Провери URL
            current_url = self.driver.current_url
            url_lower = current_url.lower()

            # URL patterns
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
                        print(f"    🔍 Категорија од URL: {category}")
                        return category

            # 2. Провери meta tags
            try:
                meta_keywords = self.driver.find_element(By.CSS_SELECTOR, "meta[name='keywords']")
                keywords_content = meta_keywords.get_attribute('content')
                if keywords_content:
                    detected_cat = self.detect_category_from_text(keywords_content)
                    if detected_cat != 'event':
                        print(f"    🔍 Категорија од meta keywords: {detected_cat}")
                        return detected_cat
            except:
                pass

            # 3. Провери JSON-LD structured data
            page_source = self.driver.page_source
            json_ld_data = self.extract_json_ld_data(page_source)
            if json_ld_data.get('category'):
                detected_cat = self.detect_category_from_text(json_ld_data['category'])
                if detected_cat != 'event':
                    print(f"    🔍 Категорија од JSON-LD: {detected_cat}")
                    return detected_cat

            # 4. Анализирај breadcrumbs
            try:
                breadcrumbs = self.driver.find_elements(By.CSS_SELECTOR, ".breadcrumb, [class*='breadcrumb']")
                for breadcrumb in breadcrumbs:
                    text = breadcrumb.text
                    detected_cat = self.detect_category_from_text(text)
                    if detected_cat != 'event':
                        print(f"    🔍 Категорија од breadcrumbs: {detected_cat}")
                        return detected_cat
            except:
                pass

            # 5. Анализирај наслов и опис
            try:
                # Земи наслов
                title_elem = self.driver.find_element(By.CSS_SELECTOR, "h1, .event-title, .eps-heading-1")
                title = title_elem.text if title_elem else ""

                # Земи опис
                desc_elem = self.driver.find_element(By.CSS_SELECTOR, ".event-description, .event-description-html")
                description = desc_elem.text if desc_elem else ""

                # Комбинирај текст
                combined_text = f"{title} {description}"
                detected_cat = self.detect_category_from_text(combined_text)
                if detected_cat != 'event':
                    print(f"    🔍 Категорија од содржина: {detected_cat}")
                    return detected_cat
            except:
                pass

            # 6. Анализирај tags или labels
            try:
                tags = self.driver.find_elements(By.CSS_SELECTOR,
                                                 ".tag, .label, .category, [class*='tag'], [class*='category']")
                all_tags_text = " ".join([tag.text for tag in tags])
                detected_cat = self.detect_category_from_text(all_tags_text)
                if detected_cat != 'event':
                    print(f"    🔍 Категорија од tags: {detected_cat}")
                    return detected_cat
            except:
                pass

        except Exception as e:
            print(f"    ⚠️ Грешка при детекција на категорија: {e}")

        return 'event'

    def scrape_event_details(self, event_url: str) -> Dict:
        """Влегува во линкот на настанот и скрепира детални податоци"""
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
            print(f"  📄 Влегувам во: {event_url}")
            self.driver.get(event_url)
            time.sleep(4)

            # Извлечи JSON-LD data ако постои
            page_source = self.driver.page_source
            json_ld_data = self.extract_json_ld_data(page_source)

            # 1. Организатор
            try:
                # Прво провери JSON-LD
                if json_ld_data.get('organizer'):
                    details['organizer'] = json_ld_data['organizer']
                else:
                    org_element = self.driver.find_element(By.CSS_SELECTOR,
                                                           ".eps-org-name, .organizer, [class*='organizer']")
                    if org_element and org_element.text.strip():
                        details['organizer'] = org_element.text.strip()

                if details['organizer']:
                    print(f"    🏢 Организатор: {details['organizer']}")
            except:
                print(f"    ⚠️ Организатор не најден")

            # 2. Duration - од времето
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
                        print(f"    ⏱️ Времетраење: {details['duration']}")
            except:
                try:
                    duration_elements = self.driver.find_elements(By.XPATH,
                                                                  "//i[@class='icon icon-duration']/following-sibling::span")
                    for elem in duration_elements:
                        if elem.text.strip():
                            details['duration'] = elem.text.strip()
                            print(f"    ⏱️ Времетраење: {details['duration']}")
                            break
                except:
                    print(f"    ⚠️ Времетраење не најдено")

            # 3. Полна локација
            try:
                # Прво провери JSON-LD
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
                    print(f"    📍 Локација: {details['location_full'][:50]}...")
            except:
                print(f"    ⚠️ Локација не најдена")

            # 4. КАТЕГОРИЈА - Автоматска детекција
            try:
                # Детектирај категорија од целата страница
                detected_category = self.detect_category_from_page()
                details['category'] = detected_category
                print(f"    🎭 Категорија: {details['category']}")
            except Exception as e:
                print(f"    ⚠️ Грешка при детекција на категорија: {e}")
                details['category'] = 'event'

            # 5. Опис
            try:
                # Прво провери JSON-LD
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
                    print(f"    📝 Опис: {details['description_full'][:50]}...")
            except:
                print(f"    ⚠️ Опис не најден")

        except Exception as e:
            print(f"    ❌ Грешка при скрепирање: {e}")

        return details

    def detect_category_from_card(self, item) -> str:
        """Детектира категорија од event card на главната страница"""
        try:
            # Земи целиот текст од картичката
            card_text = item.text

            # Провери за tags или labels во картичката
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

            # Анализирај целиот текст на картичката
            category = self.detect_category_from_text(card_text)
            return category

        except:
            return 'event'

    def scrape_events(self) -> List[Dict]:
        print("🔍 Скрепирам настани од AllEvents Скопје...")

        self.driver.get(self.skopje_url)
        time.sleep(8)
        self.load_all_events(max_clicks=15, wait_time=4)

        print(f"\n📋 Обработувам настани од главната страница...")

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
                print(f"   Користам селектор: {selector} - најдени {len(event_items)} items")
                break

        if not event_items:
            print("   ❌ Нема настани на страницата")
            return []

        all_events = self.extract_basic_event_data(event_items, "Главна страница")

        print(f"\n✅ ФАЗА 1 завршена: Собрани {len(all_events)} настани")

        # Отстрани дупликати
        unique_events = []
        seen_events = set()
        for event in all_events:
            event_key = f"{event['title'].lower()}_{event['date_start']}"
            if event_key not in seen_events and event['title']:
                unique_events.append(event)
                seen_events.add(event_key)

        print(f"🧹 После отстранување дупликати: {len(unique_events)} уникатни настани")

        # ФАЗА 2: Детално скрепирање
        print("\n🎬 ФАЗА 2: Собирам детални податоци...")
        detailed_events = []

        for i, event in enumerate(unique_events):
            print(f"\n🎭 {i + 1}/{len(unique_events)} - {event['title']}")

            if event['url'] and event['url'] != self.base_url:
                try:
                    event_details = self.scrape_event_details(event['url'])

                    # Ажурирај со детални податоци
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
                    print(f"    ❌ Грешка при детално скрепирање: {e}")
            else:
                print(f"    ⏭️ Прескокнувам (нема валиден линк)")

            detailed_events.append(event)

        print(f"\n✅ ФАЗА 2 завршена: {len(detailed_events)} настани со детали")

        # Печати статистика за категории
        category_stats = {}
        for event in detailed_events:
            cat = event.get('category', 'event')
            category_stats[cat] = category_stats.get(cat, 0) + 1

        print("\n📊 Статистика на категории:")
        for cat, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"   {cat}: {count} настани")

        return detailed_events

    def load_all_events(self, max_clicks=10, wait_time=5):
        clicks = 0
        while clicks < max_clicks:
            try:
                load_more_button = self.driver.find_element(By.ID, "show_more_events")
                if load_more_button.is_displayed() and load_more_button.is_enabled():
                    print(f"Кликам на 'View More' ({clicks + 1}/{max_clicks})...")
                    self.driver.execute_script("arguments[0].click();", load_more_button)
                    time.sleep(wait_time)
                    clicks += 1
                else:
                    print("Копчето 'View More' не е достапно.")
                    break
            except Exception as e:
                print("Нема повеќе 'View More' копче или грешка:", e)
                break
        print(f"Заврши со кликање на 'View More' {clicks} пати.")

    def extract_basic_event_data(self, event_items, source_name: str) -> List[Dict]:
        """Извлечи основни податоци од листа на event items"""
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

                # Извлечи URL
                try:
                    links = item.find_elements(By.CSS_SELECTOR, 'a[href*="/skopje/"]')
                    if links:
                        href = links[0].get_attribute('href')
                        if href:
                            event_data['url'] = href
                            event_data['ticket_url'] = href
                except:
                    pass

                # Извлечи наслов
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

                # Детектирај категорија од картичката
                try:
                    event_data['category'] = self.detect_category_from_card(item)
                except:
                    event_data['category'] = 'event'

                # Извлечи датум и време
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

                # Извлечи локација
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

                # Генерирај ID и додај основен опис
                if event_data['title']:
                    event_data['event_id'] = self.generate_event_id(event_data['title'], event_data['date_start'])
                    event_data['description'] = f"Настан: {event_data['title']}"

                    events.append(event_data)
                    print(f"   {len(events)}. {event_data['title']} [{event_data['category']}]")

            except Exception as e:
                continue

        print(f"   ✅ {source_name}: {len(events)} настани")
        return events

    def save_data(self, events: List[Dict]):
        if not events:
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Raw data
        df_raw = pd.DataFrame(events)
        raw_path = f"{self.raw_data_dir}/allevents_raw_{timestamp}.csv"
        df_raw.to_csv(raw_path, index=False, encoding='utf-8')

        # Processed data
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
                'category': event.get('category', 'event'),  # Единечна категорија
                'categories': [event.get('category', 'event')],  # Листа за компатибилност
                'organizer': event.get('organizer', ''),
                'duration': event.get('duration', '')
            }
            processed_events.append(processed_event)

        df_processed = pd.DataFrame(processed_events)
        processed_path = f"{self.processed_data_dir}/allevents_events_{timestamp}.csv"
        df_processed.to_csv(processed_path, index=False, encoding='utf-8')

        print(f"💾 Зачувано:")
        print(f"   📄 Raw data: {raw_path}")
        print(f"   📊 Processed data: {processed_path}")

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
        print(f"✅ Најдени {results['total_events']} настани!")
    else:
        print("❌ Нема резултати")


if __name__ == "__main__":
    main()