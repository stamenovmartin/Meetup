#!/usr/bin/env python3

import time
import re
from datetime import datetime, timedelta
from typing import List, Dict
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

            # 1. Организатор
            try:
                org_element = self.driver.find_element(By.CSS_SELECTOR, ".eps-org-name")
                if org_element and org_element.text.strip():
                    details['organizer'] = org_element.text.strip()
                    print(f"    🏢 Организатор: {details['organizer']}")
            except:
                print(f"    ⚠️ Организатор не најден")

            # 2. Duration - од времето
            try:
                time_element = self.driver.find_element(By.CSS_SELECTOR, ".event-time-label")
                if time_element and time_element.text.strip():
                    time_text = time_element.text.strip()
                    # Пример: "Sat, 13 Sep, 2025 at 02:00 pm to 11:45 pm (CEST)"
                    time_match = re.search(r'(\d{1,2}:\d{2}\s*[ap]m)\s*to\s*(\d{1,2}:\d{2}\s*[ap]m)', time_text)
                    if time_match:
                        start_time = time_match.group(1)
                        end_time = time_match.group(2)
                        details['duration'] = f"{start_time} - {end_time}"
                        print(f"    ⏱️ Времетраење: {details['duration']}")
            except:
                # Алтернативен начин - icon-duration
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
                location_elements = self.driver.find_elements(By.CSS_SELECTOR, ".event-location")
                full_location = ""
                for elem in location_elements:
                    if elem.text.strip():
                        if full_location:
                            full_location += ", " + elem.text.strip()
                        else:
                            full_location = elem.text.strip()

                if full_location:
                    details['location_full'] = full_location
                    print(f"    📍 Локација: {details['location_full'][:50]}...")
            except:
                print(f"    ⚠️ Локација не најдена")

            # 4. Категорија - од URL или content
            try:
                # Пробај да ја извлечеш од URL структурата
                if '/festivals/' in event_url:
                    details['category'] = 'festival'
                elif '/music/' in event_url:
                    details['category'] = 'music'
                elif '/concerts/' in event_url:
                    details['category'] = 'concert'
                else:
                    details['category'] = 'event'
                print(f"    🎭 Категорија: {details['category']}")
            except:
                details['category'] = 'event'

            # 5. Опис
            try:
                description_element = self.driver.find_element(By.CSS_SELECTOR, ".event-description-html")
                if description_element and description_element.text.strip():
                    desc_text = description_element.text.strip()
                    # Исчисти HTML тагови и излишни празни места
                    desc_text = re.sub(r'\s+', ' ', desc_text)
                    details['description_full'] = desc_text
                    print(f"    📝 Опис: {desc_text[:50]}...")
            except:
                print(f"    ⚠️ Опис не најден")

        except Exception as e:
            print(f"    ❌ Грешка при скрепирање: {e}")

        return details

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
                    if event_details['category']:
                        event['category'] = event_details['category']

                except Exception as e:
                    print(f"    ❌ Грешка при детално скрепирање: {e}")
            else:
                print(f"    ⏭️ Прескокнувам (нема валиден линк)")

            detailed_events.append(event)

        print(f"\n✅ ФАЗА 2 завршена: {len(detailed_events)} настани со детали")
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
                    'ticket_free': True,  # AllEvents има главно бесплатни или unknown
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

                    # Ако нема наслов, пробај од link text
                    if not event_data['title']:
                        links = item.find_elements(By.CSS_SELECTOR, 'a')
                        for link in links:
                            if link.text.strip() and len(link.text.strip()) > 5:
                                event_data['title'] = link.text.strip()
                                break
                except:
                    pass

                # Извлечи датум и време
                try:
                    # Пробај различни date селектори
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
                            # Пробај да парсираш датум
                            date_match = re.search(r'(\d{1,2})\s+(\w+)[,\s]+(\d{4})', text)
                            if date_match:
                                day, month_name, year = date_match.groups()
                                # Конвертирај во македонски формат
                                month_mapping = {
                                    'jan': 'Јануари', 'feb': 'Февруари', 'mar': 'Март', 'apr': 'Април',
                                    'may': 'Мај', 'jun': 'Јуни', 'jul': 'Јули', 'aug': 'Август',
                                    'sep': 'Септември', 'oct': 'Октомври', 'nov': 'Ноември', 'dec': 'Декември'
                                }
                                month_short = month_name.lower()[:3]
                                if month_short in month_mapping:
                                    event_data['date_start'] = f"{int(day)} {month_mapping[month_short]}"
                                    break

                            # Пробај за време
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
                    print(f"   {len(events)}. {event_data['title']}")

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

        # Processed data (според форматот на Филхармонијата)
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
                'categories': [event.get('category', 'event')],
                'organizer': event.get('organizer', ''),
                'duration': event.get('duration', '')
            }
            processed_events.append(processed_event)

        # Зачувај processed data
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