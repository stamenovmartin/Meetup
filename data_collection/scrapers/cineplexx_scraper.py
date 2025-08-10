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


class CineplexxScraper:

    def __init__(self, target_date: str = None):
        self.base_url = "https://www.cineplexx.mk"
        self.driver = None
        self.raw_data_dir = "../raw_data"
        self.processed_data_dir = "../processed_data"

        if target_date:
            self.target_date = datetime.strptime(target_date, '%Y-%m-%d')
        else:
            self.target_date = datetime.now() + timedelta(days=1)

        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)

    def clean_old_files(self):
        old_raw_files = glob.glob(os.path.join(self.raw_data_dir, "cineplexx_raw_*.csv"))
        old_processed_files = glob.glob(os.path.join(self.processed_data_dir, "cineplexx_events_*.csv"))

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

    def scrape_movie_details(self, movie_url: str) -> Dict:
        """Влегува во линкот на филмот и скрепира само опис"""
        details = {'description_full': ''}

        if not movie_url or movie_url == self.base_url:
            return details

        try:
            print(f"  📄 Влегувам во: {movie_url}")
            self.driver.get(movie_url)
            time.sleep(3)

            # Опис на филмот
            description_selectors = [
                ".movie-description",
                ".film-synopsis",
                ".movie-synopsis",
                "[class*='synopsis']",
                "[class*='description']"
            ]

            for selector in description_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements and elements[0].text.strip():
                        desc_text = elements[0].text.strip()
                        if len(desc_text) > 20:
                            details['description_full'] = desc_text
                            print(f"    📝 Опис: {desc_text[:50]}...")
                            break
                except:
                    continue

        except Exception as e:
            print(f"    ❌ Грешка при скрепирање: {e}")

        return details

    def scrape_movies(self) -> List[Dict]:
        self.driver.get(self.base_url)
        time.sleep(8)

        # ФАЗА 1: Собери ги сите основни податоци од СИТЕ ТАБОВИ
        print("🔍 ФАЗА 1: Собирам основни податоци од сите табови...")
        all_basic_movies = []

        # Дефинирај табови за проверка
        tabs_to_try = [
            ("//span[contains(text(), 'Препорачано')]/..", "Препорачано"),
            ("//span[contains(text(), 'Во кино')]/..", "Во кино"),
            ("//span[contains(text(), 'Наскоро')]/..", "Наскоро")
        ]

        selectors_to_try = [
            ".movie-list ul.l-entity li.l-entity__item",
            ".movie-list li",
            ".l-entity__item"
        ]

        # Прво земи филмови од тековниот приказ (без кликање табови)
        print("📋 Собирам од почетниот приказ...")
        movie_items = []
        for selector in selectors_to_try:
            movie_items = self.driver.find_elements(By.CSS_SELECTOR, selector)
            if movie_items:
                print(f"   Користам селектор: {selector} - најдени {len(movie_items)} items")
                break

        if movie_items:
            initial_movies = self.extract_basic_movie_data(movie_items, "Почетен приказ")
            all_basic_movies.extend(initial_movies)

        # Потоа земи филмови од секој таб
        for tab_xpath, tab_name in tabs_to_try:
            try:
                print(f"\n🎭 Собирам од таб: {tab_name}")

                # Најди и кликни на табот
                tab_elements = self.driver.find_elements(By.XPATH, tab_xpath)
                if not tab_elements:
                    print(f"   ⚠️ Табот '{tab_name}' не е најден")
                    continue

                # Кликни на табот
                self.driver.execute_script("arguments[0].click();", tab_elements[0])
                time.sleep(4)

                # Земи филмови од овој таб
                movie_items = []
                for selector in selectors_to_try:
                    movie_items = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if movie_items:
                        print(f"   Користам селектор: {selector} - најдени {len(movie_items)} items")
                        break

                if movie_items:
                    tab_movies = self.extract_basic_movie_data(movie_items, tab_name)
                    all_basic_movies.extend(tab_movies)
                else:
                    print(f"   ❌ Нема филмови во табот '{tab_name}'")

            except Exception as e:
                print(f"   ❌ Грешка со таб '{tab_name}': {e}")
                continue

        print(f"\n✅ ФАЗА 1 завршена: Собрани {len(all_basic_movies)} филмови од сите табови")

        # Отстрани дупликати пред детално скрепирање
        unique_basic_movies = []
        seen_movies = set()
        for movie in all_basic_movies:
            movie_id = f"{movie['title'].lower()}_{movie['date_start']}"
            if movie_id not in seen_movies and movie['title']:
                unique_basic_movies.append(movie)
                seen_movies.add(movie_id)

        print(f"🧹 После отстранување дупликати: {len(unique_basic_movies)} уникатни филмови")

        # ФАЗА 2: Влегувај во секој линк и собирај детални податоци
        print("\n🎬 ФАЗА 2: Собирам детални податоци...")
        detailed_movies = []

        for i, movie in enumerate(unique_basic_movies):
            print(f"\n🎭 {i + 1}/{len(unique_basic_movies)} - {movie['title']}")

            # Скрепирај детални податоци ако има валиден URL
            if movie['url'] != self.base_url and '/film/' in movie['url']:
                try:
                    movie_details = self.scrape_movie_details(movie['url'])

                    # Ако има подетален опис, користи го
                    if movie_details['description_full']:
                        movie['description'] = movie_details['description_full']

                except Exception as e:
                    print(f"    ❌ Грешка при детално скрепирање: {e}")
            else:
                print(f"    ⏭️ Прескокнувам (нема валиден линк)")

            detailed_movies.append(movie)

        print(f"\n✅ ФАЗА 2 завршена: {len(detailed_movies)} филмови со детали")
        print(f"🎯 Финално: {len(detailed_movies)} филмови")
        return detailed_movies

    def extract_basic_movie_data(self, movie_items, source_name: str) -> List[Dict]:
        """Извлечи основни податоци од листа на movie items"""
        movies = []

        for i, item in enumerate(movie_items):
            try:
                movie_data = {
                    'event_id': '',
                    'url': self.base_url,
                    'title': '',
                    'date_start': self.target_date.strftime('%d %B'),
                    'time_start': '19:00',
                    'location': 'Cineplexx - Skopje',
                    'ticket_url': '',
                    'ticket_price_text': '250 MKD',
                    'ticket_free': False,
                    'description': '',
                    'category': 'movie',
                    'organizer': 'Cineplexx'
                }

                # Извлечи URL
                try:
                    links = item.find_elements(By.CSS_SELECTOR, 'a')
                    if links:
                        href = links[0].get_attribute('href')
                        if href:
                            movie_data['url'] = href if href.startswith('http') else self.base_url + href
                            movie_data['ticket_url'] = movie_data['url']

                            # Извлечи датум од URL
                            date_match = re.search(r'date=(\d{4}-\d{2}-\d{2})', href)
                            if date_match:
                                date_obj = datetime.strptime(date_match.group(1), '%Y-%m-%d')
                                # Форматирај како "21 Март"
                                months_mk = {
                                    1: 'Јануари', 2: 'Февруари', 3: 'Март', 4: 'Април',
                                    5: 'Мај', 6: 'Јуни', 7: 'Јули', 8: 'Август',
                                    9: 'Септември', 10: 'Октомври', 11: 'Ноември', 12: 'Декември'
                                }
                                movie_data['date_start'] = f"{date_obj.day} {months_mk[date_obj.month]}"
                except:
                    pass

                # Извлечи наслов
                try:
                    imgs = item.find_elements(By.CSS_SELECTOR, 'img')
                    if imgs:
                        alt_text = imgs[0].get_attribute('alt')
                        if alt_text:
                            movie_data['title'] = re.sub(r'\s+', ' ', alt_text.strip())
                except:
                    pass

                # Алтернативен наслов
                if not movie_data['title']:
                    try:
                        captions = item.find_elements(By.CSS_SELECTOR, '.l-entity__figure-caption')
                        if captions and captions[0].text.strip():
                            movie_data['title'] = re.sub(r'\s+', ' ', captions[0].text.strip())
                    except:
                        pass

                # Извлечи датум од DOM ако не е земен од URL
                if movie_data['date_start'] == self.target_date.strftime('%d %B'):
                    try:
                        date_elements = item.find_elements(By.CSS_SELECTOR, '.l-entity__figure-caption_startDate')
                        if date_elements:
                            date_text = date_elements[0].text
                            match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_text)
                            if match:
                                day, month, year = match.groups()
                                date_obj = datetime(int(year), int(month), int(day))
                                months_mk = {
                                    1: 'Јануари', 2: 'Февруари', 3: 'Март', 4: 'Април',
                                    5: 'Мај', 6: 'Јуни', 7: 'Јули', 8: 'Август',
                                    9: 'Септември', 10: 'Октомври', 11: 'Ноември', 12: 'Декември'
                                }
                                movie_data['date_start'] = f"{date_obj.day} {months_mk[date_obj.month]}"
                    except:
                        pass

                # Додај ако има наслов
                if movie_data['title']:
                    movie_data['event_id'] = self.generate_event_id(movie_data['title'], movie_data['date_start'])
                    movie_data['description'] = f"Кино проекција на {movie_data['title']}"

                    movies.append(movie_data)
                    print(f"   {len(movies)}. {movie_data['title']}")

            except Exception as e:
                continue

        print(f"   ✅ {source_name}: {len(movies)} филмови")
        return movies

    def save_data(self, movies: List[Dict]):
        if not movies:
            return

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Raw data (оригинални колони за backup)
        df_raw = pd.DataFrame(movies)
        raw_path = f"{self.raw_data_dir}/cineplexx_raw_{timestamp}.csv"
        df_raw.to_csv(raw_path, index=False, encoding='utf-8')

        # Processed data (според форматот на Филхармонијата)
        processed_events = []
        for movie in movies:
            processed_event = {
                'event_id': movie.get('event_id', ''),
                'url': movie.get('url', ''),
                'title': movie.get('title', ''),
                'date_start': movie.get('date_start', ''),
                'time_start': movie.get('time_start', ''),
                'location': movie.get('location', ''),
                'ticket_url': movie.get('ticket_url', ''),
                'ticket_price_text': movie.get('ticket_price_text', ''),
                'ticket_free': movie.get('ticket_free', False),
                'description': movie.get('description', ''),
                'categories': [movie.get('category', 'movie')],
                'organizer': movie.get('organizer', '')
            }
            processed_events.append(processed_event)

        # Зачувај processed data
        df_processed = pd.DataFrame(processed_events)
        processed_path = f"{self.processed_data_dir}/cineplexx_events_{timestamp}.csv"
        df_processed.to_csv(processed_path, index=False, encoding='utf-8')

        print(f"💾 Зачувано:")
        print(f"   📄 Raw data: {raw_path}")
        print(f"   📊 Processed data: {processed_path}")

    def run_scraping(self) -> Dict:
        self.clean_old_files()
        self.setup_driver()

        movies = self.scrape_movies()

        if movies:
            self.save_data(movies)

        self.close_driver()

        return {
            'movies': movies,
            'total_movies': len(movies),
            'target_date': self.target_date.strftime('%Y-%m-%d'),
            'scraping_timestamp': datetime.now().isoformat()
        }


def main():
    scraper = CineplexxScraper()
    results = scraper.run_scraping()

    if results['total_movies'] > 0:
        print(f"✅ Најдени {results['total_movies']} филмови!")
    else:
        print("❌ Нема резултати")


if __name__ == "__main__":
    main()