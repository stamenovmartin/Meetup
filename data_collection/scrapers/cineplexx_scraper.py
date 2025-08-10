#!/usr/bin/env python3

import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os
import glob


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

    def get_movie_genre(self, title: str) -> str:
        title_lower = title.lower()

        if any(keyword in title_lower for keyword in
               ['како да го дресирате', 'лило и стич', 'штрумфовите', 'фантастичната 4', 'лошите момци']):
            return 'анимиран'
        if any(keyword in title_lower for keyword in
               ['бетмен', 'супермен', 'оружја', 'светот јура', 'невозможна мисија', 'f1', 'војна на ѕвездите']):
            return 'акција'
        if any(keyword in title_lower for keyword in ['повикувања', 'последни обреди']):
            return 'хорор'
        if any(keyword in title_lower for keyword in ['хари потер', 'почеток', 'inception']):
            return 'фантазија'
        if any(keyword in title_lower for keyword in ['naked gun', 'уште пооткачен петок', 'бал-кан-кан']):
            return 'комедија'
        if any(keyword in title_lower for keyword in ['исцелител', 'bring her back']):
            return 'драма'
        if any(keyword in title_lower for keyword in ['runseokjin', 'live viewing', 'amsterdam']):
            return 'музички'
        if any(keyword in title_lower for keyword in ['project hail mary', 'zweitland']):
            return 'научна фантастика'

        return 'акција'

    def get_extra_movie_data(self, url: str) -> Dict:
        """БРЗО извлекување на дополнителни податоци без да влегува во секој линк"""
        extra_data = {
            'duration': '120 мин',
            'director': '',
            'actors': '',
            'year': '',
            'rating': ''
        }

        # Извлечи датум од URL
        date_match = re.search(r'date=(\d{4}-\d{2}-\d{2})', url)
        if date_match:
            extra_data['url_date'] = date_match.group(1)

        # Извлечи ID на филмот од URL за можна понатамошна употреба
        film_match = re.search(r'/film/([^?]+)', url)
        if film_match:
            extra_data['film_slug'] = film_match.group(1)

        return extra_data

    def scrape_movie_details(self, movie_url: str) -> Dict:
        """Влегува во линкот на филмот и скрепира детални информации"""
        details = {
            'duration': '120 мин',
            'director': '',
            'actors': '',
            'year': '',
            'rating': '',
            'genre_from_page': '',
            'description_full': ''
        }

        if not movie_url or movie_url == self.base_url:
            return details

        try:
            print(f"  📄 Влегувам во: {movie_url}")
            self.driver.get(movie_url)
            time.sleep(3)

            # 1. Времетраење на филмот
            duration_selectors = [
                "//*[contains(text(), 'мин')]",
                "//*[contains(text(), 'min')]",
                "//*[contains(text(), 'час')]",
                "//*[contains(text(), 'hour')]",
                "//*[contains(text(), 'H')]",
                ".movie-duration",
                ".film-duration"
            ]

            for selector in duration_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    for elem in elements:
                        text = elem.text.strip()

                        # Формат: "2H 7min" или "2h 7min"
                        hour_min_match = re.search(r'(\d+)[Hh]\s*(\d+)(?:min|мин)', text)
                        if hour_min_match:
                            hours = int(hour_min_match.group(1))
                            minutes = int(hour_min_match.group(2))
                            total_minutes = hours * 60 + minutes
                            details['duration'] = f"{total_minutes} мин"
                            print(f"    ⏱️ Времетраење: {details['duration']} ({text})")
                            break

                        # Формат: "127 мин" или "127 min"
                        min_match = re.search(r'(\d+)\s*(?:мин|min)', text)
                        if min_match:
                            details['duration'] = f"{min_match.group(1)} мин"
                            print(f"    ⏱️ Времетраење: {details['duration']}")
                            break

                        # Формат: "2 час" или "2 hours"
                        hour_match = re.search(r'(\d+)\s*(?:час|hour)', text)
                        if hour_match:
                            hours = int(hour_match.group(1))
                            total_minutes = hours * 60
                            details['duration'] = f"{total_minutes} мин"
                            print(f"    ⏱️ Времетраење: {details['duration']} ({text})")
                            break

                    if details['duration'] != '120 мин':
                        break
                except:
                    continue

            # 2. Жанр од страницата
            genre_selectors = [
                ".movie-genre",
                ".film-genre",
                "[class*='genre']",
                "//*[contains(text(), 'Жанр')]/following-sibling::*",
                "//*[contains(text(), 'Genre')]/following-sibling::*"
            ]

            for selector in genre_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    if elements and elements[0].text.strip():
                        genre_text = elements[0].text.strip()
                        # Земи го првиот жанр ако има повеќе
                        first_genre = genre_text.split(',')[0].strip()
                        if first_genre and len(first_genre) > 2:
                            details['genre_from_page'] = first_genre
                            print(f"    🎭 Жанр: {details['genre_from_page']}")
                            break
                except:
                    continue

            # 3. Режисер
            director_selectors = [
                "//*[contains(text(), 'Режисер')]/following-sibling::*",
                "//*[contains(text(), 'Director')]/following-sibling::*",
                ".movie-director",
                ".film-director"
            ]

            for selector in director_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    if elements and elements[0].text.strip():
                        details['director'] = elements[0].text.strip()
                        print(f"    🎬 Режисер: {details['director']}")
                        break
                except:
                    continue

            # 4. Актери/Cast
            actors_selectors = [
                "//*[contains(text(), 'Актери')]/following-sibling::*",
                "//*[contains(text(), 'Cast')]/following-sibling::*",
                "//*[contains(text(), 'Starring')]/following-sibling::*",
                ".movie-cast",
                ".film-cast"
            ]

            for selector in actors_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    if elements and elements[0].text.strip():
                        details['actors'] = elements[0].text.strip()
                        print(f"    🎭 Актери: {details['actors'][:50]}...")
                        break
                except:
                    continue

            # 5. Година
            year_selectors = [
                "//*[contains(text(), 'Година')]/following-sibling::*",
                "//*[contains(text(), 'Year')]/following-sibling::*",
                ".movie-year",
                ".film-year"
            ]

            for selector in year_selectors:
                try:
                    if selector.startswith("//"):
                        elements = self.driver.find_elements(By.XPATH, selector)
                    else:
                        elements = self.driver.find_elements(By.CSS_SELECTOR, selector)

                    for elem in elements:
                        year_match = re.search(r'(19|20)\d{2}', elem.text)
                        if year_match:
                            details['year'] = year_match.group(0)
                            print(f"    📅 Година: {details['year']}")
                            break
                    if details['year']:
                        break
                except:
                    continue

            # 6. Рејтинг
            rating_selectors = [
                ".movie-rating",
                ".film-rating",
                "[class*='rating']",
                "[class*='imdb']"
            ]

            for selector in rating_selectors:
                try:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements and elements[0].text.strip():
                        rating_text = elements[0].text.strip()
                        rating_match = re.search(r'(\d+\.?\d*)', rating_text)
                        if rating_match:
                            details['rating'] = rating_match.group(1)
                            print(f"    ⭐ Рејтинг: {details['rating']}")
                            break
                except:
                    continue

            # 7. Опис на филмот
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
                        if len(desc_text) > 20:  # Само ако е подолг опис
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

                    # Ажурирај со новите податоци
                    movie['duration'] = movie_details['duration']
                    movie['director'] = movie_details['director']
                    movie['actors'] = movie_details['actors']
                    movie['year'] = movie_details['year']
                    movie['rating'] = movie_details['rating']

                    # Ако има жанр од страницата, користи го
                    if movie_details['genre_from_page']:
                        movie['genre'] = movie_details['genre_from_page']

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
                    'date_start': self.target_date.strftime('%Y-%m-%d'),
                    'time_start': '19:00',
                    'location': 'Cineplexx Skopje City Mall',
                    'ticket_price': '250 MKD',
                    'ticket_url': self.base_url,
                    'description': '',
                    'category': 'cinema',
                    'organiser': 'Cineplexx',
                    'duration': '120 мин',
                    'genre': '',
                    'director': '',
                    'actors': '',
                    'year': '',
                    'rating': '',
                    'poster_url': '',
                    'scraped_at': datetime.now().isoformat()
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
                                movie_data['date_start'] = date_match.group(1)
                except:
                    pass

                # Извлечи наслов и poster
                try:
                    imgs = item.find_elements(By.CSS_SELECTOR, 'img')
                    if imgs:
                        alt_text = imgs[0].get_attribute('alt')
                        if alt_text:
                            movie_data['title'] = re.sub(r'\s+', ' ', alt_text.strip())
                            movie_data['poster_url'] = imgs[0].get_attribute('src')
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
                if movie_data['date_start'] == self.target_date.strftime('%Y-%m-%d'):
                    try:
                        date_elements = item.find_elements(By.CSS_SELECTOR, '.l-entity__figure-caption_startDate')
                        if date_elements:
                            date_text = date_elements[0].text
                            match = re.search(r'(\d{1,2})\.(\d{1,2})\.(\d{4})', date_text)
                            if match:
                                day, month, year = match.groups()
                                movie_data['date_start'] = f"{year}-{month:0>2}-{day:0>2}"
                    except:
                        pass

                # Додај ако има наслов
                if movie_data['title']:
                    clean_title = re.sub(r'[^\w\s]', '', movie_data['title'])
                    date_str = movie_data['date_start'].replace('-', '')
                    movie_data['event_id'] = f"cineplexx_{date_str}_{hash(clean_title) % 10000}"
                    movie_data['genre'] = self.get_movie_genre(movie_data['title'])
                    movie_data['description'] = f"Кинo проекција на {movie_data['title']}"

                    if "(Open Air Cinema)" in movie_data['title']:
                        movie_data['category'] = 'outdoor_cinema'

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

        # Processed data (стандардизирани колони според спецификацијата)
        processed_events = []
        for movie in movies:
            # Конвертирај timestamp
            try:
                dt = datetime.strptime(f"{movie['date_start']} {movie['time_start']}", '%Y-%m-%d %H:%M')
                timestamp_unix = int(dt.timestamp())
            except:
                timestamp_unix = int(datetime.now().timestamp())

            # Одреди дали е бесплатен или со цена
            is_free = False
            if 'бесплатен' in movie.get('ticket_price', '').lower() or movie.get('ticket_price', '') == '0':
                is_free = True

            processed_event = {
                'Event ID': movie.get('event_id', ''),
                'Title': movie.get('title', ''),
                'Category/Tags': movie.get('genre', 'cinema'),
                'Date/Time': f"{movie.get('date_start', '')} {movie.get('time_start', '')}",
                'Timestamp': timestamp_unix,
                'Location': movie.get('location', 'Cineplexx Skopje City Mall'),
                'Description': movie.get('description', ''),
                'Organizer Name': movie.get('organiser', 'Cineplexx'),
                'Ticket Price': movie.get('ticket_price', '250 MKD'),
                'Free Flag': is_free,
                'Ticket URL': movie.get('ticket_url', movie.get('url', ''))
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