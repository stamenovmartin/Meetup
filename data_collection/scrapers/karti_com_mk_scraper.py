#!/usr/bin/env python3

import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import pandas as pd
import selenium
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

# За парсирање на HTML
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
    """
    Скрепер за настани од karti.com.mk
    со детално скрепирање на информации од секој настан
    """

    def __init__(self, debug=True):
        self.base_url = "https://karti.com.mk"
        self.events_url = "https://karti.com.mk"
        self.driver = None
        self.wait = None
        self.debug = debug
        self.raw_data_dir = "../raw_data"
        self.processed_data_dir = "../processed_data"

        # Setup logging
        logging.basicConfig(
            level=logging.INFO if debug else logging.WARNING,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)

    def clean_old_files(self):
        """Отстрани стари фајлови"""
        old_raw_files = glob.glob(os.path.join(self.raw_data_dir, "karti_events_raw_*.csv"))
        old_processed_files = glob.glob(os.path.join(self.processed_data_dir, "karti_events_*.csv"))

        for file_path in old_raw_files + old_processed_files:
            os.remove(file_path)
            self.logger.info(f"Отстранет фајл: {file_path}")

    def setup_driver(self):
        """Setup Chrome driver со подобрени опции"""
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
            self.logger.info("✅ Chrome driver успешно поставен")
        except Exception as e:
            self.logger.error(f"❌ Грешка при поставување на driver: {e}")
            raise

    def close_driver(self):
        """Затвори го driver-ot"""
        if self.driver:
            self.driver.quit()
            self.logger.info("🔒 Driver затворен")

    def generate_event_id(self, title: str, date: str = "") -> str:
        """Генерира уникатен event_id"""
        clean_title = re.sub(r'[^\w\s]', '', title.lower())
        combined = f"{clean_title}_{date}".strip('_')
        return hashlib.md5(combined.encode()).hexdigest()

    def parse_date(self, date_text: str) -> str:
        """Парсирај датум од различни формати"""
        if not date_text:
            return ""

        # Почисти го текстот
        date_text = date_text.strip()

        # Македонски месеци mapping
        mk_months = {
            'Јануари': '01', 'Февруари': '02', 'Март': '03', 'Април': '04',
            'Мај': '05', 'Јуни': '06', 'Јули': '07', 'Август': '08',
            'Септември': '09', 'Октомври': '10', 'Ноември': '11', 'Декември': '12',
            'јануари': '01', 'февруари': '02', 'март': '03', 'април': '04',
            'мај': '05', 'јуни': '06', 'јули': '07', 'август': '08',
            'септември': '09', 'октомври': '10', 'ноември': '11', 'декември': '12'
        }

        # Ако е во формат "22 Август 2025"
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
        """Парсирај цена информации"""
        result = {
            'price_text': price_text,
            'price_min': None,
            'price_max': None,
            'currency': 'MKD',
            'is_free': False
        }

        if not price_text:
            return result

        price_text = price_text.strip()

        # Провери дали е бесплатно
        if any(word in price_text.lower() for word in ['бесплатно', 'free', 'бесплатен']):
            result['is_free'] = True
            return result

        # Пронајди броеви во текстот
        numbers = re.findall(r'\d+', price_text)
        if numbers:
            if len(numbers) == 1:
                result['price_min'] = int(numbers[0])
                result['price_max'] = int(numbers[0])
            elif len(numbers) >= 2:
                result['price_min'] = int(numbers[0])
                result['price_max'] = int(numbers[-1])

        # Детектирај валута
        if 'EUR' in price_text.upper() or '€' in price_text:
            result['currency'] = 'EUR'
        elif 'USD' in price_text.upper() or '$' in price_text:
            result['currency'] = 'USD'

        return result

    def extract_event_from_card(self, event_card) -> Dict:
        """Извлечи основни податоци од event card"""
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
            # 1. URL - event_card самиот е линк
            href = event_card.get_attribute('href')
            if href:
                if not href.startswith('http'):
                    href = self.base_url + '/' + href.lstrip('/')
                event_data['url'] = href
                event_data['ticket_url'] = href

            # 2. Наслов
            title_element = event_card.find_element(By.CSS_SELECTOR, ".k-event-list-event-title")
            if title_element:
                event_data['title'] = title_element.text.strip()

            # 3. Датум
            date_element = event_card.find_element(By.CSS_SELECTOR, ".k-events-event-date")
            if date_element:
                date_text = date_element.text.strip()
                event_data['date_start'] = self.parse_date(date_text)

                # Ако има range (нпр "22-23 Август 2025")
                if '-' in date_text and not date_text.startswith('http'):
                    parts = date_text.split('-')
                    if len(parts) >= 2:
                        # Се обидуваме да направиме end date
                        end_part = parts[1].strip()
                        event_data['date_end'] = self.parse_date(end_part)

            # 4. Локација/Venue
            venue_element = event_card.find_element(By.CSS_SELECTOR, ".k-events-venue-details")
            if venue_element:
                venue_text = venue_element.text.strip()
                event_data['venue'] = venue_text
                event_data['location'] = venue_text

            # 5. Цена
            try:
                price_element = event_card.find_element(By.CSS_SELECTOR, ".cost")
                if price_element:
                    price_text = price_element.text.strip()
                    event_data['ticket_price_text'] = price_text

                    # Парсирај ја цената
                    price_info = self.parse_price(price_text)
                    event_data.update(price_info)
                    event_data['ticket_free'] = price_info['is_free']
            except NoSuchElementException:
                pass

            # 6. Слика
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

            # 7. Категорија од CSS класите на самиот card
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

            # 8. Генерирај event_id
            if event_data['title']:
                event_data['event_id'] = self.generate_event_id(
                    event_data['title'],
                    event_data['date_start']
                )

                # Основен опис
                event_data['description'] = f"{event_data['category']}: {event_data['title']}"

            return event_data

        except Exception as e:
            self.logger.error(f"Грешка при извлекување од card: {e}")
            return event_data

    def parse_description_details(self, description: str) -> Dict:
        """Парсирај дополнителни детали од описот"""
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

        # 1. Извлечи цени од описот
        # Пример: "2990-3990 мкд", "70 eur / 4300 мкд", "300 мкд"
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

        # 2. Извлечи време
        # Пример: "20:00", "22:00", "09:00"
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

        # 3. Извлечи venue/локација од описот
        # Обично е по време, пред опис
        venue_patterns = [
            r'(\d{1,2}:\d{2})\s+([^А-Ш]+?)(?=[А-Ш]|$)',  # После време до прв македонски збор
            r'мкд\s+\d{1,2}:\d{2}\s+([^А-Ш\n]+)',  # После цена и време
            r'eur\s+\d{1,2}:\d{2}\s+([^А-Ш\n]+)'  # После EUR цена и време
        ]

        for pattern in venue_patterns:
            match = re.search(pattern, description, re.IGNORECASE)
            if match:
                venue = match.group(-1).strip()
                if len(venue) > 5 and len(venue) < 100:  # Разумна должина
                    details['parsed_venue'] = venue
                    break

        # 4. Контакт информации
        contact_patterns = [
            r'тел\.?\s*:?\s*(\d{2,3}[-\s]*\d{3}[-\s]*\d{3,4})',
            r'телефон\s*:?\s*(\d{2,3}[-\s]*\d{3}[-\s]*\d{3,4})',
            r'контакт\s*:?\s*(\d{2,3}[-\s]*\d{3}[-\s]*\d{3,4})',
            r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',  # email
            r'(www\.[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',  # website
            r'(facebook\.com/[a-zA-Z0-9._-]+)',
            r'(instagram\.com/[a-zA-Z0-9._-]+)'
        ]

        contacts = []
        for pattern in contact_patterns:
            matches = re.findall(pattern, description, re.IGNORECASE)
            contacts.extend(matches)

        if contacts:
            details['contact_info'] = ' | '.join(contacts)

        # 5. Тип на настан од клучни зборови
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
        """Влегува во линкот на настанот и скрепира детални податоци"""
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
            self.logger.info(f"  📄 Влегувам во: {event_url}")
            self.driver.get(event_url)
            time.sleep(3)

            # 1. Основен опис/содржина - земи ГИ СИТЕ можни извори
            description_selectors = [
                "body",  # Цела страница ако е потребно
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
                        # Отстрани вишок whitespace
                        desc_text = re.sub(r'\s+', ' ', desc_text)

                        # Земи го најдолгиот опис
                        if len(desc_text) > len(best_description):
                            best_description = desc_text

                except NoSuchElementException:
                    continue

            if best_description and len(best_description) > 20:
                details['description_full'] = best_description
                self.logger.info(f"    📝 Опис: {best_description[:100]}...")

                # Парсирај дополнителни детали од описот
                parsed = self.parse_description_details(best_description)
                details['parsed_details'] = parsed

                if parsed['parsed_price']:
                    self.logger.info(f"    💰 Парсирани цени: {parsed['parsed_price']}")
                if parsed['parsed_time']:
                    self.logger.info(f"    🕐 Парсирано време: {parsed['parsed_time']}")
                if parsed['parsed_venue']:
                    self.logger.info(f"    📍 Парсиран venue: {parsed['parsed_venue']}")
                if parsed['contact_info']:
                    self.logger.info(f"    📞 Контакт: {parsed['contact_info']}")

            # 2. Пробај да најдеш специфични организатори/продавачи
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
                        if len(org_text) < 100:  # Не преголем текст
                            details['organizer'] = org_text
                            self.logger.info(f"    🏢 Организатор: {details['organizer']}")
                            break
                except NoSuchElementException:
                    continue

            # 3. Дополнителни детали - земи сè што е достапно
            try:
                # Пробај да земеш сè од страницата
                page_source = self.driver.page_source
                if page_source and len(page_source) > 1000:
                    # Отстрани HTML тагови и извлечи чист текст
                    if HAS_BS4:
                        try:
                            soup = BeautifulSoup(page_source, 'html.parser')
                            clean_text = soup.get_text()
                            # Почисти го текстот
                            clean_text = re.sub(r'\s+', ' ', clean_text).strip()

                            if len(clean_text) > len(details.get('description_full', '')):
                                details['additional_info'] = clean_text[:5000]  # Првите 5000 карактери
                        except Exception as e:
                            self.logger.debug(f"BeautifulSoup грешка: {e}")
                    else:
                        # Ако нема BeautifulSoup, користи regex
                        clean_text = re.sub(r'<[^>]+>', '', page_source)
                        clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                        details['additional_info'] = clean_text[:3000]

            except Exception as e:
                self.logger.debug(f"Не можам да земам дополнителни информации: {e}")

            # 4. Специфични билет информации
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
                self.logger.info(f"    🎫 Билет инфо: {details['ticket_info'][:100]}...")

        except Exception as e:
            self.logger.error(f"    ❌ Грешка при скрепирање детали: {e}")

        return details

    def find_event_cards(self) -> List:
        """Најди ги сите event cards на страницата"""
        card_selectors = [
            "a.k_event_link",  # Од дадениот HTML
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
                    self.logger.info(f"✅ Користам селектор '{selector}' - најдени {len(cards)} cards")
                    return cards
            except Exception as e:
                self.logger.debug(f"Селектор '{selector}' не работи: {e}")
                continue

        self.logger.warning("⚠️ Не можам да најдам event cards!")
        return []

    def scroll_and_load_more(self):
        """Скролај и пробај да вчиташ повеќе настани"""
        try:
            # Скролај до дното
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Пробај да најдеш "Load More" копче
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
                        self.logger.info(f"🔄 Кликам на Load More копче")
                        self.driver.execute_script("arguments[0].click();", button)
                        time.sleep(3)
                        return True
                except NoSuchElementException:
                    continue

            return False

        except Exception as e:
            self.logger.error(f"Грешка при scroll/load more: {e}")
            return False

    def scrape_events(self, max_load_attempts: int = 3) -> List[Dict]:
        """Главна функција за скрепирање настани"""
        self.logger.info("🚀 Започнувам скрепирање настани од karti.com.mk...")

        try:
            self.driver.get(self.events_url)
            self.logger.info(f"📖 Вчитана страница: {self.events_url}")
            time.sleep(5)

            all_events = []

            # Пробај да вчиташ повеќе настани
            for attempt in range(max_load_attempts):
                self.logger.info(f"\n🔍 === Обид {attempt + 1}/{max_load_attempts} ===")

                # Најди ги event cards
                event_cards = self.find_event_cards()

                if not event_cards:
                    self.logger.warning("❌ Нема event cards")
                    break

                self.logger.info(f"📊 Најдени {len(event_cards)} event cards")

                # Извлечи податоци од секој card
                current_events = []
                for i, card in enumerate(event_cards):
                    try:
                        if self.debug and i < 3:  # Debug првите 3
                            self.logger.info(f"\n--- Card {i + 1} ---")

                        event_data = self.extract_event_from_card(card)

                        if event_data['title'] and event_data['event_id']:
                            current_events.append(event_data)
                            if self.debug and i < 3:
                                self.logger.info(f"✅ {event_data['title']}")
                                self.logger.info(f"   📅 {event_data['date_start']}")
                                self.logger.info(f"   🏢 {event_data['venue']}")
                                self.logger.info(f"   💰 {event_data['ticket_price_text']}")
                        else:
                            if self.debug and i < 3:
                                self.logger.warning(f"❌ Card {i + 1}: Нема валидни податоци")

                    except Exception as e:
                        self.logger.error(f"⚠️ Грешка при обработка на card {i + 1}: {e}")
                        continue

                self.logger.info(f"✅ Обид {attempt + 1}: Собрани {len(current_events)} валидни настани")
                all_events.extend(current_events)

                # Пробај да вчиташ повеќе
                if attempt < max_load_attempts - 1:
                    if not self.scroll_and_load_more():
                        self.logger.info("🔚 Нема повеќе настани за вчитување")
                        break

            # Отстрани дупликати
            unique_events = self.remove_duplicates(all_events)
            self.logger.info(f"🧹 После отстранување дупликати: {len(unique_events)} уникатни настани")

            # Фаза 2: Детални податоци
            detailed_events = self.scrape_detailed_data(unique_events)

            return detailed_events

        except Exception as e:
            self.logger.error(f"❌ Критична грешка при скрепирање: {e}")
            return []

    def remove_duplicates(self, events: List[Dict]) -> List[Dict]:
        """Отстрани дупликати врз база на event_id"""
        unique_events = []
        seen_ids = set()

        for event in events:
            event_id = event.get('event_id', '')
            if event_id and event_id not in seen_ids:
                unique_events.append(event)
                seen_ids.add(event_id)
            elif self.debug:
                self.logger.debug(f"🗑️ Дупликат отстранет: {event.get('title', 'No title')}")

        return unique_events

    def scrape_detailed_data(self, events: List[Dict]) -> List[Dict]:
        """Фаза 2: Собирај детални податоци од секој настан"""
        if not events:
            return []

        self.logger.info(f"\n🎬 === ФАЗА 2: Детални податоци за {len(events)} настани ===")

        detailed_events = []
        for i, event in enumerate(events):
            self.logger.info(f"\n🎭 {i + 1}/{len(events)} - {event['title']}")

            if event.get('url'):
                try:
                    details = self.scrape_event_details(event['url'])

                    # Мерџирај ги деталите
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

                    # Додај парсирани детали како посебни полиња
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

                    # Додај сите останати детали
                    event.update({k: v for k, v in details.items() if v and k != 'parsed_details'})

                except Exception as e:
                    self.logger.error(f"    ❌ Грешка при детално скрепирање: {e}")
            else:
                self.logger.info("    ⏭️ Прескокнувам (нема валиден линк)")

            detailed_events.append(event)

        self.logger.info(f"\n✅ ФАЗА 2 завршена: {len(detailed_events)} настани со детали")
        return detailed_events

    def save_to_csv(self, events: List[Dict], filename_suffix: str = "") -> str:
        """Зачувај ги настаните во CSV"""
        if not events:
            self.logger.warning("Нема настани за зачувување")
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
            self.logger.info(f"💾 Зачувани {len(events)} настани во: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"❌ Грешка при зачувување: {e}")
            return ""

    def print_summary(self, events: List[Dict]):
        """Прикажи резиме од скрепирањето"""
        if not events:
            self.logger.info("📊 Нема настани за приказ")
            return

        self.logger.info(f"\n📊 === РЕЗИМЕ ===")
        self.logger.info(f"Вкупно настани: {len(events)}")

        # Статистики
        with_description = sum(1 for e in events if e.get('description') and len(e['description']) > 50)
        with_organizer = sum(1 for e in events if e.get('organizer'))
        with_price = sum(1 for e in events if e.get('ticket_price_text'))
        with_parsed_price = sum(1 for e in events if e.get('parsed_price'))
        with_parsed_time = sum(1 for e in events if e.get('parsed_time'))
        with_venue = sum(1 for e in events if e.get('venue'))
        with_contact = sum(1 for e in events if e.get('contact_info') or e.get('parsed_contact'))
        free_events = sum(1 for e in events if e.get('ticket_free'))

        self.logger.info(f"Со опис: {with_description}")
        self.logger.info(f"Со организатор: {with_organizer}")
        self.logger.info(f"Со цена: {with_price}")
        self.logger.info(f"Со парсирани цени: {with_parsed_price}")
        self.logger.info(f"Со парсирано време: {with_parsed_time}")
        self.logger.info(f"Со venue: {with_venue}")
        self.logger.info(f"Со контакт: {with_contact}")
        self.logger.info(f"Бесплатни: {free_events}")

        # Категории
        categories = {}
        for event in events:
            cat = event.get('category', 'Непознато')
            categories[cat] = categories.get(cat, 0) + 1

        self.logger.info(f"\nКатегории:")
        for cat, count in categories.items():
            self.logger.info(f"  {cat}: {count}")

        # Парсирани типови настани
        parsed_types = {}
        for event in events:
            p_type = event.get('parsed_event_type', 'Непознато')
            parsed_types[p_type] = parsed_types.get(p_type, 0) + 1

        if any(t != 'Непознато' for t in parsed_types.keys()):
            self.logger.info(f"\nПарсирани типови:")
            for p_type, count in parsed_types.items():
                if p_type != 'Непознато':
                    self.logger.info(f"  {p_type}: {count}")

        # Примери
        self.logger.info(f"\n📋 Првите 3 настани:")
        for i, event in enumerate(events[:3]):
            self.logger.info(f"{i + 1}. {event.get('title', 'Без наслов')}")
            self.logger.info(f"   📅 Датум: {event.get('date_start', 'Без датум')}")
            self.logger.info(f"   🏢 Venue: {event.get('venue', 'Без venue')}")
            self.logger.info(f"   💰 Оригинална цена: {event.get('ticket_price_text', 'Без цена')}")
            if event.get('parsed_price'):
                self.logger.info(f"   💰 Парсирани цени: {event.get('parsed_price')}")
            if event.get('parsed_time'):
                self.logger.info(f"   🕐 Време: {event.get('parsed_time')}")
            if event.get('parsed_event_type'):
                self.logger.info(f"   🎭 Тип: {event.get('parsed_event_type')}")
            self.logger.info(f"   🔗 URL: {event.get('url', 'Без URL')}")

    def run_full_scrape(self, max_load_attempts: int = 3, save_results: bool = True) -> List[Dict]:
        """Изврши целосно скрепирање"""
        try:
            self.logger.info("🚀 === ЗАПОЧНУВАМ СКРЕПИРАЊЕ ===")

            # Setup
            self.setup_driver()

            # Скрепирај
            events = self.scrape_events(max_load_attempts)

            # Зачувај ако е потребно
            if save_results and events:
                self.save_to_csv(events, "final")

            # Прикажи резиме
            self.print_summary(events)

            self.logger.info("✅ === СКРЕПИРАЊЕТО ЗАВРШЕНО ===")
            return events

        except Exception as e:
            self.logger.error(f"❌ Критична грешка: {e}")
            return []
        finally:
            self.close_driver()


def main():
    """Главна функција"""
    print("🎯 Karti.com.mk Events Scraper")
    print("=" * 50)

    # Креирај скрепер со debug
    scraper = KartiEventsScraper(debug=True)

    try:
        # Избриши стари фајлови
        scraper.clean_old_files()

        # Скрепирај настани
        events = scraper.run_full_scrape(max_load_attempts=3, save_results=True)

        if events:
            print(f"\n🎉 Успешно скрепирани {len(events)} настани!")

            # Прикажи неколку примери
            print("\n📋 Примери:")
            for i, event in enumerate(events[:5]):
                print(f"{i + 1}. {event.get('title', 'Без наслов')}")
                print(f"   📅 Датум: {event.get('date_start', 'Без датум')}")
                print(f"   🏢 Venue: {event.get('venue', 'Непознат')}")
                print(f"   💰 Цена: {event.get('ticket_price_text', 'Без цена')}")
                print(f"   🎭 Категорија: {event.get('category', 'Без категорија')}")
                print(f"   🔗 URL: {event.get('url', 'Без URL')}")
                print(f"   📝 Опис: {event.get('description', 'Без опис')[:100]}...")
                print()
        else:
            print("❌ Не се скрепирани настани. Проверете ја конфигурацијата.")

    except KeyboardInterrupt:
        print("\n⏹️ Скрепирањето е прекинато од корисникот")
    except Exception as e:
        print(f"\n❌ Неочекувана грешка: {e}")
    finally:
        print("\n👋 Крај на програмата")


# Дополнителни utility функции
def scrape_single_event(url: str) -> Dict:
    """Скрепирај еден конкретен настан"""
    scraper = KartiEventsScraper(debug=True)
    try:
        scraper.setup_driver()
        details = scraper.scrape_event_details(url)
        return details
    finally:
        scraper.close_driver()


def test_selectors():
    """Тест функција за да се тестираат селекторите"""
    scraper = KartiEventsScraper(debug=True)
    try:
        scraper.setup_driver()
        scraper.driver.get("https://karti.com.mk")
        time.sleep(5)

        print("🔍 Тестирам селектори...")

        # Тестирај различни селектори
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
                print(f"✅ '{selector}': {len(elements)} елементи")
                if elements and len(elements) > 0:
                    print(f"   Пример: {elements[0].text[:50]}...")
            except Exception as e:
                print(f"❌ '{selector}': Грешка - {e}")

        # Тестирај еден card детално
        cards = scraper.driver.find_elements(By.CSS_SELECTOR, "a.k_event_link")
        if cards:
            print(f"\n🔬 Детална анализа на првиот card:")
            card = cards[0]
            print(f"   Tag: {card.tag_name}")
            print(f"   Class: {card.get_attribute('class')}")
            print(f"   Href: {card.get_attribute('href')}")
            print(f"   HTML: {card.get_attribute('outerHTML')[:300]}...")

            # Проверка на под-елементи
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
                    print(f"   ✅ {sub_sel}: '{sub_elem.text[:30]}'")
                except:
                    print(f"   ❌ {sub_sel}: Не најден")

    finally:
        scraper.close_driver()


if __name__ == "__main__":
    main()