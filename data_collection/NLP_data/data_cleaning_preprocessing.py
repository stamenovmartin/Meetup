#!/usr/bin/env python3
"""
 Практична Data Cleaning скрипта за Event Data
================================================

Специјално дизајнирана за:
- AlleventsEvents + CineplexEvents интеграција
- Schema унификација
- Македонски/Англиски текст стандардизација
- Missing values интелигентно пополнување
- GNN-ready dataset подготовка
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


class EventDataCleaner:
    """Практичен cleaner за event податоци"""

    def __init__(self, data_dir="../processed_data", output_dir="cleaned_data"):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Статистики
        self.stats = {
            'original_rows': 0,
            'final_rows': 0,
            'duplicates_removed': 0,
            'values_filled': 0,
            'standardizations': 0
        }

    def load_all_csvs(self):
        """Вчитај ги сите CSV фајлови"""
        dataframes = {}

        csv_files = list(self.data_dir.glob("*.csv"))
        print(f" Пронајдени {len(csv_files)} CSV фајлови")

        # Очекувани извори
        expected_sources = ['allevents', 'cineplexx', 'filharmonija', 'it_events', 'karti', 'mktickets']
        found_sources = []

        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path, encoding='utf-8-sig')
                source_name = file_path.stem
                dataframes[source_name] = df

                # Детектирај извор
                detected_source = 'unknown'
                for expected in expected_sources:
                    if expected in source_name.lower():
                        detected_source = expected
                        found_sources.append(expected)
                        break

                print(f"    {file_path.name}: {len(df)} rows [{detected_source}]")
                self.stats['original_rows'] += len(df)
            except Exception as e:
                print(f"    {file_path.name}: {e}")

        # Рапортирај за недостасувачки извори
        missing_sources = set(expected_sources) - set(found_sources)
        if missing_sources:
            print(f"    Недостасуваат: {', '.join(missing_sources)}")

        return dataframes

    def unify_schema(self, dataframes):
        """Унифицирај schema меѓу различни извори"""
        print("\n Унификување на schema...")

        unified_dfs = []

        for source_name, df in dataframes.items():
            print(f"    Обработувам: {source_name}")

            # Креирај стандардна schema
            unified_df = pd.DataFrame()

            # Задолжителни колони
            unified_df['event_id'] = df.get('event_id', range(len(df)))
            unified_df['title'] = df.get('title', 'Без наслов')
            unified_df['description'] = self._extract_description(df)
            unified_df['date_start'] = self._extract_date(df)
            unified_df['time_start'] = self._extract_time(df)
            unified_df['location'] = self._extract_location(df)
            unified_df['category'] = self._extract_category(df)
            unified_df['organizer'] = self._extract_organizer(df, source_name)
            unified_df['price_text'] = self._extract_price(df)
            unified_df['is_free'] = self._extract_is_free(df)
            unified_df['url'] = df.get('url', df.get('ticket_url', ''))
            unified_df['source'] = source_name
            unified_df['scraped_at'] = datetime.now().strftime('%Y-%m-%d')

            unified_dfs.append(unified_df)
            print(f"       Унифициран во {len(unified_df.columns)} колони")

        # Спои ги сите
        combined_df = pd.concat(unified_dfs, ignore_index=True)
        print(f"    Комбиниран dataset: {len(combined_df)} rows")

        return combined_df

    def _extract_description(self, df):
        """Извлечи опис од различни колони"""
        # Пробај различни комбинации според изворот
        if 'description' in df.columns:
            return df['description'].fillna('')
        elif 'cast' in df.columns and 'genre' in df.columns:
            # За филмови (Cineplexx) - комбинирај genre + cast + director
            descriptions = []
            for _, row in df.iterrows():
                desc_parts = []
                if pd.notna(row.get('genre', '')):
                    desc_parts.append(f"Жанр: {row['genre']}")
                if pd.notna(row.get('director', '')):
                    desc_parts.append(f"Режисер: {row['director']}")
                if pd.notna(row.get('cast', '')):
                    cast_clean = str(row['cast'])[:200]  # Лимитирај должина
                    desc_parts.append(f"Глумци: {cast_clean}")
                descriptions.append('. '.join(desc_parts))
            return pd.Series(descriptions)
        elif 'categories' in df.columns:
            # За настани со категории
            return df['categories'].astype(str).apply(
                lambda x: f"Категорија: {x}" if x and x != 'nan' else ''
            )
        elif 'organizer' in df.columns:
            # Ако има само организатор, користи го тој
            return df['organizer'].astype(str).apply(
                lambda x: f"Организатор: {x}" if x and x != 'nan' else ''
            )
        else:
            # Fallback - празен опис
            return pd.Series([''] * len(df))

    def _extract_date(self, df):
        """Стандардизирај датуми"""
        date_col = df.get('date_start', '')

        def parse_date(date_str):
            if pd.isna(date_str) or date_str == '':
                return ''

            date_str = str(date_str).strip()

            # Мапирања за месеци
            months_mk = {
                'јануари': '01', 'февруари': '02', 'март': '03', 'април': '04',
                'мај': '05', 'јуни': '06', 'јули': '07', 'август': '08',
                'септември': '09', 'октомври': '10', 'ноември': '11', 'декември': '12'
            }

            months_en = {
                'january': '01', 'february': '02', 'march': '03', 'april': '04',
                'may': '05', 'june': '06', 'july': '07', 'august': '08',
                'september': '09', 'october': '10', 'november': '11', 'december': '12'
            }

            # Pattern: "11 August" -> "2025-08-11"
            match = re.search(r'(\d+)\s+([a-zA-Zа-шА-Ш]+)', date_str)
            if match:
                day, month_name = match.groups()
                month_name = month_name.lower()

                month_num = months_en.get(month_name) or months_mk.get(month_name)
                if month_num:
                    return f"2025-{month_num}-{day.zfill(2)}"

            # Pattern: "2025-08-11" (веќе стандардизиран)
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                return date_str

            return ''

        return date_col.apply(parse_date)

    def _extract_time(self, df):
        """Стандардизирај време"""
        time_col = df.get('time_start', df.get('duration', ''))

        def parse_time(time_str):
            if pd.isna(time_str) or time_str == '':
                return ''

            time_str = str(time_str).strip()

            # Pattern: "19:00" или "7:00 pm"
            match = re.search(r'(\d{1,2}):(\d{2})', time_str)
            if match:
                hour, minute = match.groups()
                hour = int(hour)

                # Провери за PM
                if 'pm' in time_str.lower() and hour < 12:
                    hour += 12
                elif 'am' in time_str.lower() and hour == 12:
                    hour = 0

                if 0 <= hour <= 23:
                    return f"{hour:02d}:{minute}"

            # Pattern: "20 часот"
            match = re.search(r'(\d{1,2})\s*час', time_str)
            if match:
                hour = int(match.group(1))
                if 0 <= hour <= 23:
                    return f"{hour:02d}:00"

            return ''

        return time_col.apply(parse_time)

    def _extract_location(self, df):
        """Стандардизирај локации"""
        location_col = df.get('location', '')

        # Речник за нормализација на локации - проширено
        location_mapping = {
            'cineplexx - skopje': 'Cineplexx - Скопје',
            'cineplexx': 'Cineplexx - Скопје',
            'скопје': 'Скопје',
            'skopje': 'Скопје',
            'stadion na arm': 'Стадион на АРМ',
            'стадион на арм': 'Стадион на АРМ',
            'arm stadium': 'Стадион на АРМ',
            'mladinski kulturen centar': 'Младински Културен Центар',
            'младински културен центар': 'Младински Културен Центар',
            'mkc': 'Младински Културен Центар',
            'мкц': 'Младински Културен Центар',
            'public room': 'Public Room',
            'филхармонија': 'Македонска Филхармонија',
            'macedonian philharmonic': 'Македонска Филхармонија',
            'филармонија': 'Македонска Филхармонија',
            'камерна сала': 'Камерна сала - Филхармонија',
            'мнт': 'Македонски Народен Театар',
            '11 март': 'Македонски Народен Театар',
            'македонски народен театар': 'Македонски Народен Театар',
            'la kaña': 'La Kaña',
            'станица 26': 'Станица 26',
            'stanica 26': 'Станица 26',
            'куршумли ан': 'Куршумли Ан',
            'градски парк': 'Градски Парк - Скопје',
            'city park': 'Градски Парк - Скопје',
            'парк': 'Градски Парк - Скопје',
            'boris trajkovski': 'Спортски Центар Борис Трајковски',
            'спортски центар': 'Спортски Центар Борис Трајковски',
            'holiday inn': 'Holiday Inn - Скопје',
            'marriott': 'Skopje Marriott Hotel',
            'diamond mall': 'Diamond Mall - Скопје',
            'онлајн': 'Онлајн',
            'online': 'Онлајн',
            'virtual': 'Онлајн'
        }

        def clean_location(location):
            if pd.isna(location) or location == '':
                return 'Скопје'  # Default

            location = str(location).strip()

            # Отстрани шум
            location = re.sub(r'[^\w\s\-АаБбВвГгДдЃѓЕеЖжЗзСсИиЈјКкЛлЉљМмНнЊњОоПпРрСсТтЌќУуФфХхЦцЧчЏџШш]', '', location)
            location = re.sub(r'\s+', ' ', location).strip()

            # Нормализирај
            location_lower = location.lower()
            for key, normalized in location_mapping.items():
                if key in location_lower:
                    return normalized

            # Ако е кратко, додај "Скопје"
            if len(location) < 3:
                return 'Скопје'

            return location.title()

        return location_col.apply(clean_location)

    def _extract_category(self, df):
        """Стандардизирај категории"""
        # Пробај различни колони
        category_col = df.get('categories', df.get('category', df.get('genre', '')))

        def normalize_category(cat):
            if pd.isna(cat) or cat == '':
                return 'Останато'

            cat_str = str(cat).lower()

            # Отстрани листи формат ['Music'] -> Music
            if cat_str.startswith('[') and cat_str.endswith(']'):
                cat_str = cat_str.strip('[]').strip("'\"")

            # Категорија мапирања - проширено за сите извори
            category_mapping = {
                'music': 'Музика',
                'музика': 'Музика',
                'концерт': 'Музика',
                'concert': 'Музика',
                'classical': 'Класична музика',
                'класична': 'Класична музика',
                'opera': 'Опера',
                'опера': 'Опера',
                'балет': 'Балет',
                'ballet': 'Балет',

                'movie': 'Филм',
                'филм': 'Филм',
                'cinema': 'Филм',
                'кино': 'Филм',
                'хорор': 'Филм',
                'комедија': 'Филм',
                'драма': 'Филм',
                'акција': 'Филм',

                'theater': 'Театар',
                'театар': 'Театар',
                'theatre': 'Театар',
                'pretстava': 'Театар',
                'претстава': 'Театар',

                'sport': 'Спорт',
                'спорт': 'Спорт',
                'sports': 'Спорт',
                'фудбал': 'Спорт',
                'football': 'Спорт',
                'баскет': 'Спорт',

                'business': 'Деловно',
                'conference': 'Конференција',
                'конференција': 'Конференција',
                'seminar': 'Семинар',
                'семинар': 'Семинар',
                'workshop': 'Работилница',
                'работилница': 'Работилница',

                'it': 'IT/Технологија',
                'technology': 'IT/Технологија',
                'tech': 'IT/Технологија',
                'технологија': 'IT/Технологија',
                'it event': 'IT/Технологија',

                'food': 'Храна',
                'храна': 'Храна',
                'гастрономија': 'Храна',
                'фестивал на храна': 'Храна',

                'art': 'Уметност',
                'уметност': 'Уметност',
                'изложба': 'Уметност',
                'exhibition': 'Уметност',
                'галерија': 'Уметност',

                'education': 'Едукација',
                'едукација': 'Едукација',
                'обука': 'Едукација',
                'курс': 'Едукација',
                'course': 'Едукација',

                'kids': 'За деца',
                'деца': 'За деца',
                'детски': 'За деца',
                'children': 'За деца',
                'family': 'Семејно',
                'семејно': 'Семејно',

                'event': 'Настан',
                'настан': 'Настан'
            }

            for key, normalized in category_mapping.items():
                if key in cat_str:
                    return normalized

            return 'Останато'

        return category_col.apply(normalize_category)

    def _extract_organizer(self, df, source_name):
        """Извлечи организатор"""
        organizer_col = df.get('organizer', '')

        def get_organizer(org):
            if pd.isna(org) or org == '':
                # Default по извор - проширено за сите 6 извори
                source_defaults = {
                    'cineplexx': 'Cineplexx',
                    'allevents': 'AllEvents Contributors',
                    'filharmonija': 'Македонска Филхармонија',
                    'it_events': 'IT.mk',
                    'karti': 'Karti.mk',
                    'mktickets': 'MKTickets.mk'
                }

                for key, default_org in source_defaults.items():
                    if key in source_name.lower():
                        return default_org

                return 'Непознат'
            return str(org).strip()

        return organizer_col.apply(get_organizer)

    def _extract_price(self, df):
        """Стандардизирај цени"""
        price_col = df.get('ticket_price_text', df.get('price_text', ''))

        def normalize_price(price):
            if pd.isna(price) or price == '':
                return 'Бесплатно'  # Default: бесплатно наместо "се уточува"

            price_str = str(price).strip()

            # Стандардизирај
            if any(word in price_str.lower() for word in ['бесплатно', 'free', 'без']):
                return 'Бесплатно'

            # Нормализирај "250 ден." -> "250 MKD"
            price_str = re.sub(r'ден\.?', 'MKD', price_str)
            price_str = re.sub(r'денар[и]?', 'MKD', price_str)
            price_str = re.sub(r'евр[а]?', 'EUR', price_str)

            return price_str

        return price_col.apply(normalize_price)

    def _extract_is_free(self, df):
        """Определи дали е бесплатно"""
        # Провери од повеќе колони
        free_col = df.get('ticket_free', df.get('is_free', True))
        price_col = df.get('ticket_price_text', df.get('price_text', ''))

        def determine_is_free(row_idx):
            # Првин провери explicit free колона
            if not pd.isna(free_col.iloc[row_idx]):
                return bool(free_col.iloc[row_idx])

            # Провери од цена
            price = str(price_col.iloc[row_idx]).lower()
            if any(word in price for word in ['бесплатно', 'free', 'без']):
                return True
            elif any(word in price for word in ['ден', 'mkd', 'eur', '€', 'денар']):
                return False

            return True  # Default бесплатно (според новата логика)

        return [determine_is_free(i) for i in range(len(df))]

    def clean_data(self, df):
        """Главна функција за чистење"""
        print("\n Чистење на податоци...")

        original_count = len(df)

        # 1. Отстрани дупликати
        print("    Отстранување дупликати...")
        duplicates_mask = df.duplicated(subset=['title', 'date_start', 'location'], keep='first')
        duplicates_count = duplicates_mask.sum()

        if duplicates_count > 0:
            df = df[~duplicates_mask].reset_index(drop=True)
            self.stats['duplicates_removed'] = duplicates_count
            print(f"       Отстранети {duplicates_count} дупликати")

        # 2. Пополни празни вредности
        print("    Пополнување празни вредности...")

        fill_count = 0

        # Пополни празни наслови
        empty_titles = df['title'].isna() | (df['title'] == '')
        if empty_titles.any():
            df.loc[empty_titles, 'title'] = 'Настан без наслов'
            fill_count += empty_titles.sum()

        # Пополни празни датуми со "Не се знае сеуште"
        empty_dates = df['date_start'] == ''
        if empty_dates.any():
            df.loc[empty_dates, 'date_start'] = 'Не се знае сеуште'
            fill_count += empty_dates.sum()

        # Пополни празни времиња со default вредности по категорија
        empty_times = df['time_start'] == ''
        if empty_times.any():
            def default_time(category):
                mapping = {
                    'Филм': '19:00',
                    'Музика': '20:00',
                    'Класична музика': '19:30',
                    'Опера': '19:00',
                    'Балет': '19:00',
                    'Театар': '19:30',
                    'Спорт': '18:00',
                    'Конференција': '10:00',
                    'IT/Технологија': '18:00',
                    'Семинар': '14:00',
                    'Работилница': '10:00',
                    'За деца': '16:00',
                    'Семејно': '17:00'
                }
                return mapping.get(category, '19:00')

            df.loc[empty_times, 'time_start'] = df.loc[empty_times, 'category'].apply(default_time)
            fill_count += empty_times.sum()

        self.stats['values_filled'] = fill_count
        print(f"       Пополнети {fill_count} празни вредности")

        # 3. Стандардизирај текст
        print("    Стандардизација на текст...")

        # Стандардизирај наслови (Title Case)
        df['title'] = df['title'].str.strip().str.title()

        # Лимитирај должина на опис
        df['description'] = df['description'].str[:500]

        self.stats['standardizations'] += len(df)

        final_count = len(df)
        self.stats['final_rows'] = final_count

        print(f"    Финален dataset: {final_count} rows")
        print(f"    Задржани: {final_count / original_count * 100:.1f}% од оригиналните податоци")

        return df

    def validate_data(self, df):
        """Валидирај ги податоците"""
        print("\n Валидација на податоци...")

        issues = []

        # Провери задолжителни полиња
        required_fields = ['title', 'location', 'category']
        for field in required_fields:
            empty_count = (df[field].isna() | (df[field] == '')).sum()
            if empty_count > 0:
                issues.append(f"{field}: {empty_count} празни вредности")

        # Провери датуми
        invalid_dates = df['date_start'].apply(
            lambda x: x not in ['Не се знае сеуште', ''] and not re.match(r'\d{4}-\d{2}-\d{2}', str(x))
        ).sum()
        if invalid_dates > 0:
            issues.append(f"date_start: {invalid_dates} невалидни датуми")

        # Провери времиња
        invalid_times = df['time_start'].apply(
            lambda x: x != '' and not re.match(r'\d{2}:\d{2}', str(x))
        ).sum()
        if invalid_times > 0:
            issues.append(f"time_start: {invalid_times} невалидни времиња")

        if issues:
            print("    Пронајдени проблеми:")
            for issue in issues:
                print(f"      - {issue}")
        else:
            print("    Сите валидации поминати!")

        return len(issues) == 0

    def generate_summary(self, df):
        """Генерирај резиме"""
        print("\n РЕЗИМЕ ОД ЧИСТЕЊЕТО")
        print("=" * 40)
        print(f"Оригинални rows: {self.stats['original_rows']:,}")
        print(f"Финални rows: {self.stats['final_rows']:,}")
        print(f"Дупликати отстранети: {self.stats['duplicates_removed']:,}")
        print(f"Вредности пополнети: {self.stats['values_filled']:,}")
        print(f"Успешност: {self.stats['final_rows'] / self.stats['original_rows'] * 100:.1f}%")

        print(f"\n ДИСТРИБУЦИЈА ПО КАТЕГОРИИ:")
        category_counts = df['category'].value_counts()
        for category, count in category_counts.head(10).items():
            print(f"   {category}: {count}")

        print(f"\n ТОП ЛОКАЦИИ:")
        location_counts = df['location'].value_counts()
        for location, count in location_counts.head(5).items():
            print(f"   {location}: {count}")

        print(f"\n ТОП ОРГАНИЗАТОРИ:")
        organizer_counts = df['organizer'].value_counts()
        for organizer, count in organizer_counts.head(5).items():
            print(f"   {organizer}: {count}")

        print(f"\n ЦЕНИ:")
        free_events = df['is_free'].sum()
        paid_events = len(df) - free_events
        print(f"   Бесплатни настани: {free_events}")
        print(f"   Платени настани: {paid_events}")
        if free_events > paid_events:
            print(f"    Забелешка: Настани без позната цена се третираат како бесплатни")

    def run_full_cleaning(self):
        """Изврши целосно чистење"""
        print(" СТАРТУВАЊЕ НА DATA CLEANING ПРОЦЕС")
        print("=" * 50)

        # 1. Вчитај податоци
        dataframes = self.load_all_csvs()
        if not dataframes:
            print(" Нема податоци за обработка!")
            return None

        # 2. Унифицирај schema
        unified_df = self.unify_schema(dataframes)

        # 3. Исчисти податоци
        cleaned_df = self.clean_data(unified_df)

        # 4. Валидирај
        is_valid = self.validate_data(cleaned_df)

        # 5. Зачувај
        output_file = self.output_dir / f"events_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        cleaned_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"\n Зачуван во: {output_file}")

        # 6. Резиме
        self.generate_summary(cleaned_df)

        # 7. Создај GNN-ready верзија
        gnn_file = self.output_dir / "events_gnn_ready.csv"

        # Додај уникатни ID-ja
        cleaned_df['node_id'] = range(len(cleaned_df))
        cleaned_df.to_csv(gnn_file, index=False, encoding='utf-8-sig')
        print(f" GNN-ready dataset: {gnn_file}")

        print(f"\n DATA CLEANING ЗАВРШЕН УСПЕШНО!")
        print(f" Провери ја папката 'cleaned_data' за резултати")

        return cleaned_df


def main():
    """Главна функција"""
    try:
        cleaner = EventDataCleaner()
        result = cleaner.run_full_cleaning()

        if result is not None:
            print("\n Процесот завршен успешно!")
            print(" Наредни чекори:")
            print("   1. Провери го cleaned dataset")
            print("   2. Започни со graph construction")
            print("   3. Развивај GNN модели")

    except Exception as e:
        print(f" Грешка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()