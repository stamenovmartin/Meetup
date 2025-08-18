import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import json
from datetime import datetime
import re
import warnings

warnings.filterwarnings('ignore')

plt.rcParams['font.family'] = ['DejaVu Sans', 'Arial Unicode MS', 'sans-serif']
plt.rcParams['axes.unicode_minus'] = False

plt.style.use('seaborn-v0_8')
sns.set_palette("husl")


class EventAnalyticsDashboard:
    """Анализа на clean event податоци"""

    def __init__(self, data_dir="cleaned_data"):
        self.data_dir = Path(data_dir)
        self.output_dir = Path("analytics_output")
        self.output_dir.mkdir(exist_ok=True)

        self.df = pd.DataFrame()
        self.stats = {}

    def load_data(self):
        cleaned_files = list(self.data_dir.glob("events_cleaned_*.csv"))

        if not cleaned_files:
            cleaned_files = list(self.data_dir.glob("*_cleaned.csv"))

        if not cleaned_files:
            print("Нема cleaned CSV фајлови!")
            return False

        latest_file = max(cleaned_files, key=lambda p: p.stat().st_mtime)

        try:
            self.df = pd.read_csv(latest_file, encoding='utf-8-sig')
            print(f"Вчитани се {len(self.df):,} настани од {latest_file.name}")

            print(f"Колони: {list(self.df.columns)}")
            return True

        except Exception as e:
            print(f"Грешка :{e}")
            return False

    def prepare_data(self):
        """Подготви податоци за анализа"""

        required_cols = ['title', 'category', 'location', 'source']
        missing_cols = [col for col in required_cols if col not in self.df.columns]

        if missing_cols: print(f" Недостасуваат колони: {missing_cols}")

        self.df = self.df.dropna(subset=['title'])
        self.prepare_price_data()
        self.prepare_date_data()

        print(f"Подготвени {len(self.df)} валидни настани")

    def prepare_price_data(self):
        self.df['is_free_bool'] = False
        self.df['price_numeric'] = 0

        if 'is_free' in self.df.columns: self.df['is_free_bool'] = self.df['is_free'].fillna(True)

        if 'price_text' in self.df.columns:
            for idx, row in self.df.iterrows():
                price_text = str(row.get('price_text', '')).lower()

                if 'бесплатно' in price_text:
                    self.df.at[idx, 'is_free_bool'] = True
                    self.df.at[idx, 'price_numeric'] = 0
                else:
                    numbers = re.findall(r'\d+', price_text)
                    if numbers:
                        self.df.at[idx, 'price_numeric'] = int(numbers[0])
                        self.df.at[idx, 'is_free_bool'] = False

    def prepare_date_data(self):
        """Подготви податоци за датуми"""
        self.df['has_date'] = False
        self.df['month'] = None

        if 'date_start' in self.df.columns:
            valid_dates = ~self.df['date_start'].isin(['Не се знае сеуште', '', 'TBD'])
            self.df['has_date'] = valid_dates

            try:
                parsed_dates = pd.to_datetime(self.df['date_start'], errors='coerce')
                self.df['month'] = parsed_dates.dt.month
                self.df['weekday'] = parsed_dates.dt.day_name()
            except:
                pass

    def analyze_venues(self):
        """Анализа на места/venues"""

        if 'location' not in self.df.columns:
            print("Нема податоци за локации")
            return

        venue_counts = self.df['location'].value_counts().head(12)

        plt.figure(figsize=(14, 8))
        colors = sns.color_palette("viridis", len(venue_counts))
        bars = plt.barh(range(len(venue_counts)), venue_counts.values, color=colors)

        plt.yticks(range(len(venue_counts)), venue_counts.index)
        plt.xlabel('Број на настани', fontsize=12)
        plt.title('Најпопуларни места за настани', fontsize=16, fontweight='bold', pad=20)
        plt.gca().invert_yaxis()

        # Додај бројки на bars
        for i, (bar, value) in enumerate(zip(bars, venue_counts.values)):
            plt.text(value + 0.5, i, str(value), va='center', fontweight='bold', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'venue_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        self.stats['top_venues'] = venue_counts.to_dict()

    def analyze_categories(self):
        """Анализа на категории"""
        if 'category' not in self.df.columns:
            print("Нема податоци за категории")
            return

        category_counts = self.df['category'].value_counts()

        plt.figure(figsize=(16, 10))

        color_map = {
            'IT/Технологија': '#1f77b4',  # Сина
            'Музика': '#ff7f0e',  # Оранжева
            'Останато': '#2ca02c',  # Зелена
            'Театар': '#d62728',  # Црвена
            'Спорт': '#9467bd',  # Виолетова
            'Филм': '#8c564b',  # Кафена
            'Балет': '#e377c2',  # Розова
            'Конференција': '#7f7f7f',  # Сива
            'Деловно': '#bcbd22',  # Маслинеста
            'Фестивал': '#17becf',  # Светло сина
            'Едукација': '#ff9896',  # Светло црвена
            'Семинар': '#c5b0d5',  # Светло виолетова
            'Уметност': '#c49c94',  # Светло кафена
            'Храна': '#f7b6d3',  # Светло розова
            'За деца': '#c7c7c7',  # Светло сива
            'Работилница': '#dbdb8d'  # Светло маслинеста
        }

        colors = []
        for category in category_counts.index:
            if category in color_map:
                colors.append(color_map[category])
            else:
                colors.append('#1f77b4')

        wedges, texts, autotexts = plt.pie(
            category_counts.values,
            labels=None,
            autopct='%1.1f%%',
            colors=colors,
            startangle=90,
            pctdistance=0.8,
            textprops={'fontsize': 12, 'fontweight': 'bold'}
        )

        plt.title('Распределба на категории настани', fontsize=18, fontweight='bold', pad=30)

        for autotext in autotexts:
            autotext.set_color('black')
            autotext.set_fontweight('bold')
            autotext.set_fontsize(12)

        plt.legend(
            wedges,
            [f'{cat} ({count})' for cat, count in zip(category_counts.index, category_counts.values)],
            title="Категории",
            loc="center left",
            bbox_to_anchor=(1, 0, 0.5, 1),
            fontsize=12,
            title_fontsize=14,
            frameon=True,
            fancybox=True,
            shadow=True
        )

        plt.axis('equal')
        plt.tight_layout()
        plt.savefig(self.output_dir / 'category_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        self.stats['categories'] = category_counts.to_dict()

    def analyze_prices(self):
        """Анализа на цени"""

        free_count = self.df['is_free_bool'].sum()
        paid_count = len(self.df) - free_count

        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        categories = ['Бесплатни', 'Платени']
        counts = [free_count, paid_count]
        colors = ['#2ecc71', '#e74c3c']

        bars = axes[0, 0].bar(categories, counts, color=colors, alpha=0.8)
        axes[0, 0].set_title('Бесплатни vs Платени настани', fontweight='bold', fontsize=14)
        axes[0, 0].set_ylabel('Број на настани')

        for bar, count in zip(bars, counts):
            axes[0, 0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 1,
                            str(count), ha='center', va='bottom', fontweight='bold', fontsize=12)

        axes[0, 1].pie([free_count, paid_count], labels=categories, colors=colors,
                       autopct='%1.1f%%', startangle=90)
        axes[0, 1].set_title('Процентна распределба', fontweight='bold', fontsize=14)

        paid_events = self.df[~self.df['is_free_bool'] & (self.df['price_numeric'] > 0)]

        if len(paid_events) > 5:
            category_prices = []
            categories = []

            for cat in paid_events['category'].unique():
                cat_prices = paid_events[paid_events['category'] == cat]['price_numeric']
                if len(cat_prices) >= 2:
                    category_prices.append(cat_prices.tolist())
                    categories.append(cat)

            if category_prices:
                bp = axes[1, 0].boxplot(category_prices, labels=categories, patch_artist=True)
                axes[1, 0].set_title('Цени по категорија', fontweight='bold', fontsize=14)
                axes[1, 0].set_xlabel('Категорија')
                axes[1, 0].set_ylabel('Цена (MKD)')
                axes[1, 0].tick_params(axis='x', rotation=45)

                colors_box = sns.color_palette("husl", len(bp['boxes']))
                for patch, color in zip(bp['boxes'], colors_box):
                    patch.set_facecolor(color)

        if 'source' in self.df.columns:
            source_free = self.df.groupby('source')['is_free_bool'].agg(['sum', 'count'])
            source_free['paid'] = source_free['count'] - source_free['sum']

            x_pos = np.arange(len(source_free))
            width = 0.35

            bars1 = axes[1, 1].bar(x_pos - width / 2, source_free['sum'], width,
                                   label='Бесплатни', color='#2ecc71', alpha=0.8)
            bars2 = axes[1, 1].bar(x_pos + width / 2, source_free['paid'], width,
                                   label='Платени', color='#e74c3c', alpha=0.8)

            axes[1, 1].set_title('💰 Цени по извор', fontweight='bold', fontsize=14)
            axes[1, 1].set_xlabel('Извор')
            axes[1, 1].set_ylabel('Број на настани')
            axes[1, 1].set_xticks(x_pos)
            axes[1, 1].set_xticklabels([s.replace('_', '\n') for s in source_free.index], rotation=45)
            axes[1, 1].legend()

        plt.tight_layout()
        plt.savefig(self.output_dir / 'price_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        avg_price = paid_events['price_numeric'].mean() if len(paid_events) > 0 else 0

        self.stats['prices'] = {
            'free_events': int(free_count),
            'paid_events': int(paid_count),
            'free_percentage': round(free_count / len(self.df) * 100, 1),
            'average_price': round(avg_price, 0) if avg_price > 0 else 0
        }

    def analyze_sources(self):
        """Анализа на извори на податоци"""

        if 'source' not in self.df.columns:
            print("Нема податоци за извори")
            return

        source_counts = self.df['source'].value_counts()

        plt.figure(figsize=(12, 8))
        colors = sns.color_palette("muted", len(source_counts))
        bars = plt.barh(range(len(source_counts)), source_counts.values, color=colors)

        clean_names = [name.replace('_', ' ').title() for name in source_counts.index]
        plt.yticks(range(len(source_counts)), clean_names)
        plt.xlabel('Број на настани', fontsize=12)
        plt.title('Извори на податоци', fontsize=16, fontweight='bold', pad=20)
        plt.gca().invert_yaxis()

        for i, (bar, value) in enumerate(zip(bars, source_counts.values)):
            plt.text(value + 1, i, str(value), va='center', fontweight='bold', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'source_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        self.stats['sources'] = source_counts.to_dict()

    def analyze_organizers(self):
        """Анализа на организатори"""

        if 'organizer' not in self.df.columns:
            print("Нема податоци за организатори")
            return

        org_counts = self.df['organizer'].value_counts().head(10)

        plt.figure(figsize=(14, 8))
        colors = sns.color_palette("coolwarm", len(org_counts))
        bars = plt.barh(range(len(org_counts)), org_counts.values, color=colors)

        plt.yticks(range(len(org_counts)), org_counts.index)
        plt.xlabel('Број на настани', fontsize=12)
        plt.title('Најактивни организатори', fontsize=16, fontweight='bold', pad=20)
        plt.gca().invert_yaxis()

        for i, (bar, value) in enumerate(zip(bars, org_counts.values)):
            plt.text(value + 0.5, i, str(value), va='center', fontweight='bold', fontsize=10)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'organizer_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        self.stats['organizers'] = org_counts.to_dict()

    def analyze_dates(self):
        """Анализа на датуми"""

        if 'date_start' not in self.df.columns:
            print("Нема податоци за датуми")
            return

        total_events = len(self.df)
        with_dates = self.df['has_date'].sum()
        without_dates = total_events - with_dates

        fig, axes = plt.subplots(1, 2, figsize=(16, 6))

        categories = ['Со датум', 'Без датум']
        counts = [with_dates, without_dates]
        colors = ['#3498db', '#95a5a6']

        bars = axes[0].bar(categories, counts, color=colors, alpha=0.8)
        axes[0].set_title('Достапност на датуми', fontweight='bold', fontsize=14)
        axes[0].set_ylabel('Број на настани')

        for bar, count in zip(bars, counts):
            percentage = count / total_events * 100
            axes[0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 5,
                         f'{count}\n({percentage:.1f}%)', ha='center', va='bottom',
                         fontweight='bold', fontsize=11)

        if 'month' in self.df.columns and self.df['month'].notna().any():
            valid_months = self.df['month'].dropna()
            valid_months = valid_months[(valid_months >= 1) & (valid_months <= 12)]

            if len(valid_months) > 0:
                month_counts = valid_months.value_counts().sort_index()
                month_names = ['Јан', 'Фев', 'Мар', 'Апр', 'Мај', 'Јун',
                               'Јул', 'Авг', 'Сеп', 'Окт', 'Ное', 'Дек']

                month_labels = []
                for month_num in month_counts.index:
                    if isinstance(month_num, (int, float)) and not pd.isna(month_num):
                        month_idx = int(month_num) - 1
                        if 0 <= month_idx < 12:
                            month_labels.append(month_names[month_idx])
                        else:
                            month_labels.append(f"М{int(month_num)}")
                    else:
                        month_labels.append("N/A")

                bars_month = axes[1].bar(range(len(month_counts)), month_counts.values,
                                         color=sns.color_palette("viridis", len(month_counts)), alpha=0.8)
                axes[1].set_title('Настани по месец', fontweight='bold', fontsize=14)
                axes[1].set_xlabel('Месец')
                axes[1].set_ylabel('Број на настани')
                axes[1].set_xticks(range(len(month_counts)))
                axes[1].set_xticklabels(month_labels, rotation=45)

                for bar, value in zip(bars_month, month_counts.values):
                    axes[1].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                                 str(value), ha='center', va='bottom', fontweight='bold', fontsize=10)
            else:
                axes[1].text(0.5, 0.5, 'Нема валидни датуми\nза месечна анализа',
                             ha='center', va='center', transform=axes[1].transAxes,
                             fontsize=14, bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray"))
                axes[1].set_title('Настани по месец', fontweight='bold', fontsize=14)
        else:
            axes[1].text(0.5, 0.5, 'Нема податоци\nза месечна анализа',
                         ha='center', va='center', transform=axes[1].transAxes,
                         fontsize=14, bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgray"))
            axes[1].set_title('Настани по месец', fontweight='bold', fontsize=14)

        plt.tight_layout()
        plt.savefig(self.output_dir / 'date_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

        self.stats['dates'] = {
            'with_dates': int(with_dates),
            'without_dates': int(without_dates),
            'date_availability': round(with_dates / total_events * 100, 1)
        }

    def generate_summary(self):
        """Генерирај резиме"""

        summary = {
            'timestamp': datetime.now().isoformat(),
            'total_events': len(self.df),
            'statistics': self.stats,
            'key_insights': {
                'most_popular_venue': list(self.stats.get('top_venues', {}).keys())[0] if self.stats.get(
                    'top_venues') else 'N/A',
                'most_common_category': list(self.stats.get('categories', {}).keys())[0] if self.stats.get(
                    'categories') else 'N/A',
                'free_events_percentage': self.stats.get('prices', {}).get('free_percentage', 0),
                'main_data_source': list(self.stats.get('sources', {}).keys())[0] if self.stats.get(
                    'sources') else 'N/A',
                'data_quality': {
                    'has_venues': 'location' in self.df.columns,
                    'has_categories': 'category' in self.df.columns,
                    'has_prices': 'price_text' in self.df.columns,
                    'has_dates': self.stats.get('dates', {}).get('date_availability', 0)
                }
            }
        }

        with open(self.output_dir / 'analytics_summary.json', 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        print("РЕЗУЛТАТИ ОД АНАЛИЗАТА")
        print(f"Вкупно настани: {len(self.df):,}")
        print(f"Најпопуларно место: {summary['key_insights']['most_popular_venue']}")
        print(f"Најчеста категорија: {summary['key_insights']['most_common_category']}")
        print(f"Бесплатни настани: {summary['key_insights']['free_events_percentage']:.1f}%")
        print(f"Главен извор: {summary['key_insights']['main_data_source']}")
        print(f"Настани со датуми: {self.stats.get('dates', {}).get('date_availability', 0):.1f}%")

        if self.stats.get('prices', {}).get('average_price', 0) > 0:
            print(f"Просечна цена: {self.stats['prices']['average_price']:.0f} MKD")

        print(f"\nСите графики зачувани во: {self.output_dir}")
        print("Анализата завршена!")

        return summary

    def run_full_analysis(self):
        """Изврши целосна анализа"""

        if not self.load_data():
            return False

        self.prepare_data()

        self.analyze_venues()
        self.analyze_categories()
        self.analyze_prices()
        self.analyze_sources()
        self.analyze_organizers()
        self.analyze_dates()

        summary = self.generate_summary()

        return True


def main():
    """Главна функција"""
    try:
        dashboard = EventAnalyticsDashboard()
        success = dashboard.run_full_analysis()

        if success:
            print("\nDashboard создаден успешно!")
        else:
            print("\nПроблем при создавање на dashboard")

    except Exception as e:
        print(f"Грешка: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()