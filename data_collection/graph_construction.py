import pandas as pd
import numpy as np
import networkx as nx
import torch
from torch_geometric.data import Data, HeteroData
from torch_geometric.utils import from_networkx, to_networkx
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import LabelEncoder, StandardScaler
import matplotlib.pyplot as plt
from pathlib import Path
import json
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')


class GraphConstructor:
    """Креира графови од event податоци"""

    def __init__(self, data_dir=None, output_dir="../graph_construction/graph_data"):
        if data_dir is None:
            data_dir = self.find_cleaned_data_dir()

        self.data_dir = Path(data_dir) if data_dir else None
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.df = pd.DataFrame()
        self.graphs = {}
        self.stats = {}

        # Feature encoders
        self.label_encoders = {}
        self.scaler = StandardScaler()

    def find_cleaned_data_dir(self):
        """FIXED: Подобрена автоматска детекција на cleaned_data папката"""
        print("🔍 Барам cleaned_data папка...")

        # Проширена листа на можни патишта
        possible_paths = [
            # Тековна папка
            Path("cleaned_data"),
            Path("NLP_data/cleaned_data"),

            # data_collection папка
            Path("data_collection/cleaned_data"),
            Path("data_collection/NLP_data/cleaned_data"),  # ✅ ДОДАДЕНО!

            # Еден ниво нагоре
            Path("../cleaned_data"),
            Path("../data_collection/cleaned_data"),
            Path("../data_collection/NLP_data/cleaned_data"),  # ✅ ДОДАДЕНО!

            # graph_construction папка
            Path("../graph_construction/graph_data"),
            Path("graph_construction/graph_data"),

            # Root на проектот
            Path("../../data_collection/NLP_data/cleaned_data"),  # ✅ ДОДАДЕНО!
        ]

        for path in possible_paths:
            print(f"   🔍 Проверувам: {path}")
            if path.exists():
                # Провери дали има CSV фајлови
                csv_files = list(path.glob("*.csv"))
                if csv_files:
                    print(f"   ✅ Пронајдена со {len(csv_files)} CSV фајлови: {path}")
                    return str(path)
                else:
                    print(f"   ⚠️ Папката постои но нема CSV фајлови: {path}")
            else:
                print(f"   ❌ Не постои: {path}")

        print("❌ Не можам да најдам cleaned_data папка!")
        return None

    def load_data(self):
        """Вчитај cleaned податоци"""
        print("📂 Вчитување на cleaned податоци...")

        if self.data_dir is None:
            print("❌ Нема валидна патека до податоци!")
            return False

        print(f"🔍 Користам папка: {self.data_dir}")

        if not self.data_dir.exists():
            print(f"❌ Папката не постои: {self.data_dir}")
            return False

        # FIXED: Подобрено барање на фајлови
        print("📁 Содржина на папката:")
        files_found = []
        for item in self.data_dir.iterdir():
            print(f"   - {item.name}")
            if item.is_file() and item.suffix == '.csv':
                files_found.append(item)

        if not files_found:
            print("❌ Нема CSV фајлови во папката!")
            return False

        # Приоритет на фајлови
        file_priorities = [
            "events_gnn_ready.csv",
            "events_cleaned_",  # Ќе земе било кој што почнува со ова
            ".csv"  # Било кој CSV
        ]

        selected_file = None
        for priority in file_priorities:
            for file in files_found:
                if priority in file.name:
                    selected_file = file
                    break
            if selected_file:
                break

        if not selected_file:
            # Земи го првиот CSV фајл
            selected_file = files_found[0]

        print(f"📄 Користам фајл: {selected_file.name}")

        try:
            # FIXED: Пробај различни encoding опции
            encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']

            for encoding in encodings:
                try:
                    self.df = pd.read_csv(selected_file, encoding=encoding)
                    print(f"✅ Успешно вчитано со {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    print(f"   ❌ {encoding} не работи, пробувам следен...")
                    continue
            else:
                print("❌ Не можам да го прочитам фајлот со ниеден encoding!")
                return False

            print(f"✅ Вчитани {len(self.df):,} записи")
            print(f"📊 Колони ({len(self.df.columns)}): {list(self.df.columns)}")

            # Прикажи првите неколку записи
            print(f"📋 Примери податоци:")
            print(self.df.head(2).to_string())

            return True

        except Exception as e:
            print(f"❌ Грешка при вчитување: {e}")
            return False

    def prepare_features(self):
        """FIXED: Подобрена подготовка на features"""
        print("🔧 Подготовка на features...")

        # Креирај уникатни ID-ја
        if 'node_id' not in self.df.columns:
            self.df['node_id'] = range(len(self.df))

        # Проверка на потребни колони
        required_cols = ['title', 'description']
        missing_cols = [col for col in required_cols if col not in self.df.columns]

        if missing_cols:
            print(f"⚠️ Недостасуваат колони: {missing_cols}")
            # Креирај празни колони
            for col in missing_cols:
                self.df[col] = ''

        # Подготви features
        try:
            self.prepare_text_features()
            self.prepare_categorical_features()
            self.prepare_numerical_features()
            print(f"✅ Features подготвени за {len(self.df)} настани")
            return True
        except Exception as e:
            print(f"❌ Грешка при подготовка на features: {e}")
            return False

    def prepare_text_features(self):
        """FIXED: Подобрена TF-IDF обработка"""
        print("   📝 TF-IDF features...")

        # FIXED: Правилно комбинирање на pandas колони
        combined_parts = []

        for col in ['title', 'description', 'category']:
            if col in self.df.columns:
                cleaned_col = self.df[col].fillna('').astype(str)
                combined_parts.append(cleaned_col)

        if not combined_parts:
            print("   ⚠️ Нема текстуални колони, користам dummy features")
            self.tfidf_features = np.ones((len(self.df), 10))  # Dummy features
            self.feature_names = [f"dummy_feature_{i}" for i in range(10)]
            return

        # FIXED: Користи pandas за комбинирање наместо join()
        if len(combined_parts) == 1:
            self.df['combined_text'] = combined_parts[0].str.strip()
        else:
            # Комбинирај ги колоните со space
            self.df['combined_text'] = combined_parts[0]
            for part in combined_parts[1:]:
                self.df['combined_text'] = self.df['combined_text'] + ' ' + part
            self.df['combined_text'] = self.df['combined_text'].str.strip()

        # Филтрирај празни текстови
        non_empty_mask = self.df['combined_text'].str.len() > 0
        if non_empty_mask.sum() == 0:
            print("   ⚠️ Сите текстови се празни, користам dummy features")
            self.tfidf_features = np.ones((len(self.df), 10))
            self.feature_names = [f"dummy_feature_{i}" for i in range(10)]
            return

        try:
            # TF-IDF векторизација
            vectorizer = TfidfVectorizer(
                max_features=50,  # Намалено за помала комплексност
                stop_words='english',
                ngram_range=(1, 2),
                min_df=1,  # Намалено за мали dataset-и
                token_pattern=r'\b[a-zA-Z]{2,}\b'  # Само букви, мин 2 карактери
            )

            tfidf_matrix = vectorizer.fit_transform(self.df['combined_text'])
            self.tfidf_features = tfidf_matrix.toarray()
            self.feature_names = vectorizer.get_feature_names_out()

            print(f"      ✅ TF-IDF: {self.tfidf_features.shape}")

        except Exception as e:
            print(f"   ⚠️ TF-IDF грешка: {e}, користам dummy features")
            self.tfidf_features = np.ones((len(self.df), 10))
            self.feature_names = [f"dummy_feature_{i}" for i in range(10)]

        # Филтрирај празни текстови
        non_empty_mask = self.df['combined_text'].str.len() > 0
        if non_empty_mask.sum() == 0:
            print("   ⚠️ Сите текстови се празни, користам dummy features")
            self.tfidf_features = np.ones((len(self.df), 10))
            self.feature_names = [f"dummy_feature_{i}" for i in range(10)]
            return

        try:
            # TF-IDF векторизација
            vectorizer = TfidfVectorizer(
                max_features=50,  # Намалено за помала комплексност
                stop_words='english',
                ngram_range=(1, 2),
                min_df=1,  # Намалено за мали dataset-и
                token_pattern=r'\b[a-zA-Z]{2,}\b'  # Само букви, мин 2 карактери
            )

            tfidf_matrix = vectorizer.fit_transform(self.df['combined_text'])
            self.tfidf_features = tfidf_matrix.toarray()
            self.feature_names = vectorizer.get_feature_names_out()

            print(f"      ✅ TF-IDF: {self.tfidf_features.shape}")

        except Exception as e:
            print(f"   ⚠️ TF-IDF грешка: {e}, користам dummy features")
            self.tfidf_features = np.ones((len(self.df), 10))
            self.feature_names = [f"dummy_feature_{i}" for i in range(10)]

    def prepare_categorical_features(self):
        """FIXED: Подобрена обработка на категорички features"""
        print("   🏷️ Категорички features...")

        categorical_cols = ['category', 'organizer', 'location', 'source']
        available_cols = [col for col in categorical_cols if col in self.df.columns]

        if not available_cols:
            print("   ⚠️ Нема категорички колони, користам dummy features")
            self.categorical_features = np.zeros((len(self.df), 1))
            return

        self.categorical_features = []

        for col in available_cols:
            try:
                # Филтрирај NaN и празни вредности
                cleaned_col = self.df[col].fillna('Unknown').astype(str)
                cleaned_col = cleaned_col.replace('', 'Unknown')

                le = LabelEncoder()
                encoded = le.fit_transform(cleaned_col)
                self.categorical_features.append(encoded)
                self.label_encoders[col] = le
                print(f"      ✅ {col}: {len(le.classes_)} уникатни вредности")

            except Exception as e:
                print(f"      ⚠️ Проблем со {col}: {e}")

        if self.categorical_features:
            self.categorical_features = np.column_stack(self.categorical_features)
            print(f"      ✅ Категорички: {self.categorical_features.shape}")
        else:
            self.categorical_features = np.zeros((len(self.df), 1))

    def prepare_numerical_features(self):
        """FIXED: Подобрена обработка на нумерички features"""
        print("   🔢 Нумерички features...")

        numerical_features = []

        # Основни features што секогаш постојат
        # 1. Должина на наслов
        if 'title' in self.df.columns:
            title_lengths = self.df['title'].fillna('').astype(str).str.len()
            numerical_features.append(title_lengths)

        # 2. Должина на опис
        if 'description' in self.df.columns:
            desc_lengths = self.df['description'].fillna('').astype(str).str.len()
            numerical_features.append(desc_lengths)

        # 3. Дали е бесплатен (ако постои)
        if 'is_free' in self.df.columns:
            is_free = pd.to_numeric(self.df['is_free'], errors='coerce').fillna(0)
            numerical_features.append(is_free)
        else:
            # Default: се сметаат како платени
            numerical_features.append(np.zeros(len(self.df)))

        # 4. Дали има дата (ако постои)
        if 'date_start' in self.df.columns:
            has_valid_date = ~self.df['date_start'].isin(['Не се знае сеуште', '', 'TBD', None])
            numerical_features.append(has_valid_date.astype(int))
        else:
            numerical_features.append(np.zeros(len(self.df)))

        # 5. Број карактери во комбиниран текст
        if hasattr(self, 'df') and 'combined_text' in self.df.columns:
            combined_lengths = self.df['combined_text'].str.len()
            numerical_features.append(combined_lengths)

        if numerical_features:
            self.numerical_features = np.column_stack(numerical_features)

            # FIXED: Справување со inf и NaN вредности
            self.numerical_features = np.nan_to_num(self.numerical_features,
                                                    nan=0.0, posinf=1.0, neginf=0.0)

            # Стандардизирај
            try:
                self.numerical_features = self.scaler.fit_transform(self.numerical_features)
                print(f"      ✅ Нумерички: {self.numerical_features.shape}")
            except Exception as e:
                print(f"      ⚠️ Проблем со стандардизација: {e}")
                # Fallback: нормализирај рачно
                for i in range(self.numerical_features.shape[1]):
                    col = self.numerical_features[:, i]
                    if col.std() > 0:
                        self.numerical_features[:, i] = (col - col.mean()) / col.std()
        else:
            self.numerical_features = np.zeros((len(self.df), 1))

    def create_event_similarity_graph(self, similarity_threshold=0.1):  # FIXED: Намален threshold
        """FIXED: Креирај Event Similarity Graph со подобра обработка"""
        print(f"🔗 Креирање Event Similarity Graph (threshold={similarity_threshold})...")

        try:
            # Пресметај TF-IDF сличност
            if self.tfidf_features.shape[1] > 1:
                similarity_matrix = cosine_similarity(self.tfidf_features)
            else:
                # Fallback: random similarity matrix
                similarity_matrix = np.random.rand(len(self.df), len(self.df))
                np.fill_diagonal(similarity_matrix, 1.0)

            # Креирај NetworkX граф
            G = nx.Graph()

            # Додај nodes со features
            for idx, row in self.df.iterrows():
                node_attrs = {
                    'title': str(row.get('title', f'Event_{idx}')),
                    'node_id': idx
                }

                # Додај дополнителни атрибути ако постојат
                for attr in ['category', 'organizer', 'location']:
                    if attr in row:
                        node_attrs[attr] = str(row[attr]) if pd.notna(row[attr]) else 'Unknown'

                G.add_node(idx, **node_attrs)

            # Додај edges врз основа на сличност
            edges_added = 0
            for i in range(len(self.df)):
                for j in range(i + 1, len(self.df)):
                    similarity = similarity_matrix[i][j]
                    if similarity > similarity_threshold:
                        G.add_edge(i, j, weight=float(similarity), edge_type='similarity')
                        edges_added += 1

            print(f"   ✅ {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

            # Ако нема доволно edges, намали го threshold
            if G.number_of_edges() < 10 and similarity_threshold > 0.05:
                print(f"   ⚠️ Малку edges ({G.number_of_edges()}), пробувам со threshold=0.05")
                return self.create_event_similarity_graph(similarity_threshold=0.05)

            # Конвертирај во PyTorch Geometric
            if G.number_of_edges() > 0:
                data = from_networkx(G)
            else:
                # Креирај празен граф
                print("   ⚠️ Нема edges, креирам празен граф")
                data = Data()
                data.num_nodes = len(self.df)
                data.edge_index = torch.zeros((2, 0), dtype=torch.long)

            # Додај node features
            all_features = np.concatenate([
                self.tfidf_features,
                self.categorical_features,
                self.numerical_features
            ], axis=1)

            data.x = torch.tensor(all_features, dtype=torch.float)
            data.num_nodes = len(self.df)

            self.graphs['event_similarity'] = data
            self.stats['event_similarity'] = {
                'nodes': G.number_of_nodes(),
                'edges': G.number_of_edges(),
                'density': nx.density(G) if G.number_of_edges() > 0 else 0.0,
                'avg_degree': sum(dict(G.degree()).values()) / G.number_of_nodes() if G.number_of_nodes() > 0 else 0
            }

            return G, data

        except Exception as e:
            print(f"   ❌ Грешка при креирање similarity граф: {e}")
            return None, None

    def create_heterogeneous_graph(self):
        """FIXED: Креирај Heterogeneous Graph со подобра error handling"""
        print("🌐 Креирање Heterogeneous Graph...")

        try:
            hetero_data = HeteroData()

            # EVENT NODES
            all_features = np.concatenate([
                self.tfidf_features,
                self.categorical_features,
                self.numerical_features
            ], axis=1)

            hetero_data['event'].x = torch.tensor(all_features, dtype=torch.float)
            hetero_data['event'].num_nodes = len(self.df)

            # ORGANIZER NODES
            if 'organizer' in self.df.columns:
                organizers = self.df['organizer'].dropna().unique()
                organizers = [str(org) for org in organizers if str(org) != 'nan']
            else:
                organizers = ['Unknown_Organizer']

            organizer_to_idx = {org: idx for idx, org in enumerate(organizers)}

            # Organizer features
            organizer_features = []
            for org in organizers:
                if 'organizer' in self.df.columns:
                    org_events = self.df[self.df['organizer'].astype(str) == org]
                else:
                    org_events = self.df  # Fallback

                features = [
                    len(org_events),  # Број настани
                    org_events.get('is_free', pd.Series([0])).mean(),  # % бесплатни
                    len(org_events.get('category', pd.Series(['Unknown'])).unique())  # Број категории
                ]
                organizer_features.append(features)

            hetero_data['organizer'].x = torch.tensor(organizer_features, dtype=torch.float)
            hetero_data['organizer'].num_nodes = len(organizers)

            # VENUE NODES
            if 'location' in self.df.columns:
                venues = self.df['location'].dropna().unique()
                venues = [str(venue) for venue in venues if str(venue) != 'nan']
            else:
                venues = ['Unknown_Venue']

            venue_to_idx = {venue: idx for idx, venue in enumerate(venues)}

            # Venue features
            venue_features = []
            for venue in venues:
                if 'location' in self.df.columns:
                    venue_events = self.df[self.df['location'].astype(str) == venue]
                else:
                    venue_events = self.df  # Fallback

                features = [
                    len(venue_events),  # Број настани
                    len(venue_events.get('category', pd.Series(['Unknown'])).unique()),  # Број категории
                    venue_events.get('is_free', pd.Series([0])).mean()  # % бесплатни
                ]
                venue_features.append(features)

            hetero_data['venue'].x = torch.tensor(venue_features, dtype=torch.float)
            hetero_data['venue'].num_nodes = len(venues)

            # EDGES: Event -> Organizer
            event_org_edges = []
            if 'organizer' in self.df.columns:
                for idx, row in self.df.iterrows():
                    org = str(row.get('organizer', ''))
                    if org in organizer_to_idx:
                        org_idx = organizer_to_idx[org]
                        event_org_edges.append([idx, org_idx])

            if event_org_edges:
                hetero_data['event', 'organized_by', 'organizer'].edge_index = \
                    torch.tensor(event_org_edges, dtype=torch.long).t().contiguous()

            # EDGES: Event -> Venue
            event_venue_edges = []
            if 'location' in self.df.columns:
                for idx, row in self.df.iterrows():
                    venue = str(row.get('location', ''))
                    if venue in venue_to_idx:
                        venue_idx = venue_to_idx[venue]
                        event_venue_edges.append([idx, venue_idx])

            if event_venue_edges:
                hetero_data['event', 'located_at', 'venue'].edge_index = \
                    torch.tensor(event_venue_edges, dtype=torch.long).t().contiguous()

            print(f"   ✅ Events: {hetero_data['event'].num_nodes}")
            print(f"   ✅ Organizers: {hetero_data['organizer'].num_nodes}")
            print(f"   ✅ Venues: {hetero_data['venue'].num_nodes}")

            self.graphs['heterogeneous'] = hetero_data
            self.stats['heterogeneous'] = {
                'event_nodes': hetero_data['event'].num_nodes,
                'organizer_nodes': hetero_data['organizer'].num_nodes,
                'venue_nodes': hetero_data['venue'].num_nodes,
                'event_org_edges': len(event_org_edges),
                'event_venue_edges': len(event_venue_edges)
            }

            return hetero_data

        except Exception as e:
            print(f"   ❌ Грешка при креирање heterogeneous граф: {e}")
            return None

    def visualize_graphs(self):
        """FIXED: Подобрена визуализација со error handling"""
        print("🎨 Визуализација на графови...")

        vis_dir = self.output_dir / "visualizations"
        vis_dir.mkdir(exist_ok=True)

        try:
            # 1. Event Similarity Graph
            if 'event_similarity' in self.graphs:
                self.visualize_similarity_graph(vis_dir)

            # 2. Heterogeneous Graph Overview
            if 'heterogeneous' in self.graphs:
                self.visualize_hetero_overview(vis_dir)

            # 3. Graph Statistics
            self.visualize_graph_stats(vis_dir)

            print(f"   ✅ Визуализации зачувани во: {vis_dir}")

        except Exception as e:
            print(f"   ⚠️ Проблем со визуализација: {e}")

    def visualize_similarity_graph(self, vis_dir):
        """FIXED: Подобрена визуализација на similarity граф"""
        try:
            data = self.graphs['event_similarity']

            # Конвертирај назад во NetworkX за визуализација
            if hasattr(data, 'edge_index') and data.edge_index.shape[1] > 0:
                G = to_networkx(data, to_undirected=True)
            else:
                # Креирај граф само со nodes
                G = nx.Graph()
                for i in range(len(self.df)):
                    title = self.df.iloc[i].get('title', f'Event_{i}')
                    G.add_node(i, title=str(title)[:20])

            # Земи subset за visualization (ако е премногу голем)
            if G.number_of_nodes() > 100:
                degrees = dict(G.degree())
                top_nodes = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:50]
                subgraph_nodes = [node for node, degree in top_nodes]
                G = G.subgraph(subgraph_nodes)

            plt.figure(figsize=(12, 8))

            # Layout
            if G.number_of_edges() > 0:
                pos = nx.spring_layout(G, k=2, iterations=30)
            else:
                # Random layout за nodes без edges
                pos = {node: (np.random.rand(), np.random.rand()) for node in G.nodes()}

            # Node colors по категорија
            if 'category' in self.df.columns:
                categories = []
                for node in G.nodes():
                    if node < len(self.df):
                        cat = self.df.iloc[node].get('category', 'Unknown')
                        categories.append(str(cat) if pd.notna(cat) else 'Unknown')
                    else:
                        categories.append('Unknown')
            else:
                categories = ['Unknown'] * len(G.nodes())

            unique_categories = list(set(categories))
            colors = plt.cm.Set3(np.linspace(0, 1, len(unique_categories)))
            color_map = dict(zip(unique_categories, colors))
            node_colors = [color_map[cat] for cat in categories]

            # Цртај граф
            nx.draw_networkx_nodes(G, pos, node_color=node_colors,
                                   node_size=100, alpha=0.8)

            if G.number_of_edges() > 0:
                nx.draw_networkx_edges(G, pos, alpha=0.3, width=0.5)

            # Легенда
            for cat, color in color_map.items():
                plt.scatter([], [], c=[color], label=cat[:15], s=100)

            plt.legend(title="Категории", bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.title("🔗 Event Similarity Graph", size=14, fontweight='bold')
            plt.axis('off')
            plt.tight_layout()
            plt.savefig(vis_dir / 'similarity_graph.png', dpi=200, bbox_inches='tight')
            plt.close()  # FIXED: Затвори ја фигурата

        except Exception as e:
            print(f"      ⚠️ Проблем со similarity visualization: {e}")

    def visualize_hetero_overview(self, vis_dir):
        """FIXED: Подобрена visualizacija на heterogeneous граф"""
        try:
            hetero_data = self.graphs['heterogeneous']

            fig, axes = plt.subplots(2, 2, figsize=(14, 10))

            # 1. Node counts
            node_types = ['Events', 'Organizers', 'Venues']
            node_counts = [
                hetero_data['event'].num_nodes,
                hetero_data['organizer'].num_nodes,
                hetero_data['venue'].num_nodes
            ]

            bars = axes[0, 0].bar(node_types, node_counts, color=['#1f77b4', '#ff7f0e', '#2ca02c'])
            axes[0, 0].set_title('🌐 Node Types', fontweight='bold')
            axes[0, 0].set_ylabel('Број nodes')

            for bar, count in zip(bars, node_counts):
                axes[0, 0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(node_counts) * 0.01,
                                str(count), ha='center', va='bottom', fontweight='bold')

            # 2. Edge types
            edge_types = ['Event→Organizer', 'Event→Venue']
            edge_counts = [
                self.stats['heterogeneous']['event_org_edges'],
                self.stats['heterogeneous']['event_venue_edges']
            ]

            bars = axes[0, 1].bar(edge_types, edge_counts, color=['#d62728', '#9467bd'])
            axes[0, 1].set_title('🔗 Edge Types', fontweight='bold')
            axes[0, 1].set_ylabel('Број edges')
            axes[0, 1].tick_params(axis='x', rotation=15)

            for bar, count in zip(bars, edge_counts):
                axes[0, 1].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(edge_counts) * 0.01,
                                str(count), ha='center', va='bottom', fontweight='bold')

            # 3. Organizer distribution
            if 'organizer' in self.df.columns:
                organizer_event_counts = self.df['organizer'].value_counts().head(10)
            else:
                organizer_event_counts = pd.Series(['Unknown'], index=['Unknown_Organizer'])

            y_pos = range(len(organizer_event_counts))
            axes[1, 0].barh(y_pos, organizer_event_counts.values)
            axes[1, 0].set_yticks(y_pos)
            axes[1, 0].set_yticklabels([org[:15] + '...' if len(str(org)) > 15 else str(org)
                                        for org in organizer_event_counts.index])
            axes[1, 0].set_title('🏢 Top Organizers', fontweight='bold')
            axes[1, 0].set_xlabel('Број настани')
            axes[1, 0].invert_yaxis()

            # 4. Venue distribution
            if 'location' in self.df.columns:
                venue_event_counts = self.df['location'].value_counts().head(10)
            else:
                venue_event_counts = pd.Series([len(self.df)], index=['Unknown_Venue'])

            y_pos = range(len(venue_event_counts))
            axes[1, 1].barh(y_pos, venue_event_counts.values)
            axes[1, 1].set_yticks(y_pos)
            axes[1, 1].set_yticklabels([venue[:15] + '...' if len(str(venue)) > 15 else str(venue)
                                        for venue in venue_event_counts.index])
            axes[1, 1].set_title('🏛️ Top Venues', fontweight='bold')
            axes[1, 1].set_xlabel('Број настани')
            axes[1, 1].invert_yaxis()

            plt.tight_layout()
            plt.savefig(vis_dir / 'heterogeneous_overview.png', dpi=200, bbox_inches='tight')
            plt.close()  # FIXED: Затвори ја фигурата

        except Exception as e:
            print(f"      ⚠️ Проблем со hetero visualization: {e}")

    def visualize_graph_stats(self, vis_dir):
        """FIXED: Подобрена статистичка визуализација"""
        if not self.stats:
            return

        try:
            fig, axes = plt.subplots(2, 2, figsize=(14, 8))

            # 1. Node counts comparison
            graph_names = list(self.stats.keys())
            node_counts = []

            for graph_name in graph_names:
                if graph_name == 'heterogeneous':
                    node_counts.append(self.stats[graph_name]['event_nodes'])
                else:
                    node_counts.append(self.stats[graph_name]['nodes'])

            bars = axes[0, 0].bar(graph_names, node_counts, alpha=0.8)
            axes[0, 0].set_title('📊 Nodes по тип граф', fontweight='bold')
            axes[0, 0].set_ylabel('Број nodes')
            axes[0, 0].tick_params(axis='x', rotation=15)

            for bar, count in zip(bars, node_counts):
                axes[0, 0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(node_counts) * 0.01,
                                str(count), ha='center', va='bottom', fontweight='bold')

            # 2. Edge counts comparison
            edge_counts = []
            for graph_name in graph_names:
                if graph_name == 'heterogeneous':
                    total_edges = (self.stats[graph_name]['event_org_edges'] +
                                   self.stats[graph_name]['event_venue_edges'])
                    edge_counts.append(total_edges)
                else:
                    edge_counts.append(self.stats[graph_name]['edges'])

            bars = axes[0, 1].bar(graph_names, edge_counts, alpha=0.8, color='orange')
            axes[0, 1].set_title('🔗 Edges по тип граф', fontweight='bold')
            axes[0, 1].set_ylabel('Број edges')
            axes[0, 1].tick_params(axis='x', rotation=15)

            for bar, count in zip(bars, edge_counts):
                axes[0, 1].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(edge_counts) * 0.01,
                                str(count), ha='center', va='bottom', fontweight='bold')

            # 3. Feature dimensions
            feature_info = {
                'TF-IDF': self.tfidf_features.shape[1],
                'Categorical': self.categorical_features.shape[1],
                'Numerical': self.numerical_features.shape[1]
            }

            bars = axes[1, 0].bar(feature_info.keys(), feature_info.values(),
                                  alpha=0.8, color='purple')
            axes[1, 0].set_title('🎯 Feature Dimensions', fontweight='bold')
            axes[1, 0].set_ylabel('Број features')

            for bar, count in zip(bars, feature_info.values()):
                axes[1, 0].text(bar.get_x() + bar.get_width() / 2, bar.get_height() + max(feature_info.values()) * 0.01,
                                str(count), ha='center', va='bottom', fontweight='bold')

            # 4. Data quality overview
            quality_metrics = {
                'Total Events': len(self.df),
                'With Title': len(self.df[self.df.get('title', pd.Series()).fillna('').astype(str).str.len() > 0]),
                'With Category': len(
                    self.df[self.df.get('category', pd.Series()).notna()]) if 'category' in self.df.columns else 0,
                'With Organizer': len(
                    self.df[self.df.get('organizer', pd.Series()).notna()]) if 'organizer' in self.df.columns else 0
            }

            bars = axes[1, 1].bar(quality_metrics.keys(), quality_metrics.values(),
                                  alpha=0.8, color='green')
            axes[1, 1].set_title('📋 Data Quality', fontweight='bold')
            axes[1, 1].set_ylabel('Број записи')
            axes[1, 1].tick_params(axis='x', rotation=15)

            for bar, count in zip(bars, quality_metrics.values()):
                axes[1, 1].text(bar.get_x() + bar.get_width() / 2,
                                bar.get_height() + max(quality_metrics.values()) * 0.01,
                                str(count), ha='center', va='bottom', fontweight='bold')

            plt.tight_layout()
            plt.savefig(vis_dir / 'graph_statistics.png', dpi=200, bbox_inches='tight')
            plt.close()  # FIXED: Затвори ја фигурата

        except Exception as e:
            print(f"      ⚠️ Проблем со stats visualization: {e}")

    def save_graphs(self):
        """FIXED: Подобрено зачувување со error handling"""
        print("💾 Зачувување на графови...")

        try:
            # PyTorch Geometric формат
            saved_count = 0
            for name, graph in self.graphs.items():
                try:
                    output_file = self.output_dir / f"{name}_graph.pt"
                    torch.save(graph, output_file)
                    print(f"   ✅ {name}_graph.pt")
                    saved_count += 1
                except Exception as e:
                    print(f"   ❌ Проблем со {name}: {e}")

            # Метаподатоци
            metadata = {
                'created_at': datetime.now().isoformat(),
                'total_events': len(self.df),
                'graphs_created': list(self.graphs.keys()),
                'feature_dimensions': {
                    'tfidf': int(self.tfidf_features.shape[1]),
                    'categorical': int(self.categorical_features.shape[1]),
                    'numerical': int(self.numerical_features.shape[1])
                },
                'statistics': self.stats,
                'data_columns': list(self.df.columns)
            }

            with open(self.output_dir / 'graph_metadata.json', 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)

            print(f"   ✅ graph_metadata.json")
            print(f"💾 {saved_count} графови зачувани во: {self.output_dir}")
            return True

        except Exception as e:
            print(f"❌ Проблем со зачувување: {e}")
            return False

    def generate_summary(self):
        """FIXED: Подобрено резиме со error handling"""
        print("\n" + "=" * 60)
        print("🔗 GRAPH CONSTRUCTION РЕЗУЛТАТИ")
        print("=" * 60)
        print(f"📊 Dataset: {len(self.df):,} настани")

        if hasattr(self, 'tfidf_features') and hasattr(self, 'categorical_features') and hasattr(self,
                                                                                                 'numerical_features'):
            total_features = (self.tfidf_features.shape[1] +
                              self.categorical_features.shape[1] +
                              self.numerical_features.shape[1])
            print(f"🎯 Feature dimensions: {total_features}")

        print(f"📈 Графови создадени: {len(self.graphs)}")

        for name, stats in self.stats.items():
            print(f"\n🔗 {name.title()} Graph:")
            if 'nodes' in stats:
                print(f"   Nodes: {stats['nodes']:,}")
                print(f"   Edges: {stats['edges']:,}")
                print(f"   Density: {stats['density']:.4f}")
                if 'avg_degree' in stats:
                    print(f"   Avg Degree: {stats['avg_degree']:.2f}")
            else:
                for key, value in stats.items():
                    print(f"   {key}: {value}")

        print(f"\n📁 Output папка: {self.output_dir}")
        print("✅ Graph Construction завршен!")

    def run_full_construction(self):
        """FIXED: Главна функција со подобрен error handling"""
        print("🔗 Graph Construction System")
        print("=" * 50)

        try:
            # 1. Load data
            if not self.load_data():
                print("❌ Не можам да ги вчитам податоците!")
                return False

            # 2. Prepare features
            if not self.prepare_features():
                print("❌ Проблем со подготовка на features!")
                return False

            # 3. Create graphs
            print("\n🔨 Креирање графови...")
            graphs_created = 0

            # Event Similarity Graph
            try:
                result = self.create_event_similarity_graph()
                if result[0] is not None:
                    graphs_created += 1
                    print("   ✅ Event Similarity Graph")
                else:
                    print("   ⚠️ Event Similarity Graph не е креиран")
            except Exception as e:
                print(f"   ❌ Similarity graph: {e}")

            # Heterogeneous Graph
            try:
                hetero_result = self.create_heterogeneous_graph()
                if hetero_result is not None:
                    graphs_created += 1
                    print("   ✅ Heterogeneous Graph")
                else:
                    print("   ⚠️ Heterogeneous Graph не е креиран")
            except Exception as e:
                print(f"   ❌ Heterogeneous graph: {e}")

            if graphs_created == 0:
                print("❌ Ниеден граф не е креиран!")
                return False

            # 4. Visualize
            self.visualize_graphs()

            # 5. Save
            if not self.save_graphs():
                print("⚠️ Проблем со зачувување, но графовите се креирани")

            # 6. Summary
            self.generate_summary()

            return True

        except Exception as e:
            print(f"❌ Критична грешка: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """FIXED: Главна функција со подобрена детекција"""
    try:
        print("🔗 Graph Construction System - FIXED VERSION")
        print("=" * 50)

        # Креирај конструктор (автоматски ќе ги детектира патиштата)
        constructor = GraphConstructor()

        if constructor.data_dir is None:
            print("\n❌ Не можам да најдам cleaned_data!")
            print("📋 Предлози:")
            print("   1. Провери дали постои папката 'data_collection/NLP_data/cleaned_data/'")
            print("   2. Провери дали има CSV фајлови во таа папка")
            print("   3. Стартувај ја скриптата од root папката на проектот")
            return False

        success = constructor.run_full_construction()

        if success:
            print("\n🎉 Graph Construction завршен успешно!")
            print(f"\n📂 Провери ја папката '{constructor.output_dir}' за:")
            print("   - PyTorch Geometric graph фајлови (.pt)")
            print("   - Визуализации (PNG)")
            print("   - Метаподатоци (JSON)")
            print("\n💡 Следен чекор: Користи ги .pt фајловите за GNN тренирање!")
        else:
            print("\n❌ Проблем при graph construction")
            print("📋 Можни решенија:")
            print("   1. Провери ги патиштата до податоците")
            print("   2. Провери дали CSV фајловите се читливи")
            print("   3. Инсталирај ги потребните библиотеки")

        return success

    except Exception as e:
        print(f"❌ Критична грешка: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    main()