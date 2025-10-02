#!/usr/bin/env python3
"""
ENHANCED Graph Construction for Event Recommendation
Creates MULTI-DIMENSIONAL graph with:
- Temporal edges (same time period, day of week, season, hour of day)
- Thematic edges (similar tags/categories, description TF-IDF similarity)
- Location edges (same venue/city, geographic proximity)
- Organizer edges (same organizer, organizer category similarity)
- Popularity edges (based on attendance patterns if available)
- Multi-layer connections with different edge weights
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import torch
from torch_geometric.data import Data, HeteroData
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.preprocessing import StandardScaler, LabelEncoder
import networkx as nx
import os
import logging
from pathlib import Path
import json
import warnings

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EnhancedGraphConstructor:
    """Enhanced Multi-Dimensional Graph Constructor for Events"""

    def __init__(self, data_path=None, output_dir="graph_construction/graph_data"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        if data_path is None:
            data_path = self.find_latest_cleaned_data()

        self.data_path = data_path
        self.df = None
        self.graphs = {}

    def find_latest_cleaned_data(self):
        """Find the latest cleaned data CSV"""
        possible_paths = [
            "data_collection/NLP_data/cleaned_data",
            "../data_collection/NLP_data/cleaned_data",
            "cleaned_data"
        ]

        for path in possible_paths:
            path = Path(path)
            if path.exists():
                csv_files = sorted(path.glob("events_gnn_ready.csv"))
                if not csv_files:
                    csv_files = sorted(path.glob("events_cleaned_*.csv"))
                if csv_files:
                    logger.info(f"✅ Found data: {csv_files[-1]}")
                    return str(csv_files[-1])

        logger.error("❌ No cleaned data found!")
        return None

    def load_data(self):
        """Load and preprocess event data"""
        logger.info("📂 Loading event data...")

        if not self.data_path:
            logger.error("No data path specified!")
            return False

        try:
            self.df = pd.read_csv(self.data_path, encoding='utf-8-sig')
            logger.info(f"✅ Loaded {len(self.df)} events")

            # Add node IDs
            if 'node_id' not in self.df.columns:
                self.df['node_id'] = range(len(self.df))

            # Parse dates
            if 'date_start' in self.df.columns:
                self.df['parsed_date'] = pd.to_datetime(self.df['date_start'], errors='coerce')
                self.df['day_of_week'] = self.df['parsed_date'].dt.dayofweek
                self.df['month'] = self.df['parsed_date'].dt.month
                self.df['hour'] = pd.to_datetime(self.df.get('time_start', '19:00'), format='%H:%M', errors='coerce').dt.hour

            return True

        except Exception as e:
            logger.error(f"❌ Error loading data: {e}")
            return False

    def create_temporal_edges(self, threshold_days=7):
        """
        Create temporal edges between events
        - Same day of week: weight 1.0
        - Within 7 days: weight 0.7
        - Same month: weight 0.5
        - Same season: weight 0.3
        """
        logger.info("⏰ Creating temporal edges...")

        edges = []
        weights = []

        for i in range(len(self.df)):
            for j in range(i + 1, len(self.df)):
                edge_weight = 0.0

                # Same day of week
                if pd.notna(self.df.iloc[i]['day_of_week']) and pd.notna(self.df.iloc[j]['day_of_week']):
                    if self.df.iloc[i]['day_of_week'] == self.df.iloc[j]['day_of_week']:
                        edge_weight += 1.0

                # Within N days
                if pd.notna(self.df.iloc[i]['parsed_date']) and pd.notna(self.df.iloc[j]['parsed_date']):
                    date_diff = abs((self.df.iloc[i]['parsed_date'] - self.df.iloc[j]['parsed_date']).days)
                    if date_diff <= threshold_days:
                        edge_weight += 0.7 * (1 - date_diff / threshold_days)

                # Same month
                if pd.notna(self.df.iloc[i]['month']) and pd.notna(self.df.iloc[j]['month']):
                    if self.df.iloc[i]['month'] == self.df.iloc[j]['month']:
                        edge_weight += 0.5

                # Similar time of day
                if pd.notna(self.df.iloc[i]['hour']) and pd.notna(self.df.iloc[j]['hour']):
                    hour_diff = abs(self.df.iloc[i]['hour'] - self.df.iloc[j]['hour'])
                    if hour_diff <= 2:
                        edge_weight += 0.4

                if edge_weight > 0.3:  # Only keep meaningful temporal connections
                    edges.append([i, j])
                    weights.append(edge_weight)

        logger.info(f"   ✅ Created {len(edges)} temporal edges")
        return edges, weights

    def create_thematic_edges(self, similarity_threshold=0.15):
        """
        Create thematic edges based on:
        - Category similarity
        - Tag/keyword overlap
        - Description TF-IDF similarity
        """
        logger.info("🎯 Creating thematic edges...")

        # Combine text features
        combined_text = self.df['title'].fillna('') + ' ' + \
                       self.df['description'].fillna('') + ' ' + \
                       self.df['category'].fillna('')

        # TF-IDF vectorization
        vectorizer = TfidfVectorizer(max_features=100, stop_words='english', ngram_range=(1, 2))
        tfidf_matrix = vectorizer.fit_transform(combined_text)

        # Compute cosine similarity
        similarity_matrix = cosine_similarity(tfidf_matrix)

        edges = []
        weights = []

        for i in range(len(self.df)):
            for j in range(i + 1, len(self.df)):
                similarity = similarity_matrix[i, j]

                # Bonus for same category
                if 'category' in self.df.columns:
                    if self.df.iloc[i]['category'] == self.df.iloc[j]['category']:
                        similarity += 0.2

                if similarity > similarity_threshold:
                    edges.append([i, j])
                    weights.append(float(similarity))

        logger.info(f"   ✅ Created {len(edges)} thematic edges")
        return edges, weights

    def create_location_edges(self):
        """
        Create location-based edges:
        - Same venue: weight 1.0
        - Same city: weight 0.6
        """
        logger.info("📍 Creating location edges...")

        edges = []
        weights = []

        if 'location' not in self.df.columns:
            logger.warning("   ⚠️ No location column found")
            return edges, weights

        for i in range(len(self.df)):
            for j in range(i + 1, len(self.df)):
                weight = 0.0

                loc_i = str(self.df.iloc[i]['location']).lower()
                loc_j = str(self.df.iloc[j]['location']).lower()

                # Same venue
                if loc_i == loc_j and loc_i != 'nan':
                    weight = 1.0
                # Same city (if location contains city name)
                elif any(city in loc_i and city in loc_j for city in ['скопје', 'skopje', 'битола', 'bitola']):
                    weight = 0.6

                if weight > 0:
                    edges.append([i, j])
                    weights.append(weight)

        logger.info(f"   ✅ Created {len(edges)} location edges")
        return edges, weights

    def create_organizer_edges(self):
        """
        Create organizer-based edges:
        - Same organizer: weight 1.0
        """
        logger.info("🏢 Creating organizer edges...")

        edges = []
        weights = []

        if 'organizer' not in self.df.columns:
            logger.warning("   ⚠️ No organizer column found")
            return edges, weights

        for i in range(len(self.df)):
            for j in range(i + 1, len(self.df)):
                org_i = str(self.df.iloc[i]['organizer']).lower()
                org_j = str(self.df.iloc[j]['organizer']).lower()

                if org_i == org_j and org_i != 'nan' and org_i != 'непознат':
                    edges.append([i, j])
                    weights.append(1.0)

        logger.info(f"   ✅ Created {len(edges)} organizer edges")
        return edges, weights

    def prepare_node_features(self):
        """Prepare comprehensive node features"""
        logger.info("🎯 Preparing node features...")

        features = []

        # Text features (TF-IDF)
        combined_text = self.df['title'].fillna('') + ' ' + self.df['description'].fillna('')
        vectorizer = TfidfVectorizer(max_features=50, stop_words='english')
        tfidf_features = vectorizer.fit_transform(combined_text).toarray()
        features.append(tfidf_features)

        # Categorical features
        if 'category' in self.df.columns:
            le = LabelEncoder()
            category_encoded = le.fit_transform(self.df['category'].fillna('Unknown'))
            features.append(category_encoded.reshape(-1, 1))

        # Temporal features
        if 'day_of_week' in self.df.columns:
            features.append(self.df['day_of_week'].fillna(-1).values.reshape(-1, 1))
        if 'month' in self.df.columns:
            features.append(self.df['month'].fillna(-1).values.reshape(-1, 1))
        if 'hour' in self.df.columns:
            features.append(self.df['hour'].fillna(19).values.reshape(-1, 1))

        # Binary features
        if 'is_free' in self.df.columns:
            features.append(self.df['is_free'].fillna(True).astype(int).values.reshape(-1, 1))

        # Combine all features
        all_features = np.hstack(features)

        # Standardize
        scaler = StandardScaler()
        all_features = scaler.fit_transform(all_features)

        logger.info(f"   ✅ Feature shape: {all_features.shape}")
        return torch.tensor(all_features, dtype=torch.float)

    def create_multi_dimensional_graph(self):
        """Create enhanced multi-dimensional graph"""
        logger.info("🌐 Creating multi-dimensional graph...")

        # Get all edge types
        temporal_edges, temporal_weights = self.create_temporal_edges()
        thematic_edges, thematic_weights = self.create_thematic_edges()
        location_edges, location_weights = self.create_location_edges()
        organizer_edges, organizer_weights = self.create_organizer_edges()

        # Combine all edges with their type labels
        all_edges = []
        all_weights = []
        edge_types = []

        # Add temporal edges
        for edge, weight in zip(temporal_edges, temporal_weights):
            all_edges.append(edge)
            all_weights.append(weight)
            edge_types.append(0)  # 0 = temporal

        # Add thematic edges
        for edge, weight in zip(thematic_edges, thematic_weights):
            all_edges.append(edge)
            all_weights.append(weight)
            edge_types.append(1)  # 1 = thematic

        # Add location edges
        for edge, weight in zip(location_edges, location_weights):
            all_edges.append(edge)
            all_weights.append(weight)
            edge_types.append(2)  # 2 = location

        # Add organizer edges
        for edge, weight in zip(organizer_edges, organizer_weights):
            all_edges.append(edge)
            all_weights.append(weight)
            edge_types.append(3)  # 3 = organizer

        # Create PyTorch Geometric Data object
        if len(all_edges) == 0:
            logger.warning("⚠️ No edges created!")
            edge_index = torch.zeros((2, 0), dtype=torch.long)
            edge_attr = torch.zeros((0, 2), dtype=torch.float)
        else:
            edge_index = torch.tensor(all_edges, dtype=torch.long).t().contiguous()
            # Make undirected
            edge_index = torch.cat([edge_index, edge_index.flip(0)], dim=1)

            edge_attr = torch.tensor([[w, t] for w, t in zip(all_weights + all_weights,
                                                               edge_types + edge_types)], dtype=torch.float)

        # Prepare node features
        node_features = self.prepare_node_features()

        # Create Data object
        data = Data(
            x=node_features,
            edge_index=edge_index,
            edge_attr=edge_attr,
            num_nodes=len(self.df)
        )

        logger.info(f"   ✅ Graph created: {data.num_nodes} nodes, {edge_index.shape[1]} edges")
        logger.info(f"   📊 Edge distribution:")
        logger.info(f"      - Temporal: {len(temporal_edges)}")
        logger.info(f"      - Thematic: {len(thematic_edges)}")
        logger.info(f"      - Location: {len(location_edges)}")
        logger.info(f"      - Organizer: {len(organizer_edges)}")

        return data

    def save_graph(self, graph, name="enhanced_event_graph"):
        """Save graph to file"""
        output_path = self.output_dir / f"{name}.pt"
        torch.save(graph, output_path)
        logger.info(f"💾 Saved graph to: {output_path}")

        # Save metadata
        metadata = {
            'created_at': datetime.now().isoformat(),
            'num_nodes': graph.num_nodes,
            'num_edges': graph.edge_index.shape[1] // 2,  # Undirected
            'feature_dim': graph.x.shape[1],
            'edge_types': {
                '0': 'temporal',
                '1': 'thematic',
                '2': 'location',
                '3': 'organizer'
            }
        }

        metadata_path = self.output_dir / f"{name}_metadata.json"
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"📄 Saved metadata to: {metadata_path}")

    def run(self):
        """Run the complete graph construction pipeline"""
        logger.info("🚀 Enhanced Graph Construction Pipeline")
        logger.info("=" * 60)

        # Load data
        if not self.load_data():
            return False

        # Create graph
        graph = self.create_multi_dimensional_graph()

        # Save graph
        self.save_graph(graph)

        logger.info("\n✅ Graph construction completed successfully!")
        logger.info(f"📁 Output directory: {self.output_dir}")

        return True


def main():
    """Main entry point"""
    constructor = EnhancedGraphConstructor()
    success = constructor.run()

    if success:
        print("\n🎉 Enhanced graph construction completed!")
        print("📋 Next steps:")
        print("   1. Use the generated .pt file for GNN training")
        print("   2. Train GraphSAGE, GCN, or GAT models")
        print("   3. Evaluate recommendations")
    else:
        print("\n❌ Graph construction failed!")
        print("Please check the logs above for errors.")


if __name__ == "__main__":
    main()