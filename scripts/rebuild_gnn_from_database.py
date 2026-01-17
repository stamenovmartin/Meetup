#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Rebuild GNN Graph and Embeddings from Current Database
=======================================================
Креира нов GNN graph директно од тековна database (548 events)
и генерира нови embeddings што ќе се користат за препораки.

Стратегија:
1. Export events од database
2. Build similarity graph врз основа на tags
3. Train lightweight GraphSAGE model
4. Save embeddings за recommender system

Author: Martin Stamenov
Date: 2025-09-30
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import create_app
from models.db_models import db, Event
import torch
import torch.nn.functional as F
from torch_geometric.data import Data
from torch_geometric.nn import SAGEConv
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import networkx as nx


class SimplifiedGraphSAGE(torch.nn.Module):
    """Едноставен GraphSAGE model за event embeddings"""

    def __init__(self, in_channels, hidden_channels=64, out_channels=59):
        super().__init__()
        self.conv1 = SAGEConv(in_channels, hidden_channels)
        self.conv2 = SAGEConv(hidden_channels, out_channels)

    def forward(self, x, edge_index):
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.conv2(x, edge_index)
        return x


def create_graph_from_database(similarity_threshold=0.15):
    """
    Креирај graph директно од database events

    Args:
        similarity_threshold: Минимална сличност за edge creation

    Returns:
        PyTorch Geometric Data object
    """
    print("=" * 80)
    print(" BUILDING GNN GRAPH FROM DATABASE")
    print("=" * 80)

    # Земи ги сите events
    events = Event.query.all()
    print(f"\n Found {len(events)} events in database")

    if len(events) == 0:
        print(" No events found!")
        return None

    # Создади features од tags (TF-IDF)
    print("\n Creating TF-IDF features from tags...")

    # Собери ги сите tags
    tag_texts = []
    event_ids = []

    for event in events:
        tags = event.tags if event.tags else ""
        # Замени запирки со spaces за TF-IDF
        tag_text = tags.replace(",", " ")
        tag_texts.append(tag_text)
        event_ids.append(event.id)

    # TF-IDF векторизација
    vectorizer = TfidfVectorizer(
        max_features=100,
        token_pattern=r'\b\w+\b',
        min_df=1
    )

    try:
        tfidf_matrix = vectorizer.fit_transform(tag_texts)
        features = torch.tensor(tfidf_matrix.toarray(), dtype=torch.float)
        print(f"   TF-IDF features: {features.shape}")
    except:
        # Fallback: random features
        print("   TF-IDF failed, using random features")
        features = torch.randn(len(events), 100)

    # Креирај edges врз основа на tag similarity
    print(f"\n Creating edges (threshold={similarity_threshold})...")

    similarity_matrix = cosine_similarity(features.numpy())

    edge_list = []
    edge_weights = []

    for i in range(len(events)):
        for j in range(i + 1, len(events)):
            sim = similarity_matrix[i, j]
            if sim > similarity_threshold:
                edge_list.append([i, j])
                edge_list.append([j, i])  # Undirected
                edge_weights.append(sim)
                edge_weights.append(sim)

    if len(edge_list) == 0:
        print(f"   No edges with threshold={similarity_threshold}, lowering to 0.05")
        similarity_threshold = 0.05
        edge_list = []
        edge_weights = []

        for i in range(len(events)):
            for j in range(i + 1, len(events)):
                sim = similarity_matrix[i, j]
                if sim > similarity_threshold:
                    edge_list.append([i, j])
                    edge_list.append([j, i])
                    edge_weights.append(sim)
                    edge_weights.append(sim)

    edge_index = torch.tensor(edge_list, dtype=torch.long).t().contiguous()

    print(f"   Created {len(edge_list)} edges between {len(events)} nodes")
    print(f"   Avg degree: {len(edge_list) / len(events):.2f}")

    # Креирај Data object
    data = Data(
        x=features,
        edge_index=edge_index,
        num_nodes=len(events)
    )

    return data, event_ids


def train_graphsage(data, epochs=100):
    """
    Тренирај GraphSAGE model за unsupervised embeddings

    Args:
        data: PyTorch Geometric Data
        epochs: Број на епохи

    Returns:
        Trained embeddings tensor
    """
    print(f"\n Training GraphSAGE model ({epochs} epochs)...")

    model = SimplifiedGraphSAGE(
        in_channels=data.x.shape[1],
        hidden_channels=64,
        out_channels=59  # Match existing embedding size
    )

    optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)

    model.train()
    for epoch in range(epochs):
        optimizer.zero_grad()

        # Forward pass
        embeddings = model(data.x, data.edge_index)

        # Unsupervised loss: reconstruct adjacency matrix
        # Симплифициран: normalize embeddings
        embeddings_norm = F.normalize(embeddings, p=2, dim=1)

        # Sample edges and non-edges for training
        num_edges = data.edge_index.shape[1]
        edge_sample_size = min(1000, num_edges)

        edge_indices = torch.randperm(num_edges)[:edge_sample_size]
        sampled_edges = data.edge_index[:, edge_indices]

        # Positive edges (should have high similarity)
        pos_src = embeddings_norm[sampled_edges[0]]
        pos_dst = embeddings_norm[sampled_edges[1]]
        pos_scores = (pos_src * pos_dst).sum(dim=1)

        # Negative edges (random pairs, should have low similarity)
        neg_src_idx = torch.randint(0, data.num_nodes, (edge_sample_size,))
        neg_dst_idx = torch.randint(0, data.num_nodes, (edge_sample_size,))
        neg_src = embeddings_norm[neg_src_idx]
        neg_dst = embeddings_norm[neg_dst_idx]
        neg_scores = (neg_src * neg_dst).sum(dim=1)

        # Binary cross-entropy loss
        pos_loss = F.binary_cross_entropy_with_logits(
            pos_scores,
            torch.ones_like(pos_scores)
        )
        neg_loss = F.binary_cross_entropy_with_logits(
            neg_scores,
            torch.zeros_like(neg_scores)
        )

        loss = pos_loss + neg_loss
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 10 == 0:
            print(f"  Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}")

    # Extract final embeddings
    model.eval()
    with torch.no_grad():
        embeddings = model(data.x, data.edge_index)

    print(f"   Training complete! Embeddings shape: {embeddings.shape}")

    return embeddings


def save_embeddings(embeddings, event_ids, output_path="graph_construction/graph_data/event_similarity_graph.pt"):
    """
    Зачувај embeddings во формат што го очекува recommender

    Args:
        embeddings: Torch tensor (num_events, embedding_dim)
        event_ids: List на event IDs
        output_path: Патека за чување
    """
    print(f"\n Saving embeddings to {output_path}...")

    # Create event_id -> embedding mapping
    embedding_dict = {}
    for i, event_id in enumerate(event_ids):
        embedding_dict[event_id] = embeddings[i].numpy()

    # Save со metadata
    save_data = {
        'embeddings': embeddings,
        'event_ids': event_ids,
        'embedding_dict': embedding_dict,
        'num_events': len(event_ids),
        'embedding_dim': embeddings.shape[1]
    }

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    torch.save(save_data, output_path)

    print(f"   Saved embeddings for {len(event_ids)} events")
    print(f"   File size: {os.path.getsize(output_path) / 1024:.1f} KB")


def rebuild_gnn_pipeline(epochs=100):
    """Главна функција - complete rebuild pipeline"""

    app = create_app()

    with app.app_context():
        # Step 1: Build graph
        data, event_ids = create_graph_from_database()

        if data is None:
            print(" Failed to build graph!")
            return False

        # Step 2: Train GraphSAGE
        embeddings = train_graphsage(data, epochs=epochs)

        # Step 3: Save embeddings
        save_embeddings(embeddings, event_ids)

        print("\n" + "=" * 80)
        print(" GNN REBUILD COMPLETE!")
        print("=" * 80)
        print(f"\n Summary:")
        print(f"  • Events processed: {len(event_ids)}")
        print(f"  • Embedding dimension: {embeddings.shape[1]}")
        print(f"  • Graph edges: {data.edge_index.shape[1]}")
        print(f"  • Avg node degree: {data.edge_index.shape[1] / len(event_ids):.2f}")

        print(f"\n Next steps:")
        print(f"  1. Restart Flask backend (python main.py)")
        print(f"  2. Test препораки на frontend")
        print(f"  3. Run evaluation: python scripts/gnn_comprehensive_evaluation.py 3,4,5,6,7")

        return True


if __name__ == '__main__':
    epochs = 100  # Default

    if len(sys.argv) > 1:
        epochs = int(sys.argv[1])

    rebuild_gnn_pipeline(epochs=epochs)
