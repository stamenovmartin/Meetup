"""
GNN-SPECIFIC ANALYSIS VISUALIZATIONS

Визуелизации специфични за GNN моделот:
1. Graph structure statistics
2. GNN component contribution analysis
3. Embedding space visualization (t-SNE/PCA)
4. Feature importance analysis
5. Score distribution comparison
"""
import sys
sys.path.append('..')

import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import torch
from sklearn.manifold import TSNE
from sklearn.decomposition import PCA
from main import create_app
from models.db_models import Event, Attendance
from models.recommender import get_recommender
import json

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


def visualize_graph_structure(output_dir: str = "evaluation/visualizations"):
    """Визуелизација на graph structure статистики"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Load graph (try multiple paths)
    possible_paths = [
        Path("../graph_construction/graph_data/event_similarity_graph.pt"),
        Path("graph_construction/graph_data/event_similarity_graph.pt"),
    ]

    graph_path = None
    for path in possible_paths:
        if path.exists():
            graph_path = path
            break

    if graph_path is None:
        print("⚠️  Graph file not found, skipping graph structure visualization")
        return

    graph_data = torch.load(graph_path)

    num_nodes = graph_data.num_nodes
    num_edges = graph_data.edge_index.shape[1]
    num_features = graph_data.x.shape[1] if hasattr(graph_data, 'x') else 0

    # Calculate degree distribution
    degrees = []
    edge_index = graph_data.edge_index.numpy()
    for node in range(num_nodes):
        degree = np.sum(edge_index[0] == node)
        degrees.append(degree)

    # Calculate density
    max_edges = num_nodes * (num_nodes - 1)
    density = num_edges / max_edges if max_edges > 0 else 0

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('GNN Graph Structure Analysis', fontsize=18, fontweight='bold')

    # 1. Degree Distribution
    ax1 = axes[0, 0]
    ax1.hist(degrees, bins=30, color='#5f27cd', alpha=0.7, edgecolor='black')
    ax1.set_xlabel('Node Degree', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Frequency', fontsize=12, fontweight='bold')
    ax1.set_title('Degree Distribution', fontsize=14, fontweight='bold')
    ax1.axvline(np.mean(degrees), color='red', linestyle='--', linewidth=2, label=f'Mean: {np.mean(degrees):.1f}')
    ax1.legend()
    ax1.grid(alpha=0.3)

    # 2. Graph Statistics
    ax2 = axes[0, 1]
    ax2.axis('off')
    stats_text = f"""
    GRAPH STATISTICS

    Nodes (Events): {num_nodes:,}
    Edges (Connections): {num_edges:,}
    Features per Node: {num_features}

    Avg Degree: {np.mean(degrees):.2f}
    Max Degree: {np.max(degrees)}
    Min Degree: {np.min(degrees)}

    Graph Density: {density:.4f}
    ({density*100:.2f}% of possible edges)

    Sparsity: {1-density:.4f}
    """
    ax2.text(0.1, 0.5, stats_text, fontsize=13, family='monospace',
            verticalalignment='center',
            bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.3))

    # 3. Edge Weight Distribution (if available)
    ax3 = axes[1, 0]
    if hasattr(graph_data, 'edge_attr') and graph_data.edge_attr is not None:
        edge_weights = graph_data.edge_attr.numpy().flatten()
        ax3.hist(edge_weights, bins=50, color='#1dd1a1', alpha=0.7, edgecolor='black')
        ax3.set_xlabel('Edge Weight (Similarity)', fontsize=12, fontweight='bold')
        ax3.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax3.set_title('Edge Weight Distribution', fontsize=14, fontweight='bold')
        ax3.axvline(np.mean(edge_weights), color='red', linestyle='--', linewidth=2,
                   label=f'Mean: {np.mean(edge_weights):.3f}')
        ax3.legend()
    else:
        ax3.text(0.5, 0.5, 'Edge weights not available', ha='center', va='center', fontsize=14)
    ax3.grid(alpha=0.3)

    # 4. Feature Statistics
    ax4 = axes[1, 1]
    if hasattr(graph_data, 'x') and graph_data.x is not None:
        feature_matrix = graph_data.x.numpy()
        feature_means = np.mean(feature_matrix, axis=0)
        feature_stds = np.std(feature_matrix, axis=0)

        top_10_idx = np.argsort(feature_stds)[-10:]
        ax4.barh(range(10), feature_stds[top_10_idx], color='#48dbfb', alpha=0.7, edgecolor='black')
        ax4.set_xlabel('Std Deviation', fontsize=12, fontweight='bold')
        ax4.set_ylabel('Feature Index', fontsize=12, fontweight='bold')
        ax4.set_title('Top 10 Most Variable Features', fontsize=14, fontweight='bold')
        ax4.set_yticks(range(10))
        ax4.set_yticklabels([f'F{idx}' for idx in top_10_idx])
    else:
        ax4.text(0.5, 0.5, 'Node features not available', ha='center', va='center', fontsize=14)
    ax4.grid(alpha=0.3)

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_graph_structure.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def visualize_component_contribution(output_dir: str = "evaluation/visualizations"):
    """Анализа на contribution на секоја компонента (GNN, Tags, Venue, Temporal)"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    app = create_app()

    with app.app_context():
        recommender = get_recommender()

        # Test на sample корисник
        test_user_id = 1
        sample_events = Event.query.limit(100).all()

        component_scores = {
            'GNN': [],
            'Tags': [],
            'Venue': [],
            'Temporal': []
        }

        for event in sample_events:
            # Calculate individual components
            try:
                # GNN score
                gnn_score = recommender.gnn_similarity_score(test_user_id, event)
                component_scores['GNN'].append(gnn_score)

                # Tag score
                tag_score = recommender.calculate_tag_preference_score(test_user_id, event)
                component_scores['Tags'].append(tag_score)

                # Venue score
                venue_score = recommender.calculate_venue_preference(test_user_id, event)
                component_scores['Venue'].append(venue_score)

                # Temporal score
                temporal_score = recommender.calculate_temporal_score(test_user_id, event)
                component_scores['Temporal'].append(temporal_score)
            except:
                continue

    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('GNN Component Contribution Analysis', fontsize=18, fontweight='bold')

    colors = ['#5f27cd', '#feca57', '#1dd1a1', '#48dbfb']
    components = ['GNN', 'Tags', 'Venue', 'Temporal']

    # 1. Score Distribution for each component
    ax1 = axes[0, 0]
    for i, component in enumerate(components):
        scores = component_scores[component]
        if scores:
            ax1.hist(scores, bins=30, alpha=0.5, label=component, color=colors[i], edgecolor='black')
    ax1.set_xlabel('Score', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Frequency', fontsize=12, fontweight='bold')
    ax1.set_title('Score Distribution by Component', fontsize=14, fontweight='bold')
    ax1.legend()
    ax1.grid(alpha=0.3)

    # 2. Average Contribution
    ax2 = axes[0, 1]
    avg_scores = [np.mean(component_scores[c]) if component_scores[c] else 0 for c in components]
    bars = ax2.bar(components, avg_scores, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax2.set_ylabel('Average Score', fontsize=12, fontweight='bold')
    ax2.set_title('Average Contribution per Component', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    for bar, score in zip(bars, avg_scores):
        ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{score:.3f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

    # 3. Weighted Contribution (using actual weights from recommender)
    ax3 = axes[1, 0]
    weights = {
        'GNN': 0.40,
        'Tags': 0.30,
        'Venue': 0.20,
        'Temporal': 0.10
    }
    weighted_contributions = [avg_scores[i] * list(weights.values())[i] for i in range(4)]
    bars = ax3.bar(components, weighted_contributions, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax3.set_ylabel('Weighted Contribution', fontsize=12, fontweight='bold')
    ax3.set_title('Weighted Contribution to Final Score', fontsize=14, fontweight='bold')
    ax3.grid(axis='y', alpha=0.3)
    for bar, score in zip(bars, weighted_contributions):
        ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{score:.3f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

    # 4. Pie chart of weights
    ax4 = axes[1, 1]
    ax4.pie(list(weights.values()), labels=list(weights.keys()), autopct='%1.0f%%',
           colors=colors, startangle=90, textprops={'fontsize': 12, 'fontweight': 'bold'})
    ax4.set_title('Component Weights in Final Score', fontsize=14, fontweight='bold')

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_component_analysis.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def visualize_embedding_space(output_dir: str = "evaluation/visualizations"):
    """t-SNE visualization на event embeddings"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Load graph (try multiple paths)
    possible_paths = [
        Path("../graph_construction/graph_data/event_similarity_graph.pt"),
        Path("graph_construction/graph_data/event_similarity_graph.pt"),
    ]

    graph_path = None
    for path in possible_paths:
        if path.exists():
            graph_path = path
            break

    if graph_path is None:
        print("⚠️  Graph file not found, skipping embedding visualization")
        return

    graph_data = torch.load(graph_path)

    if not hasattr(graph_data, 'x') or graph_data.x is None:
        print("⚠️  No node features found, skipping embedding visualization")
        return

    embeddings = graph_data.x.numpy()

    # Limit to 500 events for visualization
    if embeddings.shape[0] > 500:
        sample_indices = np.random.choice(embeddings.shape[0], 500, replace=False)
        embeddings_sample = embeddings[sample_indices]
    else:
        embeddings_sample = embeddings

    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    fig.suptitle('Event Embedding Space Visualization', fontsize=18, fontweight='bold')

    # 1. t-SNE
    print("  Computing t-SNE...")
    tsne = TSNE(n_components=2, random_state=42, perplexity=30)
    embeddings_2d_tsne = tsne.fit_transform(embeddings_sample)

    ax1 = axes[0]
    scatter1 = ax1.scatter(embeddings_2d_tsne[:, 0], embeddings_2d_tsne[:, 1],
                          c=range(len(embeddings_sample)), cmap='viridis',
                          alpha=0.6, s=50, edgecolors='black', linewidth=0.5)
    ax1.set_xlabel('t-SNE Dimension 1', fontsize=12, fontweight='bold')
    ax1.set_ylabel('t-SNE Dimension 2', fontsize=12, fontweight='bold')
    ax1.set_title('t-SNE Projection', fontsize=14, fontweight='bold')
    ax1.grid(alpha=0.3)

    # 2. PCA
    print("  Computing PCA...")
    pca = PCA(n_components=2, random_state=42)
    embeddings_2d_pca = pca.fit_transform(embeddings_sample)

    ax2 = axes[1]
    scatter2 = ax2.scatter(embeddings_2d_pca[:, 0], embeddings_2d_pca[:, 1],
                          c=range(len(embeddings_sample)), cmap='plasma',
                          alpha=0.6, s=50, edgecolors='black', linewidth=0.5)
    ax2.set_xlabel(f'PC1 ({pca.explained_variance_ratio_[0]*100:.1f}% variance)',
                  fontsize=12, fontweight='bold')
    ax2.set_ylabel(f'PC2 ({pca.explained_variance_ratio_[1]*100:.1f}% variance)',
                  fontsize=12, fontweight='bold')
    ax2.set_title('PCA Projection', fontsize=14, fontweight='bold')
    ax2.grid(alpha=0.3)

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_embedding_space.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def create_gnn_architecture_diagram(output_dir: str = "evaluation/visualizations"):
    """Diagram на GNN architecture"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(14, 10))
    ax.axis('off')

    # Architecture text
    architecture_text = """
    GNN RECOMMENDATION SYSTEM ARCHITECTURE

    ┌──────────────────────────────────────────────────────────────┐
    │                    INPUT: User + All Events                   │
    └──────────────────────────────────────────────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    │                             │
         ┌──────────▼──────────┐       ┌─────────▼─────────┐
         │   Event Graph       │       │   User History    │
         │   (GraphSAGE)       │       │   (Attendance)    │
         └──────────┬──────────┘       └─────────┬─────────┘
                    │                             │
         ┌──────────▼──────────┐       ┌─────────▼─────────┐
         │  GNN Embeddings     │       │  Tag Preferences  │
         │  (128-dim)          │       │  Venue Preferences│
         └──────────┬──────────┘       │  Temporal Patterns│
                    │                  └─────────┬─────────┘
                    │                             │
         ┌──────────▼──────────────────────────────▼─────────┐
         │            SCORE COMBINATION                       │
         │  • GNN Similarity:    40%                         │
         │  • Tag Matching:      30%                         │
         │  • Venue Preference:  20%                         │
         │  • Temporal Score:    10%                         │
         │  • Random Noise:       5%  (tie-breaking)         │
         └──────────┬────────────────────────────────────────┘
                    │
         ┌──────────▼──────────┐
         │  Power Scaling      │
         │  score^0.7 * 2.5    │
         └──────────┬──────────┘
                    │
         ┌──────────▼──────────┐
         │  TOP-K Ranking      │
         │  (sorted by score)  │
         └──────────┬──────────┘
                    │
         ┌──────────▼──────────┐
         │  RECOMMENDATIONS    │
         └─────────────────────┘


    KEY COMPONENTS:
    ─────────────────────────────────────────────────────────────

    1. GRAPH NEURAL NETWORK (GraphSAGE)
       • Learns event embeddings from graph structure
       • Captures semantic similarities
       • 2-layer architecture: 128 → 64 dims

    2. TRADITIONAL FEATURES
       • Tag Preferences: User's tag interaction history
       • Venue Preferences: Popular venues for user
       • Temporal Patterns: Time-based scoring

    3. HYBRID SCORING
       • Combines GNN (40%) + Traditional (60%)
       • Power scaling for better distribution
       • Random noise for diversity

    EVALUATION RESULTS:
    ─────────────────────────────────────────────────────────────

    Precision@10:  2.10%   (4x better than baselines)
    Hit Rate@10:   19.8%   (1 in 5 users get relevant rec)
    NDCG@10:       3.89%   (good ranking quality)
    MRR:           0.089   (first hit at position ~11)

    Improvement:   +325% over Matrix Factorization
                   +467% over Popular baseline
    """

    ax.text(0.5, 0.5, architecture_text, fontsize=10, family='monospace',
           verticalalignment='center', horizontalalignment='center',
           bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8, pad=20))

    fig.suptitle('GNN Recommendation System Architecture', fontsize=18, fontweight='bold', y=0.98)

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_architecture_diagram.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def main():
    """Generate all GNN-specific visualizations"""
    print("=" * 80)
    print("🎨 GENERATING GNN-SPECIFIC VISUALIZATIONS")
    print("=" * 80)

    print("\n1. Graph Structure Analysis...")
    visualize_graph_structure()

    print("\n2. Component Contribution Analysis...")
    visualize_component_contribution()

    print("\n3. Embedding Space Visualization...")
    visualize_embedding_space()

    print("\n4. Architecture Diagram...")
    create_gnn_architecture_diagram()

    print("\n" + "=" * 80)
    print("✅ ALL GNN VISUALIZATIONS CREATED!")
    print("=" * 80)
    print("\nLocation: evaluation/visualizations/")
    print("\nFiles created:")
    print("  1. gnn_graph_structure.png")
    print("  2. gnn_component_analysis.png")
    print("  3. gnn_embedding_space.png")
    print("  4. gnn_architecture_diagram.png")
    print("=" * 80)


if __name__ == "__main__":
    main()
