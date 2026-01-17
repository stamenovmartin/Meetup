import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv, GATConv, SAGEConv, HeteroConv, Linear
from torch_geometric.utils import train_test_split_edges
from sklearn.metrics import accuracy_score, f1_score, classification_report, roc_auc_score
import matplotlib.pyplot as plt
from pathlib import Path
import json
import warnings

warnings.filterwarnings('ignore')


class EventGCN(nn.Module):

    def __init__(self, input_dim, hidden_dim=64, output_dim=32, num_layers=2, dropout=0.5):
        super(EventGCN, self).__init__()

        self.convs = nn.ModuleList()
        self.convs.append(GCNConv(input_dim, hidden_dim))

        for _ in range(num_layers - 2):
            self.convs.append(GCNConv(hidden_dim, hidden_dim))

        self.convs.append(GCNConv(hidden_dim, output_dim))

        self.dropout = dropout

    def forward(self, x, edge_index):
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i < len(self.convs) - 1:
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)
        return x


class EventGAT(nn.Module):

    def __init__(self, input_dim, hidden_dim=64, output_dim=32, num_layers=2, heads=4, dropout=0.5):
        super(EventGAT, self).__init__()

        self.convs = nn.ModuleList()
        self.convs.append(GATConv(input_dim, hidden_dim // heads, heads=heads, dropout=dropout))

        for _ in range(num_layers - 2):
            self.convs.append(GATConv(hidden_dim, hidden_dim // heads, heads=heads, dropout=dropout))

        self.convs.append(GATConv(hidden_dim, output_dim, heads=1, dropout=dropout))

        self.dropout = dropout

    def forward(self, x, edge_index):
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i < len(self.convs) - 1:
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)
        return x


class EventGraphSAGE(nn.Module):

    def __init__(self, input_dim, hidden_dim=64, output_dim=32, num_layers=2, dropout=0.5):
        super(EventGraphSAGE, self).__init__()

        self.convs = nn.ModuleList()
        self.convs.append(SAGEConv(input_dim, hidden_dim))

        for _ in range(num_layers - 2):
            self.convs.append(SAGEConv(hidden_dim, hidden_dim))

        self.convs.append(SAGEConv(hidden_dim, output_dim))
        self.dropout = dropout

    def forward(self, x, edge_index):
        for i, conv in enumerate(self.convs):
            x = conv(x, edge_index)
            if i < len(self.convs) - 1:
                x = F.relu(x)
                x = F.dropout(x, p=self.dropout, training=self.training)
        return x


class GNNTrainer:

    def __init__(self, graph_data_dir="graph_construction/graph_data", output_dir="gnn_results"):
        self.graph_data_dir = Path(graph_data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        self.graphs = {}
        self.models = {}
        self.results = {}

        print(f"Graph data dir: {self.graph_data_dir.absolute()}")
        print(f"Output dir: {self.output_dir.absolute()}")

        self.load_metadata()

    def load_metadata(self):
        metadata_file = self.graph_data_dir / "graph_metadata.json"
        if metadata_file.exists():
            with open(metadata_file, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
            print(f" Метаподатоци вчитани: {len(self.metadata.get('graphs_created', []))} графови")
        else:
            print(" Нема метаподатоци, користам default вредности")
            self.metadata = {}

    def load_graphs(self):
        print(" Вчитување графови...")

        if not self.graph_data_dir.exists():
            print(f" Папката не постои: {self.graph_data_dir}")
            return False

        graph_files = list(self.graph_data_dir.glob("*_graph.pt"))

        if not graph_files:
            print(" Нема .pt фајлови во graph_data папката!")
            print(f" Проверувам папка: {self.graph_data_dir}")
            print(" Содржина:")
            for item in self.graph_data_dir.iterdir():
                print(f"   - {item.name}")
            return False

        for graph_file in graph_files:
            graph_name = graph_file.stem.replace("_graph", "")
            try:
                graph = torch.load(graph_file, map_location='cpu', weights_only=False)
                self.graphs[graph_name] = graph
                print(f"    {graph_name}: {type(graph).__name__}")

                if hasattr(graph, 'x') and hasattr(graph, 'edge_index'):
                    print(f"      Nodes: {graph.x.shape[0]}, Features: {graph.x.shape[1]}")
                    if graph.edge_index.shape[1] > 0:
                        print(f"      Edges: {graph.edge_index.shape[1]}")
                    else:
                        print(f"      Edges: 0 (само nodes)")

            except Exception as e:
                print(f"    Проблем со {graph_name}: {e}")

        return len(self.graphs) > 0

    def prepare_node_classification_data(self, graph_name="event_similarity"):
        print(f" Подготовка за node classification ({graph_name})...")

        if graph_name not in self.graphs:
            print(f" Графот {graph_name} не постои!")
            available_graphs = list(self.graphs.keys())
            print(f" Достапни графови: {available_graphs}")
            if available_graphs:
                graph_name = available_graphs[0]
                print(f" Користам {graph_name} наместо тоа")
            else:
                return None

        graph = self.graphs[graph_name]

        # Handle dict format with embeddings (from graph_construction.py)
        if isinstance(graph, dict):
            from torch_geometric.data import Data
            print(f"    Converting dict to PyG Data object...")
            print(f"    Dict keys: {list(graph.keys())}")

            # Check if this is an embeddings-only dict
            if 'embeddings' in graph and 'event_ids' in graph:
                print(f"   ℹ This is an embeddings dict, creating graph from embeddings...")
                embeddings = graph['embeddings']
                if isinstance(embeddings, np.ndarray):
                    embeddings = torch.tensor(embeddings, dtype=torch.float)

                # Create edges based on cosine similarity
                from sklearn.metrics.pairwise import cosine_similarity
                print(f"    Creating edges based on embedding similarity...")

                # Compute cosine similarity
                emb_np = embeddings.numpy() if isinstance(embeddings, torch.Tensor) else embeddings
                similarity_matrix = cosine_similarity(emb_np)

                # Create edges for top-k similar nodes (k=10)
                k = min(10, len(emb_np) - 1)
                edge_list = []
                for i in range(len(emb_np)):
                    # Get top-k most similar nodes (excluding self)
                    similarities = similarity_matrix[i]
                    similarities[i] = -1  # Exclude self
                    top_k_indices = np.argsort(similarities)[-k:]

                    for j in top_k_indices:
                        if similarities[j] > 0.5:  # Only add if similarity > 0.5
                            edge_list.append([i, j])

                if edge_list:
                    edge_index = torch.tensor(edge_list, dtype=torch.long).t()
                else:
                    edge_index = torch.zeros((2, 0), dtype=torch.long)

                # Create a simple graph: nodes with embeddings + similarity-based edges
                graph = Data(
                    x=embeddings,
                    edge_index=edge_index,
                    num_nodes=embeddings.shape[0]
                )
                print(f"    Created graph with {graph.num_nodes} nodes, {embeddings.shape[1]} features, {edge_index.shape[1]} edges")
                self.graphs[graph_name] = graph

            elif 'x' in graph and 'edge_index' in graph:
                # Standard PyG dict format
                graph = Data(
                    x=graph['x'],
                    edge_index=graph['edge_index'],
                    edge_attr=graph.get('edge_attr', None)
                )
                self.graphs[graph_name] = graph
            else:
                print(f"    Dict missing required keys! Has: {list(graph.keys())}")
                return None

        # Validate graph has required attributes
        if not hasattr(graph, 'x') or graph.x is None:
            print(f"    Graph has no 'x' attribute or it's None!")
            print(f"    Graph type: {type(graph)}")
            print(f"    Graph attributes: {[attr for attr in dir(graph) if not attr.startswith('_')]}")
            return None

        num_nodes = graph.x.shape[0]

        features = graph.x.numpy() if isinstance(graph.x, torch.Tensor) else graph.x

        # Use real category labels if available from graph metadata
        if hasattr(graph, 'category_labels') and graph.category_labels is not None:
            from sklearn.preprocessing import LabelEncoder
            le = LabelEncoder()
            labels = le.fit_transform(graph.category_labels)
            print(f"    Using real category labels: {len(np.unique(labels))} classes")
        elif features.shape[1] > 10:  # Ако имаме TF-IDF features
            from sklearn.cluster import KMeans
            n_clusters = min(5, max(2, num_nodes // 10))  # 2-5 кластери
            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            labels = kmeans.fit_predict(features)
            print(f"   ℹ Using KMeans clustering: {n_clusters} clusters")
        else:
            # Last resort: use simple heuristic based on features
            labels = (features.sum(axis=1) % 3).astype(int)
            print(f"    Using feature-based labels: 3 classes")

        train_mask = torch.zeros(num_nodes, dtype=torch.bool)
        test_mask = torch.zeros(num_nodes, dtype=torch.bool)
        val_mask = torch.zeros(num_nodes, dtype=torch.bool)

        indices = np.random.permutation(num_nodes)
        train_size = int(0.6 * num_nodes)
        val_size = int(0.2 * num_nodes)

        train_mask[indices[:train_size]] = True
        val_mask[indices[train_size:train_size + val_size]] = True
        test_mask[indices[train_size + val_size:]] = True

        graph.y = torch.tensor(labels, dtype=torch.long)
        graph.train_mask = train_mask
        graph.val_mask = val_mask
        graph.test_mask = test_mask

        print(f"    Labels: {len(np.unique(labels))} класи")
        print(f"    Train: {train_mask.sum()}, Val: {val_mask.sum()}, Test: {test_mask.sum()}")

        return graph

    def train_node_classification(self, graph_name="event_similarity", model_type="GCN"):
        """Тренирај модел за node classification"""
        print(f" Тренирање {model_type} за node classification...")

        graph = self.prepare_node_classification_data(graph_name)
        if graph is None:
            return None

        input_dim = graph.x.shape[1]
        num_classes = len(torch.unique(graph.y))

        if model_type == "GCN":
            model = EventGCN(input_dim, hidden_dim=64, output_dim=num_classes)
        elif model_type == "GAT":
            model = EventGAT(input_dim, hidden_dim=64, output_dim=num_classes)
        elif model_type == "GraphSAGE":
            model = EventGraphSAGE(input_dim, hidden_dim=64, output_dim=num_classes)
        else:
            print(f" Непознат model type: {model_type}")
            return None

        optimizer = torch.optim.Adam(model.parameters(), lr=0.01, weight_decay=5e-4)
        criterion = nn.CrossEntropyLoss()

        model.train()
        train_losses = []
        val_accuracies = []

        for epoch in range(200):
            optimizer.zero_grad()

            out = model(graph.x, graph.edge_index)
            loss = criterion(out[graph.train_mask], graph.y[graph.train_mask])

            loss.backward()
            optimizer.step()

            if epoch % 20 == 0:
                model.eval()
                with torch.no_grad():
                    val_out = model(graph.x, graph.edge_index)
                    val_pred = val_out[graph.val_mask].argmax(dim=1)
                    val_acc = accuracy_score(graph.y[graph.val_mask].cpu(), val_pred.cpu())
                    val_accuracies.append(val_acc)

                    print(f"   Epoch {epoch:3d}: Loss={loss:.4f}, Val Acc={val_acc:.4f}")

                model.train()

            train_losses.append(loss.item())

        model.eval()
        with torch.no_grad():
            test_out = model(graph.x, graph.edge_index)
            test_pred = test_out[graph.test_mask].argmax(dim=1)
            test_acc = accuracy_score(graph.y[graph.test_mask].cpu(), test_pred.cpu())
            test_f1 = f1_score(graph.y[graph.test_mask].cpu(), test_pred.cpu(), average='weighted')

        results = {
            'model_type': model_type,
            'graph_name': graph_name,
            'test_accuracy': test_acc,
            'test_f1': test_f1,
            'num_classes': num_classes,
            'train_losses': train_losses,
            'val_accuracies': val_accuracies
        }

        print(f"    Test Accuracy: {test_acc:.4f}")
        print(f"    Test F1: {test_f1:.4f}")

        model_key = f"{model_type}_{graph_name}"
        self.models[model_key] = model
        self.results[model_key] = results

        return results

    def train_link_prediction(self, graph_name="event_similarity"):
        print(f" Тренирање за link prediction ({graph_name})...")

        if graph_name not in self.graphs:
            available_graphs = list(self.graphs.keys())
            if available_graphs:
                graph_name = available_graphs[0]
                print(f" Користам {graph_name} наместо event_similarity")
            else:
                print(" Нема достапни графови!")
                return None

        graph = self.graphs[graph_name]

        if not hasattr(graph, 'edge_index') or graph.edge_index.shape[1] == 0:
            print(" Графот нема edges за link prediction!")
            return None

        data = train_test_split_edges(graph, val_ratio=0.1, test_ratio=0.2)

        model = EventGraphSAGE(
            input_dim=graph.x.shape[1],
            hidden_dim=64,
            output_dim=32
        )

        optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

        model.train()
        for epoch in range(100):
            optimizer.zero_grad()

            z = model(data.x, data.train_pos_edge_index)

            edge_index = data.train_pos_edge_index
            pos_scores = (z[edge_index[0]] * z[edge_index[1]]).sum(dim=1)

            neg_edge_index = torch.randint(0, data.num_nodes, edge_index.shape)
            neg_scores = (z[neg_edge_index[0]] * z[neg_edge_index[1]]).sum(dim=1)

            scores = torch.cat([pos_scores, neg_scores])
            labels = torch.cat([torch.ones(pos_scores.size(0)), torch.zeros(neg_scores.size(0))])

            loss = F.binary_cross_entropy_with_logits(scores, labels)
            loss.backward()
            optimizer.step()

            if epoch % 20 == 0:
                print(f"   Epoch {epoch:3d}: Loss={loss:.4f}")

        model.eval()
        with torch.no_grad():
            z = model(data.x, data.train_pos_edge_index)

            test_pos_scores = (z[data.test_pos_edge_index[0]] * z[data.test_pos_edge_index[1]]).sum(dim=1)
            test_neg_scores = (z[data.test_neg_edge_index[0]] * z[data.test_neg_edge_index[1]]).sum(dim=1)

            test_scores = torch.cat([test_pos_scores, test_neg_scores])
            test_labels = torch.cat([torch.ones(test_pos_scores.size(0)), torch.zeros(test_neg_scores.size(0))])

            test_auc = roc_auc_score(test_labels.cpu(), test_scores.cpu())

        results = {
            'model_type': 'GraphSAGE_LinkPred',
            'graph_name': graph_name,
            'test_auc': test_auc,
            'embeddings': z.detach().cpu().numpy()
        }

        print(f"    Test AUC: {test_auc:.4f}")

        self.results[f"LinkPred_{graph_name}"] = results
        return results

    def visualize_results(self):
        print(" Визуализација на резултати...")

        if not self.results:
            print(" Нема резултати за визуализација!")
            return

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        node_clf_results = {k: v for k, v in self.results.items() if 'test_accuracy' in v}

        if node_clf_results:
            models = list(node_clf_results.keys())
            accuracies = [node_clf_results[m]['test_accuracy'] for m in models]
            f1_scores = [node_clf_results[m]['test_f1'] for m in models]

            x = range(len(models))
            width = 0.35

            axes[0, 0].bar([i - width / 2 for i in x], accuracies, width, label='Accuracy', alpha=0.8)
            axes[0, 0].bar([i + width / 2 for i in x], f1_scores, width, label='F1-Score', alpha=0.8)
            axes[0, 0].set_ylabel('Score')
            axes[0, 0].set_title(' Node Classification Performance')
            axes[0, 0].set_xticks(x)
            axes[0, 0].set_xticklabels([m.replace('_', '\n') for m in models], rotation=45)
            axes[0, 0].legend()
            axes[0, 0].grid(True, alpha=0.3)

        first_model = list(node_clf_results.keys())[0] if node_clf_results else None
        if first_model and 'train_losses' in self.results[first_model]:
            losses = self.results[first_model]['train_losses']
            axes[0, 1].plot(losses, label='Training Loss')
            axes[0, 1].set_xlabel('Epoch')
            axes[0, 1].set_ylabel('Loss')
            axes[0, 1].set_title(' Training Progress')
            axes[0, 1].legend()
            axes[0, 1].grid(True, alpha=0.3)

        link_pred_results = {k: v for k, v in self.results.items() if 'test_auc' in v}

        if link_pred_results:
            models = list(link_pred_results.keys())
            aucs = [link_pred_results[m]['test_auc'] for m in models]

            axes[1, 0].bar(range(len(models)), aucs, alpha=0.8, color='green')
            axes[1, 0].set_ylabel('AUC Score')
            axes[1, 0].set_title(' Link Prediction Performance')
            axes[1, 0].set_xticks(range(len(models)))
            axes[1, 0].set_xticklabels([m.replace('_', '\n') for m in models])
            axes[1, 0].grid(True, alpha=0.3)

        total_models = len(self.results)
        avg_accuracy = np.mean([r['test_accuracy'] for r in node_clf_results.values()]) if node_clf_results else 0
        avg_auc = np.mean([r['test_auc'] for r in link_pred_results.values()]) if link_pred_results else 0

        summary_text = f"""
         РЕЗУЛТАТИ РЕЗИМЕ

         Модели тренирани: {total_models}

         Node Classification:
        • Просечна точност: {avg_accuracy:.3f}

         Link Prediction:
        • Просечен AUC: {avg_auc:.3f}

         Графови анализирани:
        {', '.join(set(r.get('graph_name', 'unknown') for r in self.results.values()))}
        """

        axes[1, 1].text(0.1, 0.5, summary_text, transform=axes[1, 1].transAxes,
                        fontsize=10, verticalalignment='center',
                        bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.7))
        axes[1, 1].set_xlim(0, 1)
        axes[1, 1].set_ylim(0, 1)
        axes[1, 1].axis('off')

        plt.tight_layout()
        plt.savefig(self.output_dir / 'gnn_training_results.png', dpi=200, bbox_inches='tight')
        plt.close()

        print(f"    Резултати зачувани во: {self.output_dir}")

    def save_results(self):
        print(" Зачувување модели и резултати...")

        models_dir = self.output_dir / "models"
        models_dir.mkdir(exist_ok=True)

        for model_name, model in self.models.items():
            model_path = models_dir / f"{model_name}.pt"
            torch.save(model.state_dict(), model_path)
            print(f"    {model_name}.pt")

        results_clean = {}
        for key, result in self.results.items():
            clean_result = {k: v for k, v in result.items()
                            if not isinstance(v, np.ndarray)}  # Избегни numpy arrays во JSON
            results_clean[key] = clean_result

        with open(self.output_dir / 'training_results.json', 'w', encoding='utf-8') as f:
            json.dump(results_clean, f, indent=2, ensure_ascii=False)

        print(f"    training_results.json")
        print(f" Сè зачувано во: {self.output_dir}")

    def run_full_training(self):
        """Главна функција за тренирање"""
        print(" GNN Training System")
        print("=" * 50)

        if not self.load_graphs():
            print(" Не можам да ги вчитам графовите!")
            return False

        print("\n NODE CLASSIFICATION")
        print("-" * 30)

        available_graphs = list(self.graphs.keys())
        target_graph = available_graphs[0] if available_graphs else "event_similarity"

        for model_type in ["GCN", "GAT", "GraphSAGE"]:
            try:
                self.train_node_classification(target_graph, model_type)
            except Exception as e:
                print(f" Проблем со {model_type}: {e}")

        print("\n LINK PREDICTION")
        print("-" * 30)

        try:
            self.train_link_prediction(target_graph)
        except Exception as e:
            print(f" Проблем со link prediction: {e}")

        self.visualize_results()
        self.save_results()

        print("\n GNN Training завршен!")
        return True


def main():
    trainer = GNNTrainer()
    success = trainer.run_full_training()

    if success:
        print("\n Сè готово! Провери ја 'gnn_results' папката за:")
        print("    training_results.json - детални резултати")
        print("    models/ - тренирани модели")
        print("    gnn_training_results.png - визуализации")
        print("\n Следен чекор: Експериментирај со различни hyperparameters!")

    return success


if __name__ == "__main__":
    main()