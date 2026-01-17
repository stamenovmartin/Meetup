# 🎯 GNN ПОДОБРУВАЊА - Roadmap

Ова е **конкретен план** за да го подобриш GNN делот на проектот и да го направиш академски валиден.

---

## 📊 ФАЗА 1: EVALUATION (Прв приоритет!) ✅ ГОТОВО

### Што е направено:

1. **Metrics Framework** (`recommendation_metrics.py`)
   - ✅ Precision@K, Recall@K, NDCG@K, MRR, Hit Rate
   - ✅ Standard metrics за recommendation systems

2. **Train/Test Split** (`train_test_split.py`)
   - ✅ Temporal split (80/20)
   - ✅ Leave-one-out split
   - ✅ Reproducible (save/load)

3. **Baselines** (`baseline_models.py`)
   - ✅ Random (worst case)
   - ✅ Popular (hard baseline!)
   - ✅ Item-KNN (collaborative filtering)
   - ✅ Matrix Factorization (SVD) (strong baseline!)

4. **Evaluation Pipeline** (`run_evaluation.py`)
   - ✅ Automated testing на сите модели
   - ✅ Comparison table
   - ✅ Statistical analysis

### Како да run-увам:

```bash
cd evaluation/

# Step 1: Create train/test split
python train_test_split.py

# Step 2: Test baselines
python baseline_models.py

# Step 3: Run full evaluation
python run_evaluation.py
```

### Што ќе добиеш:

```
===========================================================
Model                   P@5        P@10      R@10    NDCG@10
-----------------------------------------------------------
Random               0.0123     0.0145    0.0298     0.0201
Popular              0.1234     0.1456    0.3214     0.2145
Item-KNN             0.1567     0.1789    0.3987     0.2678
MF                   0.1890     0.2134    0.4123     0.2987
GNN (Yours)          0.????     0.????    0.????     0.????
===========================================================
```

**Твојот GNN МОРА да биде подобар од Popular и MF!**

---

## 🚀 ФАЗА 2: SUPERVISED GNN TRAINING (Следен чекор!)

### Проблем сега:

Твојот GNN е **unsupervised**:
```python
# Сега:
Loss = reconstruction_loss(node_features)  # ❌ Не учи од ratings!
```

### Решение: Supervised Training

Направи GNN кој **директно учи од user-item interactions**:

```python
# Треба:
Loss = BPR_loss(user, positive_item, negative_item)  # ✅ Учи од likes!
```

### Имплементација:

**File: `graph_construction/supervised_gnn_training.py`**

```python
import torch
import torch.nn as nn
from torch_geometric.nn import SAGEConv, GCNConv

class RecommenderGNN(nn.Module):
    """
    Supervised GNN за recommendation

    Graph structure:
    - User nodes
    - Item nodes
    - Edges: User --liked--> Item

    Task: Predict user-item ratings
    """

    def __init__(self, num_users, num_items, embedding_dim=64, hidden_dim=128):
        super().__init__()

        # User & Item embeddings
        self.user_emb = nn.Embedding(num_users, embedding_dim)
        self.item_emb = nn.Embedding(num_items, embedding_dim)

        # Graph layers
        self.conv1 = SAGEConv(embedding_dim, hidden_dim)
        self.conv2 = SAGEConv(hidden_dim, embedding_dim)

        # Prediction layer
        self.predictor = nn.Linear(embedding_dim * 2, 1)

    def forward(self, edge_index, user_ids, item_ids):
        # Get embeddings
        x_users = self.user_emb(user_ids)
        x_items = self.item_emb(item_ids)

        # Concatenate all nodes
        x = torch.cat([x_users, x_items], dim=0)

        # Graph convolutions
        x = self.conv1(x, edge_index)
        x = torch.relu(x)
        x = self.conv2(x, edge_index)

        # Split back
        user_emb_out = x[:len(user_ids)]
        item_emb_out = x[len(user_ids):]

        # Prediction: concat user + item embeddings
        combined = torch.cat([user_emb_out, item_emb_out], dim=1)
        pred = self.predictor(combined)

        return pred.squeeze()


class BPRLoss(nn.Module):
    """
    Bayesian Personalized Ranking Loss

    Идеја: Liked items треба да имаат повисок score од non-liked
    Loss = -log(sigmoid(score_positive - score_negative))
    """

    def forward(self, pos_scores, neg_scores):
        return -torch.log(torch.sigmoid(pos_scores - neg_scores)).mean()


def train_supervised_gnn(train_data, epochs=50):
    """
    Training loop за supervised GNN
    """
    # Build heterogeneous graph: User --edge--> Item
    edge_index = build_user_item_graph(train_data)

    # Model
    model = RecommenderGNN(num_users=100, num_items=1327)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    criterion = BPRLoss()

    for epoch in range(epochs):
        model.train()

        # Sample batch: (user, positive_item, negative_item)
        batch = sample_bpr_batch(train_data, batch_size=512)

        user_ids = batch['users']
        pos_items = batch['pos_items']
        neg_items = batch['neg_items']

        # Forward pass
        pos_scores = model(edge_index, user_ids, pos_items)
        neg_scores = model(edge_index, user_ids, neg_items)

        # Loss
        loss = criterion(pos_scores, neg_scores)

        # Backward
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if (epoch + 1) % 10 == 0:
            print(f"Epoch {epoch+1}/{epochs}, Loss: {loss.item():.4f}")

    return model
```

### Зошто е подобро:

| Unsupervised (сега) | Supervised (подобрување) |
|---------------------|--------------------------|
| Учи само од features | Учи од user preferences |
| Не знае што е "good recommendation" | Знае: liked > non-liked |
| Generic embeddings | Personalized embeddings |
| P@10 ≈ 10-15% | P@10 ≈ 20-30% |

---

## 🏗️ ФАЗА 3: HETEROGENEOUS GRAPH (User + Item Nodes)

### Проблем сега:

Твојот graph е само **Event-Event**:
```
Graph = Event --similarity--> Event
```

### Решение: Додај User Nodes

```
Graph = User --rated--> Event --venue--> Venue
        User --friends--> User
        Event --similar--> Event
```

### Имплементација:

**File: `graph_construction/heterogeneous_graph.py`**

```python
from torch_geometric.data import HeteroData

def build_heterogeneous_graph(users, events, venues, ratings, friendships):
    """
    Heterogeneous graph со 3 типа nodes:
    - Users
    - Events
    - Venues

    Edge types:
    - (user, rated, event)
    - (user, friend, user)
    - (event, held_at, venue)
    - (event, similar, event)
    """

    data = HeteroData()

    # Node features
    data['user'].x = user_features  # (num_users, user_feat_dim)
    data['event'].x = event_features  # (num_events, event_feat_dim)
    data['venue'].x = venue_features  # (num_venues, venue_feat_dim)

    # Edge indices
    data['user', 'rated', 'event'].edge_index = rating_edges
    data['user', 'friend', 'user'].edge_index = friendship_edges
    data['event', 'held_at', 'venue'].edge_index = venue_edges
    data['event', 'similar', 'event'].edge_index = similarity_edges

    return data
```

### GNN Architecture за Heterogeneous Graph:

```python
from torch_geometric.nn import HeteroConv, SAGEConv

class HeteroGNNRecommender(nn.Module):
    def __init__(self, metadata, hidden_channels):
        super().__init__()

        # Layer 1: Process сите edge types
        self.conv1 = HeteroConv({
            ('user', 'rated', 'event'): SAGEConv((-1, -1), hidden_channels),
            ('user', 'friend', 'user'): SAGEConv((-1, -1), hidden_channels),
            ('event', 'held_at', 'venue'): SAGEConv((-1, -1), hidden_channels),
            ('event', 'similar', 'event'): SAGEConv((-1, -1), hidden_channels),
        }, aggr='sum')

        # Layer 2
        self.conv2 = HeteroConv({...}, aggr='sum')

    def forward(self, x_dict, edge_index_dict):
        x_dict = self.conv1(x_dict, edge_index_dict)
        x_dict = {key: x.relu() for key, x in x_dict.items()}
        x_dict = self.conv2(x_dict, edge_index_dict)
        return x_dict
```

---

## 📈 ФАЗА 4: ABLATION STUDIES

Покажи што **точно** придонесува GNN:

### Експерименти:

```python
results = {}

# 1. Traditional only (without GNN)
results['Traditional'] = evaluate(traditional_features_only)

# 2. GNN only (without traditional)
results['GNN_only'] = evaluate(gnn_embeddings_only)

# 3. GNN + Traditional (hybrid - твој сегашен)
results['Hybrid'] = evaluate(gnn_with_traditional)

# 4. Different GNN architectures
results['GraphSAGE'] = evaluate(graphsage_model)
results['GCN'] = evaluate(gcn_model)
results['GAT'] = evaluate(gat_model)

# 5. Different embedding dimensions
for dim in [32, 64, 128, 256]:
    results[f'GNN_dim{dim}'] = evaluate(gnn_model(dim=dim))
```

### Visualize Results:

```python
import matplotlib.pyplot as plt

models = list(results.keys())
p10_scores = [results[m]['P@10'] for m in models]

plt.bar(models, p10_scores)
plt.ylabel('Precision@10')
plt.title('Ablation Study: Component Contribution')
plt.xticks(rotation=45)
plt.savefig('ablation_study.png')
```

---

## 🎓 ФАЗА 5: WRITE-UP (За академски контекст)

### Structure на paper/thesis:

```
1. INTRODUCTION
   - Problem: Event recommendation
   - Challenge: Sparsity, cold start
   - Solution: GNN для learning from graph structure

2. RELATED WORK
   - Collaborative Filtering (MF, NCF)
   - Graph-based Recommendations (NGCF, LightGCN)
   - Event recommendation systems

3. METHODOLOGY
   3.1 Graph Construction
       - Node types: Users, Events, Venues
       - Edge types: Ratings, Similarity, Location

   3.2 GNN Architecture
       - GraphSAGE aggregation
       - Heterogeneous message passing

   3.3 Training
       - BPR loss for ranking
       - Negative sampling strategy

4. EXPERIMENTS
   4.1 Dataset
       - 1327 events, 84 users, 4020 interactions
       - Train/test split (80/20 temporal)

   4.2 Baselines
       - Popular, Item-KNN, Matrix Factorization

   4.3 Evaluation Metrics
       - Precision@K, NDCG@K, MRR

   4.4 Results
       [Table showing GNN beats baselines]

   4.5 Ablation Studies
       [Bar chart showing component contributions]

5. DISCUSSION
   - Why GNN helps: Captures event relationships
   - Limitations: Small dataset, computational cost

6. CONCLUSION
   - GNN improves recommendation by X%
   - Future work: Multi-modal features, temporal dynamics
```

---

## 📋 PRIORITY CHECKLIST

### Must Have (за 8/10 оценка):
- [x] ✅ Evaluation metrics (Precision, NDCG)
- [x] ✅ Train/test split
- [x] ✅ Baseline comparisons (Popular, MF)
- [ ] ⏳ Supervised GNN training
- [ ] ⏳ Statistical significance test

### Should Have (за 9/10 оценка):
- [ ] ⏳ Heterogeneous graph (User nodes)
- [ ] ⏳ Ablation studies
- [ ] ⏳ Hyperparameter tuning
- [ ] ⏳ Embedding visualization (t-SNE)

### Nice to Have (за 10/10 оценка):
- [ ] ⏳ Multiple GNN architectures (GCN, GAT)
- [ ] ⏳ Attention mechanisms
- [ ] ⏳ Temporal dynamics
- [ ] ⏳ User study

---

## 🚀 КАКО ДА ПОЧНЕШ:

### Ден 1: Run Evaluation
```bash
cd evaluation/
python train_test_split.py    # Create split
python run_evaluation.py       # Test current GNN
```

**Очекувај:** GNN може да биде понизок од MF (it's OK!)

### Ден 2-3: Supervised Training
- Имплементирај `supervised_gnn_training.py`
- Train со BPR loss
- Re-run evaluation
- **Target:** GNN > MF baseline (+10% improvement)

### Ден 4-5: Heterogeneous Graph
- Додај user nodes
- Имплементирај HeteroGNN
- Re-run evaluation
- **Target:** GNN > MF baseline (+20% improvement)

### Ден 6-7: Ablation + Write-up
- Run ablation studies
- Create visualizations
- Write report/thesis chapter

---

## 💡 НАЈВАЖНИ ПОУКИ:

1. **"No evaluation = No science"**
   - Не можеш да кажеш "GNN works" без metrics!

2. **"Beat the baselines"**
   - Popular е МНОГУ тежок baseline
   - Ако не победиш, GNN нема смисла

3. **"Supervised > Unsupervised"**
   - За recommendation, supervised training е must

4. **"Graph design matters"**
   - User nodes се клучни за personalization

5. **"Reproducibility is key"**
   - Train/test split мора да биде fixed
   - Random seed мора да биде set

---

## 📞 ПОМОШ:

Ако имаш прашања при имплементација:
1. Погледни ги примерите во файловите
2. Check PyTorch Geometric tutorials
3. Read papers: NGCF, LightGCN, Neural Graph Collaborative Filtering

**Успех! 🎯**
