# GNN (Graph Neural Network) Документација

## Содржина
1. [Вовед во GNN](#вовед-во-gnn)
2. [Архитектура на Системот](#архитектура-на-системот)
3. [Graph Construction](#graph-construction)
4. [GNN Модели](#gnn-модели)
5. [Scoring Систем](#scoring-систем)
6. [Персонализиран Препорачувач](#персонализиран-препорачувач)
7. [Тренирање на Моделот](#тренирање-на-моделот)
8. [Евалуација](#евалуација)

---

## Вовед во GNN

### Што е Graph Neural Network?

Graph Neural Networks (GNN) се тип на невронски мрежи дизајнирани за работа со **графовски структури**. За разлика од традиционалните невронски мрежи што работат со вектори или матрици, GNN можат да научат релации помеѓу ентитети поврзани во граф.

### Зошто GNN за препораки на настани?

Настаните природно формираат граф:
- **Јазли (Nodes)**: Настани
- **Ребра (Edges)**: Сличност помеѓу настани (базирана на тагови, локација, време)

```
    [Concert A] ----сличност---- [Concert B]
         |                            |
    сличност                     сличност
         |                            |
    [Festival C] ----сличност---- [Music Event D]
```

GNN може да научи **семантички embeddings** за секој настан кои ги кодираат:
- Содржината на настанот
- Неговата позиција во графот (соседи)
- Скриени релации со други настани

---

## Архитектура на Системот

### Преглед

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA PIPELINE                                │
├─────────────────────────────────────────────────────────────────────┤
│  1. Web Scraping → 2. Data Cleaning → 3. Feature Extraction        │
│                                              ↓                       │
│                                    ┌─────────────────┐              │
│                                    │  Event Features │              │
│                                    │  - Title (TF-IDF)│              │
│                                    │  - Tags          │              │
│                                    │  - Category      │              │
│                                    │  - Location      │              │
│                                    │  - Time          │              │
│                                    └────────┬────────┘              │
└─────────────────────────────────────────────┼───────────────────────┘
                                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      GRAPH CONSTRUCTION                              │
├─────────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐      ┌──────────────┐      ┌──────────────┐      │
│  │   Compute    │      │   Build      │      │   Create     │      │
│  │  Similarity  │ ───► │   Edges      │ ───► │  PyG Graph   │      │
│  │   Matrix     │      │ (threshold)  │      │   Object     │      │
│  └──────────────┘      └──────────────┘      └──────────────┘      │
│                                                      ↓              │
│                                    ┌─────────────────────────┐      │
│                                    │  Data(x, edge_index)   │      │
│                                    │  - x: node features    │      │
│                                    │  - edge_index: edges   │      │
│                                    └─────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────┘
                                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                        GNN TRAINING                                  │
├─────────────────────────────────────────────────────────────────────┤
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐             │
│  │   GCN       │    │   GAT       │    │  GraphSAGE  │  ← BEST    │
│  │  (baseline) │    │ (attention) │    │ (inductive) │             │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘             │
│         │                  │                  │                     │
│         └──────────────────┼──────────────────┘                     │
│                            ↓                                        │
│                  ┌─────────────────────┐                           │
│                  │  Event Embeddings   │                           │
│                  │   (32-dimensional)  │                           │
│                  └─────────────────────┘                           │
└─────────────────────────────────────────────────────────────────────┘
                                              ↓
┌─────────────────────────────────────────────────────────────────────┐
│                    RECOMMENDATION ENGINE                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  User History (likes/dislikes)                                      │
│         ↓                                                            │
│  ┌──────────────────────────────────────────────────────────┐       │
│  │              HYBRID SCORING                               │       │
│  │                                                           │       │
│  │  ┌────────────────┐    ┌────────────────┐               │       │
│  │  │  Traditional   │    │     GNN        │               │       │
│  │  │    Score       │    │    Score       │               │       │
│  │  │  (30% weight)  │    │  (40% weight)  │               │       │
│  │  └───────┬────────┘    └───────┬────────┘               │       │
│  │          │                     │                         │       │
│  │          │  ┌─────────────────┐│                         │       │
│  │          │  │ Venue + Temporal││                         │       │
│  │          │  │   (30% weight)  ││                         │       │
│  │          │  └────────┬────────┘│                         │       │
│  │          │           │         │                         │       │
│  │          └───────────┼─────────┘                         │       │
│  │                      ↓                                   │       │
│  │           ┌─────────────────────┐                       │       │
│  │           │  Personalized Score │                       │       │
│  │           │      (0-100%)       │                       │       │
│  │           └─────────────────────┘                       │       │
│  └──────────────────────────────────────────────────────────┘       │
│                            ↓                                        │
│                   Ranked Recommendations                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Graph Construction

### Процес на Креирање Граф

Графот се креира во `graph_construction/graph_construction.py`:

#### 1. Feature Extraction

```python
# TF-IDF за текстуални features
tfidf = TfidfVectorizer(max_features=100)
text_features = tfidf.fit_transform(df['title'] + ' ' + df['description'])

# One-hot encoding за категории
category_encoded = pd.get_dummies(df['category'])

# Numerical features (нормализирани)
numerical = StandardScaler().fit_transform(df[['lat', 'lon', 'price']])

# Комбинирај ги сите
node_features = np.hstack([text_features, category_encoded, numerical])
```

#### 2. Similarity Calculation

```python
# Cosine similarity помеѓу сите парови настани
similarity_matrix = cosine_similarity(node_features)

# Threshold за ребра (само силни врски)
SIMILARITY_THRESHOLD = 0.3  # Настрој според потреба
```

#### 3. Edge Creation

```python
edge_list = []
for i in range(n_events):
    for j in range(i+1, n_events):
        if similarity_matrix[i,j] > SIMILARITY_THRESHOLD:
            edge_list.append([i, j])
            edge_list.append([j, i])  # Undirected graph

edge_index = torch.tensor(edge_list).T
```

#### 4. PyTorch Geometric Data Object

```python
from torch_geometric.data import Data

graph = Data(
    x=torch.tensor(node_features, dtype=torch.float),
    edge_index=edge_index,
    num_nodes=n_events
)

# Зачувај
torch.save(graph, 'graph_data/event_similarity_graph.pt')
```

### Статистика на Графот

Типичен граф за ~500 настани:
- **Јазли**: 500-600
- **Ребра**: 5,000-15,000 (зависи од threshold)
- **Просечен degree**: 20-30
- **Node features**: 59-128 dimensional

---

## GNN Модели

### 1. GCN (Graph Convolutional Network)

**Основен GNN модел** - агрегира информации од соседи со проста средна вредност.

```python
class EventGCN(nn.Module):
    def __init__(self, input_dim, hidden_dim=64, output_dim=32):
        super().__init__()
        self.conv1 = GCNConv(input_dim, hidden_dim)
        self.conv2 = GCNConv(hidden_dim, output_dim)

    def forward(self, x, edge_index):
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.conv2(x, edge_index)
        return x
```

**Формула:**
```
h_v = σ(W · MEAN({h_u : u ∈ N(v) ∪ {v}}))
```

### 2. GAT (Graph Attention Network)

**Attention механизам** - различни соседи имаат различни тежини.

```python
class EventGAT(nn.Module):
    def __init__(self, input_dim, hidden_dim=64, output_dim=32, heads=4):
        super().__init__()
        self.conv1 = GATConv(input_dim, hidden_dim//heads, heads=heads)
        self.conv2 = GATConv(hidden_dim, output_dim, heads=1)

    def forward(self, x, edge_index):
        x = F.elu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.conv2(x, edge_index)
        return x
```

**Attention коефициенти:**
```
α_ij = softmax(LeakyReLU(a^T · [W·h_i || W·h_j]))
```

### 3. GraphSAGE (Sample and Aggregate) ⭐ ПРИМАРЕН

**Inductive learning** - може да генерира embeddings за нови настани без ретренирање.

```python
class EventGraphSAGE(nn.Module):
    def __init__(self, input_dim, hidden_dim=64, output_dim=32):
        super().__init__()
        self.conv1 = SAGEConv(input_dim, hidden_dim)
        self.conv2 = SAGEConv(hidden_dim, output_dim)

    def forward(self, x, edge_index):
        x = F.relu(self.conv1(x, edge_index))
        x = F.dropout(x, p=0.5, training=self.training)
        x = self.conv2(x, edge_index)
        return x
```

**Aggregation:**
```
h_v = σ(W · CONCAT(h_v, AGG({h_u : u ∈ N(v)})))
```

### Зошто GraphSAGE?

1. **Inductive** - работи со нови настани
2. **Scalable** - ефикасен за големи графови
3. **Flexible** - различни aggregation функции
4. **Proven** - најдобри резултати во експерименти

---

## Scoring Систем

### Традиционален Score

```python
def traditional_score(user_id, event):
    # 1. Tag matching
    liked_tags = get_user_liked_tags(user_id)
    disliked_tags = get_user_disliked_tags(user_id)
    event_tags = event.tags.split(',')

    positive = len(set(liked_tags) & set(event_tags))
    negative = len(set(disliked_tags) & set(event_tags))

    # Dislikes се 5x појаки!
    tag_score = positive - (negative * 5)

    # 2. Geo score
    geo_score = 1.0 / (1.0 + distance(user, venue))

    return tag_score + geo_score
```

### GNN Similarity Score

```python
def gnn_similarity_score(user_id, event):
    event_embedding = embeddings[event.id]

    # Најди embeddings на liked настани
    liked_embeddings = [embeddings[e.id] for e in user_liked_events]

    # Cosine similarity со TOP-3 најслични liked
    similarities = [cosine_sim(event_embedding, le) for le in liked_embeddings]
    top_3 = sorted(similarities, reverse=True)[:3]
    positive_score = mean(top_3)

    # Penalty од disliked настани
    disliked_embeddings = [embeddings[e.id] for e in user_disliked_events]
    max_dislike_sim = max([cosine_sim(event_embedding, de) for de in disliked_embeddings])
    penalty = max_dislike_sim * 0.5

    return max(0, positive_score - penalty)
```

### Combined Score

```python
def combined_score(user_id, event):
    trad = traditional_score(user_id, event)
    gnn = gnn_similarity_score(user_id, event)

    # Alpha = 0.6 (60% traditional, 40% GNN)
    alpha = 0.6

    # GNN score се скалира со 10 за ист range
    return alpha * trad + (1 - alpha) * gnn * 10
```

---

## Персонализиран Препорачувач

### Компоненти (Тежини)

| Компонента | Тежина | Опис |
|------------|--------|------|
| GNN Semantic | 40% | Embedding similarity |
| Tag Preference | 30% | Weighted tag matching |
| Venue Preference | 20% | Омилени локации |
| Temporal | 10% | Ден од недела, час |

### Алгоритам

```python
def personalized_score(user_id, event):
    # 1. GNN Score (40%)
    gnn_score = gnn_similarity_score(user_id, event)

    # 2. Tag Score (30%)
    liked_tags = {tag: count for tag, count in user_liked_tag_counts}
    disliked_tags = {tag: count for tag, count in user_disliked_tag_counts}

    event_tags = set(event.tags.split(','))
    positive_weight = sum(liked_tags.get(t, 0) for t in event_tags)
    negative_weight = sum(disliked_tags.get(t, 0) for t in event_tags)

    max_weight = max(liked_tags.values()) * len(event_tags)
    tag_score = (positive_weight - negative_weight * 2) / max_weight
    tag_score = clamp(tag_score, 0, 1)

    # 3. Venue Score (20%)
    liked_venues = user_liked_venue_counts
    venue_likes = liked_venues.get(event.venue_id, 0)
    venue_score = venue_likes / (sum(liked_venues.values()) + 1)

    # 4. Temporal Score (10%)
    event_dow = event.starts_at.weekday()
    event_hour = event.starts_at.hour

    dow_affinity = user_liked_dow_counts.get(event_dow, 0) / total_liked
    hour_affinity = user_liked_hour_counts.get(event_hour, 0) / total_liked
    temporal_score = (dow_affinity + hour_affinity) / 2

    # 5. Random noise (5%) за tie-breaking
    noise = (random.random() - 0.5) * 0.1

    # Final weighted combination
    raw_score = (
        0.40 * gnn_score +
        0.30 * tag_score +
        0.20 * venue_score +
        0.10 * temporal_score
    ) + noise

    # Power scaling за подобра дистрибуција
    final_score = min(1.0, pow(raw_score * 2.5, 0.7))

    return final_score * 100  # 0-100%
```

---

## Тренирање на Моделот

### Link Prediction Task

GNN моделите се тренираат со **link prediction** - предвидување дали два настани треба да бидат поврзани.

```python
def train_gnn(model, graph, epochs=200):
    optimizer = torch.optim.Adam(model.parameters(), lr=0.01)

    # Split edges: 80% train, 10% val, 10% test
    train_data = train_test_split_edges(graph)

    for epoch in range(epochs):
        model.train()
        optimizer.zero_grad()

        # Forward pass
        z = model(train_data.x, train_data.train_pos_edge_index)

        # Link prediction loss
        pos_loss = -torch.log(
            torch.sigmoid((z[train_data.train_pos_edge_index[0]] *
                          z[train_data.train_pos_edge_index[1]]).sum(dim=1))
        ).mean()

        neg_edge_index = negative_sampling(train_data.train_pos_edge_index)
        neg_loss = -torch.log(
            1 - torch.sigmoid((z[neg_edge_index[0]] *
                              z[neg_edge_index[1]]).sum(dim=1))
        ).mean()

        loss = pos_loss + neg_loss
        loss.backward()
        optimizer.step()
```

### Hyperparameters

| Parameter | Value | Опис |
|-----------|-------|------|
| Hidden dim | 64 | Скриен слој димензија |
| Output dim | 32 | Embedding димензија |
| Layers | 2 | Број на GNN слоеви |
| Dropout | 0.5 | Regularization |
| Learning rate | 0.01 | Adam optimizer |
| Epochs | 200 | Тренинг итерации |

---

## Евалуација

### Метрики

1. **AUC-ROC** - Area Under ROC Curve
2. **AP** - Average Precision
3. **Hit@K** - Дали точната препорака е во топ K
4. **NDCG** - Normalized Discounted Cumulative Gain

### Резултати

| Модел | AUC | AP | Hit@10 |
|-------|-----|----|----|
| GCN | 0.82 | 0.79 | 0.65 |
| GAT | 0.85 | 0.82 | 0.71 |
| **GraphSAGE** | **0.89** | **0.86** | **0.78** |

### Визуелизации

Генерирај со:
```bash
python scripts/gnn_analytics_visualizations.py <USER_ID>
```

Излез:
- `01_score_distribution.png` - Дистрибуција на скорови
- `02_gnn_vs_traditional.png` - Споредба GNN vs Traditional
- `03_tag_distribution.png` - Анализа на тагови
- `04_user_behavior.png` - Корисничко однесување
- `05_recommendation_quality.png` - Квалитет на препораки

---

## Фајлови

| Фајл | Опис |
|------|------|
| `models/recommender.py` | Главен GNN препорачувач |
| `models/gnn_training.py` | GNN модели и тренинг |
| `graph_construction/graph_construction.py` | Креирање на граф |
| `graph_construction/graph_data/*.pt` | Зачувани графови |
| `scripts/gnn_analytics_visualizations.py` | Визуелизации |

---

## Референци

1. **GraphSAGE**: Hamilton et al., "Inductive Representation Learning on Large Graphs" (NeurIPS 2017)
2. **GCN**: Kipf & Welling, "Semi-Supervised Classification with Graph Convolutional Networks" (ICLR 2017)
3. **GAT**: Veličković et al., "Graph Attention Networks" (ICLR 2018)
4. **PyTorch Geometric**: Fey & Lenssen, "Fast Graph Representation Learning with PyTorch Geometric" (ICLR Workshop 2019)

---

**Автори:** Martin Stamenov & Teodora Saneva
**Верзија:** 2.0.0
**Датум:** Јануари 2026
