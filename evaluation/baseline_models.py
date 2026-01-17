"""
Baseline models за споредба со GNN

ВАЖНО: GNN мора да биде ПОДОБАР од овие baselines за да има смисла!

Baselines:
1. Random - Random препораки (worst case)
2. Popular - Најпопуларни items (hard to beat!)
3. Item-KNN - Item-based collaborative filtering
4. User-KNN - User-based collaborative filtering
5. Matrix Factorization (SVD) - Traditional CF
"""
import sys
sys.path.append('..')

from models.db_models import Event, Attendance
from main import create_app
import numpy as np
from typing import Dict, List, Tuple
from collections import Counter
from sklearn.metrics.pairwise import cosine_similarity


class BaselineRecommender:
    """Base class за сите baselines"""

    def fit(self, train_data: Dict[int, List[int]]):
        """Train на training data"""
        raise NotImplementedError

    def recommend(self, user_id: int, k: int = 10) -> List[int]:
        """Препорачај TOP-K items за user"""
        raise NotImplementedError


class RandomRecommender(BaselineRecommender):
    """
    Random препораки - worst case baseline

    Accuracy: ~0%
    Цел: Секој модел мора да биде подобар од ова!
    """

    def __init__(self, all_items: List[int]):
        self.all_items = all_items

    def fit(self, train_data: Dict[int, List[int]]):
        pass  # Нема training

    def recommend(self, user_id: int, k: int = 10) -> List[int]:
        return list(np.random.choice(self.all_items, size=min(k, len(self.all_items)), replace=False))


class PopularRecommender(BaselineRecommender):
    """
    Popular Items - препорачај најпопуларни

    Зошто е важен: МНОГУ тежок baseline за да се надмине!
    Во practice, popular items често се добри препораки
    GNN мора да биде подобар од ова!

    Accuracy: Often 10-20% Precision@10
    """

    def __init__(self):
        self.item_popularity = {}
        self.popular_items = []

    def fit(self, train_data: Dict[int, List[int]]):
        """Пресметај popularity (број на likes)"""
        item_counts = Counter()

        for user_id, items in train_data.items():
            item_counts.update(items)

        self.item_popularity = dict(item_counts)

        # Сортирај по popularity
        self.popular_items = [item for item, count in item_counts.most_common()]

    def recommend(self, user_id: int, k: int = 10) -> List[int]:
        """Препорачај TOP-K најпопуларни"""
        return self.popular_items[:k]


class ItemKNNRecommender(BaselineRecommender):
    """
    Item-KNN: Item-based Collaborative Filtering

    "Users who liked item A also liked item B"

    Algorithm:
    1. Пресметај item-item similarity (cosine)
    2. За секој item што корисникот го лајкал, најди најсличните items
    3. Рангирај по weighted sum

    Accuracy: Often 15-25% Precision@10
    """

    def __init__(self, k_neighbors: int = 20):
        self.k_neighbors = k_neighbors
        self.item_similarity = None
        self.item_to_users = {}
        self.all_items = []

    def fit(self, train_data: Dict[int, List[int]]):
        """Изгради item-item similarity matrix"""
        # Инвертирај: item -> users кои го лајкале
        for user_id, items in train_data.items():
            for item_id in items:
                if item_id not in self.item_to_users:
                    self.item_to_users[item_id] = []
                self.item_to_users[item_id].append(user_id)

        self.all_items = list(self.item_to_users.keys())

        # Креирај binary user-item matrix
        num_users = max(max(users) for users in self.item_to_users.values()) + 1
        num_items = len(self.all_items)

        item_matrix = np.zeros((num_items, num_users))

        for i, item_id in enumerate(self.all_items):
            for user_id in self.item_to_users[item_id]:
                item_matrix[i, user_id] = 1.0

        # Cosine similarity помеѓу items
        self.item_similarity = cosine_similarity(item_matrix)

        print(f"Item-KNN: Built similarity matrix ({num_items}x{num_items})")

    def recommend(self, user_id: int, k: int = 10, train_items: List[int] = None) -> List[int]:
        """Препорачај базирано на item similarity"""
        if train_items is None or not train_items:
            return self.all_items[:k]

        # Пресметај score за секој candidate item
        item_scores = {}

        for candidate_item in self.all_items:
            if candidate_item in train_items:
                continue  # Не препорачувај она што веќе го има

            candidate_idx = self.all_items.index(candidate_item)

            # Score = sum of similarities со items што корисникот ги лајкал
            score = 0.0
            for liked_item in train_items:
                if liked_item in self.all_items:
                    liked_idx = self.all_items.index(liked_item)
                    score += self.item_similarity[candidate_idx, liked_idx]

            item_scores[candidate_item] = score

        # Сортирај по score
        sorted_items = sorted(item_scores.items(), key=lambda x: x[1], reverse=True)

        return [item_id for item_id, score in sorted_items[:k]]


class MatrixFactorizationRecommender(BaselineRecommender):
    """
    Matrix Factorization (SVD/ALS):

    User-Item matrix R ≈ U × V^T

    Decompose sparse matrix во:
    - User factors U (users × latent_dim)
    - Item factors V (items × latent_dim)

    Prediction: r_ui = u_i · v_j (dot product)

    Accuracy: Often 20-30% Precision@10
    Ова е силен baseline!
    """

    def __init__(self, latent_dim: int = 50, learning_rate: float = 0.01, iterations: int = 20):
        self.latent_dim = latent_dim
        self.lr = learning_rate
        self.iterations = iterations
        self.user_factors = None
        self.item_factors = None
        self.user_to_idx = {}
        self.item_to_idx = {}
        self.idx_to_item = {}

    def fit(self, train_data: Dict[int, List[int]]):
        """Train MF со SGD"""
        # Креирај mappings
        all_users = list(train_data.keys())
        all_items = list(set(item for items in train_data.values() for item in items))

        self.user_to_idx = {uid: i for i, uid in enumerate(all_users)}
        self.item_to_idx = {iid: i for i, iid in enumerate(all_items)}
        self.idx_to_item = {i: iid for iid, i in self.item_to_idx.items()}

        num_users = len(all_users)
        num_items = len(all_items)

        # Initialize factors
        self.user_factors = np.random.randn(num_users, self.latent_dim) * 0.01
        self.item_factors = np.random.randn(num_items, self.latent_dim) * 0.01

        # Training pairs
        pairs = []
        for user_id, items in train_data.items():
            u_idx = self.user_to_idx[user_id]
            for item_id in items:
                i_idx = self.item_to_idx[item_id]
                pairs.append((u_idx, i_idx))

        # SGD Training
        print(f"Training MF: {num_users} users, {num_items} items, {len(pairs)} interactions")

        for iteration in range(self.iterations):
            np.random.shuffle(pairs)
            total_error = 0.0

            for u_idx, i_idx in pairs:
                # Prediction
                pred = np.dot(self.user_factors[u_idx], self.item_factors[i_idx])

                # Error (target = 1 за positive feedback)
                error = 1.0 - pred

                # Gradient update
                self.user_factors[u_idx] += self.lr * error * self.item_factors[i_idx]
                self.item_factors[i_idx] += self.lr * error * self.user_factors[u_idx]

                total_error += error ** 2

            if (iteration + 1) % 5 == 0:
                rmse = np.sqrt(total_error / len(pairs))
                print(f"  Iteration {iteration+1}/{self.iterations}, RMSE: {rmse:.4f}")

    def recommend(self, user_id: int, k: int = 10, train_items: List[int] = None) -> List[int]:
        """Препорачај TOP-K items"""
        if user_id not in self.user_to_idx:
            return []

        u_idx = self.user_to_idx[user_id]

        # Пресметај scores за сите items
        scores = np.dot(self.user_factors[u_idx], self.item_factors.T)

        # Филтрирај already seen items
        if train_items:
            for item_id in train_items:
                if item_id in self.item_to_idx:
                    i_idx = self.item_to_idx[item_id]
                    scores[i_idx] = -np.inf

        # TOP-K
        top_indices = np.argsort(scores)[::-1][:k]

        return [self.idx_to_item[idx] for idx in top_indices if idx in self.idx_to_item]


# === TESTING ===
if __name__ == "__main__":
    print("=" * 80)
    print("TESTING BASELINE MODELS")
    print("=" * 80)

    # Load split
    from train_test_split import load_split_from_disk

    try:
        train_data, test_data = load_split_from_disk()
    except:
        print("ERROR: Run train_test_split.py first!")
        exit()

    # Get all items
    app = create_app()
    with app.app_context():
        all_items = [e.id for e in Event.query.all()]

    print(f"\nDataset: {len(train_data)} users, {len(all_items)} items")

    # Test each baseline
    print("\n" + "=" * 80)
    print("1. RANDOM BASELINE")
    print("=" * 80)
    random_rec = RandomRecommender(all_items)
    random_rec.fit(train_data)
    sample_user = list(train_data.keys())[0]
    print(f"Sample recommendations for user {sample_user}: {random_rec.recommend(sample_user, k=5)}")

    print("\n" + "=" * 80)
    print("2. POPULAR BASELINE")
    print("=" * 80)
    popular_rec = PopularRecommender()
    popular_rec.fit(train_data)
    print(f"TOP-5 popular items: {popular_rec.recommend(sample_user, k=5)}")
    print(f"Total unique items: {len(popular_rec.popular_items)}")

    print("\n" + "=" * 80)
    print("3. ITEM-KNN BASELINE")
    print("=" * 80)
    itemknn_rec = ItemKNNRecommender(k_neighbors=20)
    itemknn_rec.fit(train_data)
    print(f"Sample recommendations: {itemknn_rec.recommend(sample_user, k=5, train_items=train_data[sample_user])}")

    print("\n" + "=" * 80)
    print("4. MATRIX FACTORIZATION BASELINE")
    print("=" * 80)
    mf_rec = MatrixFactorizationRecommender(latent_dim=32, iterations=10)
    mf_rec.fit(train_data)
    print(f"Sample recommendations: {mf_rec.recommend(sample_user, k=5, train_items=train_data[sample_user])}")

    print("\n" + "=" * 80)
    print("BASELINES ГОТОВИ! Користи ги за споредба со GNN.")
    print("=" * 80)
