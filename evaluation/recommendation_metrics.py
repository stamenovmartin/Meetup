"""
Evaluation metrics за recommendation systems
Стандардни metrics: Precision@K, Recall@K, NDCG@K, MRR, MAP
"""
import numpy as np
from typing import List, Dict, Tuple


def precision_at_k(recommended: List[int], relevant: List[int], k: int = 10) -> float:
    """
    Precision@K: Колку од TOP-K препораките се релевантни?

    Args:
        recommended: Lista од препорачани item IDs (sorted по score)
        relevant: Lista од релевантни item IDs (ground truth)
        k: Број на top препораки

    Returns:
        Precision score (0.0-1.0)

    Example:
        recommended = [1, 5, 3, 8, 2, 9, 4, 7, 6, 10]
        relevant = [1, 3, 5, 11, 12]
        precision_at_k(recommended, relevant, k=10) = 3/10 = 0.3
    """
    if k <= 0 or not relevant:
        return 0.0

    recommended_at_k = set(recommended[:k])
    relevant_set = set(relevant)

    hits = len(recommended_at_k & relevant_set)
    return hits / k


def recall_at_k(recommended: List[int], relevant: List[int], k: int = 10) -> float:
    """
    Recall@K: Колку % од релевантните items ги нашле во TOP-K?

    Args:
        recommended: Lista од препорачани item IDs
        relevant: Lista од релевантни item IDs
        k: Број на top препораки

    Returns:
        Recall score (0.0-1.0)

    Example:
        recommended = [1, 5, 3, 8, 2]
        relevant = [1, 3, 5, 11, 12]
        recall_at_k(recommended, relevant, k=5) = 3/5 = 0.6
    """
    if not relevant:
        return 0.0

    recommended_at_k = set(recommended[:k])
    relevant_set = set(relevant)

    hits = len(recommended_at_k & relevant_set)
    return hits / len(relevant_set)


def ndcg_at_k(recommended: List[int], relevant: List[int], k: int = 10) -> float:
    """
    NDCG@K (Normalized Discounted Cumulative Gain):
    Precision weighted by position (релевантни на врв се повеќе важни)

    Args:
        recommended: Lista од препорачани item IDs
        relevant: Lista од релевантни item IDs
        k: Број на top препораки

    Returns:
        NDCG score (0.0-1.0)
    """
    if not relevant:
        return 0.0

    relevant_set = set(relevant)

    # DCG: Sum of (relevance / log2(position+1))
    dcg = 0.0
    for i, item_id in enumerate(recommended[:k]):
        if item_id in relevant_set:
            # Relevance = 1 (binary), position starts at 1
            dcg += 1.0 / np.log2(i + 2)

    # IDCG: Perfect ranking (сите релевантни на врв)
    idcg = sum(1.0 / np.log2(i + 2) for i in range(min(len(relevant), k)))

    if idcg == 0:
        return 0.0

    return dcg / idcg


def mean_reciprocal_rank(recommended: List[int], relevant: List[int]) -> float:
    """
    MRR (Mean Reciprocal Rank): 1 / (position на прв релевантен item)

    Args:
        recommended: Lista од препорачани item IDs
        relevant: Lista од релевантни item IDs

    Returns:
        MRR score (0.0-1.0)

    Example:
        recommended = [10, 5, 3, 1, 8]
        relevant = [1, 3]
        MRR = 1/3 = 0.333 (прв hit е на позиција 3)
    """
    if not relevant:
        return 0.0

    relevant_set = set(relevant)

    for i, item_id in enumerate(recommended):
        if item_id in relevant_set:
            return 1.0 / (i + 1)

    return 0.0


def hit_rate_at_k(recommended: List[int], relevant: List[int], k: int = 10) -> float:
    """
    Hit Rate@K: Дали барем еден релевантен item е во TOP-K? (binary)

    Returns:
        1.0 if at least one hit, else 0.0
    """
    if not relevant:
        return 0.0

    recommended_at_k = set(recommended[:k])
    relevant_set = set(relevant)

    return 1.0 if len(recommended_at_k & relevant_set) > 0 else 0.0


def evaluate_recommendations(
    recommended_items: Dict[int, List[int]],
    ground_truth: Dict[int, List[int]],
    k_values: List[int] = [5, 10, 20]
) -> Dict[str, float]:
    """
    Евалуирај препораки за сите users

    Args:
        recommended_items: {user_id: [item_ids sorted by score]}
        ground_truth: {user_id: [relevant_item_ids]}
        k_values: Values of K to evaluate

    Returns:
        Dictionary со average metrics

    Example:
        recommended = {
            1: [10, 5, 3, 8, 2],
            2: [4, 7, 1, 9, 6]
        }
        ground_truth = {
            1: [3, 5, 11],
            2: [1, 4]
        }
        metrics = evaluate_recommendations(recommended, ground_truth)
        # Returns: {'P@5': 0.3, 'R@5': 0.45, 'NDCG@5': 0.52, ...}
    """
    metrics = {}

    # Филтрирај users кои имаат и препораки и ground truth
    common_users = set(recommended_items.keys()) & set(ground_truth.keys())

    if not common_users:
        return {f"{metric}@{k}": 0.0 for k in k_values for metric in ['P', 'R', 'NDCG', 'HR']}

    for k in k_values:
        precisions = []
        recalls = []
        ndcgs = []
        hit_rates = []

        for user_id in common_users:
            rec = recommended_items[user_id]
            rel = ground_truth[user_id]

            if not rel:  # Skip users без релевантни items
                continue

            precisions.append(precision_at_k(rec, rel, k))
            recalls.append(recall_at_k(rec, rel, k))
            ndcgs.append(ndcg_at_k(rec, rel, k))
            hit_rates.append(hit_rate_at_k(rec, rel, k))

        if precisions:
            metrics[f'P@{k}'] = np.mean(precisions)
            metrics[f'R@{k}'] = np.mean(recalls)
            metrics[f'NDCG@{k}'] = np.mean(ndcgs)
            metrics[f'HR@{k}'] = np.mean(hit_rates)

    # MRR (не зависи од K)
    mrrs = []
    for user_id in common_users:
        rec = recommended_items[user_id]
        rel = ground_truth[user_id]
        if rel:
            mrrs.append(mean_reciprocal_rank(rec, rel))

    if mrrs:
        metrics['MRR'] = np.mean(mrrs)

    return metrics


def coverage(recommended_items: Dict[int, List[int]], total_items: int) -> float:
    """
    Coverage: Колку % од сите items биле препорачани барем еднаш?

    Higher is better - не препорачуваме само популарни items
    """
    all_recommended = set()
    for items in recommended_items.values():
        all_recommended.update(items)

    return len(all_recommended) / total_items if total_items > 0 else 0.0


def diversity(recommended_items: Dict[int, List[int]], item_similarity_matrix: np.ndarray = None) -> float:
    """
    Diversity: Колку се различни препорачаните items?

    Ако има similarity matrix, пресметува average dissimilarity
    Инаку, пресметува uniqueness
    """
    if item_similarity_matrix is not None:
        # Average pairwise dissimilarity
        diversities = []
        for items in recommended_items.values():
            if len(items) < 2:
                continue

            pairwise_sim = []
            for i in range(len(items)):
                for j in range(i+1, len(items)):
                    if items[i] < len(item_similarity_matrix) and items[j] < len(item_similarity_matrix):
                        pairwise_sim.append(item_similarity_matrix[items[i], items[j]])

            if pairwise_sim:
                diversities.append(1.0 - np.mean(pairwise_sim))

        return np.mean(diversities) if diversities else 0.0
    else:
        # Simple uniqueness
        all_recommended = []
        for items in recommended_items.values():
            all_recommended.extend(items)

        return len(set(all_recommended)) / len(all_recommended) if all_recommended else 0.0


# === TESTING ===
if __name__ == "__main__":
    print("=" * 80)
    print("TESTING RECOMMENDATION METRICS")
    print("=" * 80)

    # Example data
    recommended = {
        1: [10, 5, 3, 8, 2, 9, 4, 7, 6, 1],
        2: [4, 7, 1, 9, 6, 3, 8, 2, 5, 10],
        3: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    }

    ground_truth = {
        1: [1, 3, 5, 11, 12],
        2: [1, 4, 15],
        3: [2, 4, 6, 8]
    }

    metrics = evaluate_recommendations(recommended, ground_truth, k_values=[5, 10])

    print("\nMetrics:")
    for metric, value in sorted(metrics.items()):
        print(f"  {metric:10s}: {value:.4f}")

    print("\n" + "=" * 80)
    print("INTERPRETATION:")
    print("=" * 80)
    print(f"P@10 = {metrics.get('P@10', 0):.2%} → {metrics.get('P@10', 0)*10:.1f} од 10 препораки се релевантни")
    print(f"R@10 = {metrics.get('R@10', 0):.2%} → Го најдовме {metrics.get('R@10', 0)*100:.0f}% од она што го сакаме")
    print(f"NDCG@10 = {metrics.get('NDCG@10', 0):.2%} → Quality of ranking")
    print(f"MRR = {metrics.get('MRR', 0):.3f} → Просечно прв hit на позиција {1/metrics.get('MRR', 1):.1f}")
