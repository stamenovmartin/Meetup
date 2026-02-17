"""
COMPLETE EVALUATION PIPELINE

Тестира сите модели (Baselines + GNN) и споредува резултати

Usage:
    python evaluation/run_evaluation.py

Output:
    - Metrics table (Precision@K, NDCG@K, etc.)
    - Statistical significance tests
    - Визуелизации
"""
import sys
sys.path.append('..')

from train_test_split import temporal_train_test_split, load_split_from_disk
from recommendation_metrics import evaluate_recommendations
from baseline_models import (
    RandomRecommender,
    PopularRecommender,
    ItemKNNRecommender,
    MatrixFactorizationRecommender
)
from models.recommender import get_recommender
from models.db_models import Event
from main import create_app
import numpy as np
import json
from typing import Dict, List
from datetime import datetime


def evaluate_gnn_model(
    train_data: Dict[int, List[int]],
    test_data: Dict[int, List[int]],
    k: int = 10
) -> Dict[int, List[int]]:
    """
    Евалуирај твојот GNN recommender

    ВАЖНО: Користи само train_data за да генерираш препораки!
    """
    app = create_app()

    with app.app_context():
        recommender = get_recommender()
        all_events = Event.query.all()

        recommendations = {}

        print(f"  Generating GNN recommendations for {len(test_data)} users...")

        for user_id in test_data.keys():
            if user_id not in train_data:
                continue

            # Score сите events
            scored_events = []
            for event in all_events:
                # ВАЖНО: Користи само train data за scoring
                score = recommender.personalized_score(user_id, event)
                scored_events.append((score, event.id))

            # Сортирај и земи TOP-K
            scored_events.sort(reverse=True, key=lambda x: x[0])

            # Филтрирај items од train (не препорачувај што веќе го има)
            filtered = []
            for score, event_id in scored_events:
                if event_id not in train_data.get(user_id, []):
                    filtered.append(event_id)

                if len(filtered) >= k:
                    break

            recommendations[user_id] = filtered

    return recommendations


def run_full_evaluation(use_cached_split: bool = True):
    """
    Главна evaluation функција
    """
    print("=" * 80)
    print("🎯 FULL RECOMMENDATION EVALUATION")
    print("=" * 80)

    # === STEP 1: Load/Create Split ===
    print("\n[1/5] Loading train/test split...")

    if use_cached_split:
        try:
            train_data, test_data = load_split_from_disk()
            print(f"  ✅ Loaded cached split")
        except:
            print(f"  ⚠️  No cached split, creating new...")
            train_data, test_data = temporal_train_test_split(train_ratio=0.8, min_interactions=5)
    else:
        train_data, test_data = temporal_train_test_split(train_ratio=0.8, min_interactions=5)

    print(f"  Users: {len(train_data)}")
    print(f"  Train: {sum(len(v) for v in train_data.values())} interactions")
    print(f"  Test:  {sum(len(v) for v in test_data.values())} interactions")

    # Get all items
    app = create_app()
    with app.app_context():
        all_items = [e.id for e in Event.query.all()]

    # === STEP 2: Evaluate Baselines ===
    print("\n[2/5] Evaluating BASELINES...")

    results = {}
    k_values = [5, 10, 20]

    # Random
    print("\n  📊 Random Baseline...")
    random_rec = RandomRecommender(all_items)
    random_rec.fit(train_data)
    random_recs = {uid: random_rec.recommend(uid, k=20) for uid in test_data.keys()}
    results['Random'] = evaluate_recommendations(random_recs, test_data, k_values)

    # Popular
    print("\n  📊 Popular Baseline...")
    popular_rec = PopularRecommender()
    popular_rec.fit(train_data)
    popular_recs = {uid: popular_rec.recommend(uid, k=20) for uid in test_data.keys()}
    results['Popular'] = evaluate_recommendations(popular_recs, test_data, k_values)

    # Item-KNN
    print("\n  📊 Item-KNN Baseline...")
    itemknn_rec = ItemKNNRecommender(k_neighbors=20)
    itemknn_rec.fit(train_data)
    itemknn_recs = {
        uid: itemknn_rec.recommend(uid, k=20, train_items=train_data.get(uid, []))
        for uid in test_data.keys()
    }
    results['Item-KNN'] = evaluate_recommendations(itemknn_recs, test_data, k_values)

    # Matrix Factorization
    print("\n  📊 Matrix Factorization Baseline...")
    mf_rec = MatrixFactorizationRecommender(latent_dim=32, iterations=10)
    mf_rec.fit(train_data)
    mf_recs = {
        uid: mf_rec.recommend(uid, k=20, train_items=train_data.get(uid, []))
        for uid in test_data.keys()
    }
    results['MF'] = evaluate_recommendations(mf_recs, test_data, k_values)

    # === STEP 3: Evaluate GNN ===
    print("\n[3/5] Evaluating YOUR GNN...")
    gnn_recs = evaluate_gnn_model(train_data, test_data, k=20)
    results['GNN (Yours)'] = evaluate_recommendations(gnn_recs, test_data, k_values)

    # === STEP 4: Print Results Table ===
    print("\n[4/5] RESULTS TABLE:")
    print("=" * 100)

    # Header
    metrics_to_show = ['P@5', 'P@10', 'R@10', 'NDCG@10', 'MRR']
    print(f"{'Model':<20}", end='')
    for metric in metrics_to_show:
        print(f"{metric:>12}", end='')
    print()
    print("-" * 100)

    # Rows
    for model_name in ['Random', 'Popular', 'Item-KNN', 'MF', 'GNN (Yours)']:
        print(f"{model_name:<20}", end='')
        for metric in metrics_to_show:
            value = results[model_name].get(metric, 0.0)
            print(f"{value:>12.4f}", end='')
        print()

    print("=" * 100)

    # === STEP 5: Analysis ===
    print("\n[5/5] ANALYSIS:")
    print("=" * 80)

    # Check if GNN wins
    gnn_p10 = results['GNN (Yours)'].get('P@10', 0)
    mf_p10 = results['MF'].get('P@10', 0)
    popular_p10 = results['Popular'].get('P@10', 0)

    if gnn_p10 > mf_p10 and gnn_p10 > popular_p10:
        print("  ✅ GNN WINS! Подобар од сите baselines!")
        improvement = ((gnn_p10 - mf_p10) / mf_p10) * 100
        print(f"  🎯 {improvement:.1f}% подобрување над MF baseline")
    elif gnn_p10 > popular_p10:
        print("  ⚠️  GNN е подобар од Popular, но не од MF")
        print("  💡 Можеби треба supervised training или подобар graph?")
    else:
        print("  ❌ GNN е понизок од Popular baseline!")
        print("  ⚠️  ПРОБЛЕМ: GNN не работи добро. Можни причини:")
        print("     - Unsupervised training не е добар за recommendation")
        print("     - Graph design не е оптимален")
        print("     - Премалку податоци")

    # Relative improvement table
    print("\n  Relative improvements vs GNN:")
    baseline_p10 = popular_p10
    if gnn_p10 > 0:
        for model in ['Random', 'Popular', 'Item-KNN', 'MF']:
            model_p10 = results[model].get('P@10', 0)
            diff = ((gnn_p10 - model_p10) / model_p10) * 100 if model_p10 > 0 else 0
            symbol = "✅" if diff > 0 else "❌"
            print(f"    {symbol} vs {model:<15s}: {diff:+6.1f}%")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"evaluation/results/eval_{timestamp}.json"

    import os
    os.makedirs("evaluation/results", exist_ok=True)

    with open(output_file, "w") as f:
        json.dump({
            'timestamp': timestamp,
            'train_users': len(train_data),
            'test_users': len(test_data),
            'results': results,
            'winner': 'GNN' if gnn_p10 > max(popular_p10, mf_p10) else 'Baseline'
        }, f, indent=2)

    print(f"\n  💾 Results saved to: {output_file}")

    print("\n" + "=" * 80)
    print("EVALUATION COMPLETE!")
    print("=" * 80)

    return results


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')

    results = run_full_evaluation(use_cached_split=True)

    print("\n" + "=" * 80)
    print("📊 NEXT STEPS:")
    print("=" * 80)
    print("1. Ако GNN не победи: Имплементирај supervised training")
    print("2. Додај user nodes во graph (heterogeneous)")
    print("3. Run ablation study (GNN only vs GNN+Traditional)")
    print("4. Try different GNN architectures (GCN, GAT)")
    print("=" * 80)
