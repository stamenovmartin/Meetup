#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Compare Different Alpha Values (Traditional vs GNN Balance)
============================================================
Тестира 3 конфигурации:
1. Traditional Only (alpha=1.0)
2. Balanced (alpha=0.5)
3. GNN-Heavy (alpha=0.2)

За да видиме кој е најдобар!
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import create_app
from models.db_models import db, Attendance
from models.recommender import get_recommender
import numpy as np


def evaluate_alpha(alpha_value, user_ids, k=20):
    """
    Евалуирај Precision@k за даден alpha

    Args:
        alpha_value: Тежина за traditional (0-1)
        user_ids: Lista на users за test
        k: Top-k препораки

    Returns:
        Просечен Precision@k
    """
    recommender = get_recommender()
    original_alpha = recommender.alpha
    recommender.alpha = alpha_value

    precisions = []

    for user_id in user_ids:
        # Земи ground truth likes
        liked_events = set([a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()])

        if not liked_events:
            continue

        # Земи препораки
        recommendations = recommender.recommend_events(user_id=user_id, limit=k)

        if not recommendations:
            continue

        # Пресметај precision
        recommended_ids = [r['event_id'] for r in recommendations[:k]]
        hits = len(set(recommended_ids).intersection(liked_events))
        precision = hits / k

        precisions.append(precision)

    # Врати го original alpha
    recommender.alpha = original_alpha

    return np.mean(precisions) if precisions else 0.0


def compare_configurations():
    """Спореди различни alpha конфигурации"""

    print("=" * 80)
    print("🔬 ALPHA COMPARISON - Traditional vs GNN Balance")
    print("=" * 80)

    app = create_app()

    with app.app_context():
        # Test users (synthetic users со многу интеракции)
        user_ids = [3, 4, 5, 6, 7, 8, 9, 10]

        print(f"\n📊 Testing on {len(user_ids)} users")
        print(f"📏 Metric: Precision@20\n")

        # 3 конфигурации
        configs = [
            {"name": "Traditional Only", "alpha": 1.0, "gnn_weight": "0%"},
            {"name": "Balanced Hybrid", "alpha": 0.5, "gnn_weight": "50%"},
            {"name": "GNN-Heavy", "alpha": 0.2, "gnn_weight": "80%"},
            {"name": "GNN-Dominant", "alpha": 0.1, "gnn_weight": "90%"},
        ]

        results = []

        for config in configs:
            print(f"⏳ Testing {config['name']} (Traditional {int(config['alpha']*100)}%, GNN {config['gnn_weight']})...")

            precision = evaluate_alpha(config['alpha'], user_ids, k=20)

            results.append({
                'name': config['name'],
                'alpha': config['alpha'],
                'precision': precision
            })

            print(f"   ✅ Precision@20: {precision:.4f}\n")

        # Резултати
        print("=" * 80)
        print("📊 RESULTS SUMMARY")
        print("=" * 80)
        print(f"{'Configuration':<25} {'Alpha':<10} {'Precision@20':<15} {'Improvement':<15}")
        print("-" * 80)

        baseline_precision = results[0]['precision']

        for r in results:
            improvement = ((r['precision'] - baseline_precision) / baseline_precision * 100) if baseline_precision > 0 else 0
            improvement_str = f"+{improvement:.1f}%" if improvement > 0 else f"{improvement:.1f}%"

            print(f"{r['name']:<25} {r['alpha']:<10.1f} {r['precision']:<15.4f} {improvement_str:<15}")

        print("=" * 80)

        # Најди best
        best = max(results, key=lambda x: x['precision'])

        print(f"\n🏆 BEST CONFIGURATION: {best['name']}")
        print(f"   • Alpha: {best['alpha']}")
        print(f"   • Precision@20: {best['precision']:.4f}")
        print(f"   • Traditional: {int(best['alpha']*100)}%")
        print(f"   • GNN: {int((1-best['alpha'])*100)}%")

        print("\n💡 Препорака:")
        if best['alpha'] >= 0.8:
            print("   Traditional features се покорисни од GNN за овој dataset.")
            print("   Причини: Tag matching е многу силен, GNN треба подобар training.")
        elif best['alpha'] >= 0.4:
            print("   Balanced hybrid е најдобар - комбинира precision од tags со diversity од GNN.")
        else:
            print("   GNN е многу корисен! Embeddings успешно го фаќаат event similarity.")

        print("\n" + "=" * 80)

        return results


if __name__ == '__main__':
    compare_configurations()
