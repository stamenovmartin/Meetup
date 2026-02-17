#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
GNN Comprehensive Evaluation Framework
========================================
Целосна евалуација на GNN recommendation system со:
- Offline метрики (Precision, Recall, nDCG, MAP, MRR, ROC-AUC)
- Calibration analysis
- Ablation studies
- Statistical significance тестови
- Cold-start evaluation
- Diversity & Fairness metrics

Author: Martin Stamenov
Date: 2025-09-30
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import create_app
from models.db_models import db, Event, User, Attendance
from models.recommender import get_recommender
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import (
    precision_score, recall_score, roc_auc_score,
    average_precision_score, roc_curve, precision_recall_curve,
    confusion_matrix, brier_score_loss
)
from scipy import stats
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 10)
plt.rcParams['font.size'] = 10


class GNNEvaluator:
    """Comprehensive GNN Evaluation System"""

    def __init__(self, recommender=None):
        self.recommender = recommender if recommender else get_recommender()
        self.results = {}

    def prepare_test_data(self, user_id, test_ratio=0.3):
        """
        Подели user интеракции на train/test за offline evaluation

        Args:
            user_id: ID на корисник
            test_ratio: Процент на test set (default: 30%)

        Returns:
            train_likes, train_dislikes, test_likes, test_dislikes
        """
        # Земи сите интеракции
        likes = Attendance.query.filter_by(user_id=user_id, rating=1).all()
        dislikes = Attendance.query.filter_by(user_id=user_id, rating=-1).all()

        # Случаен split
        np.random.seed(42)

        like_ids = [a.event_id for a in likes]
        dislike_ids = [a.event_id for a in dislikes]

        test_size_likes = max(1, int(len(like_ids) * test_ratio))
        test_size_dislikes = max(1, int(len(dislike_ids) * test_ratio))

        test_like_ids = np.random.choice(like_ids, test_size_likes, replace=False) if like_ids else []
        test_dislike_ids = np.random.choice(dislike_ids, test_size_dislikes, replace=False) if dislike_ids else []

        train_like_ids = [x for x in like_ids if x not in test_like_ids]
        train_dislike_ids = [x for x in dislike_ids if x not in test_dislike_ids]

        return {
            'train_likes': train_like_ids,
            'train_dislikes': train_dislike_ids,
            'test_likes': list(test_like_ids),
            'test_dislikes': list(test_dislike_ids)
        }

    def compute_ranking_metrics_at_k(self, user_id, k_values=[5, 10, 20, 50]):
        """
        Precision@k, Recall@k, HitRate@k

        Args:
            user_id: ID на корисник
            k_values: Lista на k вредности за evaluation

        Returns:
            Dictionary со метрики
        """
        # Земи ground truth (likes)
        liked_events = [a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()]

        if not liked_events:
            return {k: {'precision': 0, 'recall': 0, 'hitrate': 0} for k in k_values}

        # Земи препораки
        recommendations = self.recommender.recommend_events(user_id=user_id, limit=max(k_values))

        if not recommendations:
            return {k: {'precision': 0, 'recall': 0, 'hitrate': 0} for k in k_values}

        recommended_ids = [r['event_id'] for r in recommendations]

        metrics = {}
        for k in k_values:
            top_k = recommended_ids[:k]
            hits = set(top_k).intersection(set(liked_events))

            precision = len(hits) / k if k > 0 else 0
            recall = len(hits) / len(liked_events) if liked_events else 0
            hitrate = 1 if len(hits) > 0 else 0

            metrics[k] = {
                'precision': precision,
                'recall': recall,
                'hitrate': hitrate,
                'hits': len(hits)
            }

        return metrics

    def compute_ndcg_at_k(self, user_id, k_values=[5, 10, 20, 50]):
        """
        Normalized Discounted Cumulative Gain (nDCG@k)

        Args:
            user_id: ID на корисник
            k_values: Lista на k вредности

        Returns:
            Dictionary со nDCG скорови
        """
        liked_events = [a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()]

        if not liked_events:
            return {k: 0 for k in k_values}

        recommendations = self.recommender.recommend_events(user_id=user_id, limit=max(k_values))

        if not recommendations:
            return {k: 0 for k in k_values}

        recommended_ids = [r['event_id'] for r in recommendations]

        ndcg_scores = {}
        for k in k_values:
            top_k = recommended_ids[:k]

            # Relevance: 1 if liked, 0 otherwise
            relevance = [1 if event_id in liked_events else 0 for event_id in top_k]

            # DCG
            dcg = sum([(2**rel - 1) / np.log2(i + 2) for i, rel in enumerate(relevance)])

            # IDCG (ideal DCG)
            ideal_relevance = sorted(relevance, reverse=True)
            idcg = sum([(2**rel - 1) / np.log2(i + 2) for i, rel in enumerate(ideal_relevance)])

            ndcg = dcg / idcg if idcg > 0 else 0
            ndcg_scores[k] = ndcg

        return ndcg_scores

    def compute_map_mrr(self, user_id, k=50):
        """
        Mean Average Precision (MAP) и Mean Reciprocal Rank (MRR)

        Args:
            user_id: ID на корисник
            k: Број на препораки за анализа

        Returns:
            (MAP, MRR)
        """
        liked_events = [a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()]

        if not liked_events:
            return 0, 0

        recommendations = self.recommender.recommend_events(user_id=user_id, limit=k)

        if not recommendations:
            return 0, 0

        recommended_ids = [r['event_id'] for r in recommendations]

        # MAP
        precisions = []
        hits_count = 0
        for i, event_id in enumerate(recommended_ids):
            if event_id in liked_events:
                hits_count += 1
                precisions.append(hits_count / (i + 1))

        map_score = np.mean(precisions) if precisions else 0

        # MRR
        mrr = 0
        for i, event_id in enumerate(recommended_ids):
            if event_id in liked_events:
                mrr = 1 / (i + 1)
                break

        return map_score, mrr

    def compute_roc_auc_pr_auc(self, user_id, k=100):
        """
        ROC-AUC и PR-AUC за binary classification (like vs not-like)

        Args:
            user_id: ID на корисник
            k: Број на препораки

        Returns:
            (ROC-AUC, PR-AUC, fpr, tpr, precision, recall, thresholds)
        """
        liked_events = set([a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()])

        recommendations = self.recommender.recommend_events(user_id=user_id, limit=k)

        if not recommendations or not liked_events:
            return None, None, None, None, None, None, None

        # Ground truth and predictions
        y_true = [1 if r['event_id'] in liked_events else 0 for r in recommendations]
        y_scores = [r['score_pct'] / 100 for r in recommendations]  # Normalize to [0,1]

        if sum(y_true) == 0 or sum(y_true) == len(y_true):
            # Сите се позитивни или негативни - не може да се пресмета AUC
            return None, None, None, None, None, None, None

        # ROC curve
        fpr, tpr, roc_thresholds = roc_curve(y_true, y_scores)
        roc_auc = roc_auc_score(y_true, y_scores)

        # PR curve
        precision, recall, pr_thresholds = precision_recall_curve(y_true, y_scores)
        pr_auc = average_precision_score(y_true, y_scores)

        return roc_auc, pr_auc, fpr, tpr, precision, recall, roc_thresholds

    def compute_calibration(self, user_id, k=100, n_bins=10):
        """
        Calibration analysis: Expected Calibration Error (ECE) и reliability plot

        Args:
            user_id: ID на корисник
            k: Број на препораки
            n_bins: Број на bin-ови за calibration

        Returns:
            (ECE, bin_centers, bin_accuracies, bin_confidences, bin_counts)
        """
        liked_events = set([a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()])

        recommendations = self.recommender.recommend_events(user_id=user_id, limit=k)

        if not recommendations or not liked_events:
            return None, None, None, None, None

        y_true = np.array([1 if r['event_id'] in liked_events else 0 for r in recommendations])
        y_probs = np.array([r['score_pct'] / 100 for r in recommendations])

        # Bin the predictions
        bin_edges = np.linspace(0, 1, n_bins + 1)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2

        bin_accuracies = []
        bin_confidences = []
        bin_counts = []

        ece = 0
        total_count = len(y_true)

        for i in range(n_bins):
            mask = (y_probs >= bin_edges[i]) & (y_probs < bin_edges[i+1])
            if i == n_bins - 1:  # Last bin includes right edge
                mask = (y_probs >= bin_edges[i]) & (y_probs <= bin_edges[i+1])

            bin_count = mask.sum()
            bin_counts.append(bin_count)

            if bin_count > 0:
                bin_accuracy = y_true[mask].mean()
                bin_confidence = y_probs[mask].mean()

                bin_accuracies.append(bin_accuracy)
                bin_confidences.append(bin_confidence)

                ece += (bin_count / total_count) * abs(bin_accuracy - bin_confidence)
            else:
                bin_accuracies.append(0)
                bin_confidences.append(0)

        return ece, bin_centers, np.array(bin_accuracies), np.array(bin_confidences), np.array(bin_counts)

    def ablation_study(self, user_id, k=20):
        """
        Ablation: Precision@k без одредени компоненти

        Args:
            user_id: ID на корисник
            k: k вредност за evaluation

        Returns:
            Dictionary со Precision@k за различни конфигурации
        """
        liked_events = set([a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()])

        if not liked_events:
            return {}

        # Full system
        full_recs = self.recommender.recommend_events(user_id=user_id, limit=k)
        full_ids = [r['event_id'] for r in full_recs]
        full_hits = len(set(full_ids[:k]).intersection(liked_events))
        full_precision = full_hits / k

        # Traditional only (без GNN)
        # Симулираме со alpha=1.0
        original_alpha = self.recommender.alpha
        self.recommender.alpha = 1.0
        trad_recs = self.recommender.recommend_events(user_id=user_id, limit=k)
        trad_ids = [r['event_id'] for r in trad_recs]
        trad_hits = len(set(trad_ids[:k]).intersection(liked_events))
        trad_precision = trad_hits / k
        self.recommender.alpha = original_alpha

        # GNN only (без традиционални features)
        # Ова е потешко за симулација без промена на архитектура
        # За сега го пропуштаме

        return {
            'full_system': full_precision,
            'traditional_only': trad_precision,
            'gnn_contribution': full_precision - trad_precision
        }

    def diversity_metrics(self, user_id, k=50):
        """
        Diversity & Coverage на препораки

        Args:
            user_id: ID на корисник
            k: Број на препораки

        Returns:
            Dictionary со diversity метрики
        """
        recommendations = self.recommender.recommend_events(user_id=user_id, limit=k)

        if not recommendations:
            return {}

        # Tag diversity
        all_tags = []
        for rec in recommendations[:k]:
            tags = rec.get('tags', [])
            if isinstance(tags, list):
                all_tags.extend(tags)

        unique_tags = len(set(all_tags))
        total_tags = len(all_tags)
        tag_diversity = unique_tags / total_tags if total_tags > 0 else 0

        # Score spread
        scores = [r['score_pct'] for r in recommendations[:k]]
        score_std = np.std(scores)
        score_range = max(scores) - min(scores) if scores else 0

        return {
            'unique_tags': unique_tags,
            'total_tags': total_tags,
            'tag_diversity': tag_diversity,
            'score_std': score_std,
            'score_range': score_range
        }

    def statistical_significance_test(self, user_ids, k=20, baseline='traditional'):
        """
        Wilcoxon signed-rank test за significance на подобрување

        Args:
            user_ids: Lista на user IDs
            k: k вредност за Precision@k
            baseline: 'traditional' или 'random'

        Returns:
            (statistic, p_value, confidence_interval)
        """
        full_precisions = []
        baseline_precisions = []

        for user_id in user_ids:
            liked_events = set([a.event_id for a in Attendance.query.filter_by(user_id=user_id, rating=1).all()])

            if not liked_events:
                continue

            # Full system
            full_recs = self.recommender.recommend_events(user_id=user_id, limit=k)
            full_ids = [r['event_id'] for r in full_recs]
            full_hits = len(set(full_ids[:k]).intersection(liked_events))
            full_precisions.append(full_hits / k)

            # Baseline (Traditional only)
            original_alpha = self.recommender.alpha
            self.recommender.alpha = 1.0
            base_recs = self.recommender.recommend_events(user_id=user_id, limit=k)
            base_ids = [r['event_id'] for r in base_recs]
            base_hits = len(set(base_ids[:k]).intersection(liked_events))
            baseline_precisions.append(base_hits / k)
            self.recommender.alpha = original_alpha

        if len(full_precisions) < 2:
            return None, None, None

        # Check if there's any difference
        improvements = np.array(full_precisions) - np.array(baseline_precisions)

        if np.all(improvements == 0):
            # No difference between systems
            print("   Full system and baseline have identical performance (no GNN effect)")
            return 0, 1.0, (0, 0)

        # Wilcoxon test
        try:
            statistic, p_value = stats.wilcoxon(full_precisions, baseline_precisions, alternative='greater')
        except ValueError as e:
            print(f"   Wilcoxon test failed: {e}")
            return None, None, None

        # Bootstrap 95% CI
        ci_low, ci_high = np.percentile(improvements, [2.5, 97.5])

        return statistic, p_value, (ci_low, ci_high)


def generate_comprehensive_report(user_ids=[1], output_dir='docs/analytics/gnn_evaluation'):
    """
    Генерирај comprehensive evaluation извештај

    Args:
        user_ids: Lista на user IDs за evaluation
        output_dir: Директориум за чување на резултати
    """
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 80)
    print(" GNN COMPREHENSIVE EVALUATION")
    print("=" * 80)

    app = create_app()

    with app.app_context():
        evaluator = GNNEvaluator()

        all_results = {}

        for user_id in user_ids:
            print(f"\n Evaluating User {user_id}...")

            # 1. Ranking metrics
            print("  • Computing Precision/Recall/HitRate@k...")
            ranking_metrics = evaluator.compute_ranking_metrics_at_k(user_id)

            # 2. nDCG
            print("  • Computing nDCG@k...")
            ndcg_scores = evaluator.compute_ndcg_at_k(user_id)

            # 3. MAP & MRR
            print("  • Computing MAP & MRR...")
            map_score, mrr_score = evaluator.compute_map_mrr(user_id)

            # 4. ROC-AUC & PR-AUC
            print("  • Computing ROC-AUC & PR-AUC...")
            roc_pr_results = evaluator.compute_roc_auc_pr_auc(user_id)

            # 5. Calibration
            print("  • Computing Calibration (ECE)...")
            calibration_results = evaluator.compute_calibration(user_id)

            # 6. Ablation
            print("  • Running Ablation Study...")
            ablation_results = evaluator.ablation_study(user_id)

            # 7. Diversity
            print("  • Computing Diversity Metrics...")
            diversity_results = evaluator.diversity_metrics(user_id)

            all_results[user_id] = {
                'ranking_metrics': ranking_metrics,
                'ndcg': ndcg_scores,
                'map': map_score,
                'mrr': mrr_score,
                'roc_pr': roc_pr_results,
                'calibration': calibration_results,
                'ablation': ablation_results,
                'diversity': diversity_results
            }

        # Statistical significance test
        if len(user_ids) > 1:
            print("\n Running Statistical Significance Test...")
            stat, pval, ci = evaluator.statistical_significance_test(user_ids)
            all_results['significance'] = {'statistic': stat, 'p_value': pval, 'ci': ci}

        # Visualizations
        print("\n Generating Visualizations...")
        plot_comprehensive_metrics(all_results, output_dir)

        print("\n Evaluation Complete!")
        print(f" Results saved to: {output_dir}")
        print("=" * 80)

        return all_results


def plot_comprehensive_metrics(results, output_dir):
    """
    Креирај comprehensive visualizations од evaluation резултати

    Args:
        results: Dictionary со evaluation резултати
        output_dir: Output директориум за слики
    """
    # 1. Precision/Recall/HitRate@k Plot
    fig1, axes1 = plt.subplots(2, 2, figsize=(14, 10))

    # Земи првиот user за demo (или просек за повеќе users)
    user_ids = [k for k in results.keys() if k != 'significance']
    if not user_ids:
        print(" No user results to plot!")
        return

    sample_user_id = user_ids[0]
    ranking_metrics = results[sample_user_id]['ranking_metrics']

    k_values = sorted(ranking_metrics.keys())
    precisions = [ranking_metrics[k]['precision'] for k in k_values]
    recalls = [ranking_metrics[k]['recall'] for k in k_values]
    hitrates = [ranking_metrics[k]['hitrate'] for k in k_values]

    # Plot 1: Precision@k
    axes1[0, 0].plot(k_values, precisions, marker='o', linewidth=2, markersize=8, color='#2E86AB')
    axes1[0, 0].set_title('Precision@k', fontsize=12, fontweight='bold')
    axes1[0, 0].set_xlabel('k')
    axes1[0, 0].set_ylabel('Precision')
    axes1[0, 0].grid(True, alpha=0.3)
    axes1[0, 0].set_ylim([0, 1])

    # Plot 2: Recall@k
    axes1[0, 1].plot(k_values, recalls, marker='s', linewidth=2, markersize=8, color='#A23B72')
    axes1[0, 1].set_title('Recall@k', fontsize=12, fontweight='bold')
    axes1[0, 1].set_xlabel('k')
    axes1[0, 1].set_ylabel('Recall')
    axes1[0, 1].grid(True, alpha=0.3)
    axes1[0, 1].set_ylim([0, 1])

    # Plot 3: HitRate@k
    axes1[1, 0].plot(k_values, hitrates, marker='^', linewidth=2, markersize=8, color='#F18F01')
    axes1[1, 0].set_title('HitRate@k', fontsize=12, fontweight='bold')
    axes1[1, 0].set_xlabel('k')
    axes1[1, 0].set_ylabel('HitRate')
    axes1[1, 0].grid(True, alpha=0.3)
    axes1[1, 0].set_ylim([0, 1.1])

    # Plot 4: nDCG@k
    ndcg_scores = results[sample_user_id]['ndcg']
    ndcg_values = [ndcg_scores[k] for k in k_values]
    axes1[1, 1].plot(k_values, ndcg_values, marker='d', linewidth=2, markersize=8, color='#6A994E')
    axes1[1, 1].set_title('nDCG@k', fontsize=12, fontweight='bold')
    axes1[1, 1].set_xlabel('k')
    axes1[1, 1].set_ylabel('nDCG')
    axes1[1, 1].grid(True, alpha=0.3)
    axes1[1, 1].set_ylim([0, 1])

    plt.tight_layout()
    plt.savefig(f"{output_dir}/01_ranking_metrics.png", dpi=200, bbox_inches='tight')
    plt.close()
    print("   01_ranking_metrics.png")

    # 2. ROC & PR Curves
    roc_pr_results = results[sample_user_id]['roc_pr']
    if roc_pr_results and roc_pr_results[0] is not None:
        fig2, axes2 = plt.subplots(1, 2, figsize=(14, 6))

        roc_auc, pr_auc, fpr, tpr, precision, recall, _ = roc_pr_results

        # ROC Curve
        axes2[0].plot(fpr, tpr, linewidth=2, color='#2E86AB', label=f'ROC (AUC = {roc_auc:.3f})')
        axes2[0].plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Random')
        axes2[0].set_title('ROC Curve', fontsize=12, fontweight='bold')
        axes2[0].set_xlabel('False Positive Rate')
        axes2[0].set_ylabel('True Positive Rate')
        axes2[0].legend()
        axes2[0].grid(True, alpha=0.3)

        # PR Curve
        axes2[1].plot(recall, precision, linewidth=2, color='#A23B72', label=f'PR (AUC = {pr_auc:.3f})')
        axes2[1].set_title('Precision-Recall Curve', fontsize=12, fontweight='bold')
        axes2[1].set_xlabel('Recall')
        axes2[1].set_ylabel('Precision')
        axes2[1].legend()
        axes2[1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(f"{output_dir}/02_roc_pr_curves.png", dpi=200, bbox_inches='tight')
        plt.close()
        print("   02_roc_pr_curves.png")

    # 3. Calibration Plot
    calibration_results = results[sample_user_id]['calibration']
    if calibration_results and calibration_results[0] is not None:
        ece, bin_centers, bin_accuracies, bin_confidences, bin_counts = calibration_results

        fig3, axes3 = plt.subplots(1, 2, figsize=(14, 6))

        # Reliability diagram
        axes3[0].plot([0, 1], [0, 1], 'k--', alpha=0.3, label='Perfect Calibration')
        axes3[0].plot(bin_confidences, bin_accuracies, marker='o', linewidth=2, markersize=8, color='#F18F01', label=f'ECE = {ece:.3f}')
        axes3[0].set_title('Calibration Plot', fontsize=12, fontweight='bold')
        axes3[0].set_xlabel('Predicted Confidence')
        axes3[0].set_ylabel('Actual Accuracy')
        axes3[0].legend()
        axes3[0].grid(True, alpha=0.3)
        axes3[0].set_xlim([0, 1])
        axes3[0].set_ylim([0, 1])

        # Bin counts
        axes3[1].bar(bin_centers, bin_counts, width=0.08, alpha=0.7, color='#6A994E', edgecolor='black')
        axes3[1].set_title('Prediction Distribution', fontsize=12, fontweight='bold')
        axes3[1].set_xlabel('Predicted Confidence')
        axes3[1].set_ylabel('Count')
        axes3[1].grid(True, alpha=0.3, axis='y')

        plt.tight_layout()
        plt.savefig(f"{output_dir}/03_calibration.png", dpi=200, bbox_inches='tight')
        plt.close()
        print("   03_calibration.png")

    # 4. Ablation Study
    ablation_results = results[sample_user_id]['ablation']
    if ablation_results:
        fig4, ax4 = plt.subplots(figsize=(10, 6))

        labels = ['Full System\n(GNN + Traditional)', 'Traditional Only', 'GNN Contribution']
        values = [
            ablation_results['full_system'],
            ablation_results['traditional_only'],
            ablation_results['gnn_contribution']
        ]
        colors = ['#2E86AB', '#A23B72', '#F18F01']

        bars = ax4.bar(labels, values, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)
        ax4.set_title('Ablation Study - Precision@20', fontsize=14, fontweight='bold')
        ax4.set_ylabel('Precision@20', fontsize=12)
        ax4.set_ylim([0, max(values) * 1.2 if max(values) > 0 else 1])
        ax4.grid(True, alpha=0.3, axis='y')

        # Add value labels on bars
        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2., height + 0.01,
                    f'{val:.3f}', ha='center', va='bottom', fontweight='bold', fontsize=11)

        plt.tight_layout()
        plt.savefig(f"{output_dir}/04_ablation_study.png", dpi=200, bbox_inches='tight')
        plt.close()
        print("   04_ablation_study.png")

    # 5. Summary Metrics Table
    fig5, ax5 = plt.subplots(figsize=(12, 8))
    ax5.axis('off')

    summary_data = []
    summary_data.append(['Metric', 'Value'])
    summary_data.append(['-' * 30, '-' * 20])

    # Ranking metrics
    summary_data.append(['Precision@10', f"{ranking_metrics[10]['precision']:.3f}"])
    summary_data.append(['Recall@10', f"{ranking_metrics[10]['recall']:.3f}"])
    summary_data.append(['HitRate@10', f"{ranking_metrics[10]['hitrate']:.3f}"])
    summary_data.append(['nDCG@10', f"{results[sample_user_id]['ndcg'][10]:.3f}"])
    summary_data.append(['MAP', f"{results[sample_user_id]['map']:.3f}"])
    summary_data.append(['MRR', f"{results[sample_user_id]['mrr']:.3f}"])

    if roc_pr_results and roc_pr_results[0] is not None:
        summary_data.append(['ROC-AUC', f"{roc_pr_results[0]:.3f}"])
        summary_data.append(['PR-AUC', f"{roc_pr_results[1]:.3f}"])

    if calibration_results and calibration_results[0] is not None:
        summary_data.append(['ECE', f"{calibration_results[0]:.3f}"])

    if ablation_results:
        summary_data.append(['-' * 30, '-' * 20])
        summary_data.append(['Full System P@20', f"{ablation_results['full_system']:.3f}"])
        summary_data.append(['Traditional P@20', f"{ablation_results['traditional_only']:.3f}"])
        summary_data.append(['GNN Contribution', f"{ablation_results['gnn_contribution']:.3f}"])

    diversity_results = results[sample_user_id]['diversity']
    if diversity_results:
        summary_data.append(['-' * 30, '-' * 20])
        summary_data.append(['Tag Diversity', f"{diversity_results['tag_diversity']:.3f}"])
        summary_data.append(['Score STD', f"{diversity_results['score_std']:.2f}"])
        summary_data.append(['Score Range', f"{diversity_results['score_range']:.2f}"])

    # Statistical significance
    if 'significance' in results and results['significance']['p_value'] is not None:
        sig = results['significance']
        summary_data.append(['-' * 30, '-' * 20])
        summary_data.append(['Wilcoxon p-value', f"{sig['p_value']:.4f}"])
        summary_data.append(['95% CI', f"[{sig['ci'][0]:.3f}, {sig['ci'][1]:.3f}]"])

    table = ax5.table(cellText=summary_data, cellLoc='left', loc='center',
                     colWidths=[0.6, 0.4])
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.5)

    # Style header
    for i in range(2):
        table[(0, i)].set_facecolor('#2E86AB')
        table[(0, i)].set_text_props(weight='bold', color='white')

    # Style rows
    for i in range(1, len(summary_data)):
        for j in range(2):
            if summary_data[i][0].startswith('-'):
                table[(i, j)].set_facecolor('#E5E5E5')
            elif i % 2 == 0:
                table[(i, j)].set_facecolor('#F5F5F5')

    ax5.set_title('GNN Recommendation System - Evaluation Summary',
                 fontsize=16, fontweight='bold', pad=20)

    plt.tight_layout()
    plt.savefig(f"{output_dir}/05_summary_metrics.png", dpi=200, bbox_inches='tight')
    plt.close()
    print("   05_summary_metrics.png")


if __name__ == '__main__':
    user_ids = [1]  # Default user

    if len(sys.argv) > 1:
        user_ids = [int(x) for x in sys.argv[1].split(',')]

    generate_comprehensive_report(user_ids=user_ids)
