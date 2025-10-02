#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
GNN Analytics & Visualizations Script
=======================================
Генерира детални аналитики и визуелизации за GNN recommendation системот.

Визуелизации:
1. Дистрибуција на скорови (0-100%)
2. GNN vs Traditional scoring споредба
3. Tag distribution во препораки
4. Embedding visualization (t-SNE/UMAP)
5. Recommendation accuracy метрики
6. User behavior patterns
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import create_app
from models.db_models import db, Event, User, Attendance
from models.recommender import get_recommender
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pandas as pd
from collections import Counter
from datetime import datetime

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 8)
plt.rcParams['font.size'] = 10


def analyze_score_distribution(user_id: int = 1):
    """Анализа на дистрибуција на препораки скорови"""
    print("\n📊 АНАЛIZA 1: Дистрибуција на препораки скорови\n")

    recommender = get_recommender()
    recommendations = recommender.recommend_events(user_id=user_id, limit=10000)

    if not recommendations:
        print("❌ Нема препораки за анализа!")
        return

    # Извлечи податоци
    scores = [r['score_pct'] for r in recommendations]
    raw_scores = [r['raw_score'] for r in recommendations]

    # Статистика
    print(f"Вкупно препораки: {len(scores)}")
    print(f"Score range: {min(scores):.1f}% - {max(scores):.1f}%")
    print(f"Mean score: {np.mean(scores):.1f}%")
    print(f"Median score: {np.median(scores):.1f}%")
    print(f"Std deviation: {np.std(scores):.1f}%")

    # Визуелизација 1: Histogram
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('GNN Recommendation Score Distribution Analysis', fontsize=16, fontweight='bold')

    # 1.1 Histogram на score_pct
    axes[0, 0].hist(scores, bins=50, color='skyblue', edgecolor='black', alpha=0.7)
    axes[0, 0].set_xlabel('Score Percentage (%)')
    axes[0, 0].set_ylabel('Number of Events')
    axes[0, 0].set_title('Distribution of Recommendation Scores (0-100%)')
    axes[0, 0].axvline(np.mean(scores), color='red', linestyle='--', label=f'Mean: {np.mean(scores):.1f}%')
    axes[0, 0].legend()

    # 1.2 Cumulative distribution
    sorted_scores = sorted(scores)
    cumulative = np.arange(1, len(sorted_scores) + 1) / len(sorted_scores) * 100
    axes[0, 1].plot(sorted_scores, cumulative, color='green', linewidth=2)
    axes[0, 1].set_xlabel('Score Percentage (%)')
    axes[0, 1].set_ylabel('Cumulative Percentage of Events')
    axes[0, 1].set_title('Cumulative Distribution')
    axes[0, 1].grid(True, alpha=0.3)

    # 1.3 Box plot
    axes[1, 0].boxplot([scores], vert=False, patch_artist=True,
                        boxprops=dict(facecolor='lightblue', alpha=0.7))
    axes[1, 0].set_xlabel('Score Percentage (%)')
    axes[1, 0].set_title('Score Distribution (Box Plot)')

    # 1.4 Raw score vs Normalized score
    axes[1, 1].scatter(raw_scores, scores, alpha=0.5, s=10, color='purple')
    axes[1, 1].set_xlabel('Raw Combined Score')
    axes[1, 1].set_ylabel('Normalized Score (%)')
    axes[1, 1].set_title('Raw Score vs Percentile Score')
    axes[1, 1].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig('docs/analytics/01_score_distribution.png', dpi=300, bbox_inches='tight')
    print("✅ Saved: docs/analytics/01_score_distribution.png")
    plt.close()


def compare_gnn_vs_traditional(user_id: int = 1, sample_size: int = 100):
    """Споредба помеѓу GNN и традиционален scoring"""
    print("\n📊 АНАЛИЗА 2: GNN vs Traditional Scoring\n")

    recommender = get_recommender()
    events = Event.query.limit(sample_size).all()

    trad_scores = []
    gnn_scores = []
    combined_scores = []
    event_titles = []

    for event in events:
        trad = recommender.traditional_score(user_id, event)
        gnn = recommender.gnn_similarity_score(user_id, event)
        combined = recommender.combined_score(user_id, event)

        trad_scores.append(trad)
        gnn_scores.append(gnn)
        combined_scores.append(combined)
        event_titles.append(event.title[:30])

    # Визуелизација
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('GNN vs Traditional Scoring Comparison', fontsize=16, fontweight='bold')

    # 2.1 Scatter: Traditional vs GNN
    axes[0, 0].scatter(trad_scores, gnn_scores, alpha=0.6, c=combined_scores, cmap='viridis', s=50)
    axes[0, 0].set_xlabel('Traditional Score')
    axes[0, 0].set_ylabel('GNN Similarity Score')
    axes[0, 0].set_title('Traditional vs GNN Scores (color = combined score)')
    axes[0, 0].axhline(0, color='red', linestyle='--', alpha=0.3)
    axes[0, 0].axvline(0, color='red', linestyle='--', alpha=0.3)

    # 2.2 Distribution comparison
    axes[0, 1].hist(trad_scores, bins=30, alpha=0.5, label='Traditional', color='blue', edgecolor='black')
    axes[0, 1].hist([g*10 for g in gnn_scores], bins=30, alpha=0.5, label='GNN (×10)', color='orange', edgecolor='black')
    axes[0, 1].set_xlabel('Score Value')
    axes[0, 1].set_ylabel('Frequency')
    axes[0, 1].set_title('Score Distribution Comparison')
    axes[0, 1].legend()

    # 2.3 Combined score components
    sample_indices = np.random.choice(len(events), min(20, len(events)), replace=False)
    sample_trad = [trad_scores[i] for i in sample_indices]
    sample_gnn = [gnn_scores[i] * 10 for i in sample_indices]  # Scale for visibility
    sample_titles = [event_titles[i] for i in sample_indices]

    x = np.arange(len(sample_indices))
    width = 0.35

    axes[1, 0].bar(x - width/2, sample_trad, width, label='Traditional', alpha=0.8, color='steelblue')
    axes[1, 0].bar(x + width/2, sample_gnn, width, label='GNN (×10)', alpha=0.8, color='coral')
    axes[1, 0].set_xlabel('Event Sample')
    axes[1, 0].set_ylabel('Score')
    axes[1, 0].set_title('Score Components (Sample of 20 Events)')
    axes[1, 0].legend()
    axes[1, 0].set_xticks(x)
    axes[1, 0].set_xticklabels(range(len(sample_indices)), rotation=45)

    # 2.4 Correlation analysis
    from scipy.stats import pearsonr
    corr, pval = pearsonr(trad_scores, gnn_scores)

    axes[1, 1].text(0.5, 0.6, f'Pearson Correlation: {corr:.3f}',
                    ha='center', va='center', fontsize=14, transform=axes[1, 1].transAxes)
    axes[1, 1].text(0.5, 0.5, f'P-value: {pval:.4f}',
                    ha='center', va='center', fontsize=12, transform=axes[1, 1].transAxes)
    axes[1, 1].text(0.5, 0.3,
                    f'Mean Traditional: {np.mean(trad_scores):.2f}\nMean GNN: {np.mean(gnn_scores):.3f}',
                    ha='center', va='center', fontsize=11, transform=axes[1, 1].transAxes,
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    axes[1, 1].set_title('Statistical Summary')
    axes[1, 1].axis('off')

    plt.tight_layout()
    plt.savefig('docs/analytics/02_gnn_vs_traditional.png', dpi=300, bbox_inches='tight')
    print("✅ Saved: docs/analytics/02_gnn_vs_traditional.png")
    plt.close()


def analyze_tag_distribution(user_id: int = 1):
    """Анализа на тагови во top препораки"""
    print("\n📊 АНАЛИЗА 3: Tag Distribution in Recommendations\n")

    recommender = get_recommender()
    recommendations = recommender.recommend_events(user_id=user_id, limit=100)

    # Собери тагови
    all_tags = []
    for rec in recommendations:
        tags = rec.get('tags', [])
        if isinstance(tags, list):
            all_tags.extend([t.strip() for t in tags if t.strip()])

    tag_counts = Counter(all_tags)
    top_tags = tag_counts.most_common(15)

    if not top_tags:
        print("❌ Нема тагови за анализа!")
        return

    print(f"Најчести тагови во топ 100 препораки:")
    for tag, count in top_tags:
        print(f"  • {tag}: {count} events")

    # Визуелизација
    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('Tag Distribution in Top 100 Recommendations', fontsize=16, fontweight='bold')

    # 3.1 Bar chart
    tags, counts = zip(*top_tags)
    colors = plt.cm.viridis(np.linspace(0, 1, len(tags)))

    axes[0].barh(range(len(tags)), counts, color=colors, alpha=0.8, edgecolor='black')
    axes[0].set_yticks(range(len(tags)))
    axes[0].set_yticklabels(tags)
    axes[0].set_xlabel('Number of Events')
    axes[0].set_title('Top 15 Tags in Recommendations')
    axes[0].invert_yaxis()

    # 3.2 Pie chart (top 10)
    top10_tags, top10_counts = zip(*top_tags[:10])
    axes[1].pie(top10_counts, labels=top10_tags, autopct='%1.1f%%', startangle=90, colors=colors[:10])
    axes[1].set_title('Top 10 Tags Distribution')

    plt.tight_layout()
    plt.savefig('docs/analytics/03_tag_distribution.png', dpi=300, bbox_inches='tight')
    print("✅ Saved: docs/analytics/03_tag_distribution.png")
    plt.close()


def analyze_user_behavior(user_id: int = 1):
    """Анализа на user интеракции (likes/dislikes)"""
    print("\n📊 АНАЛИЗА 4: User Behavior Patterns\n")

    # Земи user attendance
    likes = Attendance.query.filter_by(user_id=user_id, rating=1).all()
    dislikes = Attendance.query.filter_by(user_id=user_id, rating=-1).all()

    print(f"User {user_id} интеракции:")
    print(f"  • Likes: {len(likes)}")
    print(f"  • Dislikes: {len(dislikes)}")

    if not likes and not dislikes:
        print("❌ Нема user интеракции за анализа!")
        return

    # Тагови од likes/dislikes
    liked_tags = []
    disliked_tags = []

    for att in likes:
        event = Event.query.get(att.event_id)
        if event and event.tags:
            liked_tags.extend([t.strip() for t in event.tags.split(',') if t.strip()])

    for att in dislikes:
        event = Event.query.get(att.event_id)
        if event and event.tags:
            disliked_tags.extend([t.strip() for t in event.tags.split(',') if t.strip()])

    liked_counts = Counter(liked_tags).most_common(10)
    disliked_counts = Counter(disliked_tags).most_common(10)

    # Визуелизација
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle(f'User {user_id} Behavior Analysis', fontsize=16, fontweight='bold')

    # 4.1 Like/Dislike counts
    axes[0, 0].bar(['Likes', 'Dislikes'], [len(likes), len(dislikes)],
                   color=['green', 'red'], alpha=0.7, edgecolor='black')
    axes[0, 0].set_ylabel('Count')
    axes[0, 0].set_title('Total Interactions')

    # 4.2 Liked tags
    if liked_counts:
        tags, counts = zip(*liked_counts)
        axes[0, 1].barh(range(len(tags)), counts, color='green', alpha=0.7, edgecolor='black')
        axes[0, 1].set_yticks(range(len(tags)))
        axes[0, 1].set_yticklabels(tags)
        axes[0, 1].set_xlabel('Count')
        axes[0, 1].set_title('Top 10 Liked Tags')
        axes[0, 1].invert_yaxis()

    # 4.3 Disliked tags
    if disliked_counts:
        tags, counts = zip(*disliked_counts)
        axes[1, 0].barh(range(len(tags)), counts, color='red', alpha=0.7, edgecolor='black')
        axes[1, 0].set_yticks(range(len(tags)))
        axes[1, 0].set_yticklabels(tags)
        axes[1, 0].set_xlabel('Count')
        axes[1, 0].set_title('Top 10 Disliked Tags')
        axes[1, 0].invert_yaxis()

    # 4.4 Preference ratio
    all_unique_tags = set(liked_tags + disliked_tags)
    if all_unique_tags:
        tag_preferences = []
        for tag in list(all_unique_tags)[:15]:
            like_count = liked_tags.count(tag)
            dislike_count = disliked_tags.count(tag)
            if like_count + dislike_count > 0:
                ratio = (like_count - dislike_count) / (like_count + dislike_count)
                tag_preferences.append((tag, ratio, like_count + dislike_count))

        tag_preferences.sort(key=lambda x: abs(x[1]), reverse=True)

        if tag_preferences:
            tags, ratios, _ = zip(*tag_preferences[:15])
            colors_pref = ['green' if r > 0 else 'red' for r in ratios]

            axes[1, 1].barh(range(len(tags)), ratios, color=colors_pref, alpha=0.7, edgecolor='black')
            axes[1, 1].set_yticks(range(len(tags)))
            axes[1, 1].set_yticklabels(tags)
            axes[1, 1].set_xlabel('Preference Ratio (-1 to 1)')
            axes[1, 1].set_title('Tag Preferences (Green=Liked, Red=Disliked)')
            axes[1, 1].axvline(0, color='black', linestyle='--', linewidth=1)
            axes[1, 1].invert_yaxis()

    plt.tight_layout()
    plt.savefig('docs/analytics/04_user_behavior.png', dpi=300, bbox_inches='tight')
    print("✅ Saved: docs/analytics/04_user_behavior.png")
    plt.close()


def analyze_recommendation_quality(user_id: int = 1):
    """Анализа на квалитет на препораки"""
    print("\n📊 АНАЛИЗА 5: Recommendation Quality Metrics\n")

    recommender = get_recommender()

    # Земи топ 50 препораки
    recommendations = recommender.recommend_events(user_id=user_id, limit=50)

    if not recommendations:
        print("❌ Нема препораки!")
        return

    # Анализа на скорови по декили
    scores = [r['score_pct'] for r in recommendations]

    deciles = {
        'Top 10': scores[:10],
        'Top 11-20': scores[10:20],
        'Top 21-30': scores[20:30],
        'Top 31-40': scores[30:40],
        'Top 41-50': scores[40:50]
    }

    # Визуелизација
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Recommendation Quality Analysis', fontsize=16, fontweight='bold')

    # 5.1 Box plot по декили
    axes[0, 0].boxplot([deciles[k] for k in deciles.keys()],
                        labels=list(deciles.keys()),
                        patch_artist=True,
                        boxprops=dict(facecolor='lightblue', alpha=0.7))
    axes[0, 0].set_ylabel('Score (%)')
    axes[0, 0].set_title('Score Distribution by Rank Decile')
    axes[0, 0].tick_params(axis='x', rotation=45)

    # 5.2 Score decay curve
    axes[0, 1].plot(range(1, len(scores) + 1), scores, marker='o', linewidth=2, markersize=4, color='purple')
    axes[0, 1].set_xlabel('Rank')
    axes[0, 1].set_ylabel('Score (%)')
    axes[0, 1].set_title('Score Decay Curve (Top 50)')
    axes[0, 1].grid(True, alpha=0.3)

    # 5.3 Mean scores по декили
    mean_scores = [np.mean(deciles[k]) for k in deciles.keys()]
    axes[1, 0].bar(range(len(mean_scores)), mean_scores, color='coral', alpha=0.7, edgecolor='black')
    axes[1, 0].set_xticks(range(len(mean_scores)))
    axes[1, 0].set_xticklabels(list(deciles.keys()), rotation=45, ha='right')
    axes[1, 0].set_ylabel('Mean Score (%)')
    axes[1, 0].set_title('Average Score by Decile')

    # 5.4 Quality metrics summary
    quality_text = f"""
    RECOMMENDATION QUALITY METRICS:

    Total Recommendations: {len(recommendations)}

    Score Range: {min(scores):.1f}% - {max(scores):.1f}%
    Mean Score: {np.mean(scores):.1f}%
    Median Score: {np.median(scores):.1f}%
    Std Dev: {np.std(scores):.1f}%

    Top 10 Mean: {np.mean(scores[:10]):.1f}%
    Top 20 Mean: {np.mean(scores[:20]):.1f}%
    Top 50 Mean: {np.mean(scores[:50]):.1f}%

    Score Drop (1st to 50th): {scores[0] - scores[49]:.1f}%
    """

    axes[1, 1].text(0.1, 0.5, quality_text,
                    ha='left', va='center', fontsize=10,
                    transform=axes[1, 1].transAxes,
                    bbox=dict(boxstyle='round', facecolor='lightgray', alpha=0.5),
                    family='monospace')
    axes[1, 1].set_title('Quality Summary')
    axes[1, 1].axis('off')

    plt.tight_layout()
    plt.savefig('docs/analytics/05_recommendation_quality.png', dpi=300, bbox_inches='tight')
    print("✅ Saved: docs/analytics/05_recommendation_quality.png")
    plt.close()


def generate_full_report(user_id: int = 1):
    """Генерирај целосен аналитички извештај"""
    print("=" * 80)
    print("🚀 GNN RECOMMENDATION SYSTEM - FULL ANALYTICS REPORT")
    print("=" * 80)
    print(f"User ID: {user_id}")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # Креирај analytics фолдер
    os.makedirs('docs/analytics', exist_ok=True)

    try:
        analyze_score_distribution(user_id)
        compare_gnn_vs_traditional(user_id)
        analyze_tag_distribution(user_id)
        analyze_user_behavior(user_id)
        analyze_recommendation_quality(user_id)

        print("\n" + "=" * 80)
        print("✅ СИТЕ АНАЛИТИКИ ГЕНЕРИРАНИ УСПЕШНО!")
        print("=" * 80)
        print("\nГенерирани фајлови:")
        print("  📊 docs/analytics/01_score_distribution.png")
        print("  📊 docs/analytics/02_gnn_vs_traditional.png")
        print("  📊 docs/analytics/03_tag_distribution.png")
        print("  📊 docs/analytics/04_user_behavior.png")
        print("  📊 docs/analytics/05_recommendation_quality.png")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ ГРЕШКА: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    app = create_app()

    with app.app_context():
        # Default user ID
        target_user = 1

        # Провери дали има command line аргумент
        if len(sys.argv) > 1:
            try:
                target_user = int(sys.argv[1])
            except ValueError:
                print(f"⚠️  Invalid user ID, using default: {target_user}")

        generate_full_report(user_id=target_user)
