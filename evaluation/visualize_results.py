"""
GNN EVALUATION VISUALIZATIONS

Креира професионални визуелизации од evaluation резултатите.
"""
import sys
sys.path.append('..')

import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


def load_latest_results(results_dir: str = "evaluation/results") -> Dict:
    """Load најнов evaluation results file"""
    results_path = Path(results_dir)

    if not results_path.exists():
        # Try alternative path
        results_path = Path("results")

    json_files = list(results_path.glob("eval_*.json"))

    if not json_files:
        raise FileNotFoundError(f"No evaluation results found in {results_dir}")

    # Најнов file
    latest_file = max(json_files, key=lambda p: p.stat().st_mtime)

    print(f"Loading: {latest_file}")

    with open(latest_file, 'r') as f:
        return json.load(f)


def create_metric_comparison_bar_chart(results: Dict, output_dir: str = "evaluation/visualizations"):
    """
    Bar chart споредба на главните метрики
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    models = ['Random', 'Popular', 'Item-KNN', 'MF', 'GNN (Yours)']
    metrics_to_plot = ['P@5', 'P@10', 'R@10', 'NDCG@10', 'MRR']

    fig, axes = plt.subplots(2, 3, figsize=(18, 10))
    fig.suptitle('GNN vs Baselines - Metric Comparison', fontsize=20, fontweight='bold', y=0.995)

    axes = axes.flatten()

    for idx, metric in enumerate(metrics_to_plot):
        ax = axes[idx]

        values = [results['results'][model].get(metric, 0) for model in models]
        colors = ['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1', '#5f27cd']

        bars = ax.bar(range(len(models)), values, color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)

        # Highlight GNN
        bars[-1].set_edgecolor('#5f27cd')
        bars[-1].set_linewidth(3)
        bars[-1].set_alpha(1.0)

        ax.set_xticks(range(len(models)))
        ax.set_xticklabels(models, rotation=45, ha='right', fontsize=10)
        ax.set_ylabel('Score', fontsize=12, fontweight='bold')
        ax.set_title(f'{metric}', fontsize=14, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)

        # Додај вредности на барови
        for i, (bar, val) in enumerate(zip(bars, values)):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{val:.4f}',
                   ha='center', va='bottom', fontsize=9, fontweight='bold')

        # Highlight best score
        max_val = max(values)
        ax.axhline(y=max_val, color='green', linestyle='--', alpha=0.5, linewidth=2)

    # Remove extra subplot
    fig.delaxes(axes[5])

    plt.tight_layout()
    output_path = Path(output_dir) / 'metric_comparison_bars.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def create_improvement_chart(results: Dict, output_dir: str = "evaluation/visualizations"):
    """
    Chart на improvement percentages vs baselines
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    gnn_p10 = results['results']['GNN (Yours)']['P@10']

    baselines = ['Random', 'Popular', 'Item-KNN', 'MF']
    improvements = []

    for model in baselines:
        baseline_p10 = results['results'][model]['P@10']
        if baseline_p10 > 0:
            improvement = ((gnn_p10 - baseline_p10) / baseline_p10) * 100
        else:
            improvement = 0
        improvements.append(improvement)

    fig, ax = plt.subplots(figsize=(12, 7))

    colors = ['#e74c3c', '#f39c12', '#3498db', '#2ecc71']
    bars = ax.barh(baselines, improvements, color=colors, alpha=0.8, edgecolor='black', linewidth=2)

    ax.set_xlabel('Improvement over Baseline (%)', fontsize=14, fontweight='bold')
    ax.set_title('GNN Improvement vs Baselines (Precision@10)', fontsize=16, fontweight='bold')
    ax.grid(axis='x', alpha=0.3)

    # Додај вредности
    for bar, improvement in zip(bars, improvements):
        width = bar.get_width()
        ax.text(width + 10, bar.get_y() + bar.get_height()/2.,
               f'+{improvement:.1f}%',
               ha='left', va='center', fontsize=13, fontweight='bold',
               color='green')

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_improvement_percentages.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def create_metrics_by_k_plot(results: Dict, output_dir: str = "evaluation/visualizations"):
    """
    Line plot: Kako се менуваат метриките со различни K вредности
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    models = ['Random', 'Popular', 'Item-KNN', 'MF', 'GNN (Yours)']
    k_values = [5, 10, 20]

    fig, axes = plt.subplots(1, 3, figsize=(18, 5))
    fig.suptitle('Metrics @ Different K Values', fontsize=18, fontweight='bold')

    metric_prefixes = ['P', 'R', 'NDCG']
    metric_names = ['Precision', 'Recall', 'NDCG']

    colors = ['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1', '#5f27cd']
    markers = ['o', 's', '^', 'D', 'X']

    for idx, (prefix, name) in enumerate(zip(metric_prefixes, metric_names)):
        ax = axes[idx]

        for model, color, marker in zip(models, colors, markers):
            values = [results['results'][model].get(f'{prefix}@{k}', 0) for k in k_values]

            linewidth = 3 if model == 'GNN (Yours)' else 2
            markersize = 12 if model == 'GNN (Yours)' else 8

            ax.plot(k_values, values, label=model, color=color, marker=marker,
                   linewidth=linewidth, markersize=markersize, alpha=0.8)

        ax.set_xlabel('K (Top-K Recommendations)', fontsize=12, fontweight='bold')
        ax.set_ylabel(f'{name} Score', fontsize=12, fontweight='bold')
        ax.set_title(f'{name}@K', fontsize=14, fontweight='bold')
        ax.legend(loc='best', fontsize=9, framealpha=0.9)
        ax.grid(True, alpha=0.3)
        ax.set_xticks(k_values)

    plt.tight_layout()
    output_path = Path(output_dir) / 'metrics_by_k.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def create_heatmap(results: Dict, output_dir: str = "evaluation/visualizations"):
    """
    Heatmap на сите метрики за сите модели
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    models = ['Random', 'Popular', 'Item-KNN', 'MF', 'GNN (Yours)']
    metrics = ['P@5', 'P@10', 'P@20', 'R@5', 'R@10', 'R@20',
              'NDCG@5', 'NDCG@10', 'NDCG@20', 'HR@5', 'HR@10', 'HR@20', 'MRR']

    # Креирај matrix
    data = []
    for model in models:
        row = [results['results'][model].get(metric, 0) for metric in metrics]
        data.append(row)

    data = np.array(data)

    fig, ax = plt.subplots(figsize=(14, 6))

    # Normalize по колони за подобра визуелизација
    data_normalized = (data - data.min(axis=0)) / (data.max(axis=0) - data.min(axis=0) + 1e-10)

    sns.heatmap(data_normalized, annot=data, fmt='.4f', cmap='RdYlGn',
                xticklabels=metrics, yticklabels=models,
                cbar_kws={'label': 'Normalized Score'},
                linewidths=0.5, linecolor='gray',
                ax=ax, vmin=0, vmax=1)

    ax.set_title('All Metrics Heatmap (Values = Actual, Color = Normalized)',
                fontsize=16, fontweight='bold', pad=20)
    ax.set_xlabel('Metrics', fontsize=13, fontweight='bold')
    ax.set_ylabel('Models', fontsize=13, fontweight='bold')

    plt.tight_layout()
    output_path = Path(output_dir) / 'metrics_heatmap.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def create_radar_chart(results: Dict, output_dir: str = "evaluation/visualizations"):
    """
    Radar chart за споредба на GNN vs најдобар baseline
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    metrics = ['P@10', 'R@10', 'NDCG@10', 'HR@10', 'MRR']

    # GNN vs MF (strongest baseline)
    gnn_values = [results['results']['GNN (Yours)'].get(m, 0) for m in metrics]
    mf_values = [results['results']['MF'].get(m, 0) for m in metrics]
    itemknn_values = [results['results']['Item-KNN'].get(m, 0) for m in metrics]

    # Normalize за radar chart
    max_values = [max(gnn_values[i], mf_values[i], itemknn_values[i]) for i in range(len(metrics))]
    gnn_norm = [gnn_values[i] / max_values[i] if max_values[i] > 0 else 0 for i in range(len(metrics))]
    mf_norm = [mf_values[i] / max_values[i] if max_values[i] > 0 else 0 for i in range(len(metrics))]
    itemknn_norm = [itemknn_values[i] / max_values[i] if max_values[i] > 0 else 0 for i in range(len(metrics))]

    # Setup radar
    angles = np.linspace(0, 2 * np.pi, len(metrics), endpoint=False).tolist()
    gnn_norm += gnn_norm[:1]
    mf_norm += mf_norm[:1]
    itemknn_norm += itemknn_norm[:1]
    angles += angles[:1]

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))

    ax.plot(angles, gnn_norm, 'o-', linewidth=3, label='GNN (Yours)', color='#5f27cd', markersize=10)
    ax.fill(angles, gnn_norm, alpha=0.25, color='#5f27cd')

    ax.plot(angles, itemknn_norm, 's-', linewidth=2, label='Item-KNN', color='#48dbfb', markersize=8)
    ax.fill(angles, itemknn_norm, alpha=0.15, color='#48dbfb')

    ax.plot(angles, mf_norm, '^-', linewidth=2, label='MF', color='#1dd1a1', markersize=8)
    ax.fill(angles, mf_norm, alpha=0.15, color='#1dd1a1')

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(metrics, fontsize=13, fontweight='bold')
    ax.set_ylim(0, 1)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['20%', '40%', '60%', '80%', '100%'], fontsize=10)
    ax.grid(True, alpha=0.3)

    ax.set_title('GNN vs Best Baselines\n(Normalized Scores)',
                fontsize=16, fontweight='bold', pad=30)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1), fontsize=12)

    plt.tight_layout()
    output_path = Path(output_dir) / 'radar_comparison.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def create_summary_figure(results: Dict, output_dir: str = "evaluation/visualizations"):
    """
    Summary figure со клучни статистики
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('GNN Evaluation Summary', fontsize=22, fontweight='bold', y=0.995)

    # 1. Precision comparison
    ax1 = axes[0, 0]
    models = ['Random', 'Popular', 'Item-KNN', 'MF', 'GNN']
    p10_values = [
        results['results']['Random']['P@10'],
        results['results']['Popular']['P@10'],
        results['results']['Item-KNN']['P@10'],
        results['results']['MF']['P@10'],
        results['results']['GNN (Yours)']['P@10']
    ]
    colors = ['#ff6b6b', '#feca57', '#48dbfb', '#1dd1a1', '#5f27cd']
    bars1 = ax1.bar(models, p10_values, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    bars1[-1].set_linewidth(3)
    ax1.set_ylabel('Precision@10', fontsize=13, fontweight='bold')
    ax1.set_title('Precision@10 Comparison', fontsize=15, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    for bar, val in zip(bars1, p10_values):
        ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{val:.4f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

    # 2. Hit Rate comparison
    ax2 = axes[0, 1]
    hr10_values = [
        results['results']['Random']['HR@10'],
        results['results']['Popular']['HR@10'],
        results['results']['Item-KNN']['HR@10'],
        results['results']['MF']['HR@10'],
        results['results']['GNN (Yours)']['HR@10']
    ]
    bars2 = ax2.bar(models, [v*100 for v in hr10_values], color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    bars2[-1].set_linewidth(3)
    ax2.set_ylabel('Hit Rate@10 (%)', fontsize=13, fontweight='bold')
    ax2.set_title('Hit Rate@10 Comparison', fontsize=15, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    for bar, val in zip(bars2, hr10_values):
        ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{val*100:.1f}%', ha='center', va='bottom', fontsize=11, fontweight='bold')

    # 3. Improvement percentages
    ax3 = axes[1, 0]
    gnn_p10 = results['results']['GNN (Yours)']['P@10']
    baselines = ['Random', 'Popular', 'Item-KNN', 'MF']
    improvements = []
    for model in baselines:
        baseline_p10 = results['results'][model]['P@10']
        if baseline_p10 > 0:
            improvement = ((gnn_p10 - baseline_p10) / baseline_p10) * 100
        else:
            improvement = 0
        improvements.append(improvement)

    bars3 = ax3.barh(baselines, improvements, color=colors[:-1], alpha=0.8, edgecolor='black', linewidth=2)
    ax3.set_xlabel('Improvement (%)', fontsize=13, fontweight='bold')
    ax3.set_title('GNN Improvement over Baselines', fontsize=15, fontweight='bold')
    ax3.grid(axis='x', alpha=0.3)
    for bar, imp in zip(bars3, improvements):
        ax3.text(bar.get_width() + 10, bar.get_y() + bar.get_height()/2.,
                f'+{imp:.0f}%', ha='left', va='center', fontsize=12, fontweight='bold', color='green')

    # 4. Key statistics text
    ax4 = axes[1, 1]
    ax4.axis('off')

    stats_text = f"""
    📊 EVALUATION RESULTS

    Dataset:
    • Users in test set: {results['test_users']}
    • Train interactions: {results['train_users']} users

    GNN Performance:
    • Precision@10: {results['results']['GNN (Yours)']['P@10']:.4f}
    • Recall@10: {results['results']['GNN (Yours)']['R@10']:.4f}
    • NDCG@10: {results['results']['GNN (Yours)']['NDCG@10']:.4f}
    • Hit Rate@10: {results['results']['GNN (Yours)']['HR@10']*100:.1f}%
    • MRR: {results['results']['GNN (Yours)']['MRR']:.4f}

    Best Baseline (Item-KNN):
    • Precision@10: {results['results']['Item-KNN']['P@10']:.4f}

    Improvement:
    • GNN beats Item-KNN by {((gnn_p10 - results['results']['Item-KNN']['P@10'])/results['results']['Item-KNN']['P@10'])*100:.0f}%

    Winner: {results['winner']} ✅
    """

    ax4.text(0.1, 0.5, stats_text, fontsize=13, family='monospace',
            verticalalignment='center', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.3))

    plt.tight_layout()
    output_path = Path(output_dir) / 'evaluation_summary.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def main():
    """Generate сите визуелизации"""
    print("=" * 80)
    print("🎨 GENERATING GNN EVALUATION VISUALIZATIONS")
    print("=" * 80)

    # Load results
    try:
        results = load_latest_results()
    except FileNotFoundError:
        print("❌ No results found. Run evaluation first!")
        return

    print(f"\nDataset: {results['train_users']} users, Winner: {results['winner']}")
    print(f"\nGenerating visualizations...")

    # Create all visualizations
    create_metric_comparison_bar_chart(results)
    create_improvement_chart(results)
    create_metrics_by_k_plot(results)
    create_heatmap(results)
    create_radar_chart(results)
    create_summary_figure(results)

    print("\n" + "=" * 80)
    print("✅ ALL VISUALIZATIONS CREATED!")
    print("=" * 80)
    print("\nLocation: evaluation/visualizations/")
    print("\nFiles created:")
    print("  1. metric_comparison_bars.png")
    print("  2. gnn_improvement_percentages.png")
    print("  3. metrics_by_k.png")
    print("  4. metrics_heatmap.png")
    print("  5. radar_comparison.png")
    print("  6. evaluation_summary.png")
    print("=" * 80)


if __name__ == "__main__":
    main()
