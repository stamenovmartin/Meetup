"""
GNN TRAINING VISUALIZATIONS

Визуелизации за GNN training процесот:
- Training loss curves
- Validation accuracy curves
- Model comparison
- Architecture performance
"""
import json
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set style
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")


def load_training_results(results_file="../gnn_results/training_results.json"):
    """Load GNN training results"""
    with open(results_file, 'r') as f:
        return json.load(f)


def plot_training_loss_curves(results, output_dir="evaluation/visualizations"):
    """Plot training loss curves за сите модели"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(16, 10))
    fig.suptitle('GNN Training Loss Curves', fontsize=20, fontweight='bold')

    models = ['GCN_event_similarity', 'GAT_event_similarity', 'GraphSAGE_event_similarity']
    colors = ['#e74c3c', '#3498db', '#2ecc71']
    model_names = ['GCN', 'GAT', 'GraphSAGE']

    # 1. Individual loss curves
    ax1 = axes[0, 0]
    for model, color, name in zip(models, colors, model_names):
        if model in results and 'train_losses' in results[model]:
            losses = results[model]['train_losses']
            epochs = range(1, len(losses) + 1)
            ax1.plot(epochs, losses, label=name, color=color, linewidth=2, alpha=0.8)

    ax1.set_xlabel('Epoch', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Training Loss', fontsize=12, fontweight='bold')
    ax1.set_title('Training Loss Over Epochs', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11, loc='upper right')
    ax1.grid(alpha=0.3)
    ax1.set_yscale('log')  # Log scale за подобра визуелизација

    # 2. Smoothed loss curves (moving average)
    ax2 = axes[0, 1]
    window = 10
    for model, color, name in zip(models, colors, model_names):
        if model in results and 'train_losses' in results[model]:
            losses = results[model]['train_losses']
            # Moving average
            smoothed = np.convolve(losses, np.ones(window)/window, mode='valid')
            epochs = range(window, len(losses) + 1)
            ax2.plot(epochs, smoothed, label=f'{name} (smoothed)', color=color, linewidth=2.5)

    ax2.set_xlabel('Epoch', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Smoothed Training Loss', fontsize=12, fontweight='bold')
    ax2.set_title(f'Smoothed Training Loss (window={window})', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=11)
    ax2.grid(alpha=0.3)

    # 3. Loss reduction rate
    ax3 = axes[1, 0]
    for model, color, name in zip(models, colors, model_names):
        if model in results and 'train_losses' in results[model]:
            losses = results[model]['train_losses']
            initial_loss = losses[0]
            final_loss = losses[-1]
            reduction = ((initial_loss - final_loss) / initial_loss) * 100

            ax3.bar(name, reduction, color=color, alpha=0.8, edgecolor='black', linewidth=2)
            ax3.text(name, reduction + 1, f'{reduction:.1f}%',
                    ha='center', va='bottom', fontsize=11, fontweight='bold')

    ax3.set_ylabel('Loss Reduction (%)', fontsize=12, fontweight='bold')
    ax3.set_title('Total Loss Reduction', fontsize=14, fontweight='bold')
    ax3.grid(axis='y', alpha=0.3)

    # 4. Final loss comparison
    ax4 = axes[1, 1]
    final_losses = []
    for model in models:
        if model in results and 'train_losses' in results[model]:
            final_losses.append(results[model]['train_losses'][-1])
        else:
            final_losses.append(0)

    bars = ax4.bar(model_names, final_losses, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax4.set_ylabel('Final Training Loss', fontsize=12, fontweight='bold')
    ax4.set_title('Final Training Loss Comparison', fontsize=14, fontweight='bold')
    ax4.grid(axis='y', alpha=0.3)

    for bar, loss in zip(bars, final_losses):
        ax4.text(bar.get_x() + bar.get_width()/2., bar.get_height(),
                f'{loss:.3f}', ha='center', va='bottom', fontsize=11, fontweight='bold')

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_training_loss_curves.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def plot_validation_accuracy(results, output_dir="evaluation/visualizations"):
    """Plot validation accuracy curves"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(1, 2, figsize=(16, 6))
    fig.suptitle('GNN Validation Accuracy', fontsize=20, fontweight='bold')

    models = ['GCN_event_similarity', 'GAT_event_similarity', 'GraphSAGE_event_similarity']
    colors = ['#e74c3c', '#3498db', '#2ecc71']
    model_names = ['GCN', 'GAT', 'GraphSAGE']

    # 1. Validation accuracy curves
    ax1 = axes[0]
    for model, color, name in zip(models, colors, model_names):
        if model in results and 'val_accuracies' in results[model]:
            val_acc = results[model]['val_accuracies']
            epochs = range(1, len(val_acc) + 1)
            ax1.plot(epochs, [acc * 100 for acc in val_acc], label=name,
                    color=color, marker='o', markersize=8, linewidth=2.5, alpha=0.8)

    ax1.set_xlabel('Validation Check', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Validation Accuracy (%)', fontsize=12, fontweight='bold')
    ax1.set_title('Validation Accuracy Over Training', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(alpha=0.3)
    ax1.set_ylim([0, 100])

    # 2. Best validation accuracy comparison
    ax2 = axes[1]
    best_val_accs = []
    for model in models:
        if model in results and 'val_accuracies' in results[model]:
            best_val_accs.append(max(results[model]['val_accuracies']) * 100)
        else:
            best_val_accs.append(0)

    bars = ax2.bar(model_names, best_val_accs, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax2.set_ylabel('Best Validation Accuracy (%)', fontsize=12, fontweight='bold')
    ax2.set_title('Best Validation Accuracy Comparison', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    ax2.set_ylim([0, 100])

    for bar, acc in zip(bars, best_val_accs):
        ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 1,
                f'{acc:.1f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_validation_accuracy.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def plot_model_comparison(results, output_dir="evaluation/visualizations"):
    """Comprehensive model comparison"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('GNN Model Comparison Summary', fontsize=20, fontweight='bold')

    models = ['GCN_event_similarity', 'GAT_event_similarity', 'GraphSAGE_event_similarity']
    colors = ['#e74c3c', '#3498db', '#2ecc71']
    model_names = ['GCN', 'GAT', 'GraphSAGE']

    # 1. Test accuracy comparison
    ax1 = axes[0, 0]
    test_accs = []
    for model in models:
        if model in results and 'test_accuracy' in results[model]:
            test_accs.append(results[model]['test_accuracy'] * 100)
        else:
            test_accs.append(0)

    bars = ax1.bar(model_names, test_accs, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax1.set_ylabel('Test Accuracy (%)', fontsize=12, fontweight='bold')
    ax1.set_title('Test Accuracy Comparison', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    ax1.set_ylim([0, 100])

    # Highlight best
    best_idx = test_accs.index(max(test_accs))
    bars[best_idx].set_edgecolor('gold')
    bars[best_idx].set_linewidth(4)

    for bar, acc in zip(bars, test_accs):
        ax1.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 1,
                f'{acc:.1f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')

    # 2. F1 Score comparison
    ax2 = axes[0, 1]
    f1_scores = []
    for model in models:
        if model in results and 'test_f1' in results[model]:
            f1_scores.append(results[model]['test_f1'] * 100)
        else:
            f1_scores.append(0)

    bars = ax2.bar(model_names, f1_scores, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax2.set_ylabel('F1 Score (%)', fontsize=12, fontweight='bold')
    ax2.set_title('F1 Score Comparison', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    ax2.set_ylim([0, 100])

    # Highlight best
    best_idx = f1_scores.index(max(f1_scores))
    bars[best_idx].set_edgecolor('gold')
    bars[best_idx].set_linewidth(4)

    for bar, f1 in zip(bars, f1_scores):
        ax2.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 1,
                f'{f1:.1f}%', ha='center', va='bottom', fontsize=12, fontweight='bold')

    # 3. Training efficiency (epochs needed)
    ax3 = axes[1, 0]
    total_epochs = []
    for model in models:
        if model in results and 'train_losses' in results[model]:
            total_epochs.append(len(results[model]['train_losses']))
        else:
            total_epochs.append(0)

    bars = ax3.bar(model_names, total_epochs, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    ax3.set_ylabel('Total Epochs', fontsize=12, fontweight='bold')
    ax3.set_title('Training Duration', fontsize=14, fontweight='bold')
    ax3.grid(axis='y', alpha=0.3)

    for bar, epochs in zip(bars, total_epochs):
        ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 5,
                f'{epochs}', ha='center', va='bottom', fontsize=12, fontweight='bold')

    # 4. Performance summary table
    ax4 = axes[1, 1]
    ax4.axis('off')

    summary_text = "PERFORMANCE SUMMARY\n" + "="*50 + "\n\n"
    for i, model in enumerate(models):
        if model in results:
            summary_text += f"{model_names[i]}:\n"
            summary_text += f"  Test Accuracy: {results[model].get('test_accuracy', 0)*100:.2f}%\n"
            summary_text += f"  Test F1:       {results[model].get('test_f1', 0)*100:.2f}%\n"
            if 'val_accuracies' in results[model]:
                summary_text += f"  Best Val Acc:  {max(results[model]['val_accuracies'])*100:.2f}%\n"
            summary_text += f"  Total Epochs:  {len(results[model].get('train_losses', []))}\n"
            if 'train_losses' in results[model]:
                initial = results[model]['train_losses'][0]
                final = results[model]['train_losses'][-1]
                summary_text += f"  Loss: {initial:.3f} → {final:.3f}\n"
            summary_text += "\n"

    # Winner
    best_model_idx = test_accs.index(max(test_accs))
    summary_text += f"\n🏆 WINNER: {model_names[best_model_idx]}\n"
    summary_text += f"   Test Accuracy: {max(test_accs):.2f}%\n"
    summary_text += f"   F1 Score: {f1_scores[best_model_idx]:.2f}%"

    ax4.text(0.1, 0.5, summary_text, fontsize=11, family='monospace',
            verticalalignment='center',
            bbox=dict(boxstyle='round', facecolor='lightgreen', alpha=0.3))

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_model_comparison.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def plot_training_summary(results, output_dir="evaluation/visualizations"):
    """Single comprehensive training summary"""
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    fig = plt.figure(figsize=(18, 10))
    gs = fig.add_gridspec(2, 3, hspace=0.3, wspace=0.3)
    fig.suptitle('GNN Training Complete Summary', fontsize=22, fontweight='bold')

    models = ['GCN_event_similarity', 'GAT_event_similarity', 'GraphSAGE_event_similarity']
    colors = ['#e74c3c', '#3498db', '#2ecc71']
    model_names = ['GCN', 'GAT', 'GraphSAGE']

    # 1. Training loss (large plot)
    ax1 = fig.add_subplot(gs[0, :2])
    for model, color, name in zip(models, colors, model_names):
        if model in results and 'train_losses' in results[model]:
            losses = results[model]['train_losses']
            # Smoothed
            window = 10
            smoothed = np.convolve(losses, np.ones(window)/window, mode='valid')
            epochs = range(window, len(losses) + 1)
            ax1.plot(epochs, smoothed, label=name, color=color, linewidth=3, alpha=0.9)

    ax1.set_xlabel('Epoch', fontsize=13, fontweight='bold')
    ax1.set_ylabel('Training Loss (Smoothed)', fontsize=13, fontweight='bold')
    ax1.set_title('Training Loss Progression', fontsize=15, fontweight='bold')
    ax1.legend(fontsize=12)
    ax1.grid(alpha=0.3)

    # 2. Validation accuracy
    ax2 = fig.add_subplot(gs[0, 2])
    for model, color, name in zip(models, colors, model_names):
        if model in results and 'val_accuracies' in results[model]:
            val_acc = results[model]['val_accuracies']
            epochs = range(1, len(val_acc) + 1)
            ax2.plot(epochs, [acc * 100 for acc in val_acc], label=name,
                    color=color, marker='o', markersize=6, linewidth=2)

    ax2.set_xlabel('Check', fontsize=11, fontweight='bold')
    ax2.set_ylabel('Val Acc (%)', fontsize=11, fontweight='bold')
    ax2.set_title('Validation Accuracy', fontsize=13, fontweight='bold')
    ax2.legend(fontsize=9)
    ax2.grid(alpha=0.3)

    # 3. Test accuracy
    ax3 = fig.add_subplot(gs[1, 0])
    test_accs = [results[m].get('test_accuracy', 0)*100 for m in models]
    bars = ax3.bar(model_names, test_accs, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    best_idx = test_accs.index(max(test_accs))
    bars[best_idx].set_edgecolor('gold')
    bars[best_idx].set_linewidth(4)
    ax3.set_ylabel('Test Acc (%)', fontsize=11, fontweight='bold')
    ax3.set_title('Test Accuracy', fontsize=13, fontweight='bold')
    ax3.set_ylim([0, 100])
    for bar, acc in zip(bars, test_accs):
        ax3.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 2,
                f'{acc:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

    # 4. F1 Score
    ax4 = fig.add_subplot(gs[1, 1])
    f1_scores = [results[m].get('test_f1', 0)*100 for m in models]
    bars = ax4.bar(model_names, f1_scores, color=colors, alpha=0.8, edgecolor='black', linewidth=2)
    best_idx = f1_scores.index(max(f1_scores))
    bars[best_idx].set_edgecolor('gold')
    bars[best_idx].set_linewidth(4)
    ax4.set_ylabel('F1 Score (%)', fontsize=11, fontweight='bold')
    ax4.set_title('F1 Score', fontsize=13, fontweight='bold')
    ax4.set_ylim([0, 100])
    for bar, f1 in zip(bars, f1_scores):
        ax4.text(bar.get_x() + bar.get_width()/2., bar.get_height() + 2,
                f'{f1:.1f}%', ha='center', va='bottom', fontsize=10, fontweight='bold')

    # 5. Stats summary
    ax5 = fig.add_subplot(gs[1, 2])
    ax5.axis('off')

    best_model_idx = test_accs.index(max(test_accs))
    stats_text = f"""
🏆 BEST MODEL: {model_names[best_model_idx]}

Test Accuracy: {max(test_accs):.2f}%
F1 Score: {f1_scores[best_model_idx]:.2f}%

ALL MODELS:
"""
    for i, name in enumerate(model_names):
        stats_text += f"\n{name}:"
        stats_text += f"\n  Acc: {test_accs[i]:.1f}%"
        stats_text += f"\n  F1:  {f1_scores[i]:.1f}%"

    ax5.text(0.1, 0.5, stats_text, fontsize=10, family='monospace',
            verticalalignment='center',
            bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.5))

    plt.tight_layout()
    output_path = Path(output_dir) / 'gnn_training_summary.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {output_path}")
    plt.close()


def main():
    """Generate all GNN training visualizations"""
    print("=" * 80)
    print("🎨 GENERATING GNN TRAINING VISUALIZATIONS")
    print("=" * 80)

    try:
        results = load_training_results()
        print(f"\n✅ Loaded training results for {len(results)} models")

        print("\n1. Training Loss Curves...")
        plot_training_loss_curves(results)

        print("\n2. Validation Accuracy...")
        plot_validation_accuracy(results)

        print("\n3. Model Comparison...")
        plot_model_comparison(results)

        print("\n4. Training Summary...")
        plot_training_summary(results)

        print("\n" + "=" * 80)
        print("✅ ALL GNN TRAINING VISUALIZATIONS CREATED!")
        print("=" * 80)
        print("\nLocation: evaluation/visualizations/")
        print("\nFiles created:")
        print("  1. gnn_training_loss_curves.png")
        print("  2. gnn_validation_accuracy.png")
        print("  3. gnn_model_comparison.png")
        print("  4. gnn_training_summary.png")
        print("=" * 80)

    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
