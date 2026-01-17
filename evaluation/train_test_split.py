"""
Train/Test Split за recommendation evaluation

ВАЖНО: Temporal split - старите interactions за train, нови за test
Ова симулира real-world scenario каде предвидуваме идни likes
"""
import sys
sys.path.append('..')

from models.db_models import User, Event, Attendance
from main import create_app
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime


def temporal_train_test_split(
    train_ratio: float = 0.8,
    min_interactions: int = 5
) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
    """
    Temporal split: За секој user, најстари 80% interactions → train, нови 20% → test

    Args:
        train_ratio: Percentage за train (0.8 = 80%)
        min_interactions: Минимум interactions за да биде вклучен user

    Returns:
        (train_data, test_data) каде:
        - train_data: {user_id: [event_ids за training]}
        - test_data: {user_id: [event_ids за testing]}
    """
    app = create_app()

    with app.app_context():
        # Земи сите позитивни ratings (likes)
        all_interactions = Attendance.query.filter(Attendance.rating == 1).all()

        # Групирај по user
        user_interactions = {}
        for interaction in all_interactions:
            user_id = interaction.user_id
            if user_id not in user_interactions:
                user_interactions[user_id] = []

            user_interactions[user_id].append({
                'event_id': interaction.event_id,
                'timestamp': interaction.id  # Користи ID како proxy за timestamp (assuming sequential)
            })

        train_data = {}
        test_data = {}

        # Split за секој user
        for user_id, interactions in user_interactions.items():
            # Филтрирај users со малку interactions
            if len(interactions) < min_interactions:
                continue

            # Сортирај по timestamp (најстари прво)
            interactions_sorted = sorted(interactions, key=lambda x: x['timestamp'])

            # Split point
            split_idx = int(len(interactions_sorted) * train_ratio)

            # Ensure барем 1 во test
            if split_idx >= len(interactions_sorted):
                split_idx = len(interactions_sorted) - 1

            train_interactions = interactions_sorted[:split_idx]
            test_interactions = interactions_sorted[split_idx:]

            train_data[user_id] = [x['event_id'] for x in train_interactions]
            test_data[user_id] = [x['event_id'] for x in test_interactions]

        print(f"Train/Test Split завршен:")
        print(f"  Users: {len(train_data)}")
        print(f"  Train interactions: {sum(len(v) for v in train_data.values())}")
        print(f"  Test interactions: {sum(len(v) for v in test_data.values())}")
        print(f"  Avg train per user: {np.mean([len(v) for v in train_data.values()]):.1f}")
        print(f"  Avg test per user: {np.mean([len(v) for v in test_data.values()]):.1f}")

        return train_data, test_data


def leave_one_out_split() -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
    """
    Leave-One-Out split: За секој user, последниот item → test, останати → train

    Ова е стандарден approach за implicit feedback datasets
    """
    app = create_app()

    with app.app_context():
        all_interactions = Attendance.query.filter(Attendance.rating == 1).all()

        user_interactions = {}
        for interaction in all_interactions:
            user_id = interaction.user_id
            if user_id not in user_interactions:
                user_interactions[user_id] = []
            user_interactions[user_id].append({
                'event_id': interaction.event_id,
                'timestamp': interaction.id
            })

        train_data = {}
        test_data = {}

        for user_id, interactions in user_interactions.items():
            if len(interactions) < 2:  # Треба барем 2 (1 train, 1 test)
                continue

            interactions_sorted = sorted(interactions, key=lambda x: x['timestamp'])

            # Последниот за test
            test_data[user_id] = [interactions_sorted[-1]['event_id']]
            train_data[user_id] = [x['event_id'] for x in interactions_sorted[:-1]]

        print(f"Leave-One-Out Split:")
        print(f"  Users: {len(train_data)}")
        print(f"  Train interactions: {sum(len(v) for v in train_data.values())}")
        print(f"  Test interactions: {len(test_data)}")

        return train_data, test_data


def get_negative_samples(
    user_id: int,
    positive_items: List[int],
    all_items: List[int],
    num_negatives: int = 99
) -> List[int]:
    """
    Семплирај negative items (кои корисникот НЕ ги лајкал)

    Ова се користи за ranking evaluation:
    - 1 positive item
    - 99 negative items
    - Rank сите 100 и гледај каде е positive

    Args:
        user_id: User ID
        positive_items: Items кои корисникот ги лајкал
        all_items: Сите достапни items
        num_negatives: Број на negative samples

    Returns:
        List of negative item IDs
    """
    positive_set = set(positive_items)
    negative_pool = [item for item in all_items if item not in positive_set]

    if len(negative_pool) < num_negatives:
        return negative_pool

    # Random sample
    return list(np.random.choice(negative_pool, size=num_negatives, replace=False))


def save_split_to_disk(train_data: Dict, test_data: Dict, output_dir: str = "evaluation/data"):
    """
    Зачувај train/test split на disk за reproducibility
    """
    import json
    import os

    os.makedirs(output_dir, exist_ok=True)

    with open(f"{output_dir}/train_split.json", "w") as f:
        json.dump({str(k): v for k, v in train_data.items()}, f)

    with open(f"{output_dir}/test_split.json", "w") as f:
        json.dump({str(k): v for k, v in test_data.items()}, f)

    print(f"Split saved to {output_dir}/")


def load_split_from_disk(input_dir: str = "evaluation/data") -> Tuple[Dict, Dict]:
    """
    Вчитај претходно зачуван split
    """
    import json

    with open(f"{input_dir}/train_split.json", "r") as f:
        train_data = {int(k): v for k, v in json.load(f).items()}

    with open(f"{input_dir}/test_split.json", "r") as f:
        test_data = {int(k): v for k, v in json.load(f).items()}

    return train_data, test_data


# === TESTING ===
if __name__ == "__main__":
    print("=" * 80)
    print("CREATING TRAIN/TEST SPLIT")
    print("=" * 80)

    # Method 1: Temporal 80/20
    print("\n[1] Temporal Split (80/20):")
    train, test = temporal_train_test_split(train_ratio=0.8, min_interactions=5)

    # Method 2: Leave-One-Out
    print("\n[2] Leave-One-Out Split:")
    train_loo, test_loo = leave_one_out_split()

    # Save
    print("\n[3] Saving to disk...")
    save_split_to_disk(train, test)

    print("\n" + "=" * 80)
    print("SPLIT STATISTICS:")
    print("=" * 80)

    print(f"\nTemporal 80/20:")
    print(f"  Train: {sum(len(v) for v in train.values())} interactions")
    print(f"  Test:  {sum(len(v) for v in test.values())} interactions")
    print(f"  Sparsity: {sum(len(v) for v in train.values()) / (len(train) * 1327) * 100:.2f}%")

    print(f"\nExample user {list(train.keys())[0]}:")
    uid = list(train.keys())[0]
    print(f"  Train events: {train[uid][:5]}... ({len(train[uid])} total)")
    print(f"  Test events:  {test[uid][:5]}... ({len(test[uid])} total)")
