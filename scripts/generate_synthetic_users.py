#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Generate Synthetic Users with Realistic Behavior
==================================================
Генерира synthetic users со realistic likes/dislikes patterns
за да имаме доволно податоци за GNN evaluation.

Strategy:
- Креирај 20-50 synthetic users
- Секој user има preferences (tag-based)
- Генерирај 50-200 interactions per user
- Реалистички behavior patterns (не random!)
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import create_app
from models.db_models import db, Event, User, Attendance
import random
import numpy as np
from werkzeug.security import generate_password_hash

# User archetypes со преференции
USER_ARCHETYPES = {
    'tech_enthusiast': {
        'name': 'Tech Enthusiast',
        'liked_tags': ['IT', 'Technology', 'Programming', 'Development', 'Software', 'Networking'],
        'disliked_tags': ['Opera', 'Classical', 'Ballet'],
        'neutral_tags': ['Music', 'Film'],
        'like_prob': 0.7,
        'dislike_prob': 0.2
    },
    'music_lover': {
        'name': 'Music Lover',
        'liked_tags': ['Music', 'Concert', 'Jazz', 'Rock', 'Classical', 'Opera'],
        'disliked_tags': ['IT', 'Programming', 'Technology'],
        'neutral_tags': ['Film', 'Theater'],
        'like_prob': 0.65,
        'dislike_prob': 0.25
    },
    'culture_buff': {
        'name': 'Culture Buff',
        'liked_tags': ['Theater', 'Drama', 'Art', 'Culture', 'Museum', 'Gallery', 'Opera'],
        'disliked_tags': ['IT', 'Technology', 'Gaming'],
        'neutral_tags': ['Music', 'Film'],
        'like_prob': 0.6,
        'dislike_prob': 0.3
    },
    'cinema_fan': {
        'name': 'Cinema Fan',
        'liked_tags': ['Film', 'Cinema', 'Entertainment'],
        'disliked_tags': ['Sports', 'Marathon'],
        'neutral_tags': ['Music', 'Theater'],
        'like_prob': 0.7,
        'dislike_prob': 0.2
    },
    'business_professional': {
        'name': 'Business Professional',
        'liked_tags': ['Business', 'Conference', 'Networking', 'Startup', 'Entrepreneurship'],
        'disliked_tags': ['Gaming', 'Party'],
        'neutral_tags': ['IT', 'Technology'],
        'like_prob': 0.6,
        'dislike_prob': 0.3
    },
    'generalist': {
        'name': 'Generalist',
        'liked_tags': ['Entertainment', 'Fun', 'Social', 'Event'],
        'disliked_tags': [],
        'neutral_tags': ['IT', 'Music', 'Film', 'Theater'],
        'like_prob': 0.4,
        'dislike_prob': 0.1
    }
}


def create_synthetic_user(archetype_name, archetype_data, user_number):
    """
    Креирај synthetic user

    Args:
        archetype_name: Тип на корисник
        archetype_data: Преференции
        user_number: Број на корисник

    Returns:
        User object
    """
    name = f"Synthetic {archetype_data['name']} {user_number}"
    email = f"synthetic_{archetype_name}_{user_number}@synthetic.test"

    # Check if exists
    existing = User.query.filter_by(email=email).first()
    if existing:
        return existing

    user = User(
        name=name,
        email=email,
        password_hash=generate_password_hash("synthetic123"),
        lat=42.0 + random.uniform(-0.5, 0.5),  # Skopje area
        lon=21.4 + random.uniform(-0.5, 0.5)
    )

    db.session.add(user)
    db.session.commit()

    return user


def generate_interactions_for_user(user, archetype_data, num_interactions=100):
    """
    Генерирај realistic interactions за user

    Args:
        user: User object
        archetype_data: Преференции
        num_interactions: Број на интеракции

    Returns:
        Број на креирани interactions
    """
    # Земи случаен sample од настани
    all_events = Event.query.all()

    if len(all_events) < num_interactions:
        num_interactions = len(all_events)

    # Избери events што ќе се рејтираат
    sampled_events = random.sample(all_events, num_interactions)

    interactions_created = 0

    for event in sampled_events:
        # Check дали веќе постои interaction
        existing = Attendance.query.filter_by(user_id=user.id, event_id=event.id).first()
        if existing:
            continue

        # Пресметај rating врз основа на preferences
        event_tags = set((event.tags or "").split(","))
        event_tags = {t.strip() for t in event_tags if t.strip()}

        # Провери дали event има liked/disliked tags
        liked_match = bool(event_tags.intersection(set(archetype_data['liked_tags'])))
        disliked_match = bool(event_tags.intersection(set(archetype_data['disliked_tags'])))

        # Одлучи за rating
        if disliked_match:
            # Висока веројатност за dislike
            rating = -1 if random.random() < 0.8 else 0
        elif liked_match:
            # Висока веројатност за like
            rating = 1 if random.random() < archetype_data['like_prob'] else 0
        else:
            # Neutral event - случаен rating
            rand = random.random()
            if rand < 0.2:
                rating = 1
            elif rand < 0.35:
                rating = -1
            else:
                rating = 0

        # Креирај interaction
        attendance = Attendance(
            user_id=user.id,
            event_id=event.id,
            rating=rating
        )

        db.session.add(attendance)
        interactions_created += 1

    db.session.commit()

    return interactions_created


def generate_synthetic_dataset(num_users_per_archetype=5, interactions_per_user=100):
    """
    Генерирај целосен synthetic dataset

    Args:
        num_users_per_archetype: Колку users по архетип
        interactions_per_user: Колку интеракции по user

    Returns:
        Statistics dictionary
    """
    app = create_app()

    with app.app_context():
        print("=" * 80)
        print("🤖 GENERATING SYNTHETIC USERS & INTERACTIONS")
        print("=" * 80)

        stats = {
            'users_created': 0,
            'interactions_created': 0,
            'by_archetype': {}
        }

        for archetype_name, archetype_data in USER_ARCHETYPES.items():
            print(f"\n📊 Creating {archetype_name} users...")

            archetype_stats = {
                'users': 0,
                'interactions': 0
            }

            for i in range(num_users_per_archetype):
                user = create_synthetic_user(archetype_name, archetype_data, i + 1)
                stats['users_created'] += 1
                archetype_stats['users'] += 1

                # Generate interactions
                num_interactions = generate_interactions_for_user(
                    user,
                    archetype_data,
                    num_interactions=interactions_per_user
                )

                stats['interactions_created'] += num_interactions
                archetype_stats['interactions'] += num_interactions

                print(f"  • User {user.name}: {num_interactions} interactions")

            stats['by_archetype'][archetype_name] = archetype_stats

        # Final statistics
        print("\n" + "=" * 80)
        print("✅ SYNTHETIC DATA GENERATION COMPLETE!")
        print("=" * 80)
        print(f"\n📊 Summary:")
        print(f"  • Total Users Created: {stats['users_created']}")
        print(f"  • Total Interactions Created: {stats['interactions_created']}")
        print(f"  • Avg Interactions per User: {stats['interactions_created'] / stats['users_created']:.1f}")

        print(f"\n📈 By Archetype:")
        for arch, arch_stats in stats['by_archetype'].items():
            print(f"  • {arch}: {arch_stats['users']} users, {arch_stats['interactions']} interactions")

        # Database stats
        total_users = User.query.count()
        total_interactions = Attendance.query.count()
        total_likes = Attendance.query.filter_by(rating=1).count()
        total_dislikes = Attendance.query.filter_by(rating=-1).count()

        print(f"\n🗄️  Final Database Stats:")
        print(f"  • Total Users: {total_users}")
        print(f"  • Total Interactions: {total_interactions}")
        print(f"  • Likes: {total_likes} ({total_likes/total_interactions*100:.1f}%)")
        print(f"  • Dislikes: {total_dislikes} ({total_dislikes/total_interactions*100:.1f}%)")

        print("\n" + "=" * 80)
        print("🚀 Ready for GNN Evaluation!")
        print("=" * 80)

        return stats


if __name__ == '__main__':
    # Default: 5 users per archetype, 100 interactions per user
    num_users = 5
    num_interactions = 100

    if len(sys.argv) > 1:
        num_users = int(sys.argv[1])

    if len(sys.argv) > 2:
        num_interactions = int(sys.argv[2])

    generate_synthetic_dataset(
        num_users_per_archetype=num_users,
        interactions_per_user=num_interactions
    )
