#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Export Current Database Events to CSV for GNN Graph Rebuilding
===============================================================
Експортира тековни 548 events од database во CSV формат
за GNN graph reconstruction со актуелни податоци.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import create_app
from models.db_models import db, Event
import pandas as pd
from datetime import datetime


def export_events_to_csv(output_path=None):
    """
    Експортирај сите events од database во CSV

    Args:
        output_path: Патека до output CSV фајл

    Returns:
        Број на експортирани events
    """
    app = create_app()

    with app.app_context():
        print("=" * 80)
        print(" EXPORTING DATABASE EVENTS TO CSV")
        print("=" * 80)

        # Земи ги сите events од database
        events = Event.query.all()
        print(f"\n Found {len(events)} events in database")

        if len(events) == 0:
            print(" No events to export!")
            return 0

        # Конвертирај во DataFrame
        events_data = []

        for event in events:
            event_dict = {
                'id': event.id,
                'title': event.title or '',
                'description': event.description or '',
                'category': event.category or '',
                'organizer': event.organizer or '',
                'location': event.location or '',
                'date_start': event.date_start or '',
                'date_end': event.date_end or '',
                'source': event.source or '',
                'url': event.url or '',
                'image_url': event.image_url or '',
                'is_free': 1 if event.is_free else 0,
                'tags': event.tags or '',
                'lat': event.lat if event.lat else 0.0,
                'lon': event.lon if event.lon else 0.0,
                'created_at': event.created_at.isoformat() if event.created_at else ''
            }
            events_data.append(event_dict)

        df = pd.DataFrame(events_data)

        # Определи output патека
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = f"data_collection/NLP_data/cleaned_data/events_current_db_{timestamp}.csv"

        # Зачувај во CSV
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')

        print(f"\n Exported {len(events)} events to: {output_path}")
        print(f" Columns: {', '.join(df.columns)}")
        print(f" File size: {os.path.getsize(output_path) / 1024:.1f} KB")

        print("\n Dataset Summary:")
        print(f"  • Events with title: {len(df[df['title'].str.len() > 0])}")
        print(f"  • Events with category: {len(df[df['category'].notna() & (df['category'] != '')])}")
        print(f"  • Events with tags: {len(df[df['tags'].notna() & (df['tags'] != '')])}")
        print(f"  • Free events: {df['is_free'].sum()}")
        print(f"  • Paid events: {len(df) - df['is_free'].sum()}")

        # Top categories
        if len(df[df['category'].notna()]) > 0:
            print(f"\n Top 5 Categories:")
            top_categories = df['category'].value_counts().head(5)
            for cat, count in top_categories.items():
                print(f"  • {cat}: {count}")

        # Top tags
        if len(df[df['tags'].notna()]) > 0:
            all_tags = []
            for tags_str in df['tags'].dropna():
                if tags_str and isinstance(tags_str, str):
                    tags = [t.strip() for t in tags_str.split(',') if t.strip()]
                    all_tags.extend(tags)

            if all_tags:
                from collections import Counter
                tag_counts = Counter(all_tags)
                print(f"\n Top 10 Tags:")
                for tag, count in tag_counts.most_common(10):
                    print(f"  • {tag}: {count}")

        print("\n" + "=" * 80)
        print(" EXPORT COMPLETE!")
        print("=" * 80)
        print(f"\n Next step: Run graph construction with this CSV file:")
        print(f"   python graph_construction/graph_construction.py")

        return len(events)


if __name__ == '__main__':
    # Default output path
    output_path = None

    if len(sys.argv) > 1:
        output_path = sys.argv[1]

    export_events_to_csv(output_path)
