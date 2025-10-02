# models/recommender.py - Поправена верзија без ограничувања
import os
import torch
import numpy as np
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from .db_models import db, Event, Venue, User, Attendance
import logging

logger = logging.getLogger(__name__)


class GNNRecommender:
    """
    Препорачувач што комбинира традиционални features (тагови, геолокација)
    со GNN embeddings за подобри препораки.
    ЗЕМА ПРЕДВИД СИТЕ НАСТАНИ - без ограничувања!
    """

    def __init__(self, graph_path: str = None, alpha: float = 0.6):
        """
        Args:
            graph_path: Патека до .pt фајлот со graph data и embeddings
            alpha: Тежина за традиционален скор (1-alpha за GNN скор)
        """
        self.alpha = alpha
        self.graph_data = None
        self.event_embeddings = None
        self.event_id_mapping = {}

        # Автоматски најди graph фајл ако не е специфициран
        if not graph_path:
            graph_path = self._find_graph_file()

        if graph_path and os.path.exists(graph_path):
            self.load_graph_data(graph_path)
        else:
            logger.warning(f"GNN graph не е пронајден: {graph_path}")
            logger.warning("Ќе користам само традиционални features")

    def _find_graph_file(self) -> Optional[str]:
        """Автоматски пронајди graph фајл во проектот"""
        possible_paths = [
            "graph_construction/graph_data/event_similarity_graph.pt",
            "gnn_results/models/GraphSAGE_event_similarity.pt",
            "models/event_graph.pt",
            "../graph_construction/graph_data/event_similarity_graph.pt"
        ]

        for path in possible_paths:
            if os.path.exists(path):
                logger.info(f"Пронајден graph: {path}")
                return path
        return None

    def load_graph_data(self, graph_path: str):
        """Вчитај graph data и embeddings"""
        try:
            self.graph_data = torch.load(graph_path, map_location='cpu', weights_only=False)

            # Check format: dictionary (new) or PyG Data (old)
            if isinstance(self.graph_data, dict):
                # NEW FORMAT: Dictionary со embeddings и event_ids
                if 'embeddings' in self.graph_data:
                    if isinstance(self.graph_data['embeddings'], torch.Tensor):
                        self.event_embeddings = self.graph_data['embeddings'].numpy()
                    else:
                        self.event_embeddings = self.graph_data['embeddings']

                    # Креирај мапирање event_id -> embedding_index
                    if 'event_ids' in self.graph_data:
                        for i, event_id in enumerate(self.graph_data['event_ids']):
                            self.event_id_mapping[event_id] = i
                    else:
                        # Fallback: претпоставуваме sequential IDs
                        for i in range(len(self.event_embeddings)):
                            self.event_id_mapping[i + 1] = i

                    logger.info(f"Вчитани embeddings (new format): {self.event_embeddings.shape}")
                    logger.info(f"Event ID mapping: {len(self.event_id_mapping)} events")
                else:
                    logger.warning("Dictionary format но без 'embeddings' key!")

            elif hasattr(self.graph_data, 'x'):
                # OLD FORMAT: PyG Data object
                self.event_embeddings = self.graph_data.x.numpy()
                logger.info(f"Вчитани embeddings (old format): {self.event_embeddings.shape}")

                # Креирај мапирање event_id -> embedding_index
                for i in range(len(self.event_embeddings)):
                    self.event_id_mapping[i + 1] = i  # event IDs почнуваат од 1

            else:
                logger.warning(f"Unknown format: {type(self.graph_data)}")
                self.graph_data = None
                return

            logger.info("GNN graph успешно вчитан")

        except Exception as e:
            logger.error(f"Грешка при вчитување на graph: {e}")
            self.graph_data = None

    def traditional_score(self, user_id: int, event: Event) -> float:
        """Традиционален скор: тагови + геолокација - со негативни оценки"""
        # Тагови од историја (likes и dislikes)
        likes = Attendance.query.filter_by(user_id=user_id, rating=1).all()
        dislikes = Attendance.query.filter_by(user_id=user_id, rating=-1).all()

        liked_event_ids = [a.event_id for a in likes]
        disliked_event_ids = [a.event_id for a in dislikes]

        like_tags = set()
        dislike_tags = set()

        for e in Event.query.filter(Event.id.in_(liked_event_ids)).all():
            like_tags.update((e.tags or "").split(","))

        for e in Event.query.filter(Event.id.in_(disliked_event_ids)).all():
            dislike_tags.update((e.tags or "").split(","))

        # Таг-совпаѓање (позитивни и негативни)
        e_tags = set((event.tags or "").split(","))
        positive_tag_score = len(like_tags.intersection(e_tags))
        negative_tag_score = len(dislike_tags.intersection(e_tags))
        tag_score = positive_tag_score - (negative_tag_score * 5)  # Негативни се 5× појаки!

        # Гео-компонента
        venue = Venue.query.get(event.venue_id)
        user = User.query.get(user_id)
        geo_score = 0.0

        if venue and user and user.lat and user.lon and venue.lat and venue.lon:
            from math import hypot
            geo_d = hypot((user.lat - venue.lat), (user.lon - venue.lon))
            geo_score = 1.0 / (1.0 + geo_d)

        return tag_score + geo_score

    def gnn_similarity_score(self, user_id: int, event: Event) -> float:
        """GNN-базиран скор врз основа на embeddings similarity"""
        if self.event_embeddings is None or len(self.event_embeddings) == 0 or event.id not in self.event_id_mapping:
            return 0.0

        try:
            # Земи embedding за овој настан
            event_idx = self.event_id_mapping[event.id]
            event_emb = self.event_embeddings[event_idx]

            # Најди настани што корисникот ги лајкал
            likes = Attendance.query.filter_by(user_id=user_id, rating=1).all()
            liked_event_ids = [a.event_id for a in likes]

            if not liked_event_ids:
                return 0.0

            # Пресметај просечна сличност со лајкани настани
            similarities = []
            for liked_id in liked_event_ids:
                if liked_id in self.event_id_mapping:
                    liked_idx = self.event_id_mapping[liked_id]
                    liked_emb = self.event_embeddings[liked_idx]

                    # Cosine similarity
                    sim = np.dot(event_emb, liked_emb) / (
                            np.linalg.norm(event_emb) * np.linalg.norm(liked_emb) + 1e-8
                    )
                    similarities.append(max(0, sim))  # Само позитивни сличности

            return np.mean(similarities) if similarities else 0.0

        except Exception as e:
            logger.error(f"Грешка во GNN similarity: {e}")
            return 0.0

    def combined_score(self, user_id: int, event: Event) -> float:
        """Комбиниран скор: традиционален + GNN - GNN СЕКОГАШ ВЛИЈАЕ!"""
        trad_score = self.traditional_score(user_id, event)
        gnn_score = self.gnn_similarity_score(user_id, event)

        # GNN СЕКОГАШ влијае, но традиционалниот dislike penalty е силен
        # Alpha=0.5: Traditional 50%, GNN 50% (balanced)
        # GNN score се множи со 10 за да биде во ист range како traditional
        combined = self.alpha * trad_score + (1 - self.alpha) * gnn_score * 10

        return combined

    def recommend_events(self, user_id: int, start_date=None, end_date=None, limit: int = 50) -> List[Dict]:
        """
        Главна функција за препораки со процентуални скорови
        СЕГА ЗЕМА ПРЕДВИД СИТЕ НАСТАНИ БЕЗ ОГРАНИЧУВАЊА!

        Returns:
            Lista на events со score_pct (0-100)
        """
        from datetime import datetime

        # Филтрирај настани САМО по датум ако е специфициран
        q = Event.query

        if start_date:
            q = q.filter(Event.starts_at >= datetime.fromisoformat(start_date + "T00:00:00"))
        # ОТСТРАНЕНО: автоматско филтрирање за идни настани
        # Сега можат да се препорачуваат и минати настани

        if end_date:
            q = q.filter(Event.starts_at <= datetime.fromisoformat(end_date + "T23:59:59"))

        # ОТСТРАНЕНО: .limit(1000) - сега земаме СЕ!
        candidates = q.order_by(Event.starts_at.asc()).all()

        if not candidates:
            return []

        logger.info(f"Препорачувам од {len(candidates)} вкупно настани за корисник {user_id}")

        # Пресметај скорови за СИТЕ настани
        scored_events = []
        for event in candidates:
            score = self.combined_score(user_id, event)
            scored_events.append((score, event))

        # Сортирај по скор (најдобри прво)
        sorted_events = sorted(scored_events, key=lambda x: x[0], reverse=True)

        # Користи ПЕРЦЕНТИЛ-базирана нормализација (0-100%)
        # Ова дава подобра дистрибуција отколку min-max
        scores = [score for score, _ in sorted_events]

        if not scores:
            return []

        def percentile_to_score(rank, total):
            """Конвертирај ранг во процент (100% = најдобар, 0% = најлош)"""
            if total == 1:
                return 100.0
            # Линеарна скала од 100% (rank=0) до 0% (rank=total-1)
            percentile = 100.0 * (1.0 - rank / (total - 1))
            return round(percentile, 1)

        # Форматирај резултат - ги пресметуваме процентите за СИТЕ настани!
        recommendations = []
        total_candidates = len(sorted_events)  # СИТЕ 548 настани

        # ВАЖНО: Пресметај процент врз основа на СИТЕ настани, не само топ limit!
        for rank, (score, event) in enumerate(sorted_events):
            # Користи ранг од СИТЕ настани за вистински процент (0-100%)
            score_pct = percentile_to_score(rank, total_candidates)

            recommendations.append({
                "event_id": event.id,
                "title": event.title,
                "description": event.description,
                "starts_at": event.starts_at.isoformat(),
                "venue_id": event.venue_id,
                "tags": (event.tags or "").split(","),
                "score_pct": score_pct,
                "raw_score": round(score, 3),  # За debugging
                "rank": rank + 1  # Ранг (1 = најдобар)
            })

        # Врати само TOP limit за API response (но процентите се пресметани од сите!)
        logger.info(f"Враќам топ {min(limit, len(recommendations))} препораки од {len(candidates)} анализирани (scores: 0-100%)")
        return recommendations[:limit]


# Додај функција за пребарување низ СИТЕ настани
def search_all_events(user_id: int, query: str = "", filters: dict = None, limit: int = 100) -> List[Dict]:
    """
    Пребарај низ СИТЕ настани без ограничувања

    Args:
        user_id: ID на корисник
        query: Текст за пребарување
        filters: Додатни филтри (tags, city, date_range)
        limit: Колку резултати да врати
    """
    from datetime import datetime

    q = Event.query

    # Текст пребарување во наслов и опис
    if query:
        like_pattern = f"%{query}%"
        q = q.filter(
            db.or_(
                Event.title.ilike(like_pattern),
                Event.description.ilike(like_pattern)
            )
        )

    # Филтри
    if filters:
        if filters.get('tags'):
            tag_list = [t.strip().lower() for t in filters['tags'].split(",") if t.strip()]
            for tag in tag_list:
                q = q.filter(Event.tags.ilike(f"%{tag}%"))

        if filters.get('city'):
            q = q.join(Venue, Event.venue_id == Venue.id).filter(
                Venue.city.ilike(f"%{filters['city']}%")
            )

        if filters.get('date_from'):
            q = q.filter(Event.starts_at >= datetime.fromisoformat(filters['date_from'] + "T00:00:00"))

        if filters.get('date_to'):
            q = q.filter(Event.starts_at <= datetime.fromisoformat(filters['date_to'] + "T23:59:59"))

    # Земи СЕ што одговараат на критериумите
    events = q.order_by(Event.starts_at.desc()).all()

    # Додај my_rating за секој настан
    event_ratings = {}
    if user_id:
        ratings = Attendance.query.filter_by(user_id=user_id).all()
        event_ratings = {r.event_id: r.rating for r in ratings}

    # Форматирај резултат
    results = []
    for event in events[:limit]:  # limit само за performance на API
        results.append({
            "id": event.id,
            "title": event.title,
            "description": event.description,
            "starts_at": event.starts_at.isoformat(),
            "venue_id": event.venue_id,
            "tags": (event.tags or "").split(","),
            "my_rating": event_ratings.get(event.id, 0) if user_id else None
        })

    return results


# Global instance
_recommender = None


def get_recommender() -> GNNRecommender:
    """Singleton pattern за препорачувач"""
    global _recommender
    if _recommender is None:
        _recommender = GNNRecommender()
    return _recommender


def recommend_for_user(user_id: int, start_date=None, end_date=None, limit: int = 50) -> List[Dict]:
    """Convenience функција"""
    recommender = get_recommender()
    return recommender.recommend_events(user_id, start_date, end_date, limit)