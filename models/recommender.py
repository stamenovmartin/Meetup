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
        """
        БАЗИЧЕН GNN semantic similarity скор (само embeddings)
        Ова е само еден дел од вкупниот personalized score
        """
        if self.event_embeddings is None or len(self.event_embeddings) == 0 or event.id not in self.event_id_mapping:
            return 0.0

        try:
            event_idx = self.event_id_mapping[event.id]
            event_emb = self.event_embeddings[event_idx]

            likes = Attendance.query.filter_by(user_id=user_id, rating=1).all()
            dislikes = Attendance.query.filter_by(user_id=user_id, rating=-1).all()

            liked_event_ids = [a.event_id for a in likes]
            disliked_event_ids = [a.event_id for a in dislikes]

            # Positive similarities
            positive_similarities = []
            for liked_id in liked_event_ids:
                if liked_id in self.event_id_mapping:
                    liked_idx = self.event_id_mapping[liked_id]
                    liked_emb = self.event_embeddings[liked_idx]
                    sim = np.dot(event_emb, liked_emb) / (
                            np.linalg.norm(event_emb) * np.linalg.norm(liked_emb) + 1e-8
                    )
                    positive_similarities.append(max(0, sim))

            # Negative similarities
            negative_similarities = []
            for disliked_id in disliked_event_ids:
                if disliked_id in self.event_id_mapping:
                    disliked_idx = self.event_id_mapping[disliked_id]
                    disliked_emb = self.event_embeddings[disliked_idx]
                    sim = np.dot(event_emb, disliked_emb) / (
                            np.linalg.norm(event_emb) * np.linalg.norm(disliked_emb) + 1e-8
                    )
                    negative_similarities.append(max(0, sim))

            if not positive_similarities:
                return 0.0

            # TOP-3 average
            top_k = min(3, len(positive_similarities))
            top_positive = sorted(positive_similarities, reverse=True)[:top_k]
            positive_score = np.mean(top_positive)

            # Dislike penalty
            if negative_similarities:
                max_negative = max(negative_similarities)
                penalty = max_negative * 0.5
            else:
                penalty = 0.0

            final_score = max(0.0, positive_score - penalty)
            return final_score

        except Exception as e:
            logger.error(f"Грешка во GNN similarity: {e}")
            return 0.0

    def personalized_score(self, user_id: int, event: Event) -> float:
        """
        ВИСТИНСКИ ПЕРСОНАЛИЗИРАН СКОР - комбинира многу сигнали

        Враќа скор 0-1 кој одговара на "колку % би му одговарал овој настан"
        базирано на целиот кориснички профил.

        Компоненти:
        1. GNN semantic similarity (40%)
        2. Tag preference matching (30%)
        3. Venue preference (20%)
        4. Temporal patterns (10%)
        + Random noise (5%) за tie-breaking
        """
        try:
            import random
            random.seed(event.id)  # Deterministic per event
            # Get user's history
            likes = Attendance.query.filter_by(user_id=user_id, rating=1).all()
            dislikes = Attendance.query.filter_by(user_id=user_id, rating=-1).all()

            if not likes:
                # Нов корисник - fallback на GNN
                return self.gnn_similarity_score(user_id, event) * 0.5

            # === COMPONENT 1: GNN Semantic Similarity (30%) ===
            gnn_score = self.gnn_similarity_score(user_id, event)

            # === COMPONENT 2: Tag Preference Matching (25%) ===
            # Изгради weighted tag preferences
            liked_events = Event.query.filter(Event.id.in_([a.event_id for a in likes])).all()
            disliked_events = Event.query.filter(Event.id.in_([a.event_id for a in dislikes])).all()

            liked_tags = {}
            for e in liked_events:
                for tag in (e.tags or "").split(","):
                    tag = tag.strip()
                    if tag:
                        liked_tags[tag] = liked_tags.get(tag, 0) + 1

            disliked_tags = {}
            for e in disliked_events:
                for tag in (e.tags or "").split(","):
                    tag = tag.strip()
                    if tag:
                        disliked_tags[tag] = disliked_tags.get(tag, 0) + 1

            # Match event tags
            event_tags = set((event.tags or "").split(","))
            event_tags = {t.strip() for t in event_tags if t.strip()}

            tag_score = 0.0
            if event_tags and liked_tags:
                # Weighted overlap
                positive_weight = sum(liked_tags.get(t, 0) for t in event_tags)
                negative_weight = sum(disliked_tags.get(t, 0) for t in event_tags)

                max_weight = max(liked_tags.values()) * len(event_tags)
                if max_weight > 0:
                    tag_score = (positive_weight - negative_weight * 2) / max_weight
                    tag_score = max(0.0, min(1.0, tag_score))

            # === COMPONENT 3: Venue Preference (20%) ===
            liked_venues = {}
            for e in liked_events:
                if e.venue_id:
                    liked_venues[e.venue_id] = liked_venues.get(e.venue_id, 0) + 1

            disliked_venues = {}
            for e in disliked_events:
                if e.venue_id:
                    disliked_venues[e.venue_id] = disliked_venues.get(e.venue_id, 0) + 1

            venue_score = 0.0
            if event.venue_id and liked_venues:
                total_venue_likes = sum(liked_venues.values())
                venue_likes = liked_venues.get(event.venue_id, 0)
                venue_dislikes = disliked_venues.get(event.venue_id, 0)

                venue_score = (venue_likes - venue_dislikes * 2) / (total_venue_likes + 1)
                venue_score = max(0.0, min(1.0, venue_score))

            # === COMPONENT 4: Temporal Patterns (10%) ===
            from datetime import datetime

            temporal_score = 0.5  # Neutral default

            if event.starts_at:
                event_dow = event.starts_at.weekday()  # 0=Monday
                event_hour = event.starts_at.hour

                liked_dow = {}
                liked_hour = {}

                for e in liked_events:
                    if e.starts_at:
                        dow = e.starts_at.weekday()
                        hour = e.starts_at.hour
                        liked_dow[dow] = liked_dow.get(dow, 0) + 1
                        liked_hour[hour] = liked_hour.get(hour, 0) + 1

                if liked_dow:
                    total_dow = sum(liked_dow.values())
                    dow_affinity = liked_dow.get(event_dow, 0) / (total_dow + 1)

                    total_hour = sum(liked_hour.values())
                    hour_affinity = liked_hour.get(event_hour, 0) / (total_hour + 1)

                    temporal_score = (dow_affinity + hour_affinity) / 2

            # === COMPONENT 5: Random Noise (5%) - за tie-breaking ===
            # Детерминистичен noise based на event ID
            noise = (random.random() - 0.5) * 0.1  # ±5%

            # === FINAL WEIGHTED COMBINATION ===
            weights = {
                'gnn': 0.40,
                'tags': 0.30,
                'venue': 0.20,
                'temporal': 0.10
            }

            raw_score = (
                weights['gnn'] * gnn_score +
                weights['tags'] * tag_score +
                weights['venue'] * venue_score +
                weights['temporal'] * temporal_score
            )

            # Додај noise за tie-breaking
            raw_score += noise

            # === SCORE SCALING: Балансирана дистрибуција низ 0-1 range ===
            # Raw scores се обично 0.0-0.35, scaling до 0-1
            #
            # Користи power function 0.7 за balanced distribution:
            # - Не премногу агресивен како sqrt (0.5)
            # - Не премногу flat како linear (1.0)
            # - power=0.7 дава најдобра распределба

            if raw_score <= 0:
                final_score = 0.0
            else:
                # Power transformation со factor 2.5
                final_score = min(1.0, np.power(raw_score * 2.5, 0.7))

            return max(0.0, min(1.0, final_score))

        except Exception as e:
            logger.error(f"Грешка во personalized_score: {e}")
            # Fallback to GNN
            return self.gnn_similarity_score(user_id, event) * 0.5

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

        # Прво, пресметај ги сите PERSONALIZED scores за подобра нормализација
        all_gnn_scores = []
        for _, event in sorted_events:
            # Користи персонализиран score наместо само GNN embeddings
            pers_s = self.personalized_score(user_id, event)
            all_gnn_scores.append(pers_s)

        # Пресметај статистика за calibration
        if all_gnn_scores:
            mean_gnn = np.mean(all_gnn_scores)
            std_gnn = np.std(all_gnn_scores)
            max_gnn = np.max(all_gnn_scores)
        else:
            mean_gnn, std_gnn, max_gnn = 0.0, 1.0, 1.0

        # ВАЖНО: Пресметај процент врз основа на СИТЕ настани, не само топ limit!
        for rank, (score, event) in enumerate(sorted_events):
            # Користи ранг од СИТЕ настани за вистински процент (0-100%)
            score_pct = percentile_to_score(rank, total_candidates)

            # Пресметај ПЕРСОНАЛИЗИРАН скор (комбинира GNN + tags + category + venue + temporal)
            pers_score = self.personalized_score(user_id, event)
            trad_score = self.traditional_score(user_id, event)

            # ДИРЕКТНА КОНВЕРЗИЈА: personalized_score е веќе 0-1, само *100 за процент
            # Персонализираниот score веќе е калибриран и секој настан има уникатна вредност
            # затоа што комбинира многу фактори (GNN, tags, category, venue, temporal)
            #
            # Не користиме sigmoid нормализација овде - персонализираниот score
            # веќе одговара на "колку % би му одговарал овој настан"
            gnn_confidence = pers_score * 100.0
            gnn_confidence = round(max(0.0, min(100.0, gnn_confidence)), 1)

            recommendations.append({
                "event_id": event.id,
                "title": event.title,
                "description": event.description,
                "starts_at": event.starts_at.isoformat(),
                "venue_id": event.venue_id,
                "tags": (event.tags or "").split(","),
                "score_pct": score_pct,
                "gnn_score": gnn_confidence,
                "raw_score": round(score, 3),
                "rank": rank + 1
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