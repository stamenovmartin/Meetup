"""
Tests for GNN Recommender
"""
import pytest
import numpy as np
from models.recommender_improved import ImprovedGNNRecommender


class TestRecommender:
    """Tests for Recommender System"""

    @pytest.fixture
    def recommender(self):
        """Create recommender instance"""
        return ImprovedGNNRecommender()

    def test_traditional_score(self, recommender, db_session, sample_user, sample_event):
        """Test traditional scoring"""
        score = recommender.traditional_score(sample_user.id, sample_event)
        assert isinstance(score, (int, float))

    def test_gnn_score_without_graph(self, recommender, db_session, sample_user, sample_event):
        """Test GNN scoring without loaded graph"""
        score = recommender.gnn_similarity_score(sample_user.id, sample_event)
        assert score == 0.0  # Should return 0 if no graph

    def test_combined_score(self, recommender, db_session, sample_user, sample_event):
        """Test combined scoring"""
        score = recommender.combined_score(sample_user.id, sample_event)
        assert isinstance(score, (int, float))
        assert 0 <= score <= 1  # Normalized score

    def test_normalize_score(self, recommender):
        """Test score normalization"""
        recommender.trad_score_stats = {'min': 0, 'max': 10, 'mean': 5}

        assert recommender.normalize_score(0, 'traditional') == 0.0
        assert recommender.normalize_score(10, 'traditional') == 1.0
        assert recommender.normalize_score(5, 'traditional') == 0.5

    def test_recommend_events(self, recommender, db_session, sample_user, sample_event):
        """Test event recommendation"""
        recommendations = recommender.recommend_events(sample_user.id, limit=10)

        assert isinstance(recommendations, list)
        if len(recommendations) > 0:
            assert 'event_id' in recommendations[0]
            assert 'score_pct' in recommendations[0]
