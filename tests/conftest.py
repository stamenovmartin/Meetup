"""
Pytest configuration and fixtures
"""
import pytest
import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from models.db_models import db, User, Event, Venue, Attendance
from config import TestingConfig
from flask import Flask


@pytest.fixture(scope='session')
def app():
    """Create Flask app for testing"""
    app = Flask(__name__)
    app.config.from_object(TestingConfig)

    # Initialize extensions
    db.init_app(app)

    with app.app_context():
        db.create_all()
        yield app
        db.drop_all()


@pytest.fixture(scope='function')
def client(app):
    """Create test client"""
    return app.test_client()


@pytest.fixture(scope='function')
def db_session(app):
    """Create database session for testing"""
    with app.app_context():
        # Clean up before test
        db.session.remove()
        db.drop_all()
        db.create_all()

        yield db.session

        # Clean up after test
        db.session.remove()


@pytest.fixture
def sample_user(db_session):
    """Create sample user"""
    user = User(
        email='test@example.com',
        name='Test User',
        city='Skopje'
    )
    user.set_password('password123')
    db_session.add(user)
    db_session.commit()
    return user


@pytest.fixture
def sample_venue(db_session):
    """Create sample venue"""
    venue = Venue(
        name='Test Venue',
        city='Skopje',
        lat=42.0,
        lon=21.4
    )
    db_session.add(venue)
    db_session.commit()
    return venue


@pytest.fixture
def sample_event(db_session, sample_user, sample_venue):
    """Create sample event"""
    from datetime import datetime, timedelta

    event = Event(
        title='Test Event',
        description='Test description',
        starts_at=datetime.utcnow() + timedelta(days=1),
        venue_id=sample_venue.id,
        created_by=sample_user.id,
        tags='test,event'
    )
    db_session.add(event)
    db_session.commit()
    return event
