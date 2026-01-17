"""
Tests for database models
"""
import pytest
from datetime import datetime, timedelta
from models.db_models import User, Event, Venue, Attendance


class TestUser:
    """Tests for User model"""

    def test_create_user(self, db_session):
        """Test creating a user"""
        user = User(
            email='newuser@example.com',
            name='New User',
            city='Skopje'
        )
        user.set_password('password123')

        db_session.add(user)
        db_session.commit()

        assert user.id is not None
        assert user.email == 'newuser@example.com'
        assert user.check_password('password123')

    def test_password_hashing(self, sample_user):
        """Test password is hashed"""
        assert sample_user.password_hash != 'password123'
        assert sample_user.check_password('password123')
        assert not sample_user.check_password('wrongpassword')

    def test_email_lowercase(self, db_session):
        """Test email is stored in lowercase"""
        user = User(
            email='TEST@EXAMPLE.COM',
            name='Test User'
        )
        user.set_password('password123')
        db_session.add(user)
        db_session.commit()

        assert user.email == 'test@example.com'


class TestEvent:
    """Tests for Event model"""

    def test_create_event(self, db_session, sample_user, sample_venue):
        """Test creating an event"""
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

        assert event.id is not None
        assert event.title == 'Test Event'
        assert event.venue_id == sample_venue.id

    def test_event_relationships(self, sample_event):
        """Test event relationships"""
        assert sample_event.venue is not None
        assert sample_event.venue.name == 'Test Venue'


class TestAttendance:
    """Tests for Attendance model"""

    def test_create_attendance(self, db_session, sample_user, sample_event):
        """Test creating attendance record"""
        attendance = Attendance(
            user_id=sample_user.id,
            event_id=sample_event.id,
            rating=1
        )

        db_session.add(attendance)
        db_session.commit()

        assert attendance.id is not None
        assert attendance.rating == 1

    def test_unique_attendance(self, db_session, sample_user, sample_event):
        """Test user can only attend event once"""
        attendance1 = Attendance(
            user_id=sample_user.id,
            event_id=sample_event.id,
            rating=1
        )
        db_session.add(attendance1)
        db_session.commit()

        # Try to add duplicate
        attendance2 = Attendance(
            user_id=sample_user.id,
            event_id=sample_event.id,
            rating=1
        )
        db_session.add(attendance2)

        with pytest.raises(Exception):  # IntegrityError
            db_session.commit()
