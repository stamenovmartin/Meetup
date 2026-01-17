"""Add performance indexes

Revision ID: 001_performance
Revises:
Create Date: 2026-01-08

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers
revision = '001_performance'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    """Add critical performance indexes"""

    # Attendance table indexes (MOST CRITICAL!)
    op.create_index(
        'ix_attendance_user_rating',
        'attendance',
        ['user_id', 'rating'],
        unique=False
    )
    op.create_index(
        'ix_attendance_event_rating',
        'attendance',
        ['event_id', 'rating'],
        unique=False
    )
    op.create_index(
        'ix_attendance_created_at',
        'attendance',
        ['created_at'],
        unique=False
    )

    # Events table indexes
    op.create_index(
        'ix_events_starts_at',
        'events',
        ['starts_at'],
        unique=False
    )
    op.create_index(
        'ix_events_venue_id',
        'events',
        ['venue_id'],
        unique=False
    )
    op.create_index(
        'ix_events_created_by',
        'events',
        ['created_by'],
        unique=False
    )

    # Friendship indexes
    op.create_index(
        'ix_friendships_requester',
        'friendships',
        ['requester_id', 'status'],
        unique=False
    )
    op.create_index(
        'ix_friendships_addressee',
        'friendships',
        ['addressee_id', 'status'],
        unique=False
    )

    # User indexes
    op.create_index(
        'ix_users_email',
        'users',
        ['email'],
        unique=True
    )
    op.create_index(
        'ix_users_city',
        'users',
        ['city'],
        unique=False
    )


def downgrade():
    """Remove indexes"""

    # Attendance
    op.drop_index('ix_attendance_user_rating', table_name='attendance')
    op.drop_index('ix_attendance_event_rating', table_name='attendance')
    op.drop_index('ix_attendance_created_at', table_name='attendance')

    # Events
    op.drop_index('ix_events_starts_at', table_name='events')
    op.drop_index('ix_events_venue_id', table_name='events')
    op.drop_index('ix_events_created_by', table_name='events')

    # Friendships
    op.drop_index('ix_friendships_requester', table_name='friendships')
    op.drop_index('ix_friendships_addressee', table_name='friendships')

    # Users
    op.drop_index('ix_users_email', table_name='users')
    op.drop_index('ix_users_city', table_name='users')
