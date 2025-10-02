# models/db_models.py
from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

# --- Users & Friends ---

class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    password_hash = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(120), nullable=False)
    city = db.Column(db.String(120))
    lat = db.Column(db.Float)   # optional: геолокација за препораки
    lon = db.Column(db.Float)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, raw: str):
        self.password_hash = generate_password_hash(raw)

    def check_password(self, raw: str) -> bool:
        return check_password_hash(self.password_hash, raw)

class Friendship(db.Model):
    __tablename__ = "friendships"
    id = db.Column(db.Integer, primary_key=True)
    requester_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    addressee_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    status = db.Column(db.String(20), default="pending")  # pending/accepted/blocked
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# --- Events & Venues ---

class Venue(db.Model):
    __tablename__ = "venues"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    city = db.Column(db.String(120))
    lat = db.Column(db.Float)
    lon = db.Column(db.Float)
    tags = db.Column(db.String(500))  # comma-separated tags

class Event(db.Model):
    __tablename__ = "events"
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    description = db.Column(db.Text)
    starts_at = db.Column(db.DateTime, nullable=False)
    venue_id = db.Column(db.Integer, db.ForeignKey("venues.id"), nullable=False)
    created_by = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    tags = db.Column(db.String(500))  # comma-separated (music, tech, meetup,...)

# --- Attendance & Ratings ---

class Attendance(db.Model):
    """
    User says he/she WENT to an event and rates it.
    rating: -1 (dislike), 0 (neutral/unknown), +1 (like)
    """
    __tablename__ = "attendance"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    event_id = db.Column(db.Integer, db.ForeignKey("events.id"), nullable=False, index=True)
    rating = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# --- Groups ---

class Group(db.Model):
    __tablename__ = "groups"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    owner_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class GroupMember(db.Model):
    __tablename__ = "group_members"
    id = db.Column(db.Integer, primary_key=True)
    group_id = db.Column(db.Integer, db.ForeignKey("groups.id"), nullable=False, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, index=True)
    role = db.Column(db.String(20), default="member")  # member/admin
    joined_at = db.Column(db.DateTime, default=datetime.utcnow)


# Додај ги овие табели во db_models.py

class UserProfile(db.Model):
    """Додатни профил информации"""
    __tablename__ = "user_profiles"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False, unique=True)
    bio = db.Column(db.Text)  # Кратка биографија
    profile_photo_url = db.Column(db.String(500))  # URL до слика
    cover_photo_url = db.Column(db.String(500))  # Cover слика
    website = db.Column(db.String(300))
    location = db.Column(db.String(200))
    interests = db.Column(db.String(500))  # comma-separated интереси
    is_public = db.Column(db.Boolean, default=True)  # Дали профилот е јавен
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Post(db.Model):
    """Social posts - што споделуваат корисниците"""
    __tablename__ = "posts"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    content = db.Column(db.Text, nullable=False)
    event_id = db.Column(db.Integer, db.ForeignKey("events.id"), nullable=True)  # Ако е поврзано со настан
    photo_url = db.Column(db.String(500))  # Опционална слика
    post_type = db.Column(db.String(50), default="text")  # text, photo, event_share
    likes_count = db.Column(db.Integer, default=0)
    comments_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PostLike(db.Model):
    """Лајкови на постови"""
    __tablename__ = "post_likes"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    post_id = db.Column(db.Integer, db.ForeignKey("posts.id"), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (db.UniqueConstraint('user_id', 'post_id'),)  # Еден лајк по корисник


class PostComment(db.Model):
    """Коментари на постови"""
    __tablename__ = "post_comments"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    post_id = db.Column(db.Integer, db.ForeignKey("posts.id"), nullable=False)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

# Додај и relations за полесно користење:
# Во User класата додај:
# profile = db.relationship('UserProfile', backref='user', uselist=False)
# posts = db.relationship('Post', backref='author', lazy='dynamic')

# Во Event класата додај:
# posts = db.relationship('Post', backref='event', lazy='dynamic')