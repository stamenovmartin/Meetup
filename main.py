# main.py
import os
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from flask import render_template, redirect, url_for, session, flash
from datetime import timedelta, datetime
from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_jwt_extended import (
    JWTManager, create_access_token, jwt_required, get_jwt_identity
)
from dotenv import load_dotenv
from scripts.ingest_csv import ingest_from_csv
from models.db_models import db, User, Friendship, Venue, Event, Attendance, Group, GroupMember
from models.recommender import recommend_for_user, get_recommender


def setup_logging(app):
    """Configure application logging"""
    log_level = os.getenv("LOG_LEVEL", "INFO")
    log_file = os.getenv("LOG_FILE", "logs/app.log")

    # Create logs directory if it doesn't exist
    log_dir = Path(log_file).parent
    log_dir.mkdir(parents=True, exist_ok=True)

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    ))
    file_handler.setLevel(getattr(logging, log_level))

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    ))
    console_handler.setLevel(getattr(logging, log_level))

    # Configure app logger
    app.logger.addHandler(file_handler)
    app.logger.addHandler(console_handler)
    app.logger.setLevel(getattr(logging, log_level))

    # Configure root logger for other modules
    logging.basicConfig(level=getattr(logging, log_level), handlers=[file_handler, console_handler])

    app.logger.info(f"Logging initialized at level {log_level}")


# ---------- App Factory ----------

def create_app():
    load_dotenv()
    app = Flask(__name__)

    # CORS configuration - allow frontend origins
    cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
    CORS(app, origins=cors_origins, supports_credentials=True)

    # Initialize logging
    setup_logging(app)

    # Get absolute path for SQLite database
    BASE_DIR = Path(__file__).resolve().parent
    default_db_path = BASE_DIR / "instance" / "meetup.db"
    default_db_uri = f"sqlite:///{default_db_path}"

    app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret")
    app.config["JWT_SECRET_KEY"] = os.getenv("JWT_SECRET_KEY", "dev-jwt")
    app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL", default_db_uri)
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(days=7)

    db.init_app(app)
    JWTManager(app)
    app.config["SESSION_COOKIE_HTTPONLY"] = True

    with app.app_context():
        db.create_all()

    # ---------- Auth ----------

    @app.post("/api/auth/register")
    def register():
        data = request.get_json() or {}
        email = data.get("email", "").strip().lower()
        password = data.get("password", "")
        name = data.get("name", "").strip() or email.split("@")[0]
        if not email or not password:
            return jsonify({"error": "email/password required"}), 400
        if User.query.filter_by(email=email).first():
            return jsonify({"error": "email already exists"}), 409
        u = User(email=email, name=name, city=data.get("city"), lat=data.get("lat"), lon=data.get("lon"))
        u.set_password(password)
        db.session.add(u)
        db.session.commit()
        return jsonify({"id": u.id, "email": u.email, "name": u.name})

    @app.post("/api/auth/login")
    def login():
        data = request.get_json() or {}
        email = data.get("email", "").strip().lower()
        password = data.get("password", "")
        u = User.query.filter_by(email=email).first()
        if not u or not u.check_password(password):
            return jsonify({"error": "invalid credentials"}), 401
        token = create_access_token(identity=str(u.id))
        return jsonify({"access_token": token, "user": {"id": u.id, "email": u.email, "name": u.name}})

    # ---------- UI session (simple) ----------
    @app.get("/")
    def home():
        uid = session.get("uid")
        user = User.query.get(uid) if uid else None
        return render_template("home.html", user=user)

    @app.get("/auth/login")
    def ui_login_get():
        return render_template("login.html")

    @app.post("/auth/login")
    def ui_login_post():
        data = request.form
        email = (data.get("email") or "").strip().lower()
        password = data.get("password") or ""
        u = User.query.filter_by(email=email).first()
        if not u or not u.check_password(password):
            flash("Invalid credentials", "danger")
            return redirect(url_for("ui_login_get"))
        session["uid"] = u.id
        flash("Welcome, " + u.name, "success")
        return redirect(url_for("events_list"))

    @app.get("/auth/register")
    def ui_register_get():
        return render_template("register.html")

    @app.post("/auth/register")
    def ui_register_post():
        data = request.form
        email = (data.get("email") or "").strip().lower()
        name = (data.get("name") or "").strip() or email.split("@")[0]
        password = data.get("password") or ""
        city = data.get("city") or None
        if not email or not password:
            flash("Email и лозинка се задолжителни", "danger")
            return redirect(url_for("ui_register_get"))
        if User.query.filter_by(email=email).first():
            flash("Овој email веќе постои", "warning")
            return redirect(url_for("ui_register_get"))
        u = User(email=email, name=name, city=city)
        u.set_password(password)
        db.session.add(u);
        db.session.commit()
        flash("Регистрацијата е успешна. Најави се.", "success")
        return redirect(url_for("ui_login_get"))

    @app.post("/auth/logout")
    def ui_logout():
        session.pop("uid", None)
        flash("Одјавен/а.", "info")
        return redirect(url_for("home"))

    # ---------- UI: Events ----------
    @app.get("/events")
    def events_list():
        uid = session.get("uid")
        user = User.query.get(uid) if uid else None
        q = Event.query.order_by(Event.starts_at.desc()).all()  # ОТСТРАНЕТ limit(100) - сега дава СЕ!
        # map attendance for current user
        likes = {}
        if user:
            rows = Attendance.query.filter_by(user_id=user.id).all()
            for r in rows: likes[r.event_id] = r.rating
        return render_template("events.html", user=user, events=q, likes=likes, Venue=Venue)

    @app.post("/events/attend")
    def ui_attend():
        uid = session.get("uid")
        if not uid:
            flash("Најави се за да оценуваш.", "warning")
            return redirect(url_for("ui_login_get"))
        event_id = int(request.form.get("event_id"))
        rating = int(request.form.get("rating", 0))
        rec = Attendance.query.filter_by(user_id=uid, event_id=event_id).first()
        if not rec:
            rec = Attendance(user_id=uid, event_id=event_id, rating=rating)
            db.session.add(rec)
        else:
            rec.rating = rating
        db.session.commit()
        return redirect(url_for("events_list"))

    # ---------- UI: Recommendations ----------
    @app.get("/recommend")
    def ui_recommend():
        uid = session.get("uid")
        user = User.query.get(uid) if uid else None
        if not user:
            flash("Најави се за да видиш препораки.", "warning")
            return redirect(url_for("ui_login_get"))

        # Користи ја новата GNN препорачка
        recommendations = recommend_for_user(user.id, limit=30)
        return render_template("recommend.html", user=user, recommendations=recommendations, Venue=Venue)

    # ---------- UI: Admin Ingest ----------
    @app.get("/admin/ingest")
    def ui_ingest_get():
        uid = session.get("uid")
        user = User.query.get(uid) if uid else None
        return render_template("admin_ingest.html", user=user)

    @app.post("/admin/ingest")
    def ui_ingest_post():
        uid = session.get("uid")
        user = User.query.get(uid) if uid else None
        if not user or user.email.lower() != "martin.stamenov03@gmail.com":
            flash("Admin only", "danger")
            return redirect(url_for("ui_ingest_get"))
        path = (request.form.get("path") or "").strip()
        limit = request.form.get("limit")
        limit = int(limit) if limit else None
        try:
            stats = ingest_from_csv(path, created_by_user_id=user.id, limit=limit)
            flash(f"Ingest OK: {stats}", "success")
        except Exception as e:
            flash(f"Ingest error: {e}", "danger")
        return redirect(url_for("ui_ingest_get"))

    # ---------- Friends ----------

    @app.post("/api/friends/request")
    @jwt_required()
    def friend_request():
        me = int(get_jwt_identity())
        data = request.get_json() or {}

        # Input validation
        user_id_raw = data.get("user_id")
        if user_id_raw is None:
            return jsonify({"error": "user_id is required"}), 400

        try:
            to_id = int(user_id_raw)
        except (ValueError, TypeError):
            return jsonify({"error": "user_id must be a valid integer"}), 400

        if me == to_id:
            return jsonify({"error": "cannot friend yourself"}), 400

        # Check if target user exists
        target_user = User.query.get(to_id)
        if not target_user:
            return jsonify({"error": "user not found"}), 404

        if Friendship.query.filter_by(requester_id=me, addressee_id=to_id).first():
            return jsonify({"status": "already requested"})

        fr = Friendship(requester_id=me, addressee_id=to_id, status="pending")
        db.session.add(fr)
        db.session.commit()
        return jsonify({"ok": True})

    @app.post("/api/friends/accept")
    @jwt_required()
    def friend_accept():
        me = int(get_jwt_identity())
        data = request.get_json() or {}

        # Input validation
        user_id_raw = data.get("user_id")
        if user_id_raw is None:
            return jsonify({"error": "user_id is required"}), 400

        try:
            from_id = int(user_id_raw)
        except (ValueError, TypeError):
            return jsonify({"error": "user_id must be a valid integer"}), 400

        fr = Friendship.query.filter_by(requester_id=from_id, addressee_id=me, status="pending").first()
        if not fr:
            return jsonify({"error": "no pending friend request found"}), 404

        fr.status = "accepted"
        db.session.commit()
        return jsonify({"ok": True})

    @app.get("/api/friends/list")
    @jwt_required()
    def friends_list():
        me = int(get_jwt_identity())
        # Најди сите accepted friendships каде user е или requester или addressee
        friends_as_requester = db.session.query(Friendship, User).join(
            User, Friendship.addressee_id == User.id
        ).filter(
            Friendship.requester_id == me,
            Friendship.status == "accepted"
        ).all()

        friends_as_addressee = db.session.query(Friendship, User).join(
            User, Friendship.requester_id == User.id
        ).filter(
            Friendship.addressee_id == me,
            Friendship.status == "accepted"
        ).all()

        friends = []
        for _, user in friends_as_requester + friends_as_addressee:
            friends.append({
                "id": user.id,
                "name": user.name,
                "email": user.email,
                "city": user.city
            })

        return jsonify(friends)

    @app.get("/api/friends/pending")
    @jwt_required()
    def pending_friend_requests():
        """Get pending friend requests (people who want to be friends with me)"""
        me = int(get_jwt_identity())

        pending_requests = db.session.query(Friendship, User).join(
            User, Friendship.requester_id == User.id
        ).filter(
            Friendship.addressee_id == me,
            Friendship.status == "pending"
        ).all()

        result = []
        for friendship, user in pending_requests:
            result.append({
                "requester_id": user.id,
                "requester_name": user.name,
                "requester_email": user.email,
                "requester_city": user.city,
                "requested_at": friendship.created_at.isoformat()
            })

        return jsonify(result)

    @app.get("/api/users/search")
    @jwt_required()
    def search_users():
        me = int(get_jwt_identity())
        query = request.args.get("q", "").strip()

        users_query = User.query.filter(User.id != me)

        if query:
            users_query = users_query.filter(
                db.or_(
                    User.name.ilike(f"%{query}%"),
                    User.email.ilike(f"%{query}%")
                )
            )

        users = users_query.limit(20).all()

        result = []
        for user in users:
            friendship = Friendship.query.filter(
                db.or_(
                    db.and_(Friendship.requester_id == me, Friendship.addressee_id == user.id),
                    db.and_(Friendship.requester_id == user.id, Friendship.addressee_id == me)
                )
            ).first()

            friendship_status = "none"
            if friendship:
                if friendship.status == "accepted":
                    friendship_status = "friends"
                elif friendship.requester_id == me:
                    friendship_status = "requested"
                else:
                    friendship_status = "pending"

            result.append({
                "id": user.id,
                "name": user.name,
                "email": user.email,
                "city": user.city,
                "friendship_status": friendship_status
            })

        return jsonify(result)

    # ---------- Venues & Events ----------

    @app.post("/api/venues")
    @jwt_required()
    def create_venue():
        data = request.get_json() or {}
        v = Venue(name=data["name"], city=data.get("city"),
                  lat=data.get("lat"), lon=data.get("lon"),
                  tags=",".join(data.get("tags", [])))
        db.session.add(v);
        db.session.commit()
        return jsonify({"id": v.id})

    @app.get("/api/venues")
    def list_venues():
        q = Venue.query.all()
        return jsonify(
            [{"id": v.id, "name": v.name, "city": v.city, "lat": v.lat, "lon": v.lon, "tags": (v.tags or "").split(",")}
             for v in q])

    @app.post("/api/events")
    @jwt_required()
    def create_event():
        me = int(get_jwt_identity())
        data = request.get_json() or {}

        # Validate required fields
        if not data.get("title"):
            return jsonify({"error": "title is required"}), 400

        if not data.get("starts_at"):
            return jsonify({"error": "starts_at is required"}), 400

        if not data.get("venue_id"):
            return jsonify({"error": "venue_id is required"}), 400

        # Validate venue exists
        try:
            venue_id = int(data["venue_id"])
        except (ValueError, TypeError):
            return jsonify({"error": "venue_id must be a valid integer"}), 400

        venue = Venue.query.get(venue_id)
        if not venue:
            return jsonify({"error": "venue not found"}), 404

        # Validate date format
        try:
            starts_at = datetime.fromisoformat(data["starts_at"])
        except ValueError:
            return jsonify({"error": "starts_at must be a valid ISO date format"}), 400

        e = Event(
            title=data["title"],
            description=data.get("description"),
            starts_at=starts_at,
            venue_id=venue_id,
            created_by=me,
            tags=",".join(data.get("tags", []))
        )
        db.session.add(e)
        db.session.commit()
        return jsonify({"id": e.id})

    @app.get("/api/events")
    def list_events():
        # Филтри
        start = request.args.get("start")  # YYYY-MM-DD
        end = request.args.get("end")
        city = request.args.get("city")
        tags = request.args.get("tags")  # comma
        q_text = request.args.get("q")
        sort = request.args.get("sort", "starts_at")  # starts_at|title
        order = request.args.get("order", "asc")  # asc|desc

        # Пагинација (опционално)
        page = request.args.get("page", type=int)
        page_size = request.args.get("page_size", type=int)

        q = Event.query

        from datetime import datetime
        if start:
            q = q.filter(Event.starts_at >= datetime.fromisoformat(start + "T00:00:00"))
        if end:
            q = q.filter(Event.starts_at <= datetime.fromisoformat(end + "T23:59:59"))

        if city:
            q = q.join(Venue, Event.venue_id == Venue.id).filter(Venue.city.ilike(f"%{city}%"))

        if tags:
            tag_list = [t.strip().lower() for t in tags.split(",") if t.strip()]
            for t in tag_list:
                q = q.filter(Event.tags.ilike(f"%{t}%"))

        if q_text:
            like = f"%{q_text}%"
            q = q.filter(Event.title.ilike(like))

        col = Event.starts_at if sort == "starts_at" else Event.title
        q = q.order_by(col.asc() if order == "asc" else col.desc())

        # Додај my_rating ако е JWT authenticated
        me = None
        try:
            if request.headers.get('Authorization'):
                from flask_jwt_extended import verify_jwt_in_request, get_jwt_identity
                verify_jwt_in_request(optional=True)
                me = int(get_jwt_identity()) if get_jwt_identity() else None
        except Exception as e:
            app.logger.debug(f"Optional JWT verification failed: {e}")
            pass

        # Ако има page/page_size → врати страници; ако не → врати СЕ
        if page and page_size:
            total = q.count()
            items = q.offset((page - 1) * page_size).limit(page_size).all()
        else:
            items = q.all()
            total = len(items)

        # Debug logging
        app.logger.debug(f"/api/events: Returning {total} events (filters: start={start}, end={end}, city={city}, tags={tags})")

        # Додај my_rating за секој event
        event_ratings = {}
        if me:
            ratings = Attendance.query.filter_by(user_id=me).all()
            event_ratings = {r.event_id: r.rating for r in ratings}

        # Пресметај препорачувачки скорови за СИТЕ настани (ако е логиран)
        event_scores = {}
        if me:
            from models.recommender import get_recommender
            recommender = get_recommender()
            # Пресметај скорови за СИТЕ кандидати
            all_scores = []
            for e in items:
                score = recommender.combined_score(me, e)
                all_scores.append((score, e.id))

            # Сортирај по скор
            all_scores.sort(reverse=True)

            # Користи перцентил за нормализација
            total = len(all_scores)
            for rank, (score, event_id) in enumerate(all_scores):
                if total == 1:
                    score_pct = 100.0
                else:
                    score_pct = round(100.0 * (1.0 - rank / (total - 1)), 1)
                event_scores[event_id] = score_pct

        result_items = []
        for e in items:
            item_data = {
                "id": e.id,
                "title": e.title,
                "description": e.description,
                "starts_at": e.starts_at.isoformat(),
                "venue_id": e.venue_id,
                "tags": (e.tags or "").split(","),
                "my_rating": event_ratings.get(e.id, 0) if me else None
            }
            # Додај recommendation score ако е логиран
            if me and e.id in event_scores:
                item_data["score_pct"] = event_scores[e.id]
            result_items.append(item_data)

        # Сортирај по score ако корисникот е логиран (најдобри прво)
        if me and event_scores:
            result_items.sort(key=lambda x: x.get("score_pct", 0), reverse=True)
            scores = [x.get("score_pct", 0) for x in result_items]
            app.logger.debug(f"Score distribution: min={min(scores)}%, max={max(scores)}%, count={len(scores)}")

        if page and page_size:
            return jsonify({
                "total": total,
                "page": page,
                "page_size": page_size,
                "items": result_items
            })
        else:
            return jsonify(result_items)

    # ---------- Attendance & Ratings ----------

    @app.post("/api/attend")
    @jwt_required()
    def attend_and_rate():
        me = int(get_jwt_identity())
        data = request.get_json() or {}

        # Input validation
        event_id_raw = data.get("event_id")
        if event_id_raw is None:
            return jsonify({"error": "event_id is required"}), 400

        try:
            event_id = int(event_id_raw)
        except (ValueError, TypeError):
            return jsonify({"error": "event_id must be a valid integer"}), 400

        # Check if event exists
        event = Event.query.get(event_id)
        if not event:
            return jsonify({"error": "event not found"}), 404

        # Validate rating
        try:
            rating = int(data.get("rating", 0))
        except (ValueError, TypeError):
            return jsonify({"error": "rating must be an integer"}), 400

        if rating not in [-1, 0, 1]:
            return jsonify({"error": "rating must be -1, 0, or 1"}), 400

        rec = Attendance.query.filter_by(user_id=me, event_id=event_id).first()
        if not rec:
            rec = Attendance(user_id=me, event_id=event_id, rating=rating)
            db.session.add(rec)
        else:
            rec.rating = rating
        db.session.commit()
        return jsonify({"ok": True})

    # ---------- Groups ----------

    @app.post("/api/groups")
    @jwt_required()
    def create_group():
        me = int(get_jwt_identity())
        data = request.get_json() or {}
        g = Group(name=data["name"], owner_id=me)
        db.session.add(g);
        db.session.commit()
        # owner becomes member
        gm = GroupMember(group_id=g.id, user_id=me, role="admin")
        db.session.add(gm);
        db.session.commit()
        return jsonify({"id": g.id})

    @app.post("/api/groups/add")
    @jwt_required()
    def add_to_group():
        me = int(get_jwt_identity())
        data = request.get_json() or {}
        group_id = int(data["group_id"]);
        user_id = int(data["user_id"])
        # (опционално: провери дали me е admin на групата)
        if not GroupMember.query.filter_by(group_id=group_id, user_id=user_id).first():
            db.session.add(GroupMember(group_id=group_id, user_id=user_id))
            db.session.commit()
        return jsonify({"ok": True})

    # ---------- Enhanced Recommendations with GNN ----------

    @app.get("/api/recommend/me")
    @jwt_required()
    def recommend_for_me():
        """Подобрени препораки со GNN integration - сега враќа СЕ настани со скорови!"""
        me = int(get_jwt_identity())
        start = request.args.get("start")
        end = request.args.get("end")
        # ПРОМЕНЕТО: Default limit е 10000 за да земе СЕ
        limit = int(request.args.get("limit", 10000))

        try:
            recommendations = recommend_for_user(me, start, end, limit)
            return jsonify(recommendations)
        except Exception as e:
            # Fallback на традиционални препораки ако GNN не работи
            app.logger.error(f"GNN recommendations failed: {e}")
            return jsonify({"error": "Recommendations temporarily unavailable"}), 500

    # Додај ги овие endpoints во main.py после постоечките

    # ---------- Profile & Social Features ----------

    @app.get("/api/user/profile/<int:user_id>")
    @jwt_required()
    def get_user_profile(user_id):
        """Земи профил на корисник"""
        user = User.query.get_or_404(user_id)

        # Статистики
        total_attended = Attendance.query.filter_by(user_id=user_id).count()
        total_liked = Attendance.query.filter_by(user_id=user_id, rating=1).count()

        # Последни активности
        recent_activities = db.session.query(Attendance, Event).join(
            Event, Attendance.event_id == Event.id
        ).filter(Attendance.user_id == user_id).order_by(
            Attendance.created_at.desc()
        ).limit(10).all()

        activities = []
        for attendance, event in recent_activities:
            activities.append({
                "event_title": event.title,
                "rating": attendance.rating,
                "date": attendance.created_at.isoformat(),
                "event_date": event.starts_at.isoformat()
            })

        return jsonify({
            "id": user.id,
            "name": user.name,
            "email": user.email,
            "city": user.city,
            "stats": {
                "total_attended": total_attended,
                "total_liked": total_liked
            },
            "recent_activities": activities
        })

    @app.get("/api/user/favorites")
    @jwt_required()
    def get_favorites():
        """Земи листа на омилени настани (лајкани)"""
        me = int(get_jwt_identity())

        favorites = db.session.query(Attendance, Event, Venue).join(
            Event, Attendance.event_id == Event.id
        ).join(
            Venue, Event.venue_id == Venue.id
        ).filter(
            Attendance.user_id == me,
            Attendance.rating == 1
        ).order_by(Event.starts_at.desc()).all()

        result = []
        for attendance, event, venue in favorites:
            result.append({
                "id": event.id,
                "title": event.title,
                "description": event.description,
                "starts_at": event.starts_at.isoformat(),
                "venue": {"name": venue.name, "city": venue.city},
                "tags": (event.tags or "").split(","),
                "liked_at": attendance.created_at.isoformat()
            })

        return jsonify(result)

    @app.get("/api/user/attended")
    @jwt_required()
    def get_attended():
        """Земи листа на настани каде што сум бил"""
        me = int(get_jwt_identity())

        attended = db.session.query(Attendance, Event, Venue).join(
            Event, Attendance.event_id == Event.id
        ).join(
            Venue, Event.venue_id == Venue.id
        ).filter(
            Attendance.user_id == me
        ).order_by(Event.starts_at.desc()).all()

        result = []
        for attendance, event, venue in attended:
            result.append({
                "id": event.id,
                "title": event.title,
                "description": event.description,
                "starts_at": event.starts_at.isoformat(),
                "venue": {"name": venue.name, "city": venue.city},
                "tags": (event.tags or "").split(","),
                "my_rating": attendance.rating,
                "attended_at": attendance.created_at.isoformat()
            })

        return jsonify(result)

    @app.get("/api/feed")
    @jwt_required()
    def social_feed():
        """Social feed - што прават пријателите"""
        me = int(get_jwt_identity())

        # Најди пријатели
        friends_as_requester = db.session.query(Friendship.addressee_id).filter(
            Friendship.requester_id == me,
            Friendship.status == "accepted"
        ).subquery()

        friends_as_addressee = db.session.query(Friendship.requester_id).filter(
            Friendship.addressee_id == me,
            Friendship.status == "accepted"
        ).subquery()

        # Union на сите пријатели
        friend_ids_query = db.session.query(friends_as_requester.c.addressee_id).union(
            db.session.query(friends_as_addressee.c.requester_id)
        )
        friend_ids = [row[0] for row in friend_ids_query.all()]

        if not friend_ids:
            return jsonify([])

        # Активности на пријатели
        activities = db.session.query(Attendance, Event, Venue, User).join(
            Event, Attendance.event_id == Event.id
        ).join(
            Venue, Event.venue_id == Venue.id
        ).join(
            User, Attendance.user_id == User.id
        ).filter(
            Attendance.user_id.in_(friend_ids)
        ).order_by(Attendance.created_at.desc()).limit(50).all()

        feed = []
        for attendance, event, venue, user in activities:
            action = "liked" if attendance.rating == 1 else "attended" if attendance.rating == 0 else "disliked"

            feed.append({
                "user": {"name": user.name, "id": user.id},
                "action": action,
                "event": {
                    "id": event.id,
                    "title": event.title,
                    "starts_at": event.starts_at.isoformat(),
                    "venue": venue.name
                },
                "timestamp": attendance.created_at.isoformat()
            })

        return jsonify(feed)

    # ---------- Enhanced Events API ----------

    @app.patch("/api/events/<int:event_id>/toggle-favorite")
    @jwt_required()
    def toggle_favorite(event_id):
        """Toggle favorite статус на настан (лајк/unlајк)"""
        me = int(get_jwt_identity())

        attendance = Attendance.query.filter_by(user_id=me, event_id=event_id).first()

        if not attendance:
            # Прв пат - додај лајк
            attendance = Attendance(user_id=me, event_id=event_id, rating=1)
            db.session.add(attendance)
            is_favorite = True
        else:
            # Toggle rating
            if attendance.rating == 1:
                attendance.rating = 0  # Remove like
                is_favorite = False
            else:
                attendance.rating = 1  # Add like
                is_favorite = True

        db.session.commit()

        return jsonify({
            "is_favorite": is_favorite,
            "rating": attendance.rating
        })

    @app.post("/api/events/<int:event_id>/rate")
    @jwt_required()
    def rate_event(event_id):
        """Оцени настан (like=1, dislike=-1, neutral=0)"""
        me = int(get_jwt_identity())
        data = request.get_json() or {}
        rating = int(data.get("rating", 0))

        # Валидација: дозволи само -1, 0, 1
        if rating not in [-1, 0, 1]:
            return jsonify({"error": "Rating must be -1, 0, or 1"}), 400

        attendance = Attendance.query.filter_by(user_id=me, event_id=event_id).first()

        if not attendance:
            attendance = Attendance(user_id=me, event_id=event_id, rating=rating)
            db.session.add(attendance)
        else:
            attendance.rating = rating

        db.session.commit()

        return jsonify({
            "success": True,
            "rating": rating,
            "event_id": event_id
        })

    @app.patch("/api/events/<int:event_id>/toggle-attended")
    @jwt_required()
    def toggle_attended(event_id):
        """Toggle attended статус"""
        me = int(get_jwt_identity())

        attendance = Attendance.query.filter_by(user_id=me, event_id=event_id).first()

        if not attendance:
            # Означи дека сум бил
            attendance = Attendance(user_id=me, event_id=event_id, rating=0)
            db.session.add(attendance)
            has_attended = True
        else:
            # Ако веќе постои запис, само toggle
            # Можеби сакаш да го избришеш целосно или да го смениш rating-от
            db.session.delete(attendance)
            has_attended = False

        db.session.commit()

        return jsonify({
            "has_attended": has_attended
        })

    @app.get("/api/stats/dashboard")
    @jwt_required()
    def dashboard_stats():
        """Статистики за dashboard"""
        me = int(get_jwt_identity())

        # Мои статистики
        my_favorites = Attendance.query.filter_by(user_id=me, rating=1).count()
        my_attended = Attendance.query.filter_by(user_id=me).count()

        # Вкупно во системот
        total_events = Event.query.count()
        total_users = User.query.count()

        # Топ категории во мои лајкови
        liked_events = db.session.query(Event.tags).join(
            Attendance, Event.id == Attendance.event_id
        ).filter(Attendance.user_id == me, Attendance.rating == 1).all()

        tag_counts = {}
        for (tags,) in liked_events:
            if tags:
                for tag in tags.split(","):
                    tag = tag.strip()
                    if tag:
                        tag_counts[tag] = tag_counts.get(tag, 0) + 1

        top_categories = sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        return jsonify({
            "my_stats": {
                "favorites": my_favorites,
                "attended": my_attended,
                "top_categories": top_categories
            },
            "global_stats": {
                "total_events": total_events,
                "total_users": total_users
            }
        })
    @app.post("/api/recommend/group")
    @jwt_required()
    def recommend_for_group():
        """
        Групни препораки со нормализиран процент - СИТ Е НАСТАНИ (минати и идни)
        """
        data = request.get_json() or {}
        user_ids = list(map(int, data.get("user_ids", [])))

        if not user_ids:
            return jsonify([])

        # ОТСТРАНЕНО: филтрирање по датум - сега земаме СЕ
        # Земи ги сите настани, не само идните
        candidates = Event.query.all()  # ОТСТРАНЕТ limit(2000) - сега СЕ!

        if not candidates:
            return jsonify([])

        # Користи персонализиран GNN препорачувач за секој корисник
        recommender = get_recommender()
        group_scores = []
        all_personalized_scores_for_events = {}

        for event in candidates:
            total_score = 0
            total_personalized = 0
            for user_id in user_ids:
                total_score += recommender.combined_score(user_id, event)
                # ВАЖНО: користи персонализиран score наместо само GNN
                pers_score = recommender.personalized_score(user_id, event)
                total_personalized += pers_score

            avg_score = total_score / len(user_ids)
            avg_personalized = total_personalized / len(user_ids)
            group_scores.append((avg_score, event))
            all_personalized_scores_for_events[event.id] = avg_personalized

        # Сортирај и форматирај - Директно конвертираме персонализирани scores во проценти
        result = []
        for score, event in sorted(group_scores, key=lambda x: x[0], reverse=True)[:20]:
            # Персонализиран score е веќе 0-1, само *100 за процент
            avg_personalized = all_personalized_scores_for_events[event.id]
            personalized_pct = round(avg_personalized * 100.0, 1)

            result.append({
                "event_id": event.id,
                "title": event.title,
                "description": event.description,
                "starts_at": event.starts_at.isoformat(),
                "venue_id": event.venue_id,
                "tags": (event.tags or "").split(","),
                "gnn_score": personalized_pct,  # Персонализиран групен score
                "raw_score": round(score, 3)
            })

        return jsonify(result)

    @app.post("/api/admin/ingest/file")
    @jwt_required()
    def admin_ingest_file():
        """
        JSON: {"path": "C:\\\\Users\\\\...\\\\events_gnn_ready.csv", "limit": 500}
        Само admin (пример user id=1). Прилагоди услов ако треба.
        """
        me = int(get_jwt_identity())
        user = User.query.get(me)
        if not user or user.email.lower() != "martin.stamenov03@gmail.com":
            return jsonify({"error": "admin only"}), 403
        data = request.get_json() or {}
        path = data.get("path")
        if not path:
            return jsonify({"error": "path required"}), 400
        limit = data.get("limit")
        try:
            stats = ingest_from_csv(path, created_by_user_id=me, limit=limit)
            return jsonify(stats)
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @app.get("/api/health")
    def health():
        return {"ok": True}

    @app.get("/api/stats")
    def stats():
        """API за статистики"""
        total_events = Event.query.count()
        total_venues = Venue.query.count()
        total_users = User.query.count()
        total_ratings = Attendance.query.count()

        return jsonify({
            "events": total_events,
            "venues": total_venues,
            "users": total_users,
            "ratings": total_ratings
        })

    return app


# Create app instance for gunicorn (production)
app = create_app()

# ---------- Entrypoint ----------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 5000)), debug=True)