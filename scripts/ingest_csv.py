# scripts/ingest_csv.py
import os
import ast
import pandas as pd
from datetime import datetime
from dateutil import parser as dtparser

from models.db_models import db, Venue, Event

# Алијаси за колоните (case-insensitive)
ALIASES = {
    "title":       ["title", "event_title", "name"],
    "datetime":    ["start_datetime", "starts_at", "datetime"],
    "date":        ["date", "date_start", "start_date"],
    "time":        ["time", "time_start", "start_time", "hour"],
    "venue":       ["venue", "location", "place", "loc", "where"],
    "city":        ["city", "city_name", "grad"],
    "tags":        ["tags", "categories", "genres", "labels"],
    "description": ["description", "desc", "details", "long_text"],
    "url":         ["url", "link"],
}

DEFAULT_TIME = (19, 0)  # ако има само датум, става 19:00

def _col(df, key):
    """ најди ја првата постоечка колона според алијаси (case-insensitive) """
    low = {c.lower(): c for c in df.columns}
    for alias in ALIASES[key]:
        if alias.lower() in low:
            return low[alias.lower()]
    return None

def _parse_tags(val):
    if pd.isna(val):
        return ""
    s = str(val).strip()
    # ако личи на Python листа
    if s.startswith("[") and s.endswith("]"):
        try:
            arr = ast.literal_eval(s)
            arr = [str(x).strip().lower() for x in arr if str(x).strip()]
            return ",".join(sorted(set(arr)))
        except Exception:
            pass
    # иначе, дели по запирка/точка-запирка
    parts = [p.strip().lower() for p in s.replace(";", ",").split(",") if p.strip()]
    return ",".join(sorted(set(parts)))

def _parse_when(row, col_datetime, col_date, col_time):
    """ враќа datetime или None """
    if col_datetime and not pd.isna(row[col_datetime]):
        s = str(row[col_datetime]).replace("*", "").strip()
        try:
            return dtparser.parse(s, dayfirst=False)  # ќе улови ISO, RFC, и сл.
        except Exception:
            pass
    # пробај date + time
    d, t = None, None
    if col_date and not pd.isna(row[col_date]):
        s = str(row[col_date]).replace("*", "").strip()
        try:
            # поддржи 'YYYY-MM-DD' и 'DD.MM.YYYY' (auto)
            d = dtparser.parse(s, dayfirst=True).date()
        except Exception:
            pass
    if col_time and not pd.isna(row[col_time]):
        s = str(row[col_time]).strip()
        try:
            tdt = dtparser.parse(s)
            t = (tdt.hour, tdt.minute)
        except Exception:
            pass
    if d:
        if not t:
            t = DEFAULT_TIME
        return datetime(d.year, d.month, d.day, t[0], t[1])
    return None

def _get_or_create_venue(name, city=None):
    name = (name or "Unknown Venue").strip()
    v = Venue.query.filter_by(name=name).first()
    if v:
        return v
    v = Venue(name=name, city=(city or None), lat=None, lon=None, tags=None)
    db.session.add(v)
    db.session.flush()
    return v

def ingest_from_csv(csv_path: str, created_by_user_id: int = 1, limit: int | None = None):
    """
    Универзален увоз: чита CSV и мапира на Event/Venue.
    Уникатност по (title, starts_at, venue_id).
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    df = pd.read_csv(csv_path)
    if limit:
        df = df.head(limit)

    # најди колони
    col_title = _col(df, "title")
    if not col_title:
        raise ValueError("CSV нема препознатлива 'title' колона")
    col_dt    = _col(df, "datetime")
    col_date  = _col(df, "date")
    col_time  = _col(df, "time")
    col_venue = _col(df, "venue")
    col_city  = _col(df, "city")
    col_tags  = _col(df, "tags")
    col_desc  = _col(df, "description")
    col_url   = _col(df, "url")

    total, created, skipped = 0, 0, 0
    for _, row in df.iterrows():
        total += 1
        title = str(row[col_title]).strip() if not pd.isna(row[col_title]) else ""
        if not title:
            skipped += 1
            continue

        starts_at = _parse_when(row, col_dt, col_date, col_time)
        if not starts_at:
            skipped += 1
            continue

        venue_name = str(row[col_venue]).strip() if (col_venue and not pd.isna(row[col_venue])) else "Unknown Venue"
        city = str(row[col_city]).strip() if (col_city and not pd.isna(row[col_city])) else None
        v = _get_or_create_venue(venue_name, city)

        # дупликат?
        exists = Event.query.filter_by(title=title, starts_at=starts_at, venue_id=v.id).first()
        if exists:
            continue

        tags = _parse_tags(row[col_tags]) if col_tags else ""
        desc = str(row[col_desc]).strip() if (col_desc and not pd.isna(row[col_desc])) else None

        e = Event(
            title=title,
            description=desc,
            starts_at=starts_at,
            venue_id=v.id,
            created_by=created_by_user_id,
            tags=tags
        )
        db.session.add(e)
        created += 1

    db.session.commit()
    return {"total_rows_seen": int(total), "created_events": int(created), "skipped_rows": int(skipped)}
