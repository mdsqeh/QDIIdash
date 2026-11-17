"""SQLite helpers for the wealth module (same file as the QDII dashboard)."""
import os
import sqlite3
from datetime import datetime, timedelta, timezone

from flask import current_app, g

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.environ.get('DATA_DIR', PROJECT_DIR)
DB_PATH = os.environ.get('WM_DB_PATH') or os.path.join(DATA_DIR, 'qdii.db')

SHANGHAI_TZ = timezone(timedelta(hours=8))


def now_iso():
    return datetime.now(SHANGHAI_TZ).isoformat(timespec='seconds')


def today_str():
    return datetime.now(SHANGHAI_TZ).date().isoformat()


def iso(value):
    return value.isoformat() if hasattr(value, 'isoformat') else str(value)


def connect(path=None):
    path = path or DB_PATH
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


def get_db():
    if 'wm_db' not in g:
        g.wm_db = connect(current_app.config.get('WM_DB_PATH') or DB_PATH)
    return g.wm_db


def close_db(exception=None):
    db = g.pop('wm_db', None)
    if db is not None:
        db.close()
