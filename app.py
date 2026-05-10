"""
QDII 基金数据看板 — Flask + SQLite Web 应用

启动：
    pip install flask akshare pandas apscheduler
    python qdii/app.py

数据每天 21:00（北京时间）自动采集一次。
"""
import json
import sqlite3
import threading
import os
from datetime import datetime, timezone

from flask import Flask, g, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler

from fetcher import fetch_all

app = Flask(__name__)

# ── 数据库路径（放在 qdii 目录下） ──────────────────────────────
DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DB_DIR, 'qdii.db')


def get_db():
    """获取当前请求的数据库连接（每个请求自动关闭）。"""
    if 'db' not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(exception):
    db = g.pop('db', None)
    if db is not None:
        db.close()


def init_db():
    """初始化数据库表结构。"""
    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS funds (
                code        TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                ftype       TEXT,
                market      TEXT DEFAULT '场外',
                nav         TEXT,
                acc_nav     TEXT,
                ret_1y      REAL,
                ret_2y      REAL,
                ret_3y      REAL,
                ret_4y      REAL,
                ret_5y      REAL,
                ret_10y     REAL,
                ret_ann     REAL,
                total_ret   REAL,
                est_date    TEXT,
                mgmt_fee    REAL,
                cust_fee    REAL,
                sale_fee    REAL,
                purchase_status TEXT,
                daily_limit REAL,
                upd_date    TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fetch_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                status      TEXT NOT NULL,
                started_at  TEXT NOT NULL,
                finished_at TEXT,
                total_count INTEGER,
                error_msg   TEXT
            );
        """)

    # 迁移：给已有数据库加新列
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("ALTER TABLE funds ADD COLUMN market TEXT DEFAULT '场外'")
    except Exception:
        pass
    for col in ['mgmt_fee', 'cust_fee', 'sale_fee', 'total_ret', 'ret_2y', 'ret_4y', 'ret_5y', 'ret_10y', 'daily_limit']:
        try:
            with sqlite3.connect(DB_PATH) as conn:
                conn.execute(f"ALTER TABLE funds ADD COLUMN {col} REAL")
        except Exception:
            pass
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("ALTER TABLE funds ADD COLUMN purchase_status TEXT")
    except Exception:
        pass


# ── 数据采集（后台线程） ─────────────────────────────────────────

_fetch_lock = threading.Lock()


def _do_fetch():
    """后台执行数据采集，完成后写入 SQLite。"""
    if not _fetch_lock.acquire(blocking=False):
        return  # 已有采集在运行

    now = datetime.now(timezone.utc).isoformat()
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO fetch_log (status, started_at) VALUES (?, ?)",
                ('running', now)
            )
            conn.commit()

        records = fetch_all()

        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("DELETE FROM funds")
            conn.executemany(
                """INSERT INTO funds
                   (code, name, ftype, market, nav, acc_nav, ret_1y, ret_2y, ret_3y, ret_4y, ret_5y, ret_10y, ret_ann, total_ret, est_date, mgmt_fee, cust_fee, sale_fee, purchase_status, daily_limit, upd_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                [(
                    r['code'], r['name'], r.get('ftype'),
                    r.get('market', '场外'),
                    r.get('nav'), r.get('acc_nav'),
                    r.get('ret_1y'), r.get('ret_2y'), r.get('ret_3y'),
                    r.get('ret_4y'), r.get('ret_5y'), r.get('ret_10y'),
                    r.get('ret_ann'),
                    r.get('total_ret'),
                    r.get('est_date'),
                    r.get('mgmt_fee'), r.get('cust_fee'), r.get('sale_fee'),
                    r.get('purchase_status'), r.get('daily_limit'),
                    r.get('upd_date', ''),
                ) for r in records]
            )
            conn.execute(
                """UPDATE fetch_log
                   SET status='done', finished_at=?, total_count=?
                   WHERE status='running'""",
                (datetime.now(timezone.utc).isoformat(), len(records))
            )
            conn.commit()
    except Exception as e:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                """UPDATE fetch_log
                   SET status='failed', finished_at=?, error_msg=?
                   WHERE status='running'""",
                (datetime.now(timezone.utc).isoformat(), str(e))
            )
            conn.commit()
    finally:
        _fetch_lock.release()


# ── Flask 路由 ──────────────────────────────────────────────────


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/funds')
def api_funds():
    """返回基金列表，支持搜索 / 排序 / 分页。"""
    db = get_db()
    search = request.args.get('search', '').strip()
    sort = request.args.get('sort', 'code')
    order = request.args.get('order', 'asc')
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)

    allowed_sort = {'code', 'name', 'ftype', 'nav', 'acc_nav', 'ret_1y', 'ret_2y', 'ret_3y', 'ret_4y', 'ret_5y', 'ret_10y', 'ret_ann', 'total_ret', 'est_date', 'upd_date', 'market', 'mgmt_fee', 'cust_fee', 'sale_fee', 'purchase_status', 'daily_limit'}
    if sort not in allowed_sort:
        sort = 'code'
    if order not in ('asc', 'desc'):
        order = 'asc'

    where = ''
    params = []
    conditions = []
    if search:
        conditions.append("(code LIKE ? OR name LIKE ?)")
        params.extend([f'%{search}%', f'%{search}%'])
    if request.args.get('filter') == 'pos_1y':
        conditions.append("ret_1y >= 0")
    elif request.args.get('filter') == 'ann_gt_20':
        conditions.append("ret_ann > 20")
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    # 总记录数
    count_row = db.execute(f"SELECT COUNT(*) AS cnt FROM funds {where}", params).fetchone()
    total = count_row['cnt']

    # 分页数据
    offset = (page - 1) * per_page
    rows = db.execute(
        f"SELECT * FROM funds {where} ORDER BY {sort} {order} LIMIT ? OFFSET ?",
        params + [per_page, offset]
    ).fetchall()

    return jsonify({
        'total': total,
        'page': page,
        'per_page': per_page,
        'funds': [dict(r) for r in rows],
    })


@app.route('/api/stats')
def api_stats():
    """返回全量统计：总数、近1年正收益数、年化>20%基金数。"""
    db = get_db()
    total = db.execute("SELECT COUNT(*) AS cnt FROM funds").fetchone()['cnt']
    pos = db.execute("SELECT COUNT(*) AS cnt FROM funds WHERE ret_1y >= 0").fetchone()['cnt']
    ann_gt_20 = db.execute("SELECT COUNT(*) AS cnt FROM funds WHERE ret_ann > 20").fetchone()['cnt']
    return jsonify({'total': total, 'pos_1y': pos, 'ret_ann_gt_20': ann_gt_20})


@app.route('/api/fetch/status')
def api_fetch_status():
    """返回最近一次采集的状态。"""
    db = get_db()
    row = db.execute(
        "SELECT * FROM fetch_log ORDER BY id DESC LIMIT 1"
    ).fetchone()
    if row is None:
        return jsonify({'status': 'never'})
    return jsonify(dict(row))


# ── 启动 ────────────────────────────────────────────────────────

def _start_scheduler():
    """启动定时采集任务（每天北京时间 21:00）。"""
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    scheduler.add_job(_do_fetch, 'cron', hour=21, minute=0, id='daily_fetch')
    scheduler.start()
    print("📅 定时采集已启动：每天 21:00（北京时间）")


if __name__ == '__main__':
    init_db()
    _start_scheduler()

    # 首次启动或数据库为空时立即采集一次
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT COUNT(*) AS cnt FROM funds").fetchone()
        if row and row[0] == 0:
            print("🔄 数据库为空，首次采集启动中...")
            threading.Thread(target=_do_fetch, daemon=True).start()

    app.run(host='127.0.0.1', port=5000, debug=False, threaded=True)
