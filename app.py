"""
QDII 基金数据看板 — Flask + SQLite Web 应用

启动：
    cp .env.example .env   # 可选，填写 WM_PASSWORD 等
    pip install -r requirements.txt
    python app.py

QDII 数据每天 21:00（北京时间）定时采集。
"""
import json
import math
import sqlite3
import threading
import os
import sys
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

# Windows 控制台默认 GBK 时，日志里的 emoji 会触发 UnicodeEncodeError 并导致
# 启动失败。统一改成 UTF-8 输出；不支持 reconfigure 的环境直接跳过。
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except (AttributeError, OSError):
    pass

from dotenv import load_dotenv

# 在导入业务模块前加载项目目录下的 .env（已有系统环境变量优先）。
load_dotenv(Path(__file__).resolve().parent / '.env')

from flask import Flask, current_app, g, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler

from fetcher import fetch_all, _fetch_incremental, _fetch_all_codes
from quota import (
    init_quota_db,
    register_quota,
    register_quota_jobs,
    start_quota_check_async,
)
from quota_watcher import check_quotas
from wealth import register as register_wealth, register_wealth_jobs, startup_catchup
from wealth.db import connect as wealth_connect
from wealth.portfolio import build_positions

app = Flask(__name__)
register_wealth(app)

# ── 数据库路径（默认放在项目目录，可用 DATA_DIR 指向持久化目录） ──
DB_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get('DATA_DIR', DB_DIR)
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, 'qdii.db')
_FULL_FETCH_FLAG = DB_PATH + '.full_fetch_flag'

# 额度公告监测：使用同目录数据库，API 注册时自动建表并写入首批基金公司。
register_quota(app, lambda: DB_PATH)


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _ensure_user_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS fund_watchlist (
            code       TEXT PRIMARY KEY,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS fund_ratings (
            code       TEXT PRIMARY KEY,
            rating     INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        );
    """)
    conn.commit()


def _read_user_data(conn):
    return {
        'watchlist': [r['code'] for r in conn.execute(
            'SELECT code FROM fund_watchlist ORDER BY rowid'
        ).fetchall()],
        'ratings': {r['code']: r['rating'] for r in conn.execute(
            'SELECT code, rating FROM fund_ratings'
        ).fetchall()},
    }


def _clean_fund_code(value):
    return str(value or '').strip()[:32]


def _parse_rating(value):
    try:
        rating = int(value)
    except (TypeError, ValueError):
        return None
    return rating if 1 <= rating <= 5 else None


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
                premium_discount REAL,
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
            CREATE TABLE IF NOT EXISTS quota_changes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                code        TEXT NOT NULL,
                field       TEXT NOT NULL,
                old_value   TEXT,
                new_value   TEXT,
                detected_at TEXT NOT NULL,
                is_read     INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_quota_changes_code_time
                ON quota_changes (code, detected_at);
            CREATE TABLE IF NOT EXISTS quota_daily (
                code              TEXT NOT NULL,
                trade_date        TEXT NOT NULL,
                daily_limit       REAL,
                daily_limit_text  TEXT,
                purchase_status   TEXT,
                first_seen_at     TEXT NOT NULL,
                last_seen_at      TEXT NOT NULL,
                PRIMARY KEY (code, trade_date)
            );
            CREATE INDEX IF NOT EXISTS idx_quota_daily_date
                ON quota_daily (trade_date);
            CREATE TABLE IF NOT EXISTS fund_watchlist (
                code       TEXT PRIMARY KEY,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS fund_ratings (
                code       TEXT PRIMARY KEY,
                rating     INTEGER NOT NULL,
                updated_at TEXT NOT NULL
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

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("ALTER TABLE funds ADD COLUMN premium_discount REAL")
    except Exception:
        pass

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("ALTER TABLE funds ADD COLUMN daily_change REAL")
    except Exception:
        pass

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS quota_changes (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    code        TEXT NOT NULL,
                    field       TEXT NOT NULL,
                    old_value   TEXT,
                    new_value   TEXT,
                    detected_at TEXT NOT NULL,
                    is_read     INTEGER DEFAULT 0
                )
            """)
    except Exception:
        pass

    init_quota_db(DB_PATH)


# ── 数据采集（后台线程） ─────────────────────────────────────────

_fetch_lock = threading.Lock()


def _do_fetch():
    """后台执行数据采集，完成后写入 SQLite。"""
    if not _fetch_lock.acquire(blocking=False):
        print("⏭️ 采集已在运行，跳过本次", flush=True)
        return

    print("🚀 采集任务开始...", flush=True)
    t_start = datetime.now(timezone.utc)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.execute(
                "INSERT INTO fetch_log (status, started_at) VALUES (?, ?)",
                ('running', t_start.isoformat())
            )
            conn.commit()

        records = fetch_all()

        with sqlite3.connect(DB_PATH) as conn:
            conn.executemany(
                """INSERT OR REPLACE INTO funds
                   (code, name, ftype, market, nav, acc_nav, ret_1y, ret_2y, ret_3y, ret_4y, ret_5y, ret_10y, ret_ann, total_ret, est_date, mgmt_fee, cust_fee, sale_fee, purchase_status, daily_limit, premium_discount, daily_change, upd_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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
                    r.get('premium_discount'),
                    r.get('daily_change'),
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
        elapsed = (datetime.now(timezone.utc) - t_start).total_seconds()
        print(f"✅ 采集完成，耗时 {elapsed:.0f}s，共 {len(records)} 条", flush=True)
        with open(_FULL_FETCH_FLAG, 'w') as f:
            f.write(datetime.now(timezone.utc).isoformat())
    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - t_start).total_seconds()
        print(f"❌ 采集失败（{elapsed:.0f}s）: {e}", flush=True)
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


def _do_fetch_incremental():
    """增量更新：只更新 nav、daily_change、premium_discount、purchase_status、daily_limit 五个字段。
    同时检测并全量采集新增的 QDII 基金。全量采集当日跳过。
    """
    # 全量采集当日跳过增量更新
    try:
        with open(_FULL_FETCH_FLAG) as f:
            flag_date = f.read().strip()
        if flag_date == date.today().isoformat():
            print("⏭️ 今日已是全量更新日，跳过增量", flush=True)
            return
    except FileNotFoundError:
        pass

    print("⚡ 增量更新开始...", flush=True)
    t_start = datetime.now(timezone.utc)
    try:
        with sqlite3.connect(DB_PATH) as conn:
            existing_codes = {row[0] for row in conn.execute("SELECT code FROM funds").fetchall()}

            # 1. 检测新基金：拉取当前最新列表，找出 DB 中没有的
            print("  🔍 检测新增基金...", flush=True)
            all_codes = _fetch_all_codes()
            new_codes = [c for c in all_codes if c not in existing_codes]
            print(f"  📌 现有 {len(existing_codes)} 只，新增 {len(new_codes)} 只", flush=True)

            new_records = []
            if new_codes:
                # 对新增基金做全量 fetch（调用 fetch_all 再过滤）
                print(f"  📡 全量采集 {len(new_codes)} 只新增基金...", flush=True)
                all_data = fetch_all()
                all_code_set = {r['code'] for r in all_data}
                new_records = [r for r in all_data if r['code'] in all_code_set and r['code'] in set(new_codes)]

            # 2. 已有基金：只更新 5 个动态字段
            db_codes = list(existing_codes)
            records = _fetch_incremental(db_codes) if db_codes else []

            # 3. 合并写入：新增基金用 INSERT，已有基金用 UPDATE
            now = datetime.now(timezone.utc).isoformat()
            if new_records:
                conn.executemany(
                    """INSERT OR REPLACE INTO funds
                       (code, name, ftype, market, nav, acc_nav, ret_1y, ret_2y, ret_3y, ret_4y, ret_5y, ret_10y,
                        ret_ann, total_ret, est_date, mgmt_fee, cust_fee, sale_fee, purchase_status, daily_limit,
                        premium_discount, daily_change, upd_date)
                       VALUES (:code, :name, :ftype, :market, :nav, :acc_nav, :ret_1y, :ret_2y, :ret_3y, :ret_4y,
                               :ret_5y, :ret_10y, :ret_ann, :total_ret, :est_date, :mgmt_fee, :cust_fee, :sale_fee,
                               :purchase_status, :daily_limit, :premium_discount, :daily_change, :upd_date)""",
                    new_records
                )
                print(f"  ✅ 新增基金已入库: {len(new_records)} 只", flush=True)

            if records:
                conn.executemany(
                    """UPDATE funds
                       SET nav = :nav,
                           daily_change = :daily_change,
                           premium_discount = :premium_discount,
                           purchase_status = :purchase_status,
                           daily_limit = :daily_limit,
                           upd_date = :upd_date
                       WHERE code = :code""",
                    records
                )
                updated = sum(1 for r in records if r['nav'] is not None or r['daily_change'] is not None or r['premium_discount'] is not None)
                print(f"  ✅ 已有基金更新完成，共 {len(records)} 只，更新 {updated} 只", flush=True)

            conn.commit()
        elapsed = (datetime.now(timezone.utc) - t_start).total_seconds()
        print(f"✅ 增量更新完成，耗时 {elapsed:.0f}s，新增 {len(new_records)} 只", flush=True)
    except Exception as e:
        elapsed = (datetime.now(timezone.utc) - t_start).total_seconds()
        print(f"❌ 增量更新失败（{elapsed:.0f}s）: {e}", flush=True)


# ── 日额度监控（高频检测） ─────────────────────────────────────

WEBHOOK_URL = 'http://127.0.0.1:3000/webhook/cme'
WEBHOOK_SECRET = 'addressTagPWD'


def _do_check_quota():
    """后台执行 QDII 额度检测。"""
    print("📋 额度检测开始...", flush=True)
    try:
        changes = check_quotas(DB_PATH, WEBHOOK_URL, WEBHOOK_SECRET)
        if changes:
            print(f"📢 检测到 {len(changes)} 条额度变动", flush=True)
        else:
            print("✅ 额度无变动", flush=True)
    except Exception as e:
        print(f"❌ 额度检测失败: {e}", flush=True)


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

    allowed_sort = {'code', 'name', 'ftype', 'nav', 'acc_nav', 'ret_1y', 'ret_2y', 'ret_3y', 'ret_4y', 'ret_5y', 'ret_10y', 'ret_ann', 'total_ret', 'est_date', 'upd_date', 'market', 'mgmt_fee', 'cust_fee', 'sale_fee', 'purchase_status', 'daily_limit', 'premium_discount', 'daily_change'}
    if sort not in allowed_sort:
        sort = 'code'
    if order not in ('asc', 'desc'):
        order = 'asc'

    where = ''
    params = []
    conditions = []
    exclude = request.args.get('exclude', '').strip()
    if search:
        conditions.append("(code LIKE ? OR name LIKE ?)")
        params.extend([f'%{search}%', f'%{search}%'])
    if exclude:
        for term in exclude.split():
            conditions.append("(code NOT LIKE ? AND name NOT LIKE ?)")
            params.extend([f'%{term}%', f'%{term}%'])
    if request.args.get('filter') == 'pos_1y':
        conditions.append("ret_1y >= 0")
        conditions.append("(ret_2y IS NULL OR ret_2y >= 0)")
        conditions.append("(ret_3y IS NULL OR ret_3y >= 0)")
        conditions.append("(ret_4y IS NULL OR ret_4y >= 0)")
        conditions.append("(ret_5y IS NULL OR ret_5y >= 0)")
        conditions.append("(ret_10y IS NULL OR ret_10y >= 0)")
    elif request.args.get('filter') == 'ann_gt':
        ann_threshold = request.args.get('ann_threshold', 20, type=float)
        conditions.append("ret_ann > ?")
        params.append(ann_threshold)
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
    pos = db.execute("""SELECT COUNT(*) AS cnt FROM funds
        WHERE ret_1y >= 0
          AND (ret_2y IS NULL OR ret_2y >= 0)
          AND (ret_3y IS NULL OR ret_3y >= 0)
          AND (ret_4y IS NULL OR ret_4y >= 0)
          AND (ret_5y IS NULL OR ret_5y >= 0)
          AND (ret_10y IS NULL OR ret_10y >= 0)""").fetchone()['cnt']
    ann_threshold = request.args.get('ann_threshold', 20, type=float)
    ann_gt = db.execute("SELECT COUNT(*) AS cnt FROM funds WHERE ret_ann > ?", (ann_threshold,)).fetchone()['cnt']
    return jsonify({'total': total, 'pos_1y': pos, 'ret_ann_gt_20': ann_gt})


def _dashboard_db():
    db = get_db()
    _ensure_user_tables(db)
    return db


@app.route('/api/fund-user-data', methods=['GET', 'POST'])
def api_fund_user_data():
    """自选和自评级。POST 用于首次本地数据迁移或备份导入。"""
    db = _dashboard_db()
    if request.method == 'GET':
        return jsonify(_read_user_data(db))

    payload = request.get_json(silent=True) or {}
    mode = payload.get('mode') or 'replace'
    if mode not in ('replace', 'merge'):
        return jsonify({'error': 'mode 参数无效'}), 400

    now = _now_iso()
    if 'watchlist' in payload:
        raw_watchlist = payload.get('watchlist')
        if not isinstance(raw_watchlist, list):
            return jsonify({'error': '自选数据格式无效'}), 400
        codes = []
        seen = set()
        for item in raw_watchlist:
            code = _clean_fund_code(item)
            if code and code not in seen:
                seen.add(code)
                codes.append(code)
        if mode == 'replace':
            db.execute('DELETE FROM fund_watchlist')
        db.executemany(
            'INSERT OR IGNORE INTO fund_watchlist (code, created_at) VALUES (?, ?)',
            [(code, now) for code in codes],
        )

    if 'ratings' in payload:
        raw_ratings = payload.get('ratings')
        if not isinstance(raw_ratings, dict):
            return jsonify({'error': '评级数据格式无效'}), 400
        rows = []
        for code, value in raw_ratings.items():
            code = _clean_fund_code(code)
            rating = _parse_rating(value)
            if code and rating:
                rows.append((code, rating, now))
        if mode == 'replace':
            db.execute('DELETE FROM fund_ratings')
        sql = ('INSERT OR IGNORE INTO fund_ratings (code, rating, updated_at) '
               'VALUES (?, ?, ?)') if mode == 'merge' else (
               'INSERT OR REPLACE INTO fund_ratings (code, rating, updated_at) '
               'VALUES (?, ?, ?)')
        db.executemany(sql, rows)

    db.commit()
    return jsonify(_read_user_data(db))


@app.route('/api/fund-watchlist', methods=['POST'])
def api_fund_watchlist():
    payload = request.get_json(silent=True) or {}
    code = _clean_fund_code(payload.get('code'))
    if not code:
        return jsonify({'error': '缺少基金代码'}), 400
    db = _dashboard_db()
    if payload.get('active'):
        db.execute(
            'INSERT OR IGNORE INTO fund_watchlist (code, created_at) VALUES (?, ?)',
            (code, _now_iso()),
        )
    else:
        db.execute('DELETE FROM fund_watchlist WHERE code = ?', (code,))
    db.commit()
    return jsonify(_read_user_data(db))


@app.route('/api/fund-rating', methods=['POST'])
def api_fund_rating():
    payload = request.get_json(silent=True) or {}
    code = _clean_fund_code(payload.get('code'))
    if not code:
        return jsonify({'error': '缺少基金代码'}), 400
    raw_rating = payload.get('rating', 0)
    db = _dashboard_db()
    if raw_rating in (0, '0', None, ''):
        db.execute('DELETE FROM fund_ratings WHERE code = ?', (code,))
    else:
        rating = _parse_rating(raw_rating)
        if not rating:
            return jsonify({'error': '评级必须为 1-5'}), 400
        db.execute(
            'INSERT OR REPLACE INTO fund_ratings (code, rating, updated_at) '
            'VALUES (?, ?, ?)',
            (code, rating, _now_iso()),
        )
    db.commit()
    return jsonify(_read_user_data(db))


@app.route('/api/fund-holdings')
def api_fund_holdings():
    """主页持仓：财富管理中的场内/场外基金，且必须属于 QDII 基金池。"""
    db = get_db()
    wealth_db = wealth_connect(current_app.config.get('WM_DB_PATH'))
    try:
        positions, _ = build_positions(wealth_db)
    except sqlite3.OperationalError:
        return jsonify({'codes': []})
    finally:
        wealth_db.close()
    qdii_codes = {r['code'] for r in db.execute('SELECT code FROM funds').fetchall()}
    codes = set()
    for position in positions:
        if not position.get('has_position'):
            continue
        market = position.get('market')
        is_fund = market == 'FUND_CN' or (
            market == 'CN' and position.get('asset_class') in ('fund', 'etf')
        )
        symbol = _clean_fund_code(position.get('symbol'))
        if is_fund and symbol in qdii_codes:
            codes.add(symbol)
    return jsonify({'codes': sorted(codes)})


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


@app.route('/api/quota/changes')
def api_quota_changes():
    """返回未读的额度变动记录（用于 Tab 未读徽标）。"""
    db = get_db()
    rows = db.execute(
        "SELECT q.*, f.name FROM quota_changes q LEFT JOIN funds f ON q.code = f.code WHERE q.is_read = 0 ORDER BY q.id DESC LIMIT 50"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/quota/history')
def api_quota_history():
    """返回额度变动历史记录，支持日期和已读状态筛选。"""
    db = get_db()
    date_str = request.args.get('date', date.today().isoformat())
    is_read = request.args.get('is_read', '')
    where = "WHERE q.detected_at LIKE ?"
    params = [f"{date_str}%"]
    if is_read == '0':
        where += " AND q.is_read = 0"
        params.append(0)
    elif is_read == '1':
        where += " AND q.is_read = 1"
    rows = db.execute(
        f"SELECT q.*, f.name FROM quota_changes q LEFT JOIN funds f ON q.code = f.code {where} ORDER BY q.id DESC LIMIT 500",
        params
    ).fetchall()
    return jsonify([dict(r) for r in rows])


SHANGHAI_TZ = timezone(timedelta(hours=8))
UNLIMITED_LIMIT = 1_000_000_000


def _to_number(value):
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip().replace(',', '')
    if not text or text.lower() in ('nan', 'none', '不限'):
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def _to_limit(value):
    """把额度文本转成数值；'不限'、空值、NaN、10亿以上返回 None。"""
    number = _to_number(value)
    if number is None or number >= UNLIMITED_LIMIT:
        return None
    return number


def _limit_text(value):
    """统一额度展示文本，避免 1000 和 1000.0 显示成两个值。"""
    if value is None:
        return ''
    text = str(value).strip()
    if not text or text.lower() in ('nan', 'none'):
        return ''
    number = _to_number(text)
    if number is None:
        return text
    if number >= UNLIMITED_LIMIT:
        return '不限'
    if number == int(number):
        return str(int(number))
    return ('%.4f' % number).rstrip('0').rstrip('.')


def _local_date(timestamp):
    """把数据库中带时区的时间转成北京时间日期。"""
    try:
        dt = datetime.fromisoformat(timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(SHANGHAI_TZ).date()
    except (TypeError, ValueError):
        return None


def _build_quota_series(daily_rows, changes, current, days):
    """合并每日快照与历史变动事件，生成阶梯曲线数据。"""
    points_by_date = {}
    for row in daily_rows:
        points_by_date[row['trade_date']] = {
            'date': row['trade_date'],
            'limit': _to_limit(row['daily_limit']),
            'text': row['daily_limit_text'] or _limit_text(row['daily_limit']),
            'status': row['purchase_status'] or '',
        }
    daily_dates = set(points_by_date)

    limit_events = sorted(
        (c for c in changes if c['field'] == 'daily_limit'),
        key=lambda c: c['detected_at']
    )
    status_events = sorted(
        (c for c in changes if c['field'] == 'purchase_status'),
        key=lambda c: c['detected_at']
    )

    # 旧事件只有变动后的值，补一个变动前的起点，曲线才能从原值开始。
    if limit_events:
        first_date = _local_date(limit_events[0]['detected_at'])
        old_limit = _to_limit(limit_events[0]['old_value'])
        if first_date is not None and old_limit is not None:
            prev_date = (first_date - timedelta(days=1)).isoformat()
            if prev_date not in points_by_date:
                points_by_date[prev_date] = {
                    'date': prev_date,
                    'limit': old_limit,
                    'text': _limit_text(limit_events[0]['old_value']),
                    'status': '',
                }

    for change in limit_events:
        change_date = _local_date(change['detected_at'])
        if change_date is None:
            continue
        date_str = change_date.isoformat()
        if date_str in daily_dates:
            continue
        points_by_date[date_str] = {
            'date': date_str,
            'limit': _to_limit(change['new_value']),
            'text': _limit_text(change['new_value']),
            'status': '',
        }

    points = sorted(points_by_date.values(), key=lambda p: p['date'])

    # 旧事件没有每日状态，按时间顺序补上当时最后的申购状态。
    if status_events:
        status_timeline = []
        for change in status_events:
            change_date = _local_date(change['detected_at'])
            if change_date is not None:
                status_timeline.append((change_date.isoformat(), change['new_value'] or ''))
        idx = 0
        last_status = ''
        for point in points:
            while idx < len(status_timeline) and status_timeline[idx][0] <= point['date']:
                last_status = status_timeline[idx][1]
                idx += 1
            if not point['status']:
                point['status'] = last_status

    today = datetime.now(SHANGHAI_TZ).date()
    today_str = today.isoformat()
    if points and points[-1]['date'] == today_str:
        if current['limit'] is not None or current['text']:
            points[-1].update({
                'limit': current['limit'],
                'text': current['text'],
                'status': current['status'],
            })
    elif current['limit'] is not None or current['text']:
        points.append({
            'date': today_str,
            'limit': current['limit'],
            'text': current['text'],
            'status': current['status'],
        })
    if not points:
        points.append({
            'date': today_str,
            'limit': current['limit'],
            'text': current['text'],
            'status': current['status'],
        })

    if days is not None:
        cutoff = (today - timedelta(days=days - 1)).isoformat()
        before = [p for p in points if p['date'] < cutoff]
        points = [p for p in points if p['date'] >= cutoff]
        if before:
            last = before[-1]
            points.insert(0, {
                'date': cutoff,
                'limit': last['limit'],
                'text': last['text'],
                'status': last['status'],
            })

    return points


@app.route('/api/quota/fund/<code>')
def api_quota_fund(code):
    """返回单只基金的额度历史曲线和变动记录。"""
    db = get_db()
    fund = db.execute(
        "SELECT code, name, purchase_status, daily_limit FROM funds WHERE code = ?",
        (code,)
    ).fetchone()
    if fund is None:
        return jsonify({'error': 'fund not found'}), 404

    days_param = request.args.get('days', '90')
    if days_param == 'all':
        days = None
    else:
        days = request.args.get('days', 90, type=int)
        if days is None or days <= 0:
            days = 90
        days = min(days, 3650)

    daily_rows = db.execute(
        """SELECT trade_date, daily_limit, daily_limit_text, purchase_status
           FROM quota_daily WHERE code = ? ORDER BY trade_date""",
        (code,)
    ).fetchall()
    changes = db.execute(
        """SELECT detected_at, field, old_value, new_value
           FROM quota_changes WHERE code = ?
             AND field IN ('daily_limit', 'purchase_status')
           ORDER BY detected_at, id""",
        (code,)
    ).fetchall()

    latest_daily = daily_rows[-1] if daily_rows else None
    current = {
        'date': datetime.now(SHANGHAI_TZ).date().isoformat(),
        'limit': _to_limit(fund['daily_limit']),
        'text': _limit_text(fund['daily_limit']),
        'status': fund['purchase_status'] or '',
    }
    if current['limit'] is None and not current['text'] and latest_daily is not None:
        # funds 只存数值，额度为“不限”时用快照文本补上展示。
        current['text'] = latest_daily['daily_limit_text'] or ''
        current['status'] = current['status'] or latest_daily['purchase_status'] or ''

    points = _build_quota_series(daily_rows, changes, current, days)

    visible_changes = changes
    if days is not None:
        cutoff = (datetime.now(SHANGHAI_TZ).date() - timedelta(days=days - 1)).isoformat()
        visible_changes = [
            c for c in changes
            if (_local_date(c['detected_at']) or date.min).isoformat() >= cutoff
        ]

    change_count = sum(1 for c in visible_changes if c['field'] == 'daily_limit')
    return jsonify({
        'code': fund['code'],
        'name': fund['name'],
        'current': current,
        'points': points,
        'change_count': change_count,
        'events': [dict(c) for c in reversed(visible_changes)][:20],
    })


@app.route('/api/quota/ack', methods=['POST'])
def api_quota_ack():
    """将所有未读额度变动标记为已读。"""
    db = get_db()
    db.execute("UPDATE quota_changes SET is_read = 1 WHERE is_read = 0")
    db.commit()
    return jsonify({'ok': True})


@app.route('/api/quota/ack-single', methods=['POST'])
def api_quota_ack_single():
    """将单条额度变动标记为已读。"""
    db = get_db()
    data = request.get_json()
    change_id = data.get('id')
    if change_id:
        db.execute("UPDATE quota_changes SET is_read = 1 WHERE id = ?", (change_id,))
        db.commit()
    return jsonify({'ok': True})


# ── 启动 ────────────────────────────────────────────────────────

def _start_scheduler():
    """启动定时采集任务。"""
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    scheduler.add_job(_do_fetch, 'cron', day=1, hour=21, minute=0, id='monthly_fetch')
    scheduler.add_job(_do_fetch_incremental, 'cron', hour=21, minute=0, id='daily_incremental')
    scheduler.add_job(_do_check_quota, 'cron', day_of_week='mon-fri', hour='9-19', minute=0, id='quota_check')
    scheduler.add_job(_do_check_quota, 'cron', day_of_week='mon-fri', hour=21, minute=5, id='quota_check_after_fetch')
    register_quota_jobs(scheduler, lambda: DB_PATH)
    register_wealth_jobs(scheduler)
    scheduler.start()
    print("📅 定时任务已启动：每月1号 21:00 全量采集，每日 21:00 增量更新，额度监控交易日 9:00-19:00 每小时及 21:05，额度公告每日 8:10/12:10/17:10")


if __name__ == '__main__':
    init_db()
    _start_scheduler()
    threading.Thread(target=startup_catchup, daemon=True).start()

    # 启动时检查数据库是否有数据，有就跳过采集
    with sqlite3.connect(DB_PATH) as conn:
        has_qdii = conn.execute("SELECT COUNT(*) FROM funds").fetchone()[0] > 0

    if not has_qdii:
        print("🔄 数据库为空，启动后采集 QDII 基金...")
        threading.Thread(target=_do_fetch, daemon=True).start()
    else:
        print("⏭️ QDII 数据已存在，跳过启动采集")

    with sqlite3.connect(DB_PATH) as conn:
        has_quota_run = conn.execute("SELECT COUNT(*) FROM quota_run").fetchone()[0] > 0
    if not has_quota_run:
        print("🔄 额度公告暂无检查记录，启动后执行一次...")
        start_quota_check_async(DB_PATH)

    app.run(host='0.0.0.0', port=5000, debug=False, threaded=True)
