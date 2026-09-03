"""
QDII 基金数据看板 — Flask + SQLite Web 应用

启动：
    pip install flask akshare pandas apscheduler
    python qdii/app.py

QDII 数据每天 21:00（北京时间）定时采集。
"""
import json
import sqlite3
import threading
import os
from datetime import date, datetime, timezone

from flask import Flask, g, jsonify, render_template, request
from apscheduler.schedulers.background import BackgroundScheduler

from fetcher import fetch_all, _fetch_incremental, _fetch_all_codes
from quota_watcher import check_quotas

app = Flask(__name__)

# ── 数据库路径（放在 qdii 目录下） ──────────────────────────────
DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DB_DIR, 'qdii.db')
_FULL_FETCH_FLAG = DB_PATH + '.full_fetch_flag'


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
    """返回未读的额度变动记录（用于 banner 提醒）。"""
    db = get_db()
    rows = db.execute(
        "SELECT q.*, f.name FROM quota_changes q LEFT JOIN funds f ON q.code = f.code WHERE q.is_read = 0 ORDER BY q.id DESC LIMIT 50"
    ).fetchall()
    return jsonify([dict(r) for r in rows])


@app.route('/api/quota/history')
def api_quota_history():
    """返回额度变动历史记录，支持日期和已读状态筛选。"""
    db = get_db()
    date = request.args.get('date', date.today().isoformat())
    is_read = request.args.get('is_read', '')
    where = "WHERE q.detected_at LIKE ?"
    params = [f"{date}%"]
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
    scheduler.start()
    print("📅 定时任务已启动：每月1号 21:00 全量采集，每日 21:00 增量更新，额度监控交易日 9:00-19:00 每小时")


if __name__ == '__main__':
    init_db()
    _start_scheduler()

    # 启动时检查数据库是否有数据，有就跳过采集
    with sqlite3.connect(DB_PATH) as conn:
        has_qdii = conn.execute("SELECT COUNT(*) FROM funds").fetchone()[0] > 0

    if not has_qdii:
        print("🔄 数据库为空，启动后采集 QDII 基金...")
        threading.Thread(target=_do_fetch, daemon=True).start()
    else:
        print("⏭️ QDII 数据已存在，跳过启动采集")

    app.run(host='127.0.0.1', port=5000, debug=False, threaded=True)
