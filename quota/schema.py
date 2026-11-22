"""SQLite schema and seed data for the quota-monitoring tab."""
import sqlite3
from datetime import datetime, timezone


COMPANIES = [
    {
        'short_name': '南方基金',
        'full_name': '南方基金管理股份有限公司',
        'official_url': 'https://www.nffund.com',
        'announcement_url': 'https://www.nffund.com',
    },
    {
        'short_name': '万家基金',
        'full_name': '万家基金管理有限公司',
        'official_url': 'https://www.wjasset.com',
        'announcement_url': 'https://www.wjasset.com',
    },
    {
        'short_name': '大成基金',
        'full_name': '大成基金管理有限公司',
        'official_url': 'https://www.dcfund.com.cn',
        'announcement_url': 'https://www.dcfund.com.cn',
    },
    {
        'short_name': '广发基金',
        'full_name': '广发基金管理有限公司',
        'official_url': 'https://www.gffunds.com.cn',
        'announcement_url': 'https://www.gffunds.com.cn/jjgg',
    },
    {
        'short_name': '国泰基金',
        'full_name': '国泰基金管理有限公司',
        'official_url': 'https://www.gtfund.com',
        'announcement_url': 'https://www.gtfund.com',
    },
    {
        'short_name': '华安基金',
        'full_name': '华安基金管理有限公司',
        'official_url': 'https://www.huaan.com.cn',
        'announcement_url': 'https://www.huaan.com.cn',
    },
    {
        'short_name': '华夏基金',
        'full_name': '华夏基金管理有限公司',
        'official_url': 'https://www.chinaamc.com',
        'announcement_url': 'https://www.chinaamc.com',
    },
    {
        'short_name': '博时基金',
        'full_name': '博时基金管理有限公司',
        'official_url': 'https://www.bosera.com',
        'announcement_url': 'https://www.bosera.com',
    },
    {
        'short_name': '易方达基金',
        'full_name': '易方达基金管理有限公司',
        'official_url': 'https://www.efunds.com.cn',
        'announcement_url': 'https://www.efunds.com.cn',
    },
    {
        'short_name': '嘉实基金',
        'full_name': '嘉实基金管理有限公司',
        'official_url': 'https://www.jsfund.cn',
        'announcement_url': 'https://www.jsfund.cn',
    },
]


FUNDS = [
    {
        'company': '南方基金',
        'code': '016452',
        'name': '南方纳斯达克100指数发起(QDII)A',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.nffund.com',
    },
    {
        'company': '万家基金',
        'code': '019441',
        'name': '万家纳斯达克100指数发起式(QDII)A',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.wjasset.com',
    },
    {
        'company': '大成基金',
        'code': '000834',
        'name': '大成纳斯达克100ETF联接(QDII)A',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.dcfund.com.cn',
    },
    {
        'company': '广发基金',
        'code': '270042',
        'name': '广发纳斯达克100ETF联接人民币(QDII)A',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.gffunds.com.cn',
    },
    {
        'company': '国泰基金',
        'code': '160213',
        'name': '国泰纳斯达克100指数',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.gtfund.com',
    },
    {
        'company': '华安基金',
        'code': '040046',
        'name': '华安纳斯达克100ETF联接(QDII)A',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.huaan.com.cn',
    },
    {
        'company': '华夏基金',
        'code': '015299',
        'name': '华夏纳斯达克100ETF发起式联接(QDII)A',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.chinaamc.com',
    },
    {
        'company': '博时基金',
        'code': '016055',
        'name': '博时纳斯达克100ETF发起式联接(QDII)A人民币',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.bosera.com',
    },
    {
        'company': '易方达基金',
        'code': '161130',
        'name': '易方达纳斯达克100ETF联接(QDII-LOF)A(人民币)',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.efunds.com.cn',
    },
    {
        'company': '嘉实基金',
        'code': '016532',
        'name': '嘉实纳斯达克100ETF发起联接(QDII)A人民币',
        'share_class': 'A',
        'currency': 'CNY',
        'official_url': 'https://www.jsfund.cn',
    },
]


SCHEMA = """
CREATE TABLE IF NOT EXISTS quota_company (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    short_name       TEXT NOT NULL UNIQUE,
    full_name        TEXT NOT NULL,
    official_url     TEXT,
    announcement_url TEXT,
    enabled          INTEGER NOT NULL DEFAULT 1,
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS quota_fund (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id         INTEGER NOT NULL,
    code               TEXT NOT NULL UNIQUE,
    name               TEXT NOT NULL,
    share_class        TEXT NOT NULL DEFAULT 'A',
    currency           TEXT NOT NULL DEFAULT 'CNY',
    index_name         TEXT NOT NULL DEFAULT '纳斯达克100',
    official_url       TEXT,
    enabled            INTEGER NOT NULL DEFAULT 1,
    last_checked_at    TEXT,
    last_announcement_at TEXT,
    created_at         TEXT NOT NULL,
    FOREIGN KEY (company_id) REFERENCES quota_company(id)
);

CREATE TABLE IF NOT EXISTS quota_source (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fund_id         INTEGER NOT NULL,
    source_type     TEXT NOT NULL,
    url             TEXT,
    external_id     TEXT,
    last_checked_at TEXT,
    last_success_at TEXT,
    last_error      TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE (fund_id, source_type),
    FOREIGN KEY (fund_id) REFERENCES quota_fund(id)
);

CREATE TABLE IF NOT EXISTS quota_announcement (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    fund_id          INTEGER NOT NULL,
    source_type      TEXT NOT NULL,
    source_url       TEXT NOT NULL,
    external_id      TEXT,
    title            TEXT NOT NULL,
    published_at     TEXT,
    fetched_at       TEXT NOT NULL,
    text             TEXT,
    parse_status     TEXT NOT NULL DEFAULT 'pending',
    parse_confidence REAL,
    is_read          INTEGER NOT NULL DEFAULT 0,
    created_at       TEXT NOT NULL,
    UNIQUE (fund_id, source_type, external_id),
    FOREIGN KEY (fund_id) REFERENCES quota_fund(id)
);

CREATE TABLE IF NOT EXISTS quota_limit (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fund_id         INTEGER NOT NULL,
    announcement_id INTEGER,
    channel_type    TEXT NOT NULL,
    channel_name    TEXT NOT NULL,
    status          TEXT NOT NULL,
    amount          REAL,
    currency        TEXT NOT NULL DEFAULT 'CNY',
    limit_text      TEXT,
    business_type   TEXT NOT NULL DEFAULT 'subscription',
    effective_from  TEXT,
    effective_to    TEXT,
    confidence      REAL,
    is_manual       INTEGER NOT NULL DEFAULT 0,
    note            TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    FOREIGN KEY (fund_id) REFERENCES quota_fund(id),
    FOREIGN KEY (announcement_id) REFERENCES quota_announcement(id)
);

CREATE TABLE IF NOT EXISTS quota_run (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    status            TEXT NOT NULL,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    fund_count        INTEGER NOT NULL DEFAULT 0,
    announcement_count INTEGER NOT NULL DEFAULT 0,
    limit_count       INTEGER NOT NULL DEFAULT 0,
    error_msg         TEXT
);

CREATE INDEX IF NOT EXISTS idx_quota_fund_company ON quota_fund(company_id);
CREATE INDEX IF NOT EXISTS idx_quota_source_fund ON quota_source(fund_id);
CREATE INDEX IF NOT EXISTS idx_quota_announcement_fund_date ON quota_announcement(fund_id, published_at);
CREATE INDEX IF NOT EXISTS idx_quota_limit_fund_channel ON quota_limit(fund_id, channel_type, effective_from);
CREATE INDEX IF NOT EXISTS idx_quota_limit_announcement ON quota_limit(announcement_id);
CREATE INDEX IF NOT EXISTS idx_quota_run_started ON quota_run(started_at);
"""


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def init_quota_db(db_path):
    """Create quota tables and seed the first 10 fund companies."""
    with sqlite3.connect(db_path) as conn:
        conn.execute('PRAGMA journal_mode=WAL')
        conn.execute('PRAGMA busy_timeout=5000')
        conn.executescript(SCHEMA)
        now = _now_iso()
        for company in COMPANIES:
            conn.execute(
                """INSERT OR IGNORE INTO quota_company
                   (short_name, full_name, official_url, announcement_url, enabled, created_at)
                   VALUES (?, ?, ?, ?, 1, ?)""",
                (
                    company['short_name'], company['full_name'],
                    company['official_url'], company['announcement_url'], now,
                ),
            )
            row = conn.execute(
                'SELECT id FROM quota_company WHERE short_name = ?',
                (company['short_name'],),
            ).fetchone()
            company_id = row[0]
            for fund in (f for f in FUNDS if f['company'] == company['short_name']):
                conn.execute(
                    """INSERT OR IGNORE INTO quota_fund
                       (company_id, code, name, share_class, currency, index_name,
                        official_url, enabled, created_at)
                       VALUES (?, ?, ?, ?, ?, '纳斯达克100', ?, 1, ?)""",
                    (
                        company_id, fund['code'], fund['name'], fund['share_class'],
                        fund['currency'], fund['official_url'], now,
                    ),
                )
        conn.commit()
