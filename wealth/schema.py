"""Wealth module schema. All tables use the ``wm_`` prefix."""
from .db import connect, now_iso


SCHEMA = """
CREATE TABLE IF NOT EXISTS wm_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS wm_account (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    broker     TEXT,
    currency   TEXT NOT NULL DEFAULT 'CNY',
    is_active  INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    UNIQUE(name)
);

CREATE TABLE IF NOT EXISTS wm_account_balance (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id   INTEGER NOT NULL REFERENCES wm_account(id),
    balance_date TEXT NOT NULL,
    amount       REAL NOT NULL,
    currency     TEXT NOT NULL DEFAULT 'CNY',
    source       TEXT,
    created_at   TEXT NOT NULL,
    UNIQUE(account_id, balance_date)
);
CREATE INDEX IF NOT EXISTS idx_wm_account_balance
    ON wm_account_balance (account_id, balance_date);

CREATE TABLE IF NOT EXISTS wm_asset (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol         TEXT NOT NULL,
    market         TEXT NOT NULL,
    asset_class    TEXT NOT NULL,
    name           TEXT NOT NULL,
    currency       TEXT NOT NULL DEFAULT 'CNY',
    quote_source   TEXT NOT NULL DEFAULT 'manual',
    quote_key      TEXT,
    lot_size       REAL NOT NULL DEFAULT 1,
    allow_fraction INTEGER NOT NULL DEFAULT 1,
    valuation_mode TEXT NOT NULL DEFAULT 'price',
    annual_rate    REAL,
    interest_mode  TEXT NOT NULL DEFAULT 'simple',
    maturity_date  TEXT,
    is_active      INTEGER NOT NULL DEFAULT 1,
    last_price     REAL,
    last_price_date TEXT,
    last_price_at  TEXT,
    last_error     TEXT,
    note           TEXT,
    created_at     TEXT NOT NULL,
    UNIQUE(symbol, market)
);
CREATE INDEX IF NOT EXISTS idx_wm_asset_class ON wm_asset (asset_class);

CREATE TABLE IF NOT EXISTS wm_transaction (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    asset_id     INTEGER NOT NULL REFERENCES wm_asset(id),
    account_id   INTEGER REFERENCES wm_account(id),
    type         TEXT NOT NULL,
    trade_date   TEXT NOT NULL,
    units        REAL,
    price        REAL,
    amount       REAL NOT NULL,
    fee          REAL NOT NULL DEFAULT 0,
    currency     TEXT NOT NULL,
    fx_rate      REAL NOT NULL DEFAULT 1,
    split_ratio  REAL,
    source       TEXT NOT NULL DEFAULT 'manual',
    dca_order_id INTEGER,
    import_hash  TEXT UNIQUE,
    note         TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wm_tx_asset_date ON wm_transaction (asset_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_wm_tx_date ON wm_transaction (trade_date);
CREATE INDEX IF NOT EXISTS idx_wm_tx_order ON wm_transaction (dca_order_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_wm_tx_dca_unique
    ON wm_transaction (dca_order_id) WHERE dca_order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS wm_dca_plan (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    name                TEXT NOT NULL,
    asset_id            INTEGER NOT NULL REFERENCES wm_asset(id),
    account_id          INTEGER REFERENCES wm_account(id),
    amount              REAL NOT NULL,
    amount_currency     TEXT NOT NULL DEFAULT 'CNY',
    frequency           TEXT NOT NULL DEFAULT 'monthly',
    weekday             INTEGER,
    day_of_month        INTEGER,
    start_date          TEXT NOT NULL,
    end_date            TEXT,
    roll_rule           TEXT NOT NULL DEFAULT 'next',
    fee_rate            REAL NOT NULL DEFAULT 0,
    fee_min             REAL NOT NULL DEFAULT 0,
    execution_mode      TEXT NOT NULL DEFAULT 'auto',
    catch_up            INTEGER NOT NULL DEFAULT 1,
    status              TEXT NOT NULL DEFAULT 'active',
    last_generated_date TEXT,
    note                TEXT,
    created_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wm_plan_status ON wm_dca_plan (status);
CREATE INDEX IF NOT EXISTS idx_wm_plan_asset ON wm_dca_plan (asset_id);

CREATE TABLE IF NOT EXISTS wm_dca_order (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id         INTEGER NOT NULL REFERENCES wm_dca_plan(id),
    due_date        TEXT NOT NULL,
    trade_date      TEXT,
    price_date      TEXT,
    amount          REAL NOT NULL,
    amount_currency TEXT NOT NULL DEFAULT 'CNY',
    units           REAL,
    price           REAL,
    fee             REAL,
    fx_rate         REAL,
    status          TEXT NOT NULL DEFAULT 'awaiting_price',
    transaction_id  INTEGER,
    message         TEXT,
    created_at      TEXT NOT NULL,
    settled_at      TEXT,
    UNIQUE(plan_id, due_date)
);
CREATE INDEX IF NOT EXISTS idx_wm_order_status ON wm_dca_order (status);
CREATE INDEX IF NOT EXISTS idx_wm_order_plan ON wm_dca_order (plan_id, due_date);

CREATE TABLE IF NOT EXISTS wm_price (
    asset_id   INTEGER NOT NULL REFERENCES wm_asset(id),
    price_date TEXT NOT NULL,
    close      REAL NOT NULL,
    prev_close REAL,
    source     TEXT,
    fetched_at TEXT NOT NULL,
    PRIMARY KEY (asset_id, price_date)
);
CREATE INDEX IF NOT EXISTS idx_wm_price_date ON wm_price (price_date);

CREATE TABLE IF NOT EXISTS wm_fx_rate (
    rate_date TEXT NOT NULL,
    currency  TEXT NOT NULL,
    cny_rate  REAL NOT NULL,
    source    TEXT,
    PRIMARY KEY (rate_date, currency)
);

CREATE TABLE IF NOT EXISTS wm_sync_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    status      TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    finished_at TEXT,
    count       INTEGER,
    message     TEXT
);

CREATE TABLE IF NOT EXISTS wm_import_log (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    kind       TEXT NOT NULL,
    filename   TEXT,
    status     TEXT NOT NULL,
    count      INTEGER,
    message    TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wm_trade_calendar (
    market   TEXT NOT NULL,
    cal_date TEXT NOT NULL,
    is_open  INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (market, cal_date)
);
"""


def _ensure_column(conn, table, column, ddl):
    cols = {r['name'] for r in conn.execute(f'PRAGMA table_info({table})').fetchall()}
    if column not in cols:
        conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {ddl}')


def init_wealth_db(path=None):
    conn = connect(path)
    try:
        conn.executescript(SCHEMA)
        # Future migrations go here. Kept explicit so upgrades stay predictable.
        _ensure_column(conn, 'wm_transaction', 'split_ratio', 'REAL')
        _ensure_column(conn, 'wm_asset', 'last_error', 'TEXT')
        count = conn.execute('SELECT COUNT(*) AS c FROM wm_account').fetchone()['c']
        if not count:
            conn.execute(
                "INSERT INTO wm_account (name, broker, currency, created_at) VALUES ('默认账户', '', 'CNY', ?)",
                (now_iso(),),
            )
        conn.commit()
    finally:
        conn.close()
