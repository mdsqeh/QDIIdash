"""Discover Nasdaq-100 QDII funds from the dashboard's funds table.

This intentionally reuses the same name/type filter as the homepage
``matchesPresetFilter(row, 'nasdaq100')`` instead of maintaining a second
static fund list.
"""
import re
import sqlite3
from datetime import datetime, timezone

from .schema import init_quota_db


SHARE_CLASS_RE = re.compile(r'([ACIDE])(?=类|份额|人民币|美元|现汇|现钞|\(|（|$)')


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def is_nasdaq100(name, ftype=''):
    text = f'{name or ""} {ftype or ""}'.lower()
    return ('纳斯达克' in text or 'nasdaq' in text) and '100' in text


def company_prefix(short_name):
    short_name = str(short_name or '')
    return short_name[:-2] if short_name.endswith('基金') else short_name


def match_company(name, companies):
    for company in sorted(
        companies,
        key=lambda item: len(company_prefix(item['short_name'])),
        reverse=True,
    ):
        if name.startswith(company_prefix(company['short_name'])):
            return company
    return None


def parse_share_class(name):
    matches = SHARE_CLASS_RE.findall(str(name or ''))
    return matches[-1] if matches else 'A'


def parse_currency(name):
    return 'USD' if '美元' in str(name or '') else 'CNY'


def normalize_base_name(name):
    text = str(name or '')
    text = re.sub(r'[（(][^）)]*[）)]', '', text)
    text = re.sub(r'人民币|美元现汇|美元现钞|美元', '', text)
    text = SHARE_CLASS_RE.sub('', text)
    return re.sub(r'\s+', '', text)


def discover_from_funds(conn):
    """Return Nasdaq-100 OTC funds for the seeded companies.

    The result is grouped by company + base fund name so share classes can
    reuse one CSRC parent fund id.
    """
    try:
        companies = [dict(row) for row in conn.execute(
            'SELECT * FROM quota_company WHERE enabled = 1 ORDER BY id'
        ).fetchall()]
        rows = conn.execute(
            'SELECT code, name, ftype, market FROM funds'
        ).fetchall()
    except sqlite3.OperationalError:
        return []

    items = []
    for row in rows:
        if (row['market'] or '') != '场外':
            continue
        if not is_nasdaq100(row['name'], row['ftype']):
            continue
        company = match_company(row['name'], companies)
        if company is None:
            continue
        items.append({
            'company_id': company['id'],
            'company_short_name': company['short_name'],
            'company_url': company['official_url'],
            'code': str(row['code']),
            'name': row['name'],
            'share_class': parse_share_class(row['name']),
            'currency': parse_currency(row['name']),
            'base_name': normalize_base_name(row['name']),
        })

    groups = {}
    for item in items:
        groups.setdefault((item['company_id'], item['base_name']), []).append(item)
    for group in groups.values():
        parent = next(
            (
                item for item in group
                if item['share_class'] == 'A' and item['currency'] == 'CNY'
            ),
            sorted(group, key=lambda item: item['code'])[0],
        )
        for item in group:
            item['csrc_code'] = parent['code']
    return items


def sync_nasdaq100_funds(db_path):
    """Upsert discovered funds and disable funds that disappeared from a full list."""
    init_quota_db(db_path)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout=5000')
    try:
        try:
            source_count = conn.execute('SELECT COUNT(*) AS c FROM funds').fetchone()['c']
        except sqlite3.OperationalError:
            return {'discovered': 0, 'inserted': 0, 'updated': 0, 'disabled': 0, 'skipped': True}
        items = discover_from_funds(conn)
        if not items:
            return {
                'discovered': 0,
                'inserted': 0,
                'updated': 0,
                'disabled': 0,
                'skipped': True,
                'source_count': source_count,
            }

        now = _now_iso()
        inserted = 0
        updated = 0
        for item in items:
            existed = conn.execute(
                'SELECT 1 FROM quota_fund WHERE code = ?', (item['code'],)
            ).fetchone()
            conn.execute(
                """INSERT INTO quota_fund
                   (company_id, code, csrc_code, name, share_class, currency,
                    index_name, official_url, enabled, discovery_source,
                    last_seen_at, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, '纳斯达克100', ?, 1, 'homepage', ?, ?)
                   ON CONFLICT(code) DO UPDATE SET
                       company_id = excluded.company_id,
                       csrc_code = excluded.csrc_code,
                       name = excluded.name,
                       share_class = excluded.share_class,
                       currency = excluded.currency,
                       official_url = COALESCE(excluded.official_url, quota_fund.official_url),
                       enabled = 1,
                       discovery_source = 'homepage',
                       last_seen_at = excluded.last_seen_at""",
                (
                    item['company_id'], item['code'], item['csrc_code'],
                    item['name'], item['share_class'], item['currency'],
                    item['company_url'], now, now,
                ),
            )
            if existed:
                updated += 1
            else:
                inserted += 1

        disabled = 0
        if source_count >= 100:
            cur = conn.execute(
                """UPDATE quota_fund
                   SET enabled = 0
                   WHERE discovery_source = 'homepage' AND enabled = 1
                     AND (last_seen_at IS NULL OR last_seen_at < ?)""",
                (now,),
            )
            disabled = cur.rowcount
        conn.commit()
        return {
            'discovered': len(items),
            'inserted': inserted,
            'updated': updated,
            'disabled': disabled,
            'source_count': source_count,
            'skipped': False,
        }
    finally:
        conn.close()
