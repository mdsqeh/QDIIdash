"""Read models and manual overrides for the quota-monitoring API."""
import sqlite3
from datetime import datetime, timezone, timedelta


SHANGHAI_TZ = timezone(timedelta(hours=8))
CHANNEL_LABELS = {'direct': '直销', 'agency': '第三方/代销', 'all': '全渠道'}
STATUS_LABELS = {
    'amount_cap': '限额',
    'paused': '暂停',
    'normal': '正常',
    'unknown': '待确认',
}


def connect(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA busy_timeout=5000')
    return conn


def _today():
    return datetime.now(SHANGHAI_TZ).date().isoformat()


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _local_date(value):
    if not value:
        return ''
    try:
        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(SHANGHAI_TZ).date().isoformat()
    except (TypeError, ValueError):
        return str(value)[:10]


def _fund_rows(conn):
    return conn.execute(
        """SELECT f.*, c.short_name AS company, c.full_name AS company_full_name,
                  c.official_url AS company_url, c.announcement_url AS company_announcement_url
           FROM quota_fund f
           JOIN quota_company c ON c.id = f.company_id
           WHERE f.enabled = 1 AND c.enabled = 1
           ORDER BY c.short_name, f.code"""
    ).fetchall()


def _limit_rows(conn, fund_id):
    return conn.execute(
        """SELECT l.*, a.title AS announcement_title, a.published_at AS announcement_date,
                  a.source_url AS announcement_url, a.source_type AS announcement_source
           FROM quota_limit l
           LEFT JOIN quota_announcement a ON a.id = l.announcement_id
           WHERE l.fund_id = ?
           ORDER BY l.is_manual DESC,
                    COALESCE(l.effective_from, a.published_at, '') DESC,
                    CASE l.status
                        WHEN 'paused' THEN 0
                        WHEN 'amount_cap' THEN 1
                        WHEN 'normal' THEN 2
                        ELSE 3
                    END,
                    l.id DESC""",
        (fund_id,),
    ).fetchall()


def _effective_date(row):
    if row is None:
        return ''
    return (row['effective_from'] or row['announcement_date'] or '')[:10]


def _format_amount(amount):
    if amount is None:
        return ''
    if amount >= 100000000:
        return f'{amount / 100000000:.2f}亿'
    if amount >= 10000:
        return f'{amount / 10000:.2f}万'
    if amount == int(amount):
        return f'{int(amount):,}'
    return f'{amount:,.2f}'


def _compact(row):
    if row is None:
        return None
    amount = row['amount']
    return {
        'status': row['status'],
        'status_label': STATUS_LABELS.get(row['status'], row['status']),
        'amount': amount,
        'amount_text': _format_amount(amount),
        'channel_type': row['channel_type'],
        'channel_name': row['channel_name'],
        'business_type': row['business_type'],
        'effective_from': row['effective_from'],
        'announcement_title': row['announcement_title'] or '',
        'announcement_date': row['announcement_date'] or '',
        'source_url': row['announcement_url'] or '',
        'source_type': row['announcement_source'] or '',
        'confidence': row['confidence'],
        'is_manual': bool(row['is_manual']),
        'note': row['note'] or '',
    }


def _pick_direct(rows):
    manual = [r for r in rows if r['is_manual']]
    if manual:
        return manual[0], []
    if not rows:
        return None, []
    # Direct sales has sub-channels (electronic platform, counter, generic).
    # Respect the newest effective date first; on a tie prefer the electronic
    # platform, then generic direct, then counter.
    latest_date = _effective_date(rows[0])
    latest_rows = [r for r in rows if _effective_date(r) == latest_date]
    primary = None
    for name in ('直销电子交易平台', '直销', '直销柜台'):
        for row in latest_rows:
            if (row['channel_name'] or '') == name:
                primary = row
                break
        if primary is not None:
            break
    if primary is None:
        primary = rows[0]
    return primary, [r for r in rows if r['id'] != primary['id']]


def _pick_agency(rows):
    manual = [r for r in rows if r['is_manual']]
    if manual:
        return manual[0], []
    if rows:
        # Rows are sorted by effective date (and paused-before-amount on ties).
        return rows[0], rows[1:]
    return None, []


def _pick_all(rows):
    manual = [r for r in rows if r['is_manual']]
    if manual:
        return manual[0]
    return rows[0] if rows else None


def _notes_for(details):
    notes = []
    seen = set()
    for row in details:
        if row['status'] == 'paused':
            note = f"{row['channel_name']}暂停"
        elif row['status'] == 'normal':
            note = f"{row['channel_name']}正常"
        elif row['status'] == 'amount_cap':
            note = f"{row['channel_name']}限额{_format_amount(row['amount'])}"
        else:
            continue
        if note not in seen:
            seen.add(note)
            notes.append(note)
    return notes


def build_fund_summary(conn, fund):
    rows = _limit_rows(conn, fund['id'])
    by_channel = {'direct': [], 'agency': [], 'all': []}
    for row in rows:
        by_channel.setdefault(row['channel_type'], []).append(row)

    direct, direct_extra = _pick_direct(by_channel.get('direct', []))
    agency, agency_extra = _pick_agency(by_channel.get('agency', []))
    all_channel = _pick_all(by_channel.get('all', []))

    # A newer all-channel announcement overrides older direct/agency rows.
    # If direct/agency are newer, keep them and hide the stale all-channel row.
    if all_channel and (direct or agency):
        all_date = _effective_date(all_channel)
        direct_date = _effective_date(direct)
        agency_date = _effective_date(agency)
        if all_channel['status'] != 'unknown':
            if not direct or all_date > direct_date:
                direct, direct_extra = all_channel, []
            if not agency or all_date > agency_date:
                agency, agency_extra = all_channel, []
        all_channel = None

    note_rows = []
    if direct:
        note_rows += [r for r in direct_extra if r['business_type'] == direct['business_type']]
    if agency:
        note_rows += [r for r in agency_extra if r['business_type'] == agency['business_type']]

    current_rows = [r for r in (direct, agency, all_channel) if r]
    current_announcements = [
        r for r in current_rows if r['announcement_id'] and r['announcement_date']
    ]
    latest_announcement = None
    if current_announcements:
        row = max(
            current_announcements,
            key=lambda r: (r['announcement_date'], r['id']),
        )
        latest_announcement = {
            'published_at': row['announcement_date'],
            'title': row['announcement_title'],
            'source_url': row['announcement_url'],
        }
    if latest_announcement is None:
        row = conn.execute(
            """SELECT published_at, title, source_url
               FROM quota_announcement
               WHERE fund_id = ?
               ORDER BY published_at DESC, id DESC LIMIT 1""",
            (fund['id'],),
        ).fetchone()
        latest_announcement = dict(row) if row else None
    source = conn.execute(
        """SELECT source_type, url, last_checked_at, last_success_at, last_error
           FROM quota_source WHERE fund_id = ?
           ORDER BY CASE source_type WHEN 'csrc' THEN 0 ELSE 1 END, source_type""",
        (fund['id'],),
    ).fetchone()

    changed_today = any(
        (r['announcement_date'] or '')[:10] == _today() for r in current_rows
    )
    if not changed_today:
        for row in current_rows:
            if row['is_manual'] and _local_date(row['created_at']) == _today():
                changed_today = True
                break

    return {
        'code': fund['code'],
        'name': fund['name'],
        'company': fund['company'],
        'company_full_name': fund['company_full_name'],
        'share_class': fund['share_class'],
        'currency': fund['currency'],
        'official_url': fund['official_url'] or fund['company_url'],
        'company_announcement_url': fund['company_announcement_url'],
        'direct': _compact(direct),
        'agency': _compact(agency),
        'all': _compact(all_channel),
        'direct_details': [_compact(r) for r in direct_extra],
        'agency_details': [_compact(r) for r in agency_extra],
        'notes': _notes_for(note_rows),
        'last_announcement_at': latest_announcement['published_at'] if latest_announcement else None,
        'last_announcement_title': latest_announcement['title'] if latest_announcement else '',
        'last_announcement_url': latest_announcement['source_url'] if latest_announcement else '',
        'last_checked_at': fund['last_checked_at'],
        'source': dict(source) if source else None,
        'changed_today': changed_today,
    }


def _status_matches(summary, status):
    channels = [summary.get('direct'), summary.get('agency'), summary.get('all')]
    if status == 'limited':
        return any(c and c['status'] == 'amount_cap' for c in channels)
    if status == 'paused':
        return any(c and c['status'] == 'paused' for c in channels)
    if status == 'available':
        return any(c and c['status'] in ('amount_cap', 'normal') for c in channels) and not any(
            c and c['status'] == 'paused' for c in channels
        )
    return True


def _purchasable_summary(summaries):
    buckets = {
        currency: {
            'amount': 0.0,
            'amount_text': '',
            'total_count': 0,
            'limited_count': 0,
            'paused_count': 0,
            'unknown_count': 0,
            'unlimited_count': 0,
            'no_data_count': 0,
        }
        for currency in ('CNY', 'USD')
    }
    for item in summaries:
        currency = (item.get('currency') or 'CNY').upper()
        bucket = buckets.get(currency)
        if bucket is None:
            continue
        bucket['total_count'] += 1

        channels = [
            c for c in (item.get('direct'), item.get('agency'), item.get('all'))
            if c
        ]
        statuses = {c['status'] for c in channels}
        amounts = [
            c['amount'] for c in channels
            if c['status'] == 'amount_cap' and c.get('amount') is not None
        ]
        if 'normal' in statuses:
            bucket['unlimited_count'] += 1
        elif amounts:
            bucket['amount'] += max(amounts)
            bucket['limited_count'] += 1
        elif 'paused' in statuses:
            bucket['paused_count'] += 1
        elif 'unknown' in statuses:
            bucket['unknown_count'] += 1
        else:
            bucket['no_data_count'] += 1

    for bucket in buckets.values():
        bucket['amount_text'] = _format_amount(bucket['amount'])
    return buckets


def list_fund_summaries(conn, search='', company='', status='', changed_today=False):
    summaries = [build_fund_summary(conn, fund) for fund in _fund_rows(conn)]
    search = (search or '').strip().lower()
    company = (company or '').strip()
    filtered = []
    for item in summaries:
        if search:
            haystack = f"{item['code']} {item['name']} {item['company']}".lower()
            if search not in haystack:
                continue
        if company and item['company'] != company:
            continue
        if status and not _status_matches(item, status):
            continue
        if changed_today and not item['changed_today']:
            continue
        filtered.append(item)

    companies = sorted({item['company'] for item in summaries})
    last_run = conn.execute(
        'SELECT * FROM quota_run ORDER BY id DESC LIMIT 1'
    ).fetchone()
    unread = conn.execute(
        'SELECT COUNT(*) AS c FROM quota_announcement WHERE is_read = 0'
    ).fetchone()['c']
    summary = {
        'fund_count': len(summaries),
        'company_count': len(companies),
        'direct_limited': sum(
            1 for item in summaries if item['direct'] and item['direct']['status'] == 'amount_cap'
        ),
        'agency_limited': sum(
            1 for item in summaries if item['agency'] and item['agency']['status'] == 'amount_cap'
        ),
        'paused': sum(
            1 for item in summaries if any(
                c and c['status'] == 'paused'
                for c in (item['direct'], item['agency'], item['all'])
            )
        ),
        'changed_today': sum(1 for item in summaries if item['changed_today']),
        'purchasable': _purchasable_summary(summaries),
        'last_check': max(
            (item['last_checked_at'] for item in summaries if item['last_checked_at']),
            default=None,
        ),
        'last_run': dict(last_run) if last_run else None,
        'unread': unread,
    }
    return filtered, summary, companies


def get_fund_detail(conn, code):
    fund = conn.execute(
        """SELECT f.*, c.short_name AS company, c.full_name AS company_full_name,
                  c.official_url AS company_url, c.announcement_url AS company_announcement_url
           FROM quota_fund f JOIN quota_company c ON c.id = f.company_id
           WHERE f.code = ?""",
        (code,),
    ).fetchone()
    if fund is None:
        return None
    summary = build_fund_summary(conn, fund)
    announcements = []
    for row in conn.execute(
        """SELECT * FROM quota_announcement WHERE fund_id = ?
           ORDER BY published_at DESC, id DESC LIMIT 40""",
        (fund['id'],),
    ).fetchall():
        limits = conn.execute(
            """SELECT l.*, a.title AS announcement_title,
                      a.published_at AS announcement_date,
                      a.source_url AS announcement_url,
                      a.source_type AS announcement_source
               FROM quota_limit l
               LEFT JOIN quota_announcement a ON a.id = l.announcement_id
               WHERE l.announcement_id = ?
               ORDER BY l.channel_type, l.id""",
            (row['id'],),
        ).fetchall()
        announcements.append({
            **dict(row),
            'limits': [_compact(limit) for limit in limits],
        })
    sources = [dict(row) for row in conn.execute(
        'SELECT * FROM quota_source WHERE fund_id = ? ORDER BY source_type',
        (fund['id'],),
    ).fetchall()]
    return {
        'fund': summary,
        'announcements': announcements,
        'sources': sources,
    }


def set_manual_limit(conn, code, payload):
    fund = conn.execute(
        'SELECT * FROM quota_fund WHERE code = ?', (code,)
    ).fetchone()
    if fund is None:
        raise ValueError('基金不存在')
    channel_type = payload.get('channel_type') or 'all'
    if channel_type not in ('direct', 'agency', 'all'):
        raise ValueError('渠道参数无效')
    business_type = payload.get('business_type') or 'subscription'
    if business_type not in ('subscription', 'dca', 'conversion'):
        raise ValueError('业务类型无效')
    now = _now()
    if payload.get('clear'):
        conn.execute(
            """DELETE FROM quota_limit
               WHERE fund_id = ? AND channel_type = ? AND business_type = ? AND is_manual = 1""",
            (fund['id'], channel_type, business_type),
        )
        conn.commit()
        return get_fund_detail(conn, code)

    status = payload.get('status') or 'amount_cap'
    if status not in ('amount_cap', 'paused', 'normal', 'unknown'):
        raise ValueError('状态参数无效')
    amount = payload.get('amount')
    if amount in ('', None):
        amount = None
    else:
        try:
            amount = float(amount)
        except (TypeError, ValueError):
            raise ValueError('金额格式无效')
        if amount < 0:
            raise ValueError('金额不能为负数')
    note = str(payload.get('note') or '').strip()[:300]
    effective_from = str(payload.get('effective_from') or _today())[:10]
    conn.execute(
        """DELETE FROM quota_limit
           WHERE fund_id = ? AND channel_type = ? AND business_type = ? AND is_manual = 1""",
        (fund['id'], channel_type, business_type),
    )
    conn.execute(
        """INSERT INTO quota_limit
           (fund_id, announcement_id, channel_type, channel_name, status, amount,
            currency, limit_text, business_type, effective_from, effective_to,
            confidence, is_manual, note, created_at, updated_at)
           VALUES (?, NULL, ?, '人工确认', ?, ?, ?, '人工修正', ?, ?, NULL, 1.0, 1, ?, ?, ?)""",
        (
            fund['id'], channel_type, status, amount, fund['currency'],
            business_type, effective_from, note, now, now,
        ),
    )
    conn.commit()
    return get_fund_detail(conn, code)


def unread_counts(conn):
    announcements = conn.execute(
        'SELECT COUNT(*) AS c FROM quota_announcement WHERE is_read = 0'
    ).fetchone()['c']
    try:
        changes = conn.execute(
            'SELECT COUNT(*) AS c FROM quota_changes WHERE is_read = 0'
        ).fetchone()['c']
    except sqlite3.OperationalError:
        changes = 0
    return {'announcements': announcements, 'changes': changes, 'total': announcements + changes}
