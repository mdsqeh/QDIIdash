"""Incremental quota-announcement check job."""
import threading
import time
from datetime import datetime, timezone

from .discovery import sync_nasdaq100_funds
from .parser import parse_announcement
from .schema import init_quota_db
from .sources import (
    CSRC_INDEX,
    fetch_csrc_announcements,
    fetch_csrc_fund_id,
    fetch_pdf_text,
    make_session,
)
from .service import connect


_CHECK_LOCK = threading.Lock()


def _now_iso():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


def _ensure_source(conn, fund_id):
    now = _now_iso()
    conn.execute(
        """INSERT OR IGNORE INTO quota_source
           (fund_id, source_type, url, created_at)
           VALUES (?, 'csrc', ?, ?)""",
        (fund_id, CSRC_INDEX, now),
    )
    conn.commit()
    return conn.execute(
        "SELECT * FROM quota_source WHERE fund_id = ? AND source_type = 'csrc'",
        (fund_id,),
    ).fetchone()


def run_quota_check(db_path):
    """Check all seeded funds once.  Returns a small result dict."""
    if not _CHECK_LOCK.acquire(blocking=False):
        return {'skipped': True, 'reason': 'already_running'}
    init_quota_db(db_path)
    conn = connect(db_path)
    run_id = None
    announcement_count = 0
    limit_count = 0
    errors = []
    sync_info = {}
    try:
        try:
            sync_info = sync_nasdaq100_funds(db_path)
        except Exception as exc:
            errors.append(f'list_sync: {exc}')
        now = _now_iso()
        cur = conn.execute(
            """INSERT INTO quota_run (status, started_at)
               VALUES ('running', ?)""",
            (now,),
        )
        run_id = cur.lastrowid
        is_first_run = conn.execute(
            """SELECT COUNT(*) AS c FROM quota_run
               WHERE id < ? AND status IN ('done', 'partial')""",
            (run_id,),
        ).fetchone()['c'] == 0
        conn.commit()
        funds = conn.execute(
            """SELECT f.*, c.short_name AS company
               FROM quota_fund f JOIN quota_company c ON c.id = f.company_id
               WHERE f.enabled = 1 AND c.enabled = 1
               ORDER BY f.id"""
        ).fetchall()
        session = make_session()
        for fund in funds:
            source = _ensure_source(conn, fund['id'])
            checked_at = _now_iso()
            first_fund_check = not fund['last_checked_at']
            try:
                external_id = source['external_id']
                if not external_id:
                    lookup_code = fund['csrc_code'] or fund['code']
                    external_id = fetch_csrc_fund_id(session, lookup_code)
                external_id, announcements = fetch_csrc_announcements(
                    session, fund, fund_id=external_id, limit=30,
                )
                conn.execute(
                    """UPDATE quota_source
                       SET url = ?, external_id = ?, last_checked_at = ?,
                           last_success_at = ?, last_error = NULL
                       WHERE fund_id = ? AND source_type = 'csrc'""",
                    (CSRC_INDEX, external_id, checked_at, checked_at, fund['id']),
                )
                latest_date = None
                for ann in announcements:
                    exists = conn.execute(
                        """SELECT id FROM quota_announcement
                           WHERE fund_id = ? AND source_type = ? AND external_id = ?""",
                        (fund['id'], ann['source_type'], ann['external_id']),
                    ).fetchone()
                    if exists:
                        continue
                    text = fetch_pdf_text(ann['source_url'])
                    limits = parse_announcement(
                        ann['title'], text, dict(fund), ann.get('published_at'),
                    )
                    confidence = max((item['confidence'] for item in limits), default=0.0)
                    parse_status = 'parsed' if text else 'title_only'
                    cur = conn.execute(
                        """INSERT INTO quota_announcement
                           (fund_id, source_type, source_url, external_id, title,
                            published_at, fetched_at, text, parse_status,
                            parse_confidence, is_read, created_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            fund['id'], ann['source_type'], ann['source_url'],
                            ann['external_id'], ann['title'], ann.get('published_at'),
                            checked_at, text, parse_status, confidence,
                            1 if (is_first_run or first_fund_check) else 0, checked_at,
                        ),
                    )
                    announcement_id = cur.lastrowid
                    announcement_count += 1
                    for item in limits:
                        conn.execute(
                            """INSERT INTO quota_limit
                               (fund_id, announcement_id, channel_type, channel_name,
                                status, amount, currency, limit_text, business_type,
                                effective_from, effective_to, confidence, is_manual,
                                note, created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 0, NULL, ?, ?)""",
                            (
                                fund['id'], announcement_id, item['channel_type'],
                                item['channel_name'], item['status'], item['amount'],
                                item['currency'], item['limit_text'],
                                item['business_type'], item['effective_from'],
                                item['confidence'], checked_at, checked_at,
                            ),
                        )
                        limit_count += 1
                    if ann.get('published_at') and (
                        latest_date is None or ann['published_at'] > latest_date
                    ):
                        latest_date = ann['published_at']
                conn.execute(
                    """UPDATE quota_fund
                       SET last_checked_at = ?,
                           last_announcement_at = COALESCE(?, last_announcement_at)
                       WHERE id = ?""",
                    (checked_at, latest_date, fund['id']),
                )
                conn.commit()
                time.sleep(0.2)
            except Exception as exc:
                errors.append(f"{fund['code']}: {exc}")
                conn.execute(
                    """UPDATE quota_source
                       SET last_checked_at = ?, last_error = ?
                       WHERE fund_id = ? AND source_type = 'csrc'""",
                    (checked_at, str(exc)[:500], fund['id']),
                )
                conn.execute(
                    'UPDATE quota_fund SET last_checked_at = ? WHERE id = ?',
                    (checked_at, fund['id']),
                )
                conn.commit()

        status = 'done' if not errors else 'partial'
        conn.execute(
            """UPDATE quota_run
               SET status = ?, finished_at = ?, fund_count = ?,
                   announcement_count = ?, limit_count = ?, error_msg = ?
               WHERE id = ?""",
            (
                status, _now_iso(), len(funds), announcement_count, limit_count,
                '\n'.join(errors)[:2000] if errors else None, run_id,
            ),
        )
        conn.commit()
        return {
            'skipped': False,
            'status': status,
            'funds': len(funds),
            'announcements': announcement_count,
            'limits': limit_count,
            'list_sync': sync_info,
            'errors': errors,
        }
    except Exception as exc:
        if run_id:
            conn.execute(
                """UPDATE quota_run
                   SET status = 'failed', finished_at = ?, error_msg = ?
                   WHERE id = ?""",
                (_now_iso(), str(exc)[:2000], run_id),
            )
            conn.commit()
        raise
    finally:
        conn.close()
        _CHECK_LOCK.release()


def start_quota_check_async(db_path):
    if not _CHECK_LOCK.acquire(blocking=False):
        return False
    _CHECK_LOCK.release()
    thread = threading.Thread(
        target=lambda: run_quota_check(db_path), daemon=True
    )
    thread.start()
    return True
