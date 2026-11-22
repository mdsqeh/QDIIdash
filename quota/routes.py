"""Flask API for channel-level quota monitoring."""
from flask import Blueprint, current_app, jsonify, request

from .jobs import start_quota_check_async
from .service import (
    connect,
    get_fund_detail,
    list_fund_summaries,
    set_manual_limit,
    unread_counts,
)


quota_bp = Blueprint('quota_monitor', __name__)


def _db_path():
    getter = current_app.config.get('QUOTA_DB_PATH_GETTER')
    if callable(getter):
        return getter()
    return current_app.config['QUOTA_DB_PATH']


def _conn():
    return connect(_db_path())


@quota_bp.route('/api/quota/monitor/funds')
def api_quota_monitor_funds():
    conn = _conn()
    try:
        funds, summary, companies = list_fund_summaries(
            conn,
            search=request.args.get('search', ''),
            company=request.args.get('company', ''),
            status=request.args.get('status', ''),
            changed_today=request.args.get('changed') == '1',
        )
    finally:
        conn.close()
    return jsonify({'funds': funds, 'summary': summary, 'companies': companies})


@quota_bp.route('/api/quota/monitor/fund/<code>')
def api_quota_monitor_fund(code):
    conn = _conn()
    try:
        detail = get_fund_detail(conn, code)
    finally:
        conn.close()
    if detail is None:
        return jsonify({'error': 'fund not found'}), 404
    return jsonify(detail)


@quota_bp.route('/api/quota/monitor/manual', methods=['POST'])
def api_quota_monitor_manual():
    payload = request.get_json(silent=True) or {}
    code = str(payload.get('code') or '').strip()
    if not code:
        return jsonify({'error': '缺少基金代码'}), 400
    conn = _conn()
    try:
        detail = set_manual_limit(conn, code, payload)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    finally:
        conn.close()
    return jsonify(detail)


@quota_bp.route('/api/quota/monitor/check', methods=['POST'])
def api_quota_monitor_check():
    started = start_quota_check_async(_db_path())
    if not started:
        return jsonify({'started': False, 'error': '检查正在进行中'}), 409
    return jsonify({'started': True})


@quota_bp.route('/api/quota/monitor/status')
def api_quota_monitor_status():
    conn = _conn()
    try:
        run = conn.execute(
            'SELECT * FROM quota_run ORDER BY id DESC LIMIT 1'
        ).fetchone()
        sources = conn.execute(
            """SELECT COUNT(*) AS total,
                      SUM(CASE WHEN last_error IS NOT NULL THEN 1 ELSE 0 END) AS errors
               FROM quota_source"""
        ).fetchone()
    finally:
        conn.close()
    return jsonify({
        'run': dict(run) if run else None,
        'sources': dict(sources) if sources else {'total': 0, 'errors': 0},
    })


@quota_bp.route('/api/quota/monitor/unread')
def api_quota_monitor_unread():
    conn = _conn()
    try:
        counts = unread_counts(conn)
    finally:
        conn.close()
    return jsonify(counts)


@quota_bp.route('/api/quota/monitor/ack', methods=['POST'])
def api_quota_monitor_ack():
    conn = _conn()
    try:
        conn.execute('UPDATE quota_announcement SET is_read = 1 WHERE is_read = 0')
        try:
            conn.execute('UPDATE quota_changes SET is_read = 1 WHERE is_read = 0')
        except Exception:
            pass
        conn.commit()
    finally:
        conn.close()
    return jsonify({'ok': True})
