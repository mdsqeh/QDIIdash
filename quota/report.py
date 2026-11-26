"""Daily Nasdaq-100 QDII quota report (plain text, sent to a webhook)."""
from datetime import datetime

import akshare as ak

from .service import SHANGHAI_TZ, list_fund_summaries


# Only these channel statuses mean the fund can be bought right now.
PURCHASABLE_STATUSES = ('amount_cap', 'normal')
_CHANNEL_LABELS = {'direct': '直销', 'agency': '第三方', 'all': '全渠道'}

_TRADE_DAYS = {'day': None, 'days': None}


def _load_trade_days():
    today = datetime.now(SHANGHAI_TZ).date()
    if _TRADE_DAYS['day'] != today:
        try:
            frame = ak.tool_trade_date_hist_sina()
            _TRADE_DAYS['days'] = {str(value)[:10] for value in frame['trade_date']}
        except Exception:
            _TRADE_DAYS['days'] = None
        _TRADE_DAYS['day'] = today
    return _TRADE_DAYS['days']


def is_trade_day(day=None):
    """Whether ``day`` is an A-share trading day (QDII subscription follows it)."""
    day = day or datetime.now(SHANGHAI_TZ).date()
    days = _load_trade_days()
    if not days:
        return day.weekday() < 5
    key = day.isoformat()
    if key in days:
        return True
    return day.weekday() < 5 if key > max(days) else False


def _channel_text(channel, currency):
    if channel['status'] == 'normal':
        return '不限'
    symbol = '¥' if currency == 'CNY' else '$'
    return f"{symbol}{channel['amount_text']}"


def build_daily_report(conn):
    """Plain-text daily report: purchasable total + one line per buyable fund."""
    summaries, summary, _ = list_fund_summaries(conn)

    totals = []
    for currency in ('CNY', 'USD'):
        bucket = (summary.get('purchasable') or {}).get(currency) or {}
        if not bucket.get('total_count'):
            continue
        symbol = '¥' if currency == 'CNY' else '$'
        if bucket.get('unlimited_count'):
            totals.append(f'{symbol}不限')
        elif bucket.get('amount'):
            totals.append(f"{symbol}{bucket['amount_text']}")

    lines = []
    for item in summaries:
        currency = (item.get('currency') or 'CNY').upper()
        parts = []
        seen = set()
        for key in ('direct', 'agency', 'all'):
            channel = item.get(key)
            if not channel or channel['status'] not in PURCHASABLE_STATUSES:
                continue
            label = _CHANNEL_LABELS.get(channel['channel_type'], key)
            text = _channel_text(channel, currency)
            if (label, text) in seen:
                continue
            seen.add((label, text))
            parts.append(f'{label} {text}')
        if parts:
            lines.append(f"{len(lines) + 1}. {item['name']} ｜ " + ' ／ '.join(parts))

    head = [
        f"纳斯达克100场外基金额度日报 · {datetime.now(SHANGHAI_TZ).strftime('%m-%d')}",
        '',
        '可购最大总额：' + (' / '.join(totals) or '无'),
        '',
    ]
    if lines:
        head.append(f'可购买基金（{len(lines)} 只）：')
        head.append('')
        head.extend(lines)
    else:
        head.append('可购买基金：暂无')
    return '\n'.join(head)
