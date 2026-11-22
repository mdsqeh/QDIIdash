"""Heuristic parser for QDII purchase-limit announcements.

The parser is deliberately conservative: it keeps the original announcement text
and marks low-confidence results instead of guessing.  Manual overrides are
stored separately in ``quota_limit`` (``is_manual=1``).
"""
import re
from datetime import datetime


POSITIVE_TITLE_WORDS = (
    '申购', '限额', '限制', '暂停', '恢复', '大额', '上限', '金额限制',
)
NEGATIVE_TITLE_WORDS = (
    '季度报告', '中期报告', '年度报告', '招募说明书', '产品资料概要',
    '净值', '分红', '停牌', '估值', '溢价', '折价', '基金经理',
    '基金合同', '托管协议', '份额发售', '上市交易', '新增申购赎回代办',
    '代办', '申购赎回代办', '境外主要投资场所', '节假日', '投资组合',
    '风险等级',
)
DIRECT_WORDS = ('直销',)
AGENCY_WORDS = ('代销', '第三方销售', '第三方机构', '销售机构')


def normalize_text(value):
    text = str(value or '')
    text = text.replace('\u3000', ' ')
    text = re.sub(r'\s+', ' ', text)
    # PDF line breaks often split a Chinese word into "不超 过".  Spaces between
    # Chinese characters are not meaningful here, so join them back.
    text = re.sub(r'(?<=[\u4e00-\u9fff])\s+(?=[\u4e00-\u9fff])', '', text)
    return text.strip()


def is_relevant_title(title):
    title = normalize_text(title)
    if not title:
        return False
    if not any(word in title for word in POSITIVE_TITLE_WORDS):
        return False
    if not any(word in title for word in ('申购', '限额', '限制', '上限', '大额', '金额')):
        return False
    if any(word in title for word in NEGATIVE_TITLE_WORDS):
        return False
    return True


def is_share_class_relevant(title, fund):
    title = normalize_text(title)
    share_class = (fund.get('share_class') or 'A').upper()
    classes = {
        letter for letter in 'ACIDE'
        if any(marker in title for marker in (f'{letter}类', f'{letter} 类', f'{letter}份额'))
    }
    if classes and share_class not in classes:
        return False
    currency = (fund.get('currency') or 'CNY').upper()
    # A currency-specific announcement should only update that currency.
    if '美元' in title and '人民币' not in title and currency != 'USD':
        return False
    if '人民币' in title and '美元' not in title and currency != 'CNY':
        return False
    return True


def _split_sentences(text):
    text = normalize_text(text)
    if not text:
        return []
    parts = re.split(r'[。；;\n]+', text)
    return [p.strip() for p in parts if p.strip()]


def _channels_in(text):
    text = normalize_text(text)
    found = []
    if any(word in text for word in DIRECT_WORDS):
        if '直销电子交易平台' in text:
            found.append(('direct', '直销电子交易平台'))
        elif '直销柜台' in text:
            found.append(('direct', '直销柜台'))
        else:
            found.append(('direct', '直销'))
    if any(word in text for word in AGENCY_WORDS):
        if '代销' in text:
            found.append(('agency', '代销机构'))
        else:
            found.append(('agency', '第三方销售'))
    return found


def _business_type(text):
    text = normalize_text(text)
    if '定投' in text and '申购' not in text:
        return 'dca'
    if '转换' in text and '申购' not in text:
        return 'conversion'
    return 'subscription'


_AMOUNT_PATTERNS = (
    re.compile(r'(?:不超过|不得超过|限额|限制金额|金额上限|上限|限额为|上限为)\s*(?:人民币)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*(万美元|万元|美元|元)?'),
    re.compile(r'([0-9][0-9,]*(?:\.[0-9]+)?)\s*(万美元|万元|美元|元)\s*(?:以内|以下)'),
)


def _extract_amount(text, currency='CNY'):
    text = normalize_text(text)
    for pattern in _AMOUNT_PATTERNS:
        for match in pattern.finditer(text):
            number_text = match.group(1).replace(',', '')
            try:
                number = float(number_text)
            except ValueError:
                continue
            unit = match.group(2) or ''
            if unit in ('万元', '万美元'):
                number *= 10000
            if number <= 0 or number >= 1_000_000_000:
                continue
            context = text[max(0, match.start() - 24):match.end() + 8]
            if (currency or 'CNY') == 'CNY' and '美元' in context:
                continue
            return number, match.group(0).strip()
    return None, ''


def _extract_effective_date(text, published_at):
    text = normalize_text(text)
    patterns = (
        r'自\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日',
        r'(\d{4})-(\d{2})-(\d{2})',
        r'(\d{4})/(\d{1,2})/(\d{1,2})',
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            try:
                return datetime(
                    int(match.group(1)), int(match.group(2)), int(match.group(3))
                ).date().isoformat()
            except ValueError:
                continue
    return (published_at or '')[:10] or None


def _status_in(text):
    text = normalize_text(text)
    if '暂停' in text:
        return 'paused'
    if '恢复' in text or '正常' in text:
        return 'normal'
    return None


def _confidence(channel_type, status):
    if status == 'amount_cap':
        return 0.9 if channel_type != 'all' else 0.7
    if status in ('paused', 'normal'):
        return 0.7 if channel_type != 'all' else 0.5
    return 0.2


def _append_limit(limits, fund, channel_type, channel_name, status, amount,
                  limit_text, business_type, effective_from):
    key = (channel_type, channel_name, status, amount, business_type)
    if any(
        (item['channel_type'], item['channel_name'], item['status'],
         item['amount'], item['business_type']) == key
        for item in limits
    ):
        return
    limits.append({
        'channel_type': channel_type,
        'channel_name': channel_name,
        'status': status,
        'amount': amount,
        'currency': fund.get('currency') or 'CNY',
        'limit_text': (limit_text or '')[:300],
        'business_type': business_type,
        'effective_from': effective_from,
        'confidence': _confidence(channel_type, status),
    })


def parse_announcement(title, text, fund, published_at=None):
    """Return a list of channel-level limit dicts for one announcement."""
    title = normalize_text(title)
    text = normalize_text(text)
    currency = fund.get('currency') or 'CNY'
    effective_from = _extract_effective_date(text or title, published_at)
    limits = []

    # Primary path: split the PDF text into sentences and parse channel-specific
    # statements.  This handles announcements that set different limits for
    # direct sales and agency channels in the same document.
    for sentence in _split_sentences(text):
        channels = _channels_in(sentence)
        if not channels:
            continue
        amount, _ = _extract_amount(sentence, currency)
        status = 'amount_cap' if amount is not None else _status_in(sentence)
        if status is None:
            continue
        business_type = _business_type(sentence)
        for channel_type, channel_name in channels:
            _append_limit(
                limits, fund, channel_type, channel_name, status, amount,
                sentence, business_type, effective_from,
            )

    # Fallback: title-only channel or a document without channel wording.
    if not limits:
        title_channels = _channels_in(title)
        if not title_channels:
            title_channels = [('all', '全渠道')]
        amount, _ = _extract_amount(text, currency)
        status = 'amount_cap' if amount is not None else _status_in(title)
        if status is None:
            status = _status_in(text)
        if status is None:
            status = 'unknown'
        business_type = _business_type(title or text)
        for channel_type, channel_name in title_channels:
            _append_limit(
                limits, fund, channel_type, channel_name, status, amount,
                title if len(text) < 20 else text[:300], business_type,
                effective_from,
            )

    # If the document mentions both direct and agency but no sentence was
    # parsed, keep both channels rather than silently collapsing them.
    if not limits:
        _append_limit(
            limits, fund, 'all', '全渠道', 'unknown', None, title,
            'subscription', effective_from,
        )

    return limits
