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
    '代办', '申购赎回代办', '申购赎回代理', '代理机构',
    '境外主要投资场所', '节假日', '投资组合',
    '风险等级', '最低申购金额', '最低追加申购', '最低赎回', '最低持有',
    '最低认购', '单笔最低',
)
DIRECT_WORDS = ('直销',)
AGENCY_WORDS = ('代销', '第三方销售', '第三方机构', '销售机构')
_DATE_IN_TITLE_RE = re.compile(r'\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日')


def is_temporary_suspension(title, text=''):
    """Holiday / non-trading-day suspensions are not quota changes."""
    title = normalize_text(title)
    text = normalize_text(text)
    if '继续暂停' not in title and _DATE_IN_TITLE_RE.search(title):
        if '提示性公告' in title and '暂停' in title:
            return True
        if '暂停申购' in title and '赎回' in title:
            return True
    combined = title + ' ' + text
    if ('非交易日' in combined or '休市' in combined) and '暂停申购' in combined:
        if '恢复' in combined:
            return True
    return False


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
    if is_temporary_suspension(title):
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


_CODE_ROW_RE = re.compile(
    r'(?:下属分级基金(?:的)?交易代码|下属分级基金交易代码|交易代码|基金代码)'
    r'[：:]?\s*((?:\d{6}[\s、,，]*)+)'
)
_FLAG_HEADER_RE = r'是否(?:暂停|限制|恢复)[^。；;\n]{0,160}?'
_TABLE_DASHES = '-—–－'
_TABLE_ITEM = r'(?:[-—–－]|[\d,]+(?:\.\d+)?(?:\s*(?:万美元|美元|万元|元))?)'
_TABLE_ITEM_RE = re.compile(_TABLE_ITEM)
_TABLE_SEQUENCE_RE = re.compile(
    r'\s*(?:[（(]\s*单位\s*[：:][^）)]*[）)]\s*)?'
    r'(' + _TABLE_ITEM + r'(?:\s+' + _TABLE_ITEM + r')*)'
)


def _share_class_affected(text, fund):
    """Return True/False when a share-class table explicitly covers the fund.

    Some announcements list several share classes and mark only one as affected.
    Without this check a limit for the I class would also be applied to A/C and
    USD classes.
    """
    text = normalize_text(text)
    code = str(fund.get('code') or '')
    if not code:
        return None
    for code_match in _CODE_ROW_RE.finditer(text):
        codes = re.findall(r'\d{6}', code_match.group(1))
        if code not in codes:
            continue
        flag_pattern = re.compile(
            _FLAG_HEADER_RE + r'((?:[是否][\s、,，]*){%d})' % len(codes)
        )
        window = text[code_match.end():code_match.end() + 240]
        for flag_match in flag_pattern.finditer(window):
            flags = re.findall(r'[是否]', flag_match.group(1))
            if len(flags) == len(codes):
                return flags[codes.index(code)] == '是'
    return None


def _parse_table_item(item):
    """Convert a table cell such as ``1 万美元`` or ``10 元`` to an amount."""
    item = (item or '').strip()
    if item in _TABLE_DASHES:
        return None
    match = re.match(r'([\d,]+(?:\.\d+)?)\s*(万美元|美元|万元|元)?', item)
    if not match:
        return None
    try:
        amount = float(match.group(1).replace(',', ''))
    except ValueError:
        return None
    if '万' in (match.group(2) or ''):
        amount *= 10000
    return amount if amount > 0 else None


def _code_table_scope(text, fund):
    """Return ``(has_code_table, fund_code_is_listed)`` for the announcement."""
    text = normalize_text(text)
    code = str(fund.get('code') or '')
    has_table = False
    for code_match in _CODE_ROW_RE.finditer(text):
        codes = re.findall(r'\d{6}', code_match.group(1))
        if len(codes) < 2:
            continue
        has_table = True
        if code in codes:
            return True, True
    return has_table, False


def _table_limit_for_code(text, fund):
    """Read a share-class limit table when it has one value per code.

    Returns ``(found, amount, snippet)``.  ``found`` is True when the fund's
    code appears in a code row and the following limit row has the same number
    of values.  ``amount`` is None for a dash / zero, meaning that share class
    is not covered by the table and should be treated as unrestricted.
    """
    text = normalize_text(text)
    code = str(fund.get('code') or '')
    if not code:
        return False, None, ''
    for code_match in _CODE_ROW_RE.finditer(text):
        codes = re.findall(r'\d{6}', code_match.group(1))
        if code not in codes:
            continue
        window = text[code_match.end():code_match.end() + 500]
        label_match = re.search(
            r'(?:限制申购金额|限制金额)[^0-9\-—–－]{0,80}', window
        )
        if not label_match:
            continue
        values_text = window[label_match.end():label_match.end() + 240]
        sequence_match = _TABLE_SEQUENCE_RE.match(values_text)
        if not sequence_match:
            continue
        tokens = _TABLE_ITEM_RE.findall(sequence_match.group(1))
        if len(tokens) != len(codes):
            continue
        value = tokens[codes.index(code)]
        snippet = window[max(0, label_match.start() - 120):label_match.end() + 200]
        return True, _parse_table_item(value), snippet
    return False, None, ''


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


def _fallback_channels(fund, text=''):
    """Return the channel to use when an announcement does not name one.

    Some share classes are sold only through one channel.  For those funds a
    channel-less announcement still applies to that channel, not to every
    channel.
    """
    text = normalize_text(text)
    if any(word in text for word in ('全渠道', '各销售渠道', '所有销售渠道', '各渠道')):
        return [('all', '全渠道')]
    default = (fund.get('default_channel') or '').lower()
    if default == 'direct':
        return [('direct', '直销')]
    if default == 'agency':
        return [('agency', '代销机构')]
    return [('all', '全渠道')]


def _business_type(text):
    text = normalize_text(text)
    if '定投' in text and '申购' not in text:
        return 'dca'
    if '转换' in text and '申购' not in text:
        return 'conversion'
    return 'subscription'


_AMOUNT_PATTERNS = (
    re.compile(
        r'(?:不超过|不得超过|不高于|最多|限额为|限额|'
        r'限制申购金额为|限制申购金额|限制金额为|限制金额|'
        r'申购金额上限为|申购金额上限|金额上限为|金额上限|上限为|上限|'
        r'限制为|限制至|调整为|调整至|调整到|设置为|设为)'
        r'\s*(?:人民币)?\s*([0-9][0-9,]*(?:\.[0-9]+)?)\s*'
        r'(万美元|万元|美元|元)'
    ),
    re.compile(
        r'([0-9][0-9,]*(?:\.[0-9]+)?)\s*(万美元|万元|美元|元)\s*'
        r'(?:以内|以下)'
    ),
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


def _extract_effective_date(text, published_at, latest=False):
    text = normalize_text(text)
    patterns = (
        r'自\s*(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日',
        r'(\d{4})-(\d{2})-(\d{2})',
        r'(\d{4})/(\d{1,2})/(\d{1,2})',
    )
    found = []
    for pattern in patterns:
        for match in re.finditer(pattern, text):
            try:
                found.append(datetime(
                    int(match.group(1)), int(match.group(2)), int(match.group(3))
                ).date().isoformat())
            except ValueError:
                continue
    if found:
        return max(found) if latest else found[0]
    return (published_at or '')[:10] or None


def _status_in(text):
    text = normalize_text(text)
    if '暂停' in text:
        return 'paused'
    if '恢复' in text or '正常' in text:
        return 'normal'
    return None


def _downgrade_large_pause(status, text, amount):
    """A large-purchase suspension without a parsed amount is not a full pause."""
    if status == 'paused' and amount is None:
        text = normalize_text(text)
        if '大额' in text and '暂停申购' not in text:
            return 'unknown'
    return status


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
    if is_temporary_suspension(title, text):
        return []
    if not is_relevant_title(title):
        return []
    currency = fund.get('currency') or 'CNY'
    effective_from = _extract_effective_date(text or title, published_at)
    limits = []

    if _share_class_affected(text, fund) is False:
        return []

    # A full suspension is stronger than any amount mentioned elsewhere in the
    # document.  "暂停大额申购" is different and is handled as a limit.
    if '暂停申购' in title and '大额' not in title and '恢复' not in title:
        title_channels = _channels_in(title) or _fallback_channels(fund, title)
        business_type = _business_type(title or text)
        for channel_type, channel_name in title_channels:
            _append_limit(
                limits, fund, channel_type, channel_name, 'paused', None,
                title if len(text) < 20 else text[:300], business_type,
                effective_from,
            )
        return limits

    # A share-class table scopes the announcement to the listed codes.  If the
    # fund is absent, do not apply a generic sentence or another class's amount.
    has_code_table, code_listed = _code_table_scope(text, fund)
    if has_code_table and not code_listed:
        title_channels = _channels_in(title) or _fallback_channels(fund, title)
        business_type = _business_type(title or text)
        for channel_type, channel_name in title_channels:
            _append_limit(
                limits, fund, channel_type, channel_name, 'unknown', None,
                text[:300], business_type, effective_from,
            )
        return limits

    # Share-class tables are more precise than free-form sentences.  Use them
    # when every code has a value and the announcement is a limit adjustment.
    table_found, table_amount, table_text = _table_limit_for_code(text, fund)
    if (
        table_found
        and any(word in title for word in ('大额', '限制', '限额', '调整'))
        and not ('恢复' in title and '暂停' not in title)
    ):
        effective_from = _extract_effective_date(text, published_at, latest=True)
        title_channels = _channels_in(title) or _fallback_channels(fund, title)
        business_type = _business_type(title or text)
        status = 'amount_cap' if table_amount is not None else 'normal'
        for channel_type, channel_name in title_channels:
            _append_limit(
                limits, fund, channel_type, channel_name, status,
                table_amount, table_text, business_type, effective_from,
            )
        return limits

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
        status = _downgrade_large_pause(status, sentence, amount)
        business_type = _business_type(sentence)
        for channel_type, channel_name in channels:
            _append_limit(
                limits, fund, channel_type, channel_name, status, amount,
                sentence, business_type, effective_from,
            )

    # Fallback: title-only channel or a document without channel wording.
    if not limits:
        title_channels = _channels_in(title) or _fallback_channels(fund, title)
        amount, _ = _extract_amount(text, currency)
        status = 'amount_cap' if amount is not None else _status_in(title)
        if status is None:
            status = _status_in(text)
        if status is None:
            status = 'unknown'
        status = _downgrade_large_pause(status, title or text, amount)
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
        for channel_type, channel_name in _fallback_channels(fund, title):
            _append_limit(
                limits, fund, channel_type, channel_name, 'unknown', None, title,
                'subscription', effective_from,
            )

    return limits
