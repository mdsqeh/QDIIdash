"""Market data adapters.

Primary sources are chosen to work from mainland China:
- Eastmoney for A-shares, ETFs, US stocks/ETFs and HK stocks
- Eastmoney fund pages for open-end fund NAV history
- Binance public market-data mirror for crypto
- Frankfurter / er-api for CNY exchange rates
"""
import json
import re
from datetime import datetime, timedelta, timezone

import requests

from .db import SHANGHAI_TZ, iso, now_iso, today_str
from .portfolio import fx_rate, parse_date

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Referer': 'https://quote.eastmoney.com/',
}
TIMEOUT = 15
BINANCE_HOSTS = ['https://data-api.binance.vision', 'https://api.binance.com']


class PriceError(Exception):
    pass


def _get(url, **kwargs):
    kwargs.setdefault('headers', HEADERS)
    kwargs.setdefault('timeout', TIMEOUT)
    response = requests.get(url, **kwargs)
    response.raise_for_status()
    return response


def _get_json(url, **kwargs):
    return _get(url, **kwargs).json()


def _scaled(value, digits):
    if value is None or value == '-':
        return None
    try:
        return float(value) / (10 ** int(digits or 0))
    except (TypeError, ValueError):
        return None


def eastmoney_secid(symbol, market):
    symbol = str(symbol).strip().upper()
    if market == 'CN':
        return ('1.' if symbol[:1] in ('5', '6', '9') else '0.') + symbol
    if market == 'HK':
        return '116.' + symbol.zfill(5)
    return None


def resolve_eastmoney_us(symbol):
    symbol = symbol.upper()
    for market_id in ('105', '106', '107'):
        key = f'{market_id}.{symbol}'
        try:
            if eastmoney_quote(key).get('price'):
                return key
        except Exception:
            continue
    raise PriceError(f'东方财富未找到美股代码 {symbol}')


def eastmoney_quote(quote_key):
    url = ('https://push2.eastmoney.com/api/qt/stock/get'
           f'?secid={quote_key}&fields=f43,f57,f58,f59,f60,f86')
    data = _get_json(url).get('data')
    if not data:
        raise PriceError(f'东方财富行情为空: {quote_key}')
    digits = data.get('f59') or 0
    price = _scaled(data.get('f43'), digits)
    prev_close = _scaled(data.get('f60'), digits)
    if price is None:
        raise PriceError(f'东方财富价格无效: {quote_key}')
    return {
        'name': (data.get('f58') or '').strip(),
        'price': price,
        'prev_close': prev_close,
        'symbol': (data.get('f57') or '').strip(),
    }


def eastmoney_kline(quote_key, start, end):
    url = ('https://push2his.eastmoney.com/api/qt/stock/kline/get'
           f'?secid={quote_key}&klt=101&fqt=0'
           f'&beg={parse_date(start).strftime("%Y%m%d")}'
           f'&end={parse_date(end).strftime("%Y%m%d")}'
           '&fields1=f1,f2,f3&fields2=f51,f53')
    data = _get_json(url).get('data')
    if not data:
        return []
    result = []
    for line in data.get('klines') or []:
        parts = line.split(',')
        if len(parts) < 2:
            continue
        try:
            result.append((parts[0], float(parts[1])))
        except ValueError:
            continue
    return result


def parse_fund_nav_js(text):
    name_match = re.search(r'var fS_name\s*=\s*"(.*?)";', text)
    name = name_match.group(1) if name_match else ''
    trend = re.search(r'var Data_netWorthTrend\s*=\s*(\[.*?\]);', text, re.S)
    if not trend:
        return name, []
    rows = json.loads(trend.group(1))
    result = []
    for row in rows:
        try:
            dt = datetime.fromtimestamp(row['x'] / 1000, tz=SHANGHAI_TZ).date()
            nav = float(row['y'])
        except (KeyError, TypeError, ValueError, OSError):
            continue
        if nav > 0:
            result.append((dt.isoformat(), nav))
    result.sort(key=lambda x: x[0])
    return name, result


def fund_nav(code):
    url = f'https://fund.eastmoney.com/pingzhongdata/{code}.js'
    text = _get(url).content.decode('utf-8', errors='replace')
    name, rows = parse_fund_nav_js(text)
    if not rows:
        raise PriceError(f'基金净值未找到: {code}')
    return name, rows


def _binance_symbol(symbol):
    symbol = str(symbol).upper().strip()
    if symbol.endswith(('USDT', 'USDC', 'BUSD', 'FDUSD')):
        return symbol
    return symbol + 'USDT'


def _binance_get(path):
    last_error = None
    for host in BINANCE_HOSTS:
        try:
            return _get_json(host + path)
        except Exception as exc:  # try the next mirror
            last_error = exc
    raise PriceError(f'加密货币行情不可用: {last_error}')


def binance_quote(symbol):
    pair = _binance_symbol(symbol)
    data = _binance_get(f'/api/v3/ticker/price?symbol={pair}')
    return {'name': pair, 'price': float(data['price']), 'pair': pair}


def binance_klines(symbol, start, end):
    pair = _binance_symbol(symbol)
    start_ms = int(datetime.combine(parse_date(start), datetime.min.time(),
                                    tzinfo=SHANGHAI_TZ).timestamp() * 1000)
    end_ms = int(datetime.combine(parse_date(end) + timedelta(days=1), datetime.min.time(),
                                  tzinfo=SHANGHAI_TZ).timestamp() * 1000)
    result = []
    cursor = start_ms
    while cursor < end_ms:
        data = _binance_get(
            f'/api/v3/klines?symbol={pair}&interval=1d&startTime={cursor}'
            f'&endTime={end_ms}&limit=1000'
        )
        if not data:
            break
        for row in data:
            try:
                # Binance daily candles open at 00:00 UTC. Label the row by the
                # UTC open date so the close is not shifted into the next day.
                bar_date = datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc).date()
                close = float(row[4])
            except (IndexError, TypeError, ValueError, OSError):
                continue
            result.append((bar_date, close))
        last_open = int(data[-1][0])
        if last_open <= cursor:
            break
        cursor = last_open + 1
    by_date = {d.isoformat(): close for d, close in result}
    return sorted(by_date.items())


def _eastmoney_fx_rate(currency, timeout=None):
    """Latest offshore CNH rate from Eastmoney (fast inside mainland China)."""
    currency = (currency or '').upper()
    if currency not in ('USD', 'HKD'):
        return None
    kwargs = {'timeout': timeout} if timeout else {}
    data = _get_json(
        'https://push2.eastmoney.com/api/qt/stock/get'
        f'?secid=133.{currency}CNH&fields=f43,f59',
        **kwargs,
    ).get('data')
    if not data:
        return None
    price = _scaled(data.get('f43'), data.get('f59'))
    return price if price and price > 0 else None


def fx_latest(currencies, timeout=None):
    wanted = {str(c or 'CNY').upper() for c in currencies}
    found = {}
    if 'CNY' in wanted:
        found['CNY'] = 1.0
    # Eastmoney is usually fast/reachable from mainland China and provides
    # USD/CNH and HKD/CNH, close enough for a latest-rate fallback.
    for currency in ('USD', 'HKD'):
        if currency in wanted:
            rate = _eastmoney_fx_rate(currency, timeout=timeout)
            if rate:
                found[currency] = rate
                if currency == 'USD' and 'USDT' in wanted:
                    found['USDT'] = rate
    remaining = wanted - set(found) - {'CNY'}
    if remaining:
        try:
            kwargs = {'timeout': timeout} if timeout else {}
            data = _get_json('https://open.er-api.com/v6/latest/USD', **kwargs)
            rates = data.get('rates') or {}
            cny = rates.get('CNY')
            if cny:
                for currency in remaining:
                    if currency in ('USD', 'USDT'):
                        found[currency] = float(cny)
                    elif currency in rates and rates[currency]:
                        found[currency] = float(cny) / float(rates[currency])
        except Exception:
            pass
    return found


def fx_history(currency, start, end, timeout=None):
    currency = currency.upper()
    if currency == 'CNY':
        return []
    if currency == 'USDT':
        currency = 'USD'
    url = (f'https://api.frankfurter.app/{parse_date(start)}..{parse_date(end)}'
           f'?from={currency}&to=CNY')
    kwargs = {'timeout': timeout} if timeout else {}
    data = _get_json(url, **kwargs)
    result = []
    for day, values in (data.get('rates') or {}).items():
        rate = values.get('CNY')
        if rate:
            result.append((day, float(rate)))
    result.sort(key=lambda x: x[0])
    return result


def _upsert_price(conn, asset_id, price_date, close, prev_close, source):
    conn.execute(
        """INSERT INTO wm_price (asset_id, price_date, close, prev_close, source, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(asset_id, price_date) DO UPDATE SET
             close = excluded.close,
             prev_close = COALESCE(excluded.prev_close, wm_price.prev_close),
             source = excluded.source,
             fetched_at = excluded.fetched_at""",
        (asset_id, price_date, close, prev_close, source, now_iso()),
    )


def _update_asset_last(conn, asset_id, price, price_date, error=None):
    conn.execute(
        """UPDATE wm_asset SET last_price = ?, last_price_date = ?, last_price_at = ?,
                               last_error = ? WHERE id = ?""",
        (price, price_date, now_iso(), error, asset_id),
    )


def resolve_quote_key(conn, asset):
    if asset.get('quote_key'):
        return asset['quote_key']
    market = asset['market']
    symbol = asset['symbol']
    if market == 'CN':
        key = eastmoney_secid(symbol, market)
    elif market == 'HK':
        key = eastmoney_secid(symbol, market)
    elif market == 'US':
        key = resolve_eastmoney_us(symbol)
    elif market == 'CRYPTO':
        key = _binance_symbol(symbol)
    elif market == 'FUND_CN':
        key = symbol
    else:
        key = None
    if key:
        conn.execute('UPDATE wm_asset SET quote_key = ? WHERE id = ?', (key, asset['id']))
        asset['quote_key'] = key
    return key


def _first_tx_date(conn, asset_id):
    row = conn.execute(
        'SELECT MIN(trade_date) AS d FROM wm_transaction WHERE asset_id = ?',
        (asset_id,),
    ).fetchone()
    return row['d'] if row and row['d'] else None


def sync_asset(conn, asset, full=False, start=None):
    source = asset.get('quote_source') or 'manual'
    if source in ('manual', 'accrual'):
        return 0
    today = parse_date(today_str())

    asset_id = asset['id']
    if source == 'fund_em':
        name, rows = fund_nav(asset['symbol'])
        if name and (not asset.get('name') or asset.get('name') == asset.get('symbol')):
            conn.execute('UPDATE wm_asset SET name = ? WHERE id = ?', (name, asset_id))
        prev = conn.execute(
            'SELECT close FROM wm_price WHERE asset_id = ? ORDER BY price_date DESC LIMIT 1',
            (asset_id,),
        ).fetchone()
        prev_close = float(prev['close']) if prev else None
        count = 0
        for price_date, nav in rows:
            _upsert_price(conn, asset_id, price_date, nav, prev_close, 'fund_em')
            prev_close = nav
            count += 1
        latest = rows[-1] if rows else None
        if latest:
            _update_asset_last(conn, asset_id, latest[1], latest[0])
        return count

    if source == 'binance':
        pair = _binance_symbol(asset.get('quote_key') or asset['symbol'])
        if start:
            start_date = parse_date(start)
        elif full:
            start_date = parse_date(
                _first_tx_date(conn, asset_id) or (today - timedelta(days=365)))
        else:
            last = conn.execute(
                'SELECT MAX(price_date) AS d FROM wm_price WHERE asset_id = ?',
                (asset_id,),
            ).fetchone()
            start_date = (parse_date(last['d']) + timedelta(days=1)) if last and last['d'] \
                else parse_date(_first_tx_date(conn, asset_id) or (today - timedelta(days=365)))
        end = today
        rows = binance_klines(pair, start_date, end)
        prev = conn.execute(
            'SELECT close FROM wm_price WHERE asset_id = ? AND price_date < ? ORDER BY price_date DESC LIMIT 1',
            (asset_id, iso(start_date)),
        ).fetchone()
        prev_close = float(prev['close']) if prev else None
        count = 0
        for price_date, close in rows:
            _upsert_price(conn, asset_id, price_date, close, prev_close, 'binance')
            prev_close = close
            count += 1
        try:
            quote = binance_quote(pair)
            latest_row = conn.execute(
                'SELECT close FROM wm_price WHERE asset_id = ? ORDER BY price_date DESC LIMIT 1',
                (asset_id,),
            ).fetchone()
            _upsert_price(conn, asset_id, today_str(), quote['price'],
                          float(latest_row['close']) if latest_row else None, 'binance')
            _update_asset_last(conn, asset_id, quote['price'], today_str())
        except Exception:
            if rows:
                _update_asset_last(conn, asset_id, rows[-1][1], rows[-1][0])
        return count

    if source == 'eastmoney':
        key = resolve_quote_key(conn, asset)
        if not key:
            raise PriceError(f'无法解析行情代码: {asset["symbol"]}')
        start_date = parse_date(start) if start else None
        if not start_date and full:
            first = _first_tx_date(conn, asset_id)
            start_date = parse_date(first) if first else None
        if not start_date:
            last = conn.execute(
                'SELECT MAX(price_date) AS d FROM wm_price WHERE asset_id = ?',
                (asset_id,),
            ).fetchone()
            last_date = last['d'] if last and last['d'] else None
            if last_date:
                start_date = parse_date(last_date) + timedelta(days=1)
            else:
                start_date = today - timedelta(days=365)
        end = today
        rows = []
        if start_date <= end:
            rows = eastmoney_kline(key, start_date, end)
        prev = conn.execute(
            'SELECT close FROM wm_price WHERE asset_id = ? AND price_date < ? ORDER BY price_date DESC LIMIT 1',
            (asset_id, iso(start_date)),
        ).fetchone()
        prev_close = float(prev['close']) if prev else None
        count = 0
        for price_date, close in rows:
            _upsert_price(conn, asset_id, price_date, close, prev_close, 'eastmoney')
            prev_close = close
            count += 1
        # Refresh the name and use the quote when the daily bar is not published yet.
        quote = eastmoney_quote(key)
        if quote.get('name'):
            conn.execute(
                """UPDATE wm_asset SET name = ? WHERE id = ?
                   AND (name IS NULL OR name = '' OR name = ?)""",
                (quote['name'], asset_id, asset['symbol']),
            )
        last = conn.execute(
            'SELECT price_date, close FROM wm_price WHERE asset_id = ? ORDER BY price_date DESC LIMIT 1',
            (asset_id,),
        ).fetchone()
        # For US symbols the daily bar date is the US trading date; do not add a
        # second "Beijing today" row from the live quote. It would shift daily P&L.
        if quote.get('price') and (not last or (asset['market'] == 'CN' and last['price_date'] < today_str())):
            _upsert_price(conn, asset_id, today_str(), quote['price'], quote.get('prev_close'), 'eastmoney_quote')
            _update_asset_last(conn, asset_id, quote['price'], today_str())
        elif last:
            _update_asset_last(conn, asset_id, float(last['close']), last['price_date'])
        return count

    raise PriceError(f'未知行情源: {source}')


def ensure_fx(conn, currency, start, end):
    currency = (currency or 'CNY').upper()
    if currency == 'CNY':
        return
    start = parse_date(start)
    end = parse_date(end)
    have = conn.execute(
        'SELECT COUNT(*) AS c FROM wm_fx_rate WHERE currency = ? AND rate_date BETWEEN ? AND ?',
        (currency, iso(start), iso(end)),
    ).fetchone()['c']
    if not have:
        try:
            rows = fx_history(currency, start, end)
            for day, rate in rows:
                conn.execute(
                    """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(rate_date, currency) DO UPDATE SET cny_rate = excluded.cny_rate""",
                    (day, currency, rate, 'frankfurter'),
                )
        except Exception:
            pass
    latest = conn.execute(
        'SELECT rate_date FROM wm_fx_rate WHERE currency = ? ORDER BY rate_date DESC LIMIT 1',
        (currency,),
    ).fetchone()
    if not latest or latest['rate_date'] < today_str():
        try:
            rates = fx_latest([currency])
            rate = rates.get(currency)
            if rate:
                conn.execute(
                    """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(rate_date, currency) DO UPDATE SET cny_rate = excluded.cny_rate""",
                    (today_str(), currency, rate, 'latest'),
                )
        except Exception:
            pass


def ensure_fx_range(conn, currencies, start, end, timeout=None):
    start = parse_date(start)
    end = parse_date(end)
    if start > end:
        return
    for currency in set((c or 'CNY').upper() for c in currencies):
        if currency == 'CNY':
            continue
        try:
            rows = fx_history(currency, start, end, timeout=timeout)
            for day, rate in rows:
                conn.execute(
                    """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(rate_date, currency) DO UPDATE SET cny_rate = excluded.cny_rate""",
                    (day, currency, rate, 'frankfurter'),
                )
        except Exception:
            # Historical provider unavailable: try only the latest rate.
            try:
                rates = fx_latest([currency], timeout=timeout)
                rate = rates.get(currency)
                if rate:
                    conn.execute(
                        """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
                           VALUES (?, ?, ?, ?)
                           ON CONFLICT(rate_date, currency) DO UPDATE SET cny_rate = excluded.cny_rate""",
                        (today_str(), currency, rate, 'latest'),
                    )
            except Exception:
                pass


def get_fx_for_date(conn, currency, on_date):
    currency = (currency or 'CNY').upper()
    if currency == 'CNY':
        return 1.0
    day = parse_date(on_date)
    # Make sure the specific date (or the prior business day) is covered; an old
    # stored rate from months ago must not be reused for a backdated trade.
    ensure_fx(conn, currency, day - timedelta(days=7), day)
    rate = fx_rate(conn, currency, on_date)
    if rate is not None:
        return rate
    if day < parse_date(today_str()):
        raise PriceError(f'缺少 {currency}/CNY 历史汇率（{on_date}）')
    try:
        rates = fx_latest([currency])
    except Exception as exc:
        raise PriceError(f'无法获取 {currency}/CNY 汇率: {exc}')
    rate = rates.get(currency)
    if not rate:
        raise PriceError(f'无法获取 {currency}/CNY 汇率')
    # Store the latest rate under today so portfolio valuation can use it even
    # when the historical provider is unavailable.
    conn.execute(
        """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(rate_date, currency) DO UPDATE SET cny_rate = excluded.cny_rate""",
        (today_str(), currency, rate, 'latest'),
    )
    return rate


def sync_all(conn, full=False, asset_ids=None, markets=None, sources=None):
    started = now_iso()
    where = ['is_active = 1', "quote_source NOT IN ('manual', 'accrual')"]
    params = []
    if asset_ids:
        where.append('id IN (' + ','.join('?' for _ in asset_ids) + ')')
        params.extend(asset_ids)
    if markets:
        where.append('market IN (' + ','.join('?' for _ in markets) + ')')
        params.extend(markets)
    if sources:
        where.append('quote_source IN (' + ','.join('?' for _ in sources) + ')')
        params.extend(sources)
    # Only sync assets that are actually held or have an active plan.
    where.append(
        '(id IN (SELECT asset_id FROM wm_transaction) OR '
        "id IN (SELECT asset_id FROM wm_dca_plan WHERE status = 'active'))"
    )
    rows = conn.execute(
        'SELECT * FROM wm_asset WHERE ' + ' AND '.join(where), params
    ).fetchall()
    assets = [dict(r) for r in rows]
    ok, failed = 0, []
    for asset in assets:
        try:
            sync_asset(conn, asset, full=full)
            conn.execute('UPDATE wm_asset SET last_error = NULL WHERE id = ?', (asset['id'],))
            ok += 1
        except Exception as exc:
            failed.append(f'{asset["symbol"]}: {exc}')
            conn.execute('UPDATE wm_asset SET last_error = ? WHERE id = ?', (str(exc)[:300], asset['id']))
        conn.commit()
    conn.execute(
        """INSERT INTO wm_sync_log (kind, status, started_at, finished_at, count, message)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ('prices', 'done' if not failed else 'partial', started, now_iso(), ok,
         '; '.join(failed)[:1000] or None),
    )
    conn.commit()
    return {'ok': ok, 'failed': failed}


def ensure_cn_calendar(conn):
    today = parse_date(today_str())
    meta = {r['key']: r['value'] for r in conn.execute('SELECT key, value FROM wm_meta').fetchall()}
    max_date = meta.get('cn_calendar_max')
    if max_date:
        try:
            if parse_date(max_date) >= today + timedelta(days=30):
                return
        except ValueError:
            pass
    fetched_at = meta.get('cn_calendar_fetched_at')
    if fetched_at:
        try:
            if (datetime.now(SHANGHAI_TZ) - datetime.fromisoformat(fetched_at)).total_seconds() < 12 * 3600:
                return
        except (TypeError, ValueError):
            pass
    try:
        rows = eastmoney_kline('1.000001', today - timedelta(days=3650),
                               today + timedelta(days=30))
        for price_date, _ in rows:
            conn.execute(
                """INSERT OR IGNORE INTO wm_trade_calendar (market, cal_date, is_open)
                   VALUES ('CN', ?, 1)""",
                (price_date,),
            )
        if rows:
            conn.execute(
                """INSERT INTO wm_meta (key, value) VALUES ('cn_calendar_min', ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                (rows[0][0],),
            )
            conn.execute(
                """INSERT INTO wm_meta (key, value) VALUES ('cn_calendar_max', ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                (rows[-1][0],),
            )
        conn.execute(
            """INSERT INTO wm_meta (key, value) VALUES ('cn_calendar_fetched_at', ?)
               ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
            (now_iso(),),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        try:
            conn.execute(
                """INSERT INTO wm_meta (key, value) VALUES ('cn_calendar_fetched_at', ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                (now_iso(),),
            )
            conn.commit()
        except Exception:
            conn.rollback()


def is_cn_trading_day(conn, day):
    day = parse_date(day)
    meta = {r['key']: r['value'] for r in conn.execute(
        "SELECT key, value FROM wm_meta WHERE key IN ('cn_calendar_min', 'cn_calendar_max')"
    ).fetchall()}
    min_date, max_date = meta.get('cn_calendar_min'), meta.get('cn_calendar_max')
    if min_date and max_date and parse_date(min_date) <= day <= parse_date(max_date):
        row = conn.execute(
            "SELECT 1 FROM wm_trade_calendar WHERE market = 'CN' AND cal_date = ? AND is_open = 1",
            (iso(day),),
        ).fetchone()
        return row is not None
    return day.weekday() < 5


def next_trading_day(conn, market, day, direction=1):
    day = parse_date(day)
    for _ in range(40):
        if market == 'CN':
            open_day = is_cn_trading_day(conn, day)
        elif market in ('US', 'HK'):
            open_day = day.weekday() < 5
        else:
            open_day = True
        if open_day:
            return day
        day += timedelta(days=direction)
    return day


def sync_log(conn, limit=10):
    return [dict(r) for r in conn.execute(
        'SELECT * FROM wm_sync_log ORDER BY id DESC LIMIT ?', (limit,)
    ).fetchall()]
