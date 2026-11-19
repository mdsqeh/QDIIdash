"""Wealth module routes and API."""
import csv
import hashlib
import hmac
import io
import json
import math
import os
import threading
import time
from datetime import datetime, timedelta

from flask import (Blueprint, Response, current_app, jsonify, redirect,
                   render_template, request, session, url_for)
from werkzeug.exceptions import HTTPException

from . import dca, prices
from .cash import apply_transaction_cash, reverse_transaction_cash
from .db import DB_PATH, SHANGHAI_TZ, close_db, connect, get_db, iso, now_iso, today_str
from .portfolio import (account_cash, build_history, build_positions, build_summary,
                        compute_position, fx_rate as portfolio_fx_rate,
                        latest_price, parse_date)
from .schema import init_wealth_db

wealth_bp = Blueprint('wealth', __name__)

RANGES = {'1m': 30, '3m': 90, '6m': 180, '1y': 365, '3y': 1095, 'all': None}
MARKETS = {'CN', 'US', 'HK', 'FUND_CN', 'CRYPTO', 'BANK', 'MANUAL'}
CLASSES = {'stock', 'etf', 'fund', 'crypto', 'bank_wm', 'other'}
FREQUENCIES = {'daily', 'weekly', 'biweekly', 'monthly'}
TX_TYPES = {'buy', 'sell', 'dividend', 'reinvest', 'fee', 'interest', 'split', 'adjust'}
ASSET_CURRENCIES = {'CNY', 'USD', 'HKD'}
MARKET_DEFAULT_CURRENCY = {
    'CN': 'CNY', 'FUND_CN': 'CNY', 'BANK': 'CNY',
    'US': 'USD', 'HK': 'HKD', 'CRYPTO': 'USD', 'MANUAL': 'CNY',
}


# ── auth ──────────────────────────────────────────────────────────

def _password():
    return os.environ.get('WM_PASSWORD', '')


def _client_ip():
    remote = request.remote_addr or ''
    # Only trust X-Forwarded-For when the direct peer is a local reverse proxy;
    # this prevents a direct external client from spoofing 127.0.0.1.
    if remote in ('127.0.0.1', '::1'):
        forwarded = request.headers.get('X-Forwarded-For', '')
        if forwarded:
            return forwarded.split(',')[0].strip()
    return remote


def _local_request():
    return _client_ip() in ('127.0.0.1', '::1')


def _safe_next(target):
    return target if target and (target.startswith('/wealth') or target.startswith('/api/wm')) else '/wealth'


@wealth_bp.before_request
def _guard():
    path = request.path
    if path in ('/wealth/login', '/wealth/logout'):
        return None
    if not (path == '/wealth' or path.startswith('/api/wm')):
        return None
    if not _password():
        if _local_request():
            return None
        if path.startswith('/api/'):
            return jsonify({'error': '请先配置 WM_PASSWORD 环境变量'}), 503
        return render_template('wealth_login.html', configured=False,
                               error='请先配置 WM_PASSWORD 环境变量'), 503
    if session.get('wm_auth'):
        return None
    if path.startswith('/api/'):
        return jsonify({'error': 'unauthorized'}), 401
    return redirect(url_for('wealth.login', next=path))


@wealth_bp.after_request
def _no_store_api(response):
    if request.path.startswith('/api/wm/'):
        response.headers['Cache-Control'] = 'no-store'
    return response


@wealth_bp.route('/wealth/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        expected = _password()
        provided = request.form.get('password', '')
        if expected and hmac.compare_digest(provided, expected):
            session['wm_auth'] = True
            session.permanent = True
            return redirect(_safe_next(request.args.get('next')))
        time.sleep(0.4)
        return render_template('wealth_login.html', configured=bool(expected),
                               error='密码错误'), 401
    if not _password():
        if _local_request():
            return redirect('/wealth')
        return render_template('wealth_login.html', configured=False,
                               error='请先配置 WM_PASSWORD 环境变量'), 503
    if session.get('wm_auth'):
        return redirect('/wealth')
    return render_template('wealth_login.html', configured=True, error=None)


@wealth_bp.route('/wealth/logout', methods=['POST'])
def logout():
    session.pop('wm_auth', None)
    return redirect('/wealth/login')


# ── helpers ───────────────────────────────────────────────────────

def _body():
    return request.get_json(silent=True) or {}


def _error(message, code=400):
    return jsonify({'error': message}), code


def _flag(value, default=False):
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in ('1', 'true', 'yes', 'on')
    return bool(value)


def _range_days(key):
    return RANGES.get((key or '1y').lower(), 365)


def _num(value, default=0.0):
    if value is None or value == '':
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _cash_fields(data):
    """Parse an optional manual cash snapshot; blank means "leave unchanged"."""
    raw = data.get('cash_amount')
    if raw is None or str(raw).strip() == '':
        return None, None
    try:
        amount = float(raw)
    except (TypeError, ValueError):
        raise ValueError('现金余额应为数字')
    if not math.isfinite(amount):
        raise ValueError('现金余额应为有限数字')
    raw_date = str(data.get('cash_date') or '').strip()
    if raw_date:
        try:
            balance_date = iso(parse_date(raw_date))
        except (TypeError, ValueError):
            raise ValueError('现金余额日期格式无效')
    else:
        balance_date = today_str()
    if balance_date > today_str():
        raise ValueError('现金余额日期不能晚于今天')
    return amount, balance_date


def _cash_balance_items(data):
    """Parse optional per-currency manual cash rows.

    Blank amounts are ignored so a GUI can keep an empty row without clearing
    an existing balance.  ``0`` deliberately clears a currency.
    """
    raw = data.get('cash_balances')
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError('现金余额格式无效')
    items = []
    default_currency = str(data.get('currency') or 'CNY').upper()
    for entry in raw:
        if not isinstance(entry, dict):
            raise ValueError('现金余额格式无效')
        raw_amount = entry.get('amount')
        if raw_amount is None or str(raw_amount).strip() == '':
            continue
        try:
            amount = float(raw_amount)
        except (TypeError, ValueError):
            raise ValueError('现金余额应为数字')
        if not math.isfinite(amount):
            raise ValueError('现金余额应为有限数字')
        currency = str(entry.get('currency') or default_currency).strip().upper()
        if len(currency) != 3 or not currency.isalpha():
            raise ValueError('现金币种应为 3 位字母代码')
        raw_date = str(entry.get('balance_date') or entry.get('cash_date') or '').strip()
        if raw_date:
            try:
                balance_date = iso(parse_date(raw_date))
            except (TypeError, ValueError):
                raise ValueError('现金余额日期格式无效')
        else:
            balance_date = today_str()
        if balance_date > today_str():
            raise ValueError('现金余额日期不能晚于今天')
        items.append((currency, amount, balance_date))
    return items


def _upsert_account_cash(conn, account_id, amount, balance_date, currency):
    conn.execute(
        """INSERT INTO wm_account_balance
           (account_id, balance_date, amount, currency, source, created_at)
           VALUES (?, ?, ?, ?, 'manual', ?)
           ON CONFLICT(account_id, balance_date, currency) DO UPDATE SET
             amount = excluded.amount,
             source = excluded.source,
             created_at = excluded.created_at""",
        (account_id, balance_date, amount, currency, now_iso()),
    )


def _asset(conn, asset_id):
    row = conn.execute('SELECT * FROM wm_asset WHERE id = ?', (asset_id,)).fetchone()
    return dict(row) if row else None


def _account(conn, account_id):
    if not account_id:
        return None
    row = conn.execute('SELECT * FROM wm_account WHERE id = ?', (account_id,)).fetchone()
    return dict(row) if row else None


def _default_currency(market):
    return MARKET_DEFAULT_CURRENCY.get(market, 'CNY')


def _asset_native_currency(market, name=None):
    if market == 'FUND_CN':
        return prices.fund_currency_hint(name)
    return MARKET_DEFAULT_CURRENCY.get(market)


def _default_source(market):
    return {
        'CN': 'eastmoney', 'US': 'eastmoney', 'HK': 'eastmoney',
        'FUND_CN': 'fund_em', 'CRYPTO': 'binance', 'BANK': 'accrual',
        'MANUAL': 'manual',
    }.get(market, 'manual')


def _default_class(market):
    return {'FUND_CN': 'fund', 'CRYPTO': 'crypto', 'BANK': 'bank_wm'}.get(market, 'stock')


def _default_lot(asset_class, market):
    if market == 'CN' and asset_class in ('stock', 'etf'):
        return 100, 0
    if market == 'HK':
        return 100, 0
    return 1, 1


def _create_asset_record(conn, data, resolve=False):
    symbol = str(data.get('symbol') or '').strip()
    market = str(data.get('market') or '').strip().upper()
    if not symbol:
        raise ValueError('资产代码不能为空')
    if market not in MARKETS:
        raise ValueError('不支持的资产市场')
    if market == 'CN':
        upper = symbol.upper()
        if upper.endswith(('.SS', '.SZ')):
            symbol = symbol[:-3]
        elif upper.startswith(('SH', 'SZ')) and len(symbol) > 2:
            symbol = symbol[2:]
    elif market == 'HK' and symbol.upper().endswith('.HK'):
        symbol = symbol[:-3]
    if market in ('CN', 'HK', 'FUND_CN', 'US', 'CRYPTO'):
        symbol = symbol.upper()
    asset_class = str(data.get('asset_class') or _default_class(market)).strip()
    if asset_class not in CLASSES:
        raise ValueError('不支持的资产类别')
    existing = conn.execute(
        'SELECT * FROM wm_asset WHERE symbol = ? AND market = ?', (symbol, market)
    ).fetchone()
    if existing:
        return dict(existing), False

    name = str(data.get('name') or '').strip()
    if resolve and not name:
        name = _lookup_name(conn, market, symbol)
    if not name:
        name = symbol
    native_currency = _asset_native_currency(market, name)
    provided_currency = str(data.get('currency') or '').upper()
    currency = provided_currency or native_currency or _default_currency(market)
    if currency not in ASSET_CURRENCIES:
        raise ValueError(f'资产币种应为 {"/".join(sorted(ASSET_CURRENCIES))}，收到 {currency}')
    quote_source = str(data.get('quote_source') or _default_source(market))
    if native_currency and currency != native_currency and quote_source not in ('manual', 'accrual'):
        quote_source = 'manual'
    lot_size, allow_fraction = _default_lot(asset_class, market)
    maturity_date = data.get('maturity_date') or None
    if maturity_date:
        maturity_date = iso(parse_date(maturity_date))
    if data.get('lot_size') is not None:
        lot_size = max(_num(data.get('lot_size'), 1), 1e-9)
    if data.get('allow_fraction') is not None:
        allow_fraction = 1 if _flag(data.get('allow_fraction')) else 0

    quote_key = data.get('quote_key')
    if not quote_key:
        if market == 'CN':
            quote_key = prices.eastmoney_secid(symbol, market)
        elif market == 'HK':
            quote_key = prices.eastmoney_secid(symbol, market)
        elif market == 'FUND_CN':
            quote_key = symbol
        elif market == 'CRYPTO':
            quote_key = prices._binance_symbol(symbol)
    if resolve and market == 'US' and not quote_key and quote_source not in ('manual', 'accrual'):
        try:
            quote_key = prices.resolve_eastmoney_us(symbol)
        except Exception:
            quote_key = None

    cur = conn.execute(
        """INSERT INTO wm_asset
           (symbol, market, asset_class, name, currency, quote_source, quote_key,
            lot_size, allow_fraction, valuation_mode, annual_rate, interest_mode,
            maturity_date, is_active, note, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)""",
        (symbol, market, asset_class, name, currency, quote_source, quote_key,
         lot_size, allow_fraction,
         data.get('valuation_mode') or ('accrual' if market == 'BANK' else 'price'),
         _num(data.get('annual_rate'), None) if data.get('annual_rate') not in (None, '') else None,
         data.get('interest_mode') or 'simple',
         maturity_date, data.get('note'), now_iso()),
    )
    asset = _asset(conn, cur.lastrowid)
    return asset, True


def _lookup_name(conn, market, symbol):
    if market == 'FUND_CN':
        row = None
        try:
            row = conn.execute('SELECT name, nav FROM funds WHERE code = ?', (symbol,)).fetchone()
        except Exception:
            pass
        if row and row['name']:
            return row['name']
        try:
            name, rows = prices.fund_nav(symbol)
            return name
        except Exception:
            return ''
    if market in ('CN', 'US', 'HK'):
        try:
            key = prices.eastmoney_secid(symbol, market) or prices.resolve_eastmoney_us(symbol)
            return prices.eastmoney_quote(key).get('name') or ''
        except Exception:
            return ''
    return ''


def _units_before(conn, asset_id, on_date, exclude_id=None, account_id=None):
    sql = "SELECT type, units, split_ratio FROM wm_transaction WHERE asset_id = ? AND trade_date <= ?"
    params = [asset_id, iso(on_date)]
    if exclude_id:
        sql += ' AND id != ?'
        params.append(exclude_id)
    if account_id is None:
        sql += ' AND account_id IS NULL'
    else:
        sql += ' AND account_id = ?'
        params.append(account_id)
    sql += ' ORDER BY trade_date, id'
    units = 0.0
    for r in conn.execute(sql, params).fetchall():
        if r['type'] in ('buy', 'reinvest'):
            units += float(r['units'] or 0)
        elif r['type'] == 'sell':
            units -= float(r['units'] or 0)
        elif r['type'] == 'adjust':
            units += float(r['units'] or 0)
        elif r['type'] == 'split':
            units *= float(r['split_ratio'] or r['units'] or 1)
    return units


def _principal_before(conn, asset_id, on_date, exclude_id=None, account_id=None):
    sql = "SELECT type, amount FROM wm_transaction WHERE asset_id = ? AND trade_date <= ?"
    params = [asset_id, iso(on_date)]
    if exclude_id:
        sql += ' AND id != ?'
        params.append(exclude_id)
    if account_id is None:
        sql += ' AND account_id IS NULL'
    else:
        sql += ' AND account_id = ?'
        params.append(account_id)
    sql += ' ORDER BY trade_date, id'
    principal = 0.0
    for r in conn.execute(sql, params).fetchall():
        if r['type'] == 'buy':
            principal += float(r['amount'] or 0)
        elif r['type'] == 'sell':
            principal -= float(r['amount'] or 0)
        elif r['type'] == 'adjust':
            principal += float(r['amount'] or 0)
    return principal


def _create_transaction(conn, data, source='manual', import_hash=None, affect_cash=True):
    asset = _asset(conn, int(data.get('asset_id') or 0))
    if not asset:
        raise ValueError('资产不存在')
    tx_type = str(data.get('type') or '').lower()
    if tx_type not in TX_TYPES:
        raise ValueError('不支持的交易类型')
    trade_date = parse_date(data.get('trade_date') or today_str())
    account_id = data.get('account_id')
    account_id = None if account_id in (None, '', 0, '0') else int(account_id)
    if account_id:
        account = _account(conn, account_id)
        if not account:
            raise ValueError('账户不存在')
    cash_adjusted = 1 if affect_cash and account_id and tx_type != 'adjust' else 0
    provided_currency = str(data.get('currency') or '').upper()
    if provided_currency and provided_currency != str(asset.get('currency') or 'CNY').upper():
        raise ValueError(f'交易币种 {provided_currency} 与资产币种 {asset.get("currency")} 不一致')
    currency = asset['currency']
    fee = max(_num(data.get('fee'), 0), 0)
    units = _num(data.get('units'), 0)
    price = _num(data.get('price'), 0)
    amount = _num(data.get('amount'), 0)
    split_ratio = _num(data.get('split_ratio'), 0)

    if tx_type == 'adjust':
        if asset.get('valuation_mode') == 'accrual':
            units, price = amount, 0.0
        else:
            price = 0.0
        fee = 0.0
        if abs(units) < 1e-12 and abs(amount) < 1e-12:
            raise ValueError('修正数量和金额不能同时为 0')
    elif asset.get('valuation_mode') == 'accrual':
        if tx_type not in ('buy', 'sell', 'interest', 'fee'):
            raise ValueError('银行理财只支持买入、赎回、利息和费用')
        if tx_type in ('buy', 'sell'):
            if amount <= 0:
                raise ValueError('本金金额必须大于 0')
            units, price = amount, 1.0
        elif tx_type in ('interest', 'fee'):
            if amount <= 0:
                raise ValueError('金额必须大于 0')
            units, price = 0.0, 0.0
    elif tx_type == 'split':
        if split_ratio <= 0:
            raise ValueError('拆分比例必须大于 0')
        units, price, amount = 0.0, 0.0, 0.0
    elif tx_type in ('buy', 'sell', 'reinvest'):
        if amount <= 0 and units > 0 and price > 0:
            amount = units * price
        if units <= 0 and amount > 0 and price > 0:
            units = amount / price
        if price <= 0 and amount > 0 and units > 0:
            price = amount / units
        if units <= 0 or price <= 0 or amount <= 0:
            raise ValueError('数量、价格和金额必须大于 0')
    else:
        if amount <= 0:
            raise ValueError('金额必须大于 0')

    if tx_type == 'sell':
        if asset.get('valuation_mode') == 'accrual':
            held = _principal_before(conn, asset['id'], trade_date, account_id=account_id)
        else:
            held = _units_before(conn, asset['id'], trade_date, account_id=account_id)
        if units > held + 1e-6:
            raise ValueError(f'卖出数量超过当前持仓（{held:g}）')

    fx = 1.0
    if currency != 'CNY':
        provided = _num(data.get('fx_rate'), 0)
        if provided > 0:
            fx = provided
        else:
            fx = prices.get_fx_for_date(conn, currency, trade_date)
        # Keep wm_fx_rate in sync so historical valuation can find the rate
        # even when the transaction carried a manually supplied FX value.
        conn.execute(
            """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(rate_date, currency) DO UPDATE SET cny_rate = excluded.cny_rate""",
            (iso(trade_date), currency, fx, 'manual' if provided > 0 else 'transaction'),
        )

    cur = conn.execute(
        """INSERT OR IGNORE INTO wm_transaction
           (asset_id, account_id, type, trade_date, units, price, amount, fee, currency,
            fx_rate, split_ratio, cash_adjusted, source, import_hash, note, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (asset['id'], account_id, tx_type, iso(trade_date), units, price, amount, fee,
         currency, fx, split_ratio if tx_type == 'split' else None, cash_adjusted,
         source, import_hash, data.get('note'), now_iso()),
    )
    if cur.rowcount == 0:
        return None
    if cash_adjusted:
        apply_transaction_cash(conn, {
            'account_id': account_id, 'type': tx_type, 'currency': currency,
            'amount': amount, 'fee': fee, 'trade_date': trade_date,
        })
    return cur.lastrowid


def _maybe_backfill(conn, asset):
    if asset.get('quote_source') in ('manual', 'accrual'):
        return
    try:
        prices.sync_asset(conn, asset, full=True)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        try:
            conn.execute('UPDATE wm_asset SET last_error = ? WHERE id = ?',
                         (str(exc)[:300], asset['id']))
            conn.commit()
        except Exception:
            conn.rollback()


def _transaction_row(conn, tx_id):
    row = conn.execute(
        """SELECT t.*, a.symbol, a.name AS asset_name, a.market, a.asset_class,
                  ac.name AS account_name
           FROM wm_transaction t JOIN wm_asset a ON a.id = t.asset_id
           LEFT JOIN wm_account ac ON ac.id = t.account_id
           WHERE t.id = ?""", (tx_id,)
    ).fetchone()
    return dict(row) if row else None


def _downsample(points, limit=1500):
    if len(points) <= limit:
        return points
    step = math.ceil(len(points) / limit)
    result = points[::step]
    if result and result[-1] != points[-1]:
        result.append(points[-1])
    return result


# ── pages / status ────────────────────────────────────────────────

def _static_version():
    try:
        folder = current_app.static_folder
        stamps = []
        for name in ('wealth.js', 'wealth.css'):
            path = os.path.join(folder, name)
            if os.path.exists(path):
                stamps.append(os.path.getmtime(path))
        return str(int(max(stamps))) if stamps else '1'
    except Exception:
        return '1'


@wealth_bp.route('/wealth')
def index():
    response = current_app.make_response(
        render_template('wealth.html', static_version=_static_version()))
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return response


@wealth_bp.route('/api/wm/status')
def api_status():
    conn = get_db()
    counts = {
        'assets': conn.execute('SELECT COUNT(*) AS c FROM wm_asset WHERE is_active = 1').fetchone()['c'],
        'transactions': conn.execute('SELECT COUNT(*) AS c FROM wm_transaction').fetchone()['c'],
        'plans': conn.execute("SELECT COUNT(*) AS c FROM wm_dca_plan WHERE status != 'archived'").fetchone()['c'],
        'pending_orders': conn.execute(
            "SELECT COUNT(*) AS c FROM wm_dca_order WHERE status = 'awaiting_price'").fetchone()['c'],
        'failed_orders': conn.execute(
            "SELECT COUNT(*) AS c FROM wm_dca_order WHERE status = 'failed'").fetchone()['c'],
    }
    last_sync = conn.execute('SELECT * FROM wm_sync_log ORDER BY id DESC LIMIT 1').fetchone()
    return jsonify({
        'counts': counts,
        'last_sync': dict(last_sync) if last_sync else None,
        'configured': bool(_password()),
        'today': today_str(),
        'ranges': list(RANGES.keys()),
    })


@wealth_bp.route('/api/wm/summary')
def api_summary():
    conn = get_db()
    days = _range_days(request.args.get('range', '1y'))
    return jsonify(build_summary(conn, range_days=days))


@wealth_bp.route('/api/wm/history')
def api_history():
    conn = get_db()
    end = parse_date(request.args.get('end') or today_str())
    start_arg = request.args.get('start')
    if start_arg:
        start = parse_date(start_arg)
    else:
        days = _range_days(request.args.get('range', '1y'))
        first = conn.execute('SELECT MIN(trade_date) AS d FROM wm_transaction').fetchone()
        first_date = parse_date(first['d']) if first and first['d'] else None
        start = max(first_date, end - timedelta(days=days)) if (first_date and days) else (first_date or end - timedelta(days=365))
    points = build_history(conn, start, end)
    first_active = next((i for i, p in enumerate(points) if p['value'] or p['net_flow']), None)
    points = points[first_active:] if first_active is not None else []
    return jsonify({
        'start': iso(start),
        'end': iso(end),
        'points': _downsample(points),
    })


@wealth_bp.route('/api/wm/positions')
def api_positions():
    conn = get_db()
    as_of = parse_date(request.args.get('as_of') or today_str())
    positions, total = build_positions(conn, as_of)
    return jsonify({'as_of': iso(as_of), 'total': total, 'positions': positions})


def _position_base(conn, asset, account_id, as_of):
    """Position from real trades, excluding the editable correction row."""
    sql = """SELECT * FROM wm_transaction
             WHERE asset_id = ? AND trade_date <= ?
               AND COALESCE(source, 'manual') != 'adjust'"""
    params = [asset['id'], iso(as_of)]
    if account_id is None:
        sql += ' AND account_id IS NULL'
    else:
        sql += ' AND account_id = ?'
        params.append(account_id)
    sql += ' ORDER BY trade_date, id'
    txs = [dict(r) for r in conn.execute(sql, params).fetchall()]
    fx_cache = {}

    def fx_fn(currency, on_date):
        key = ((currency or 'CNY').upper(), iso(on_date))
        if key not in fx_cache:
            fx_cache[key] = portfolio_fx_rate(conn, key[0], key[1])
        return fx_cache[key]

    return compute_position(
        asset, txs, as_of, price=latest_price(conn, asset['id'], as_of), fx_fn=fx_fn)


@wealth_bp.route('/api/wm/positions/adjust', methods=['POST'])
def api_position_adjust():
    conn = get_db()
    data = _body()
    asset = _asset(conn, int(data.get('asset_id') or 0))
    if not asset:
        return _error('资产不存在', 404)
    try:
        account_id = data.get('account_id')
        account_id = None if account_id in (None, '', 0, '0') else int(account_id)
        if account_id and not _account(conn, account_id):
            raise ValueError('账户不存在')
        units = _num(data.get('units'), None)
        cost = _num(data.get('cost_native'), None)
        if units is None:
            raise ValueError('数量或成本格式不正确')
        if not math.isfinite(units):
            raise ValueError('数量或成本格式不正确')
        if units < 0:
            raise ValueError('数量和成本不能为负数')
        bank = (asset.get('valuation_mode') or 'price') == 'accrual'
        if bank:
            cost = units
        else:
            if cost is None or not math.isfinite(cost):
                raise ValueError('数量或成本格式不正确')
            if cost < 0:
                raise ValueError('数量和成本不能为负数')
            if units <= 1e-12:
                cost = 0.0
        as_of = parse_date(data.get('trade_date') or today_str())
        base = _position_base(conn, asset, account_id, as_of)
        delta_units = units - float(base.get('units') or 0)
        delta_cost = cost - float(base.get('cost_native') or 0)

        sql = "SELECT * FROM wm_transaction WHERE asset_id = ? AND source = 'adjust'"
        params = [asset['id']]
        if account_id is None:
            sql += ' AND account_id IS NULL'
        else:
            sql += ' AND account_id = ?'
            params.append(account_id)
        existing = conn.execute(sql, params).fetchall()
        for row in existing:
            if row['cash_adjusted']:
                reverse_transaction_cash(conn, row)
            conn.execute('DELETE FROM wm_transaction WHERE id = ?', (row['id'],))

        if abs(delta_units) < 1e-9 and abs(delta_cost) < 1e-9:
            conn.commit()
            return jsonify({'ok': True, 'changed': bool(existing),
                            'removed': bool(existing)})

        tx_id = _create_transaction(conn, {
            'asset_id': asset['id'], 'account_id': account_id, 'type': 'adjust',
            'trade_date': iso(as_of), 'units': delta_units, 'amount': delta_cost,
            'fee': 0, 'currency': asset['currency'],
            'note': str(data.get('note') or '持仓修正'),
        }, source='adjust', affect_cash=False)
        if tx_id is None:
            raise ValueError('无法保存持仓修正')
        conn.commit()
        return jsonify({'ok': True, 'changed': True, 'removed': False, 'id': tx_id})
    except (ValueError, TypeError, prices.PriceError) as exc:
        conn.rollback()
        return _error(str(exc))


# ── assets ────────────────────────────────────────────────────────

@wealth_bp.route('/api/wm/lookup')
def api_lookup():
    symbol = (request.args.get('symbol') or '').strip()
    market = (request.args.get('market') or '').upper()
    if not symbol:
        return jsonify({'found': False, 'error': '缺少代码'})
    if market not in MARKETS:
        return jsonify({'found': False, 'error': '缺少或错误的市场'})
    try:
        if market == 'FUND_CN':
            conn = get_db()
            try:
                row = conn.execute('SELECT code, name, nav FROM funds WHERE code = ?', (symbol,)).fetchone()
            except Exception:
                row = None
            name = row['name'] if row and row['name'] else ''
            price = row['nav'] if row else None
            currency = prices.fund_currency_hint(name)
            if not currency:
                # Local fund tables may only keep the base name without the
                # share-class suffix (人民币/美元现汇/港币). Ask Eastmoney for
                # the full name before deciding the NAV currency.
                try:
                    nav_name, rows = prices.fund_nav(symbol)
                except Exception:
                    nav_name, rows = '', []
                if nav_name:
                    name = nav_name
                if rows:
                    price = rows[-1][1]
                currency = prices.fund_currency(name)
            return jsonify({'found': True, 'name': name, 'price': price,
                            'currency': currency,
                            'quote_source': 'fund_em', 'quote_key': symbol})
        if market in ('CN', 'US', 'HK'):
            key = prices.eastmoney_secid(symbol, market) or prices.resolve_eastmoney_us(symbol)
            quote = prices.eastmoney_quote(key)
            return jsonify({'found': True, 'name': quote.get('name'), 'price': quote['price'],
                            'prev_close': quote.get('prev_close'),
                            'currency': _default_currency(market), 'quote_source': 'eastmoney',
                            'quote_key': key})
        if market == 'CRYPTO':
            quote = prices.binance_quote(symbol)
            return jsonify({'found': True, 'name': quote['name'], 'price': quote['price'],
                            'currency': 'USD', 'quote_source': 'binance',
                            'quote_key': quote['pair']})
        if market == 'BANK':
            return jsonify({'found': True, 'name': symbol, 'currency': 'CNY',
                            'quote_source': 'accrual', 'quote_key': None})
        return jsonify({'found': True, 'name': symbol, 'currency': 'CNY',
                        'quote_source': 'manual', 'quote_key': None})
    except Exception as exc:
        return jsonify({'found': False, 'error': str(exc)})


@wealth_bp.route('/api/wm/assets', methods=['GET', 'POST'])
def api_assets():
    conn = get_db()
    if request.method == 'GET':
        rows = conn.execute(
            """SELECT a.*,
                      (SELECT COUNT(*) FROM wm_transaction t WHERE t.asset_id = a.id) AS tx_count,
                      (SELECT COUNT(*) FROM wm_dca_plan p WHERE p.asset_id = a.id) AS plan_count
               FROM wm_asset a ORDER BY a.is_active DESC, a.asset_class, a.market, a.symbol"""
        ).fetchall()
        return jsonify([dict(r) for r in rows])

    data = _body()
    try:
        asset, created = _create_asset_record(conn, data, resolve=True)
        conn.commit()
        if created:
            _maybe_backfill(conn, asset)
        asset = _asset(conn, asset['id'])
        return jsonify({**asset, 'existing': not created}), 201 if created else 200
    except ValueError as exc:
        return _error(str(exc))


@wealth_bp.route('/api/wm/assets/<int:asset_id>', methods=['PUT', 'DELETE'])
def api_asset(asset_id):
    conn = get_db()
    asset = _asset(conn, asset_id)
    if not asset:
        return _error('资产不存在', 404)
    if request.method == 'DELETE':
        tx = conn.execute('SELECT COUNT(*) AS c FROM wm_transaction WHERE asset_id = ?', (asset_id,)).fetchone()['c']
        plan = conn.execute('SELECT COUNT(*) AS c FROM wm_dca_plan WHERE asset_id = ?', (asset_id,)).fetchone()['c']
        if plan:
            return _error('该资产已有关联定投计划，暂不能删除；可先停用', 409)
        force = _flag(request.args.get('force') or _body().get('force'))
        if tx and not force:
            return _error(f'该资产有 {tx} 笔交易；确认删除请使用强制删除', 409)
        if tx:
            adjusted = conn.execute(
                'SELECT * FROM wm_transaction WHERE asset_id = ? AND cash_adjusted = 1',
                (asset_id,),
            ).fetchall()
            for old_tx in adjusted:
                reverse_transaction_cash(conn, old_tx)
            conn.execute('DELETE FROM wm_transaction WHERE asset_id = ?', (asset_id,))
        conn.execute('DELETE FROM wm_price WHERE asset_id = ?', (asset_id,))
        conn.execute('DELETE FROM wm_asset WHERE id = ?', (asset_id,))
        conn.commit()
        return jsonify({'ok': True, 'deleted_transactions': tx})
    data = _body()
    if data.get('symbol') and str(data['symbol']).strip() != asset['symbol']:
        return _error('资产代码不能修改')
    if data.get('market') and str(data['market']).upper() != asset['market']:
        return _error('资产市场不能修改')
    current_currency = (asset['currency'] or 'CNY').upper()
    requested_currency = str(data.get('currency', current_currency) or current_currency).upper()
    if requested_currency not in ASSET_CURRENCIES:
        return _error(f'资产币种应为 {"/".join(sorted(ASSET_CURRENCIES))}')
    currency_changed = requested_currency != current_currency
    if currency_changed:
        used = conn.execute(
            'SELECT COUNT(*) AS c FROM wm_transaction WHERE asset_id = ?', (asset_id,)
        ).fetchone()['c']
        plans = conn.execute(
            'SELECT COUNT(*) AS c FROM wm_dca_plan WHERE asset_id = ?', (asset_id,)
        ).fetchone()['c']
        if used or plans:
            return _error('该资产已有交易或定投计划，币种不能修改；可删除后重新添加')
    fields = {
        'name': str(data.get('name', asset['name']) or asset['symbol']).strip(),
        'asset_class': data.get('asset_class', asset['asset_class']),
        'currency': requested_currency,
        'quote_source': data.get('quote_source', asset['quote_source']) or asset['quote_source'],
        'quote_key': data.get('quote_key', asset['quote_key']) or None,
        'lot_size': max(_num(data.get('lot_size'), asset['lot_size'] or 1), 1e-9),
        'allow_fraction': 1 if _flag(data.get('allow_fraction', asset['allow_fraction']),
                                     bool(asset['allow_fraction'])) else 0,
        'valuation_mode': data.get('valuation_mode', asset['valuation_mode']),
        'annual_rate': asset['annual_rate'],
        'interest_mode': data.get('interest_mode', asset['interest_mode']) or 'simple',
        'maturity_date': asset['maturity_date'],
        'is_active': 1 if _flag(data.get('is_active', asset['is_active']),
                                bool(asset['is_active'])) else 0,
        'note': data.get('note', asset['note']),
    }
    if 'annual_rate' in data:
        fields['annual_rate'] = (_num(data.get('annual_rate'), None)
                                 if data.get('annual_rate') not in (None, '') else None)
    if 'maturity_date' in data:
        maturity_date = data.get('maturity_date') or None
        if maturity_date:
            try:
                maturity_date = iso(parse_date(maturity_date))
            except (ValueError, TypeError):
                return _error('到期日格式无效')
        fields['maturity_date'] = maturity_date
    if fields['asset_class'] not in CLASSES:
        return _error('不支持的资产类别')
    if fields['valuation_mode'] not in ('price', 'accrual'):
        return _error('不支持的估值方式')
    if fields['interest_mode'] not in ('simple', 'compound'):
        return _error('不支持的计息方式')
    native_currency = _asset_native_currency(asset['market'], fields['name'])
    if (native_currency and fields['currency'] != native_currency
            and fields['quote_source'] not in ('manual', 'accrual')):
        fields['quote_source'] = 'manual'
    conn.execute(
        """UPDATE wm_asset SET name=?, asset_class=?, currency=?, quote_source=?, quote_key=?, lot_size=?,
           allow_fraction=?, valuation_mode=?, annual_rate=?, interest_mode=?,
           maturity_date=?, is_active=?, note=? WHERE id=?""",
        (*fields.values(), asset_id),
    )
    if currency_changed:
        conn.execute('DELETE FROM wm_price WHERE asset_id = ?', (asset_id,))
        conn.execute(
            """UPDATE wm_asset SET last_price = NULL, last_price_date = NULL,
               last_price_at = NULL, last_error = NULL WHERE id = ?""",
            (asset_id,),
        )
    conn.commit()
    return jsonify(_asset(conn, asset_id))


@wealth_bp.route('/api/wm/assets/<int:asset_id>/price', methods=['POST'])
def api_asset_price(asset_id):
    conn = get_db()
    asset = _asset(conn, asset_id)
    if not asset:
        return _error('资产不存在', 404)
    data = _body()
    price_date = iso(parse_date(data.get('price_date') or today_str()))
    if price_date > today_str():
        return _error('价格日期不能晚于今天')
    close = _num(data.get('close'), 0)
    if close <= 0:
        return _error('价格必须大于 0')
    prev = data.get('prev_close')
    if prev in (None, ''):
        row = conn.execute(
            'SELECT close FROM wm_price WHERE asset_id = ? AND price_date < ? ORDER BY price_date DESC LIMIT 1',
            (asset_id, price_date),
        ).fetchone()
        prev = row['close'] if row else None
    prices._upsert_price(conn, asset_id, price_date, close, _num(prev, None), 'manual')
    if not asset.get('last_price_date') or price_date >= asset['last_price_date']:
        conn.execute(
            'UPDATE wm_asset SET last_price=?, last_price_date=?, last_price_at=?, last_error=NULL WHERE id=?',
            (close, price_date, now_iso(), asset_id),
        )
    conn.commit()
    return jsonify({'ok': True})


@wealth_bp.route('/api/wm/assets/<int:asset_id>/backfill', methods=['POST'])
def api_asset_backfill(asset_id):
    conn = get_db()
    asset = _asset(conn, asset_id)
    if not asset:
        return _error('资产不存在', 404)
    try:
        count = prices.sync_asset(conn, asset, full=True)
        conn.execute('UPDATE wm_asset SET last_error = NULL WHERE id = ?', (asset_id,))
        conn.commit()
        return jsonify({'ok': True, 'count': count})
    except Exception as exc:
        conn.rollback()
        try:
            conn.execute('UPDATE wm_asset SET last_error = ? WHERE id = ?',
                         (str(exc)[:300], asset_id))
            conn.commit()
        except Exception:
            conn.rollback()
        return _error(str(exc), 502)


# ── accounts ──────────────────────────────────────────────────────

@wealth_bp.route('/api/wm/accounts', methods=['GET', 'POST'])
def api_accounts():
    conn = get_db()
    if request.method == 'GET':
        rows = conn.execute(
            """SELECT a.*,
                      (SELECT COUNT(*) FROM wm_transaction t WHERE t.account_id = a.id) AS tx_count,
                      (SELECT COUNT(*) FROM wm_dca_plan p WHERE p.account_id = a.id) AS plan_count
               FROM wm_account a ORDER BY a.is_active DESC, a.id"""
        ).fetchall()
        cash_rows, _ = account_cash(conn)
        cash_by_account = {}
        for cash in cash_rows:
            cash_by_account.setdefault(cash['account_id'], []).append(cash)
        result = []
        for row in rows:
            item = dict(row)
            balances = cash_by_account.get(row['id'], [])
            item['cash_balances'] = [{
                'currency': cash['currency'],
                'amount': cash['amount'],
                'balance_date': cash['balance_date'],
                'cny_value': round(cash['cny_value'], 2),
                'fx_rate': cash['fx_rate'],
                'fx_missing': cash['fx_missing'],
            } for cash in balances]
            item['cash_cny'] = round(sum(cash['cny_value'] for cash in balances), 2)
            primary = next(
                (cash for cash in balances
                 if cash['currency'] == (row['currency'] or 'CNY').upper()),
                balances[0] if balances else None,
            )
            item['cash_amount'] = primary['amount'] if primary else 0.0
            item['cash_currency'] = primary['currency'] if primary else (row['currency'] or 'CNY')
            item['cash_date'] = primary['balance_date'] if primary else None
            item['cash_fx_missing'] = any(cash['fx_missing'] for cash in balances)
            result.append(item)
        return jsonify(result)
    data = _body()
    name = str(data.get('name') or '').strip()
    if not name:
        return _error('账户名称不能为空')
    currency = str(data.get('currency') or 'CNY').upper()
    if len(currency) != 3 or not currency.isalpha():
        return _error('账户币种应为 3 位字母代码')
    broker = str(data.get('broker') or '').strip() or None
    try:
        cash_items = _cash_balance_items(data)
        if 'cash_balances' in data:
            cash_amount, cash_date = None, None
        else:
            cash_amount, cash_date = _cash_fields(data)
    except ValueError as exc:
        return _error(str(exc))
    try:
        cur = conn.execute(
            'INSERT INTO wm_account (name, broker, currency, created_at) VALUES (?, ?, ?, ?)',
            (name, broker, currency, now_iso()),
        )
        for cash_currency, amount, balance_date in cash_items:
            _upsert_account_cash(conn, cur.lastrowid, amount, balance_date, cash_currency)
        if cash_amount is not None:
            _upsert_account_cash(conn, cur.lastrowid, cash_amount, cash_date, currency)
        conn.commit()
        return jsonify(dict(conn.execute('SELECT * FROM wm_account WHERE id=?', (cur.lastrowid,)).fetchone())), 201
    except Exception as exc:
        conn.rollback()
        return _error(f'账户已存在或无效: {exc}')


@wealth_bp.route('/api/wm/accounts/<int:account_id>', methods=['PUT', 'DELETE'])
def api_account(account_id):
    conn = get_db()
    account = conn.execute('SELECT * FROM wm_account WHERE id = ?', (account_id,)).fetchone()
    if not account:
        return _error('账户不存在', 404)
    if request.method == 'DELETE':
        used = conn.execute(
            'SELECT COUNT(*) AS c FROM wm_transaction WHERE account_id = ?', (account_id,)
        ).fetchone()['c']
        plans = conn.execute(
            'SELECT COUNT(*) AS c FROM wm_dca_plan WHERE account_id = ?', (account_id,)
        ).fetchone()['c']
        if used or plans:
            return _error('账户已有交易或定投计划，不能删除', 409)
        conn.execute('DELETE FROM wm_account_balance WHERE account_id = ?', (account_id,))
        conn.execute('DELETE FROM wm_account WHERE id = ?', (account_id,))
        conn.commit()
        return jsonify({'ok': True})
    data = _body()
    name = str(data.get('name', account['name']) or '').strip()
    if not name:
        return _error('账户名称不能为空')
    currency = str(data.get('currency', account['currency']) or 'CNY').upper()
    if len(currency) != 3 or not currency.isalpha():
        return _error('账户币种应为 3 位字母代码')
    broker = data.get('broker', account['broker'])
    if broker is not None:
        broker = str(broker).strip() or None
    is_active = 1 if _flag(data.get('is_active', account['is_active']),
                           bool(account['is_active'])) else 0
    try:
        cash_items = _cash_balance_items(data)
        if 'cash_balances' in data:
            cash_amount, cash_date = None, None
        else:
            cash_amount, cash_date = _cash_fields(data)
    except ValueError as exc:
        return _error(str(exc))
    try:
        conn.execute(
            'UPDATE wm_account SET name=?, broker=?, currency=?, is_active=? WHERE id=?',
            (name, broker, currency, is_active, account_id),
        )
        for cash_currency, amount, balance_date in cash_items:
            _upsert_account_cash(conn, account_id, amount, balance_date, cash_currency)
        if cash_amount is not None:
            _upsert_account_cash(conn, account_id, cash_amount, cash_date, currency)
        conn.commit()
        return jsonify(dict(conn.execute('SELECT * FROM wm_account WHERE id=?', (account_id,)).fetchone()))
    except Exception as exc:
        conn.rollback()
        return _error(f'账户名称已存在或无效: {exc}')


# ── transactions ──────────────────────────────────────────────────

@wealth_bp.route('/api/wm/transactions', methods=['GET', 'POST'])
def api_transactions():
    conn = get_db()
    if request.method == 'GET':
        conditions, params = [], []
        asset_id = request.args.get('asset_id', type=int)
        if asset_id:
            conditions.append('t.asset_id = ?')
            params.append(asset_id)
        account_arg = request.args.get('account_id')
        if account_arg == 'none':
            conditions.append('t.account_id IS NULL')
        elif account_arg:
            conditions.append('t.account_id = ?')
            params.append(int(account_arg))
        tx_type = request.args.get('type')
        if tx_type:
            conditions.append('t.type = ?')
            params.append(tx_type)
        start = request.args.get('start')
        if start:
            conditions.append('t.trade_date >= ?')
            params.append(iso(parse_date(start)))
        end = request.args.get('end')
        if end:
            conditions.append('t.trade_date <= ?')
            params.append(iso(parse_date(end)))
        where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
        total = conn.execute(
            f'SELECT COUNT(*) AS c FROM wm_transaction t {where}', params
        ).fetchone()['c']
        page = max(request.args.get('page', 1, type=int), 1)
        per_page = min(max(request.args.get('per_page', 100, type=int), 1), 500)
        rows = conn.execute(
            f"""SELECT t.*, a.symbol, a.name AS asset_name, a.market, a.asset_class,
                       ac.name AS account_name
                FROM wm_transaction t JOIN wm_asset a ON a.id = t.asset_id
                LEFT JOIN wm_account ac ON ac.id = t.account_id
                {where} ORDER BY t.trade_date DESC, t.id DESC LIMIT ? OFFSET ?""",
            params + [per_page, (page - 1) * per_page],
        ).fetchall()
        return jsonify({'total': total, 'page': page, 'per_page': per_page,
                        'transactions': [dict(r) for r in rows]})

    data = _body()
    try:
        tx_id = _create_transaction(conn, data)
        if tx_id is None:
            return _error('交易已存在（重复导入）', 409)
        conn.commit()
        asset = _asset(conn, int(data.get('asset_id')))
        _maybe_backfill(conn, asset)
        return jsonify(_transaction_row(conn, tx_id)), 201
    except (ValueError, TypeError) as exc:
        conn.rollback()
        return _error(str(exc))


@wealth_bp.route('/api/wm/transactions/<int:tx_id>', methods=['GET', 'PUT', 'DELETE'])
def api_transaction(tx_id):
    conn = get_db()
    row = conn.execute('SELECT * FROM wm_transaction WHERE id = ?', (tx_id,)).fetchone()
    if not row:
        return _error('交易不存在', 404)
    if request.method == 'GET':
        return jsonify(_transaction_row(conn, tx_id))
    if row['source'] == 'dca':
        return _error('定投生成的交易请在定投页操作，不能直接编辑', 409)
    if request.method == 'PUT' and row['type'] == 'adjust':
        return _error('持仓修正记录不能直接编辑，请重新编辑持仓', 409)
    if request.method == 'DELETE':
        if row['cash_adjusted']:
            reverse_transaction_cash(conn, row)
        conn.execute('DELETE FROM wm_transaction WHERE id = ?', (tx_id,))
        conn.commit()
        return jsonify({'ok': True})

    body = _body()
    data = {**dict(row), **body}
    # Recompute FX for the (possibly changed) trade date unless the caller
    # explicitly supplied a rate.
    if 'fx_rate' not in body:
        data.pop('fx_rate', None)
    asset = _asset(conn, int(row['asset_id']))
    if not asset:
        return _error('资产不存在')
    old_adjusted = bool(row['cash_adjusted'])
    new_account = data.get('account_id')
    enable_cash = old_adjusted or (
        (row['source'] or 'manual') == 'manual'
        and row['account_id'] is None
        and new_account not in (None, '', 0, '0')
    )
    try:
        # Re-create through the same validation/FX path as a manual entry.
        # Rollback restores the old row and cash adjustment if validation fails.
        if old_adjusted:
            reverse_transaction_cash(conn, row)
        conn.execute('DELETE FROM wm_transaction WHERE id = ?', (tx_id,))
        new_id = _create_transaction(
            conn, data, source=row['source'] or 'manual',
            import_hash=row['import_hash'], affect_cash=enable_cash)
        if new_id is None:
            raise ValueError('无法保存交易（可能与已有记录重复）')
        conn.commit()
        return jsonify(_transaction_row(conn, new_id))
    except Exception as exc:
        conn.rollback()
        return _error(str(exc))


# ── DCA ───────────────────────────────────────────────────────────

def _plan_or_404(conn, plan_id):
    row = conn.execute('SELECT * FROM wm_dca_plan WHERE id = ?', (plan_id,)).fetchone()
    return dict(row) if row else None


def _validate_plan(conn, data, existing=None):
    plan = {**(existing or {}), **data}
    asset = _asset(conn, int(plan.get('asset_id') or 0))
    if not asset:
        raise ValueError('资产不存在')
    if _num(plan.get('amount'), 0) <= 0:
        raise ValueError('定投金额必须大于 0')
    frequency = str(plan.get('frequency') or 'monthly').strip().lower()
    if frequency not in FREQUENCIES:
        raise ValueError('不支持的定投频率')
    plan['frequency'] = frequency
    plan['roll_rule'] = str(plan.get('roll_rule') or 'next').strip().lower()
    if plan['roll_rule'] not in ('next', 'prev', 'skip'):
        raise ValueError('不支持的顺延规则')
    if frequency in ('weekly', 'biweekly'):
        weekday = int(plan.get('weekday') if plan.get('weekday') is not None else 0)
        if not 0 <= weekday <= 6:
            raise ValueError('星期必须在 0-6 之间')
        plan['weekday'] = weekday
        plan['day_of_month'] = None
    elif frequency == 'monthly':
        day = int(plan.get('day_of_month') or 1)
        if not 1 <= day <= 31:
            raise ValueError('每月日期必须在 1-31 之间')
        plan['day_of_month'] = day
        plan['weekday'] = None
    else:
        plan['weekday'] = None
        plan['day_of_month'] = None
    plan['start_date'] = iso(parse_date(plan.get('start_date') or today_str()))
    if plan.get('end_date'):
        plan['end_date'] = iso(parse_date(plan['end_date']))
        if plan['end_date'] < plan['start_date']:
            raise ValueError('结束日期不能早于开始日期')
    else:
        plan['end_date'] = None
    if _num(plan.get('fee_rate'), 0) < 0 or _num(plan.get('fee_min'), 0) < 0:
        raise ValueError('费率不能为负')
    plan['amount_currency'] = str(plan.get('amount_currency') or 'CNY').upper()
    plan['catch_up'] = 1 if _flag(plan.get('catch_up', True), True) else 0
    plan['execution_mode'] = 'auto'
    plan['asset_id'] = asset['id']
    account_id = plan.get('account_id')
    plan['account_id'] = None if account_id in (None, '', 0, '0') else int(account_id)
    if plan['account_id'] is not None and not _account(conn, plan['account_id']):
        raise ValueError('账户不存在')
    return plan, asset


@wealth_bp.route('/api/wm/dca/plans', methods=['GET', 'POST'])
def api_dca_plans():
    conn = get_db()
    if request.method == 'GET':
        rows = conn.execute(
            """SELECT p.*, a.symbol, a.name AS asset_name, a.market, a.currency AS asset_currency,
                      ac.name AS account_name
               FROM wm_dca_plan p JOIN wm_asset a ON a.id = p.asset_id
               LEFT JOIN wm_account ac ON ac.id = p.account_id
               WHERE p.status != 'archived' ORDER BY p.id DESC"""
        ).fetchall()
        result = []
        for row in rows:
            plan = dict(row)
            stats = dca.plan_stats(conn, plan)
            result.append({**plan, **{k: v for k, v in stats.items() if k not in plan}})
        return jsonify(result)

    try:
        data = _body()
        plan, asset = _validate_plan(conn, data)
        preview = dca.preview(conn, plan)
        if preview['backfill_count'] > 0 and not _flag(data.get('confirm_backfill')):
            return jsonify({
                'error': '该计划会补记历史期数，请确认',
                'preview': preview,
                'need_confirm': True,
            }), 409
        cur = conn.execute(
            """INSERT INTO wm_dca_plan
               (name, asset_id, account_id, amount, amount_currency, frequency, weekday,
                day_of_month, start_date, end_date, roll_rule, fee_rate, fee_min,
                execution_mode, catch_up, status, note, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'auto', ?, 'active', ?, ?)""",
            (plan.get('name') or f'{asset["name"]} 定投', asset['id'], plan.get('account_id'),
             _num(plan['amount']), plan['amount_currency'], plan['frequency'],
             plan.get('weekday'), plan.get('day_of_month'), plan['start_date'],
             plan.get('end_date'), plan.get('roll_rule') or 'next',
             _num(plan.get('fee_rate'), 0), _num(plan.get('fee_min'), 0),
             plan['catch_up'], plan.get('note'), now_iso()),
        )
        conn.commit()
        plan_id = cur.lastrowid
        if asset.get('quote_source') not in ('manual', 'accrual'):
            try:
                prices.sync_asset(conn, asset, full=True, start=plan['start_date'])
                conn.commit()
            except Exception:
                conn.rollback()
        currencies = {
            (asset.get('currency') or 'CNY').upper(),
            (plan.get('amount_currency') or 'CNY').upper(),
        }
        currencies.discard('CNY')
        if currencies and plan['start_date'] <= today_str():
            try:
                prices.ensure_fx_range(conn, currencies, plan['start_date'], today_str())
                conn.commit()
            except Exception:
                conn.rollback()
        dca.generate_orders(conn, plan_id=plan_id)
        dca.settle_orders(conn)
        row = _plan_or_404(conn, plan_id)
        return jsonify({**row, **dca.plan_stats(conn, row)}), 201
    except (ValueError, TypeError) as exc:
        conn.rollback()
        return _error(str(exc))


@wealth_bp.route('/api/wm/dca/plans/<int:plan_id>', methods=['PUT', 'DELETE'])
def api_dca_plan(plan_id):
    conn = get_db()
    plan = _plan_or_404(conn, plan_id)
    if not plan:
        return _error('定投计划不存在', 404)
    if request.method == 'DELETE':
        conn.execute("UPDATE wm_dca_plan SET status = 'archived' WHERE id = ?", (plan_id,))
        conn.commit()
        return jsonify({'ok': True})
    data = _body()
    if data.get('start_date') and iso(parse_date(data['start_date'])) != plan['start_date']:
        count = conn.execute(
            'SELECT COUNT(*) AS c FROM wm_dca_order WHERE plan_id = ?', (plan_id,)
        ).fetchone()['c']
        if count:
            return _error('已有执行记录，开始日期不能修改')
    try:
        merged, asset = _validate_plan(conn, data, existing=plan)
        schedule_changed = any(
            (merged.get(k) or None) != (plan.get(k) or None)
            for k in ('frequency', 'weekday', 'day_of_month', 'start_date', 'roll_rule', 'end_date')
        )
        revive = (
            plan['status'] == 'finished'
            and (not merged.get('end_date')
                 or parse_date(merged['end_date']) >= parse_date(today_str()))
        )
        conn.execute(
            """UPDATE wm_dca_plan SET name=?, account_id=?, amount=?, amount_currency=?,
               frequency=?, weekday=?, day_of_month=?, start_date=?, end_date=?,
               roll_rule=?, fee_rate=?, fee_min=?, catch_up=?, note=? WHERE id=?""",
            (merged.get('name') or plan['name'], merged.get('account_id'), _num(merged['amount']),
             merged['amount_currency'], merged['frequency'], merged.get('weekday'),
             merged.get('day_of_month'), merged['start_date'], merged.get('end_date'),
             merged.get('roll_rule') or 'next', _num(merged.get('fee_rate'), 0),
             _num(merged.get('fee_min'), 0), merged['catch_up'], merged.get('note'), plan_id),
        )
        if revive:
            conn.execute("UPDATE wm_dca_plan SET status = 'active' WHERE id = ?", (plan_id,))
        if schedule_changed:
            # Do not create a burst of historical orders after a schedule edit;
            # start the new schedule from the next period.
            conn.execute(
                'UPDATE wm_dca_plan SET last_generated_date = ? WHERE id = ?',
                (iso(parse_date(today_str()) - timedelta(days=1)), plan_id),
            )
        conn.commit()
        dca.generate_orders(conn, plan_id=plan_id)
        dca.settle_orders(conn)
        row = _plan_or_404(conn, plan_id)
        return jsonify({**row, **dca.plan_stats(conn, row)})
    except (ValueError, TypeError) as exc:
        conn.rollback()
        return _error(str(exc))


@wealth_bp.route('/api/wm/dca/plans/<int:plan_id>/pause', methods=['POST'])
def api_dca_pause(plan_id):
    conn = get_db()
    dca.pause_plan(conn, plan_id)
    conn.commit()
    return jsonify({'ok': True})


@wealth_bp.route('/api/wm/dca/plans/<int:plan_id>/resume', methods=['POST'])
def api_dca_resume(plan_id):
    conn = get_db()
    dca.resume_plan(conn, plan_id)
    conn.commit()
    return jsonify({'ok': True})


@wealth_bp.route('/api/wm/dca/plans/<int:plan_id>/run-now', methods=['POST'])
def api_dca_run_now(plan_id):
    conn = get_db()
    plan = _plan_or_404(conn, plan_id)
    if not plan:
        return _error('定投计划不存在', 404)
    asset = _asset(conn, plan['asset_id'])
    today = parse_date(today_str())
    now = datetime.now(SHANGHAI_TZ)
    trade_base = today
    if asset['market'] == 'FUND_CN' and now.hour >= 15:
        trade_base = today + timedelta(days=1)
    trade_date = dca.resolve_trade_date(conn, asset, trade_base, 'next')
    existing = conn.execute(
        'SELECT * FROM wm_dca_order WHERE plan_id = ? AND due_date = ?',
        (plan_id, iso(today)),
    ).fetchone()
    if existing:
        if existing['status'] in ('failed', 'skipped'):
            conn.execute(
                "UPDATE wm_dca_order SET status='awaiting_price', message=NULL, settled_at=NULL WHERE id=?",
                (existing['id'],),
            )
    else:
        conn.execute(
            """INSERT INTO wm_dca_order
               (plan_id, due_date, trade_date, amount, amount_currency, status, created_at)
               VALUES (?, ?, ?, ?, ?, 'awaiting_price', ?)""",
            (plan_id, iso(today), iso(trade_date), plan['amount'],
             plan['amount_currency'], now_iso()),
        )
    conn.commit()
    result = dca.settle_orders(conn)
    conn.commit()
    return jsonify({'ok': True, **result})


@wealth_bp.route('/api/wm/dca/preview', methods=['POST'])
def api_dca_preview():
    conn = get_db()
    try:
        data = _body()
        plan, asset = _validate_plan(conn, data)
        return jsonify(dca.preview(conn, plan))
    except (ValueError, TypeError) as exc:
        return _error(str(exc))


@wealth_bp.route('/api/wm/dca/summary')
def api_dca_summary():
    period = request.args.get('period') or 'day'
    if period not in ('day', 'week', 'month', 'year'):
        period = 'day'
    as_of = parse_date(request.args.get('as_of') or today_str())
    return jsonify(dca.summary(get_db(), period, as_of=as_of))


@wealth_bp.route('/api/wm/dca/orders')
def api_dca_orders():
    conn = get_db()
    conditions, params = [], []
    plan_id = request.args.get('plan_id', type=int)
    if plan_id:
        conditions.append('o.plan_id = ?')
        params.append(plan_id)
    status = request.args.get('status')
    if status:
        conditions.append('o.status = ?')
        params.append(status)
    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''
    page = max(request.args.get('page', 1, type=int), 1)
    per_page = min(max(request.args.get('per_page', 100, type=int), 1), 500)
    total = conn.execute(
        f'SELECT COUNT(*) AS c FROM wm_dca_order o {where}', params
    ).fetchone()['c']
    rows = conn.execute(
        f"""SELECT o.*, p.name AS plan_name, a.symbol, a.name AS asset_name, a.market,
                   a.currency AS asset_currency
            FROM wm_dca_order o JOIN wm_dca_plan p ON p.id = o.plan_id
            JOIN wm_asset a ON a.id = p.asset_id
            {where} ORDER BY o.due_date DESC, o.id DESC LIMIT ? OFFSET ?""",
        params + [per_page, (page - 1) * per_page],
    ).fetchall()
    return jsonify({'total': total, 'page': page, 'per_page': per_page,
                    'orders': [dict(r) for r in rows]})


@wealth_bp.route('/api/wm/dca/orders/<int:order_id>/skip', methods=['POST'])
def api_dca_order_skip(order_id):
    conn = get_db()
    dca.skip_order(conn, order_id)
    conn.commit()
    return jsonify({'ok': True})


@wealth_bp.route('/api/wm/dca/orders/<int:order_id>/retry', methods=['POST'])
def api_dca_order_retry(order_id):
    conn = get_db()
    dca.retry_order(conn, order_id)
    conn.commit()
    dca.settle_orders(conn)
    conn.commit()
    return jsonify({'ok': True})


@wealth_bp.route('/api/wm/dca/run', methods=['POST'])
def api_dca_run():
    result = dca.run_dca()
    return jsonify({'ok': True, **result})


# ── sync ──────────────────────────────────────────────────────────

@wealth_bp.route('/api/wm/sync', methods=['POST'])
def api_sync():
    conn = get_db()
    data = _body()
    asset_id = data.get('asset_id')
    full = _flag(data.get('full'))
    try:
        if asset_id:
            asset = _asset(conn, int(asset_id))
            if not asset:
                return _error('资产不存在', 404)
            count = prices.sync_asset(conn, asset, full=True)
            conn.execute('UPDATE wm_asset SET last_error = NULL WHERE id = ?', (asset['id'],))
            conn.commit()
            result = {'ok': 1, 'count': count}
        else:
            result = prices.sync_all(conn, full=full, markets=data.get('markets'))
        settled = dca.settle_orders(conn)
        conn.commit()
        return jsonify({**result, **settled})
    except Exception as exc:
        conn.rollback()
        if asset_id:
            try:
                conn.execute('UPDATE wm_asset SET last_error = ? WHERE id = ?',
                             (str(exc)[:300], int(asset_id)))
                conn.commit()
            except Exception:
                conn.rollback()
        return _error(str(exc), 502)


@wealth_bp.route('/api/wm/sync/status')
def api_sync_status():
    conn = get_db()
    return jsonify(prices.sync_log(conn))


# ── import / export ──────────────────────────────────────────────

COINGECKO_TO_BINANCE = {
    'bitcoin': 'BTCUSDT', 'ethereum': 'ETHUSDT', 'solana': 'SOLUSDT',
    'binancecoin': 'BNBUSDT', 'ripple': 'XRPUSDT', 'cardano': 'ADAUSDT',
    'dogecoin': 'DOGEUSDT', 'polkadot': 'DOTUSDT', 'chainlink': 'LINKUSDT',
    'litecoin': 'LTCUSDT', 'tron': 'TRXUSDT', 'avalanche-2': 'AVAXUSDT',
}


def _import_payload():
    if 'file' in request.files:
        raw = request.files['file'].read()
        return json.loads(raw.decode('utf-8-sig')), request.files['file'].filename
    return request.get_json(force=True, silent=True), 'inline.json'


def _guess_market(symbol, currency, data_source, asset_class):
    ds = (data_source or '').upper()
    currency = (currency or '').upper()
    raw = str(symbol).upper()
    if 'COINGECKO' in ds:
        return 'CRYPTO'
    if raw.endswith('.HK') or currency == 'HKD':
        return 'HK'
    if raw.endswith('.SS') or raw.endswith('.SZ'):
        return 'FUND_CN' if (asset_class or '').upper() in ('MUTUALFUND', 'FUND') else 'CN'
    # A 6-digit CNY symbol is almost always an A-share or a domestic fund,
    # even when Ghostfolio tagged its source as YAHOO/MANUAL.
    if currency == 'CNY' and str(symbol).isdigit() and len(str(symbol)) == 6:
        return 'FUND_CN' if (asset_class or '').upper() in ('MUTUALFUND', 'FUND') else 'CN'
    if ds == 'MANUAL':
        return 'MANUAL'
    if ds.startswith('YAHOO'):
        return 'US'
    if currency == 'USD' and ds:
        return 'US'
    return 'MANUAL'


def _ghostfolio_rows(payload):
    if isinstance(payload, list):
        activities = payload
        accounts = []
        asset_profiles = {}
    else:
        activities = payload.get('activities') or payload.get('data') or []
        accounts = payload.get('accounts') or []
        asset_profiles = {}
        for item in payload.get('assetProfiles') or []:
            profile_symbol = str(item.get('symbol') or '').strip().upper()
            profile_source = str(item.get('dataSource') or '').strip().upper()
            if profile_symbol:
                asset_profiles[(profile_symbol, profile_source)] = item
                asset_profiles.setdefault((profile_symbol, ''), item)
    rows = []
    for activity in activities:
        symbol = str(activity.get('symbol') or '').strip()
        data_source = activity.get('dataSource') or ''
        activity_currency = (activity.get('currency') or '').upper()
        profile = activity.get('assetProfile') or {}
        if not profile and symbol:
            profile = (asset_profiles.get((symbol.upper(), str(data_source).upper()))
                       or asset_profiles.get((symbol.upper(), '')) or {})
        if not symbol:
            symbol = str(profile.get('symbol') or '').strip()
        if not symbol:
            continue
        data_source = activity.get('dataSource') or profile.get('dataSource') or ''
        profile_currency = (profile.get('currency') or '').upper()
        if activity_currency and profile_currency and activity_currency != profile_currency:
            rows.append({
                'skip': True,
                'reason': f'{symbol} 交易币种 {activity_currency} 与资产币种 {profile_currency} 不一致',
                'symbol': symbol,
            })
            continue
        currency = activity_currency or profile_currency or 'CNY'
        asset_class = (profile.get('assetClass') or activity.get('assetClass') or '').upper()
        market = _guess_market(symbol, currency, data_source, asset_class)
        if market == 'CN' and symbol.upper().endswith(('.SS', '.SZ')):
            symbol = symbol[:-3]
        elif market == 'HK' and symbol.upper().endswith('.HK'):
            symbol = symbol[:-3]
        if market == 'CRYPTO':
            symbol = COINGECKO_TO_BINANCE.get(symbol.lower(), symbol.upper())
        tx_type = str(activity.get('type') or '').upper()
        type_map = {'BUY': 'buy', 'SELL': 'sell', 'DIVIDEND': 'dividend',
                    'FEE': 'fee', 'INTEREST': 'interest'}
        mapped = type_map.get(tx_type)
        if not mapped:
            rows.append({'skip': True, 'reason': f'不支持的 Ghostfolio 类型 {tx_type}', 'symbol': symbol})
            continue
        quantity = _num(activity.get('quantity'), 0)
        unit_price = _num(activity.get('unitPrice'), 0)
        amount = quantity * unit_price
        if amount <= 0:
            amount = _num(activity.get('value'), 0) or unit_price
        fee = max(_num(activity.get('fee'), 0), 0)
        if mapped == 'fee' and amount <= 0:
            amount = fee
        date_value = str(activity.get('date') or '')[:10]
        if not date_value:
            rows.append({'skip': True, 'reason': '缺少日期', 'symbol': symbol})
            continue
        name = profile.get('name') or activity.get('name') or symbol
        cls = {'EQUITY': 'stock', 'ETF': 'etf', 'MUTUALFUND': 'fund',
               'CRYPTO': 'crypto', 'FIXED_INCOME': 'fund'}.get(asset_class, 'other')
        if market == 'CRYPTO':
            cls = 'crypto'
        elif market == 'FUND_CN':
            cls = 'fund'
        elif market == 'CN' and cls == 'other':
            cls = 'etf' if symbol[:1] in ('5', '1') else 'stock'
        elif market in ('US', 'HK') and cls == 'other':
            cls = 'stock'
        elif market == 'MANUAL' and cls == 'other':
            cls = 'fund' if symbol.upper().startswith('GF_') else 'other'
        rows.append({
            'market': market, 'symbol': symbol, 'currency': currency, 'name': name,
            'asset_class': cls, 'type': mapped, 'trade_date': date_value,
            'units': quantity, 'price': unit_price, 'amount': amount, 'fee': fee,
            'account_id': activity.get('accountId'), 'note': activity.get('comment') or '',
        })
    return accounts, rows


def _import_hash(row):
    key = '|'.join(str(row.get(k) or '') for k in (
        'market', 'symbol', 'type', 'trade_date', 'units', 'price', 'amount', 'fee',
        'currency', 'account_id'))
    return hashlib.sha1(key.encode('utf-8')).hexdigest()


def _resolve_import_fx(conn, rows, timeout=10):
    """Resolve import FX once per currency/date, without per-row network calls.

    Returns ``(fx_map, failed_keys, warnings)``.  A recent date (<= 7 days) may
    fall back to the latest rate when the historical provider is unavailable;
    older dates are skipped rather than guessed.
    """
    currencies = set()
    dates = []
    for row in rows:
        currency = str(row.get('currency') or '').strip().upper()
        trade_date = row.get('trade_date')
        if currency and currency != 'CNY' and trade_date:
            currencies.add(currency)
            dates.append(trade_date)
    if not currencies or not dates:
        return {}, set(), []

    start = parse_date(min(dates)) - timedelta(days=7)
    end = parse_date(max(dates))
    for currency in currencies:
        try:
            prices.ensure_fx_range(conn, {currency}, start, end, timeout=timeout)
        except Exception:
            pass

    fx_map = {}
    failed = set()
    warnings = []
    latest = {}
    today = parse_date(today_str())
    for row in rows:
        currency = str(row.get('currency') or '').strip().upper()
        trade_date = row.get('trade_date')
        if not currency or currency == 'CNY' or not trade_date:
            continue
        key = (currency, trade_date)
        if key in failed or key in fx_map:
            continue
        rate = prices.fx_rate(conn, currency, trade_date)
        if rate is None:
            age = (today - parse_date(trade_date)).days
            if 0 <= age <= 7:
                if currency not in latest:
                    latest[currency] = prices.fx_rate(conn, currency, today_str())
                if latest[currency]:
                    rate = latest[currency]
                    warnings.append(
                        f'{currency} {trade_date} 历史汇率不可用，使用最新汇率 {rate:.4f}')
        if rate is None:
            failed.add(key)
        else:
            fx_map[key] = rate
    return fx_map, failed, warnings


def _prepare_import(conn, payload, dry_run=True):
    accounts, rows = _ghostfolio_rows(payload)
    account_map = {}
    for account in accounts:
        name = (account.get('name') or '').strip()
        if name:
            account_map[str(account.get('id'))] = name
    existing_accounts = {r['name']: r['id'] for r in conn.execute('SELECT id, name FROM wm_account').fetchall()}

    new_assets = {}
    metadata = {}
    prepared = []
    skipped = duplicates = 0
    errors = []
    existing_assets = {(r['symbol'], r['market']): dict(r) for r in conn.execute(
        'SELECT * FROM wm_asset').fetchall()}
    existing_hashes = {r['import_hash'] for r in conn.execute(
        'SELECT import_hash FROM wm_transaction WHERE import_hash IS NOT NULL').fetchall()}

    for row in rows:
        if row.get('skip'):
            skipped += 1
            errors.append(row.get('reason'))
            continue
        key = (row['symbol'], row['market'])
        existing = existing_assets.get(key)
        if existing and (row['currency'] or '').upper() != (existing.get('currency') or '').upper():
            skipped += 1
            errors.append(f'{row["symbol"]} {row["trade_date"]}: 币种 {row["currency"]} '
                          f'与已有资产币种 {existing.get("currency")} 不一致')
            continue
        if key not in existing_assets and row['currency'] not in ASSET_CURRENCIES:
            skipped += 1
            errors.append(f'{row["symbol"]}: 资产币种应为 '
                          f'{"/".join(sorted(ASSET_CURRENCIES))}，收到 {row["currency"]}')
            continue
        row_name = row.get('name') or row['symbol']
        row_class = row.get('asset_class') or 'other'
        previous_meta = metadata.get(key)
        if previous_meta is None:
            metadata[key] = {'name': row_name, 'asset_class': row_class}
        else:
            if previous_meta['name'] == row['symbol'] and row_name != row['symbol']:
                previous_meta['name'] = row_name
            if previous_meta['asset_class'] == 'other' and row_class != 'other':
                previous_meta['asset_class'] = row_class
        row_hash = _import_hash(row)
        if row_hash in existing_hashes:
            duplicates += 1
            continue
        existing_hashes.add(row_hash)
        if key not in existing_assets and key not in new_assets:
            new_assets[key] = {
                'symbol': row['symbol'], 'market': row['market'],
                'asset_class': row['asset_class'], 'name': row['name'],
                'currency': row['currency'],
            }
        prepared.append({**row, 'import_hash': row_hash})
    prepared.sort(key=lambda r: (r['trade_date'], 0 if r['type'] in ('buy', 'reinvest') else 1))

    return {
        'accounts': accounts,
        'account_map': account_map,
        'existing_accounts': existing_accounts,
        'rows': prepared,
        'new_assets': list(new_assets.values()),
        'metadata': metadata,
        'duplicates': duplicates,
        'skipped': skipped,
        'errors': errors,
        'dry_run': dry_run,
    }


def _backfill_assets(asset_ids):
    conn = connect()
    try:
        for asset_id in asset_ids:
            row = conn.execute('SELECT * FROM wm_asset WHERE id = ?', (asset_id,)).fetchone()
            if not row:
                continue
            try:
                prices.sync_asset(conn, dict(row), full=True)
                conn.execute('UPDATE wm_asset SET last_error = NULL WHERE id = ?', (asset_id,))
                conn.commit()
            except Exception as exc:
                conn.rollback()
                try:
                    conn.execute('UPDATE wm_asset SET last_error = ? WHERE id = ?',
                                 (str(exc)[:300], asset_id))
                    conn.commit()
                except Exception:
                    conn.rollback()
    finally:
        conn.close()


@wealth_bp.route('/api/wm/import/ghostfolio', methods=['POST'])
def api_import_ghostfolio():
    conn = get_db()
    dry_run = (request.args.get('dry_run', '1') or '1').lower() not in ('0', 'false')
    try:
        payload, filename = _import_payload()
        if not payload:
            return _error('没有可导入的数据')
        prepared = _prepare_import(conn, payload, dry_run=dry_run)
        if dry_run:
            return jsonify({
                'dry_run': True,
                'activities': len(prepared['rows']),
                'new_assets': prepared['new_assets'],
                'duplicates': prepared['duplicates'],
                'skipped': prepared['skipped'],
                'errors': prepared['errors'][:50],
                'accounts': len(prepared['accounts']),
                'balances': sum(len(a.get('balances') or []) for a in prepared['accounts']),
                'sample': prepared['rows'][:10],
            })

        # Resolve FX before starting the write transaction.  This prevents a
        # slow/unreachable FX provider from making the import appear hung (and
        # from holding the SQLite write lock while waiting on the network).
        fx_map, fx_failed, fx_warnings = _resolve_import_fx(conn, prepared['rows'])
        account_currencies = {
            str(account.get('currency') or 'CNY').upper()
            for account in prepared['accounts']
        }
        account_currencies.discard('CNY')
        account_currencies = {
            currency for currency in account_currencies
            if prices.fx_rate(conn, currency, today_str()) is None
        }
        if account_currencies:
            try:
                prices.ensure_fx_range(
                    conn, account_currencies,
                    parse_date(today_str()) - timedelta(days=7), today_str(), timeout=10)
            except Exception:
                pass
        conn.commit()

        # Create accounts referenced by the file when they are new.
        for account in prepared['accounts']:
            name = (account.get('name') or '').strip()
            if name and name not in prepared['existing_accounts']:
                cur = conn.execute(
                    'INSERT OR IGNORE INTO wm_account (name, broker, currency, created_at) VALUES (?, ?, ?, ?)',
                    (name, account.get('platformId'), (account.get('currency') or 'CNY').upper(), now_iso()),
                )
                if cur.rowcount:
                    prepared['existing_accounts'][name] = cur.lastrowid
                else:
                    row = conn.execute('SELECT id FROM wm_account WHERE name = ?', (name,)).fetchone()
                    prepared['existing_accounts'][name] = row['id'] if row else None
        account_id_by_ghost = {}
        for account in prepared['accounts']:
            name = (account.get('name') or '').strip()
            if name:
                account_id_by_ghost[str(account.get('id'))] = prepared['existing_accounts'].get(name)

        balance_count = 0
        for account in prepared['accounts']:
            local_id = account_id_by_ghost.get(str(account.get('id')))
            if not local_id:
                continue
            currency = (account.get('currency') or 'CNY').upper()
            for balance in account.get('balances') or []:
                if balance.get('value') in (None, ''):
                    continue
                try:
                    balance_date = iso(parse_date(str(balance.get('date') or '')[:10]))
                except (TypeError, ValueError):
                    continue
                conn.execute(
                    """INSERT INTO wm_account_balance
                       (account_id, balance_date, amount, currency, source, created_at)
                       VALUES (?, ?, ?, ?, 'ghostfolio', ?)
                       ON CONFLICT(account_id, balance_date, currency) DO UPDATE SET
                         amount = excluded.amount,
                         source = excluded.source""",
                    (local_id, balance_date, _num(balance.get('value'), 0),
                     currency, now_iso()),
                )
                balance_count += 1

        created_assets = []
        asset_map = {(r['symbol'], r['market']): dict(r) for r in conn.execute(
            'SELECT * FROM wm_asset').fetchall()}
        for item in prepared['new_assets']:
            try:
                asset, created = _create_asset_record(conn, item, resolve=False)
            except Exception as exc:
                prepared['errors'].append(f'{item.get("symbol")}: {exc}')
                continue
            asset_map[(asset['symbol'], asset['market'])] = asset
            if created:
                created_assets.append(asset['id'])

        metadata_updated = 0
        for key, meta in prepared.get('metadata', {}).items():
            asset = asset_map.get(key)
            if not asset:
                continue
            name = meta.get('name')
            asset_class = meta.get('asset_class')
            updates, params = [], []
            if asset_class and asset_class != 'other' and asset.get('asset_class') == 'other':
                updates.append('asset_class = ?')
                params.append(asset_class)
            if (name and name != asset['symbol']
                    and (not asset.get('name') or asset['name'] == asset['symbol'])):
                updates.append('name = ?')
                params.append(name)
            if updates:
                params.append(asset['id'])
                conn.execute(f'UPDATE wm_asset SET {", ".join(updates)} WHERE id = ?', params)
                metadata_updated += 1

        imported = 0
        for row in prepared['rows']:
            asset = asset_map.get((row['symbol'], row['market']))
            if not asset:
                continue
            currency = str(row.get('currency') or '').upper()
            fx_key = (currency, row['trade_date'])
            if currency and currency != 'CNY' and fx_key in fx_failed:
                prepared['skipped'] += 1
                prepared['errors'].append(
                    f'{row["symbol"]} {row["trade_date"]}: 缺少 {currency}/CNY 汇率，已跳过')
                continue
            account_id = account_id_by_ghost.get(str(row.get('account_id')))
            payload = {
                'asset_id': asset['id'], 'account_id': account_id,
                'type': row['type'], 'trade_date': row['trade_date'],
                'units': row['units'], 'price': row['price'], 'amount': row['amount'],
                'fee': row['fee'], 'currency': row['currency'], 'note': row['note'],
            }
            if currency and currency != 'CNY' and fx_key in fx_map:
                payload['fx_rate'] = fx_map[fx_key]
            try:
                tx_id = _create_transaction(
                    conn, payload, source='import', import_hash=row['import_hash'],
                    affect_cash=False)
                if tx_id:
                    imported += 1
            except Exception as exc:
                prepared['errors'].append(f'{row["symbol"]} {row["trade_date"]}: {exc}')
        conn.execute(
            """INSERT INTO wm_import_log (kind, filename, status, count, message, created_at)
               VALUES ('ghostfolio', ?, 'done', ?, ?, ?)""",
            (filename, imported, '; '.join(prepared['errors'][:20]) or None, now_iso()),
        )
        conn.commit()
        if created_assets:
            threading.Thread(target=_backfill_assets, args=(created_assets,), daemon=True).start()
        return jsonify({
            'dry_run': False, 'imported': imported, 'duplicates': prepared['duplicates'],
            'skipped': prepared['skipped'], 'new_assets': len(created_assets),
            'balances': balance_count, 'metadata_updated': metadata_updated,
            'errors': prepared['errors'][:50], 'warnings': fx_warnings[:50],
        })
    except Exception as exc:
        conn.rollback()
        return _error(str(exc))


CSV_FIELDS = ['market', 'symbol', 'name', 'asset_class', 'type', 'trade_date',
              'units', 'price', 'amount', 'fee', 'currency', 'account', 'split_ratio', 'note']


@wealth_bp.route('/api/wm/import/csv/template')
def api_csv_template():
    text = '\ufeff' + ','.join(CSV_FIELDS) + '\n' + \
        'CN,600519,贵州茅台,stock,buy,2026-01-05,100,1500,150000,5,CNY,默认账户,,示例\n'
    return Response(text, mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=wealth-template.csv'})


@wealth_bp.route('/api/wm/import/csv', methods=['POST'])
def api_import_csv():
    conn = get_db()
    dry_run = (request.args.get('dry_run', '1') or '1').lower() not in ('0', 'false')
    try:
        if 'file' in request.files:
            text = request.files['file'].read().decode('utf-8-sig')
        else:
            text = (request.get_json(silent=True) or {}).get('csv') or ''
        reader = csv.DictReader(io.StringIO(text))
        rows = [r for r in reader]
        if not rows:
            return _error('CSV 没有数据行')
        errors = []
        prepared = []
        for index, row in enumerate(rows, start=2):
            try:
                if not row.get('symbol') or not row.get('market'):
                    raise ValueError('缺少 market 或 symbol')
                prepared.append(row)
            except Exception as exc:
                errors.append(f'第 {index} 行: {exc}')
        if dry_run:
            return jsonify({'dry_run': True, 'rows': len(prepared), 'errors': errors[:50],
                            'fields': CSV_FIELDS})
        fx_map, fx_failed, fx_warnings = _resolve_import_fx(conn, prepared)
        conn.commit()
        imported = 0
        account_ids = {r['name']: r['id'] for r in conn.execute('SELECT id, name FROM wm_account').fetchall()}
        for row in prepared:
            market = row['market'].strip().upper()
            try:
                currency = str(row.get('currency') or '').strip().upper()
                fx_key = (currency, row.get('trade_date'))
                if currency and currency != 'CNY' and fx_key in fx_failed:
                    errors.append(f'{row.get("symbol")} {row.get("trade_date")}: 缺少 {currency}/CNY 汇率，已跳过')
                    continue
                asset = conn.execute('SELECT * FROM wm_asset WHERE symbol=? AND market=?',
                                     (row['symbol'].strip(), market)).fetchone()
                if not asset:
                    asset, _ = _create_asset_record(conn, {
                        'symbol': row['symbol'].strip(), 'market': market,
                        'asset_class': row.get('asset_class') or _default_class(market),
                        'name': row.get('name') or row['symbol'].strip(),
                        'currency': row.get('currency') or _default_currency(market),
                    }, resolve=True)
                account_id = account_ids.get((row.get('account') or '').strip())
                payload = {
                    'asset_id': asset['id'], 'account_id': account_id,
                    'type': row.get('type') or 'buy', 'trade_date': row.get('trade_date'),
                    'units': row.get('units'), 'price': row.get('price'),
                    'amount': row.get('amount'), 'fee': row.get('fee'),
                    'currency': row.get('currency'), 'split_ratio': row.get('split_ratio'),
                    'note': row.get('note'),
                }
                if currency and currency != 'CNY' and fx_key in fx_map:
                    payload['fx_rate'] = fx_map[fx_key]
                _create_transaction(conn, payload, source='import', affect_cash=False)
                imported += 1
            except Exception as exc:
                errors.append(f'{row.get("symbol")} {row.get("trade_date")}: {exc}')
        conn.commit()
        return jsonify({'dry_run': False, 'imported': imported, 'errors': errors[:50],
                        'warnings': fx_warnings[:50]})
    except Exception as exc:
        conn.rollback()
        return _error(str(exc))


@wealth_bp.route('/api/wm/export')
def api_export():
    conn = get_db()
    fmt = (request.args.get('format') or 'json').lower()
    if fmt == 'csv':
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['market', 'symbol', 'asset_name', 'asset_class', 'type', 'trade_date',
                         'units', 'price', 'amount', 'fee', 'currency', 'fx_rate', 'source',
                         'account', 'split_ratio', 'note'])
        for row in conn.execute(
            """SELECT a.market, a.symbol, a.name AS asset_name, a.asset_class,
                      t.*, ac.name AS account_name
               FROM wm_transaction t
               JOIN wm_asset a ON a.id = t.asset_id
               LEFT JOIN wm_account ac ON ac.id = t.account_id
               ORDER BY t.trade_date, t.id"""
        ).fetchall():
            writer.writerow([row['market'], row['symbol'], row['asset_name'], row['asset_class'],
                             row['type'], row['trade_date'], row['units'], row['price'],
                             row['amount'], row['fee'], row['currency'], row['fx_rate'],
                             row['source'], row['account_name'] or '', row['split_ratio'],
                             row['note']])
        return Response('\ufeff' + output.getvalue(), mimetype='text/csv',
                        headers={'Content-Disposition': 'attachment; filename=wealth-transactions.csv'})

    payload = {'exported_at': now_iso(), 'version': 1}
    for table in ('wm_account', 'wm_account_balance', 'wm_asset', 'wm_transaction',
                  'wm_dca_plan', 'wm_dca_order'):
        payload[table] = [dict(r) for r in conn.execute(f'SELECT * FROM {table}').fetchall()]
    return Response(json.dumps(payload, ensure_ascii=False, indent=2),
                    mimetype='application/json',
                    headers={'Content-Disposition': 'attachment; filename=wealth-export.json'})


@wealth_bp.errorhandler(Exception)
def _handle_error(exc):
    if isinstance(exc, HTTPException):
        if request.path.startswith('/api/'):
            return jsonify({'error': exc.description}), exc.code
        return exc
    current_app.logger.exception('wealth module error')
    if request.path.startswith('/api/'):
        return jsonify({'error': str(exc)}), 500
    raise exc


def register(app):
    if not app.secret_key:
        app.secret_key = os.environ.get('WM_SECRET_KEY') or os.environ.get('SECRET_KEY') or os.urandom(32)
    app.config.setdefault('WM_DB_PATH', os.environ.get('WM_DB_PATH') or DB_PATH)
    app.config.setdefault('SESSION_COOKIE_HTTPONLY', True)
    app.config.setdefault('SESSION_COOKIE_SAMESITE', 'Lax')
    init_wealth_db(app.config['WM_DB_PATH'])
    app.register_blueprint(wealth_bp)
    app.teardown_appcontext(close_db)
