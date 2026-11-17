"""Portfolio maths: positions, valuation, cash flows, XIRR, TWR and history."""
import math
from datetime import date, datetime, timedelta

from .db import SHANGHAI_TZ


INCOME_TYPES = {'dividend', 'interest'}


def parse_date(value):
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value)[:10], '%Y-%m-%d').date()


def iso(value):
    return value.isoformat() if hasattr(value, 'isoformat') else str(value)


def fx_rate(conn, currency, on_date):
    """CNY per one unit of ``currency`` on/just before ``on_date``."""
    if not currency or currency.upper() == 'CNY':
        return 1.0
    row = conn.execute(
        """SELECT cny_rate FROM wm_fx_rate
           WHERE currency = ? AND rate_date <= ?
           ORDER BY rate_date DESC LIMIT 1""",
        (currency.upper(), iso(on_date)),
    ).fetchone()
    return float(row['cny_rate']) if row else None


def tx_amount_cny(tx):
    """Investor cash flow in CNY (negative = money paid in)."""
    fx = float(tx['fx_rate'] or 1)
    if tx['type'] == 'buy':
        return -(float(tx['amount'] or 0) + float(tx['fee'] or 0)) * fx
    if tx['type'] == 'sell':
        return (float(tx['amount'] or 0) - float(tx['fee'] or 0)) * fx
    if tx['type'] in INCOME_TYPES:
        return (float(tx['amount'] or 0) - float(tx['fee'] or 0)) * fx
    if tx['type'] == 'fee':
        return -float(tx['amount'] or 0) * fx
    return 0.0


def external_cash_flows(conn, as_of):
    """Investor cash flows for XIRR (negative = paid in)."""
    rows = conn.execute(
        """SELECT type, trade_date, amount, fee, fx_rate FROM wm_transaction
           WHERE trade_date <= ? ORDER BY trade_date, id""",
        (iso(as_of),),
    ).fetchall()
    flows = []
    for r in rows:
        amount = tx_amount_cny(r)
        if abs(amount) > 1e-9:
            flows.append((parse_date(r['trade_date']), amount))
    return flows


def xirr(flows):
    """Money-weighted annual return. ``flows`` is [(date, signed amount)]."""
    flows = [(parse_date(d), float(a)) for d, a in flows if abs(float(a)) > 1e-9]
    if len(flows) < 2:
        return None
    if not (any(a > 0 for _, a in flows) and any(a < 0 for _, a in flows)):
        return None
    t0 = min(d for d, _ in flows)
    points = [((d - t0).days / 365.0, a) for d, a in flows]

    def npv(rate):
        if rate <= -1:
            return float('inf')
        total = 0.0
        for t, a in points:
            try:
                total += a * math.exp(-t * math.log1p(rate))
            except OverflowError:
                return float('inf') if a > 0 else float('-inf')
        return total

    lo = -0.999999999
    hi = 1.0
    f_lo = npv(lo)
    f_hi = npv(hi)
    while f_lo * f_hi > 0 and hi < 1e16:
        hi *= 10.0
        f_hi = npv(hi)
    if f_lo * f_hi > 0:
        return None
    for _ in range(300):
        mid = (lo + hi) / 2
        f_mid = npv(mid)
        if abs(f_mid) < 1e-7:
            return mid
        if f_lo * f_mid <= 0:
            hi, f_hi = mid, f_mid
        else:
            lo, f_lo = mid, f_mid
    return (lo + hi) / 2


def latest_price(conn, asset_id, as_of):
    rows = conn.execute(
        """SELECT price_date, close, prev_close FROM wm_price
           WHERE asset_id = ? AND price_date <= ?
           ORDER BY price_date DESC LIMIT 2""",
        (asset_id, iso(as_of)),
    ).fetchall()
    if not rows:
        return None
    last = rows[0]
    prev = rows[1] if len(rows) > 1 else None
    prev_close = last['prev_close']
    if prev_close is None and prev is not None:
        prev_close = prev['close']
    return {
        'price': float(last['close']),
        'date': last['price_date'],
        'prev_close': float(prev_close) if prev_close is not None else None,
    }


def _split_units(tx):
    ratio = tx['split_ratio']
    if ratio is None:
        ratio = tx['units']
    return float(ratio or 1)


def compute_position(asset, txs, as_of, price=None, fx_fn=None):
    """Compute a single position from its transaction list."""
    fx_fn = fx_fn or (lambda c, d: 1.0)
    if (asset.get('valuation_mode') or 'price') == 'accrual':
        return _compute_bank_position(asset, txs, as_of, fx_fn)

    units = 0.0
    cost_native = 0.0
    cost_cny = 0.0
    realized_native = 0.0
    realized_cny = 0.0
    fees_cny = 0.0
    income_cny = 0.0
    warnings = []

    for tx in txs:
        tx_type = tx['type']
        amount = float(tx['amount'] or 0)
        fee = float(tx['fee'] or 0)
        fx = float(tx['fx_rate'] or fx_fn(tx['currency'], tx['trade_date']) or 1)
        price_tx = float(tx['price'] or 0)

        if tx_type == 'buy':
            qty = float(tx['units'] or 0)
            if qty <= 0 and price_tx > 0:
                qty = amount / price_tx
            units += qty
            cost_native += amount + fee
            cost_cny += (amount + fee) * fx
            fees_cny += fee * fx
        elif tx_type == 'sell':
            qty = float(tx['units'] or 0)
            if qty <= 0 and price_tx > 0:
                qty = amount / price_tx
            if qty > units + 1e-9:
                warnings.append(f'{tx["trade_date"]} 卖出 {qty:.6g} 超过持仓 {units:.6g}')
                qty = max(units, 0.0)
            if units > 0:
                cost_removed_native = cost_native * (qty / units)
                cost_removed_cny = cost_cny * (qty / units)
            else:
                cost_removed_native = cost_removed_cny = 0.0
            proceeds_native = amount - fee
            realized_native += proceeds_native - cost_removed_native
            realized_cny += proceeds_native * fx - cost_removed_cny
            cost_native -= cost_removed_native
            cost_cny -= cost_removed_cny
            units -= qty
            fees_cny += fee * fx
        elif tx_type in INCOME_TYPES:
            net = amount - fee
            realized_native += net
            realized_cny += net * fx
            income_cny += net * fx
            fees_cny += fee * fx
        elif tx_type == 'reinvest':
            qty = float(tx['units'] or 0)
            if qty <= 0 and price_tx > 0:
                qty = max(amount - fee, 0) / price_tx
            units += qty
            cost_native += amount
            cost_cny += amount * fx
            fees_cny += fee * fx
        elif tx_type == 'fee':
            realized_native -= amount
            realized_cny -= amount * fx
            fees_cny += amount * fx
        elif tx_type == 'split':
            units *= _split_units(tx)
        else:
            warnings.append(f'忽略未知交易类型 {tx_type}')

    if units < 0 and units > -1e-7:
        units = 0.0

    if price and price.get('price'):
        last_price = float(price['price'])
        price_date = price.get('date')
        priced = True
    else:
        last_price = cost_native / units if units else 0.0
        price_date = None
        priced = False

    fx_latest = float(fx_fn(asset.get('currency') or 'CNY', as_of) or 1)
    market_value_native = units * last_price
    market_value_cny = market_value_native * fx_latest
    pnl_native = market_value_native + realized_native - cost_native
    pnl_cny = market_value_cny + realized_cny - cost_cny
    prev_close = price.get('prev_close') if price else None
    day_pnl_cny = 0.0
    if prev_close is not None:
        day_pnl_cny = units * (last_price - float(prev_close)) * fx_latest

    return {
        'asset_id': asset['id'],
        'symbol': asset['symbol'],
        'name': asset['name'],
        'market': asset['market'],
        'asset_class': asset['asset_class'],
        'currency': asset['currency'],
        'valuation_mode': 'price',
        'units': units,
        'avg_cost_native': cost_native / units if units else 0.0,
        'avg_cost_cny': cost_cny / units if units else 0.0,
        'cost_native': cost_native,
        'cost_cny': cost_cny,
        'price': last_price,
        'price_date': price_date,
        'priced': priced,
        'market_value_native': market_value_native,
        'market_value_cny': market_value_cny,
        'realized_native': realized_native,
        'realized_cny': realized_cny,
        'pnl_native': pnl_native,
        'pnl_cny': pnl_cny,
        'pnl_pct': (pnl_cny / cost_cny) if cost_cny else None,
        'day_pnl_cny': day_pnl_cny,
        'fees_cny': fees_cny,
        'income_cny': income_cny,
        'fx_rate': fx_latest,
        'has_position': units > 1e-9,
        'warnings': warnings,
    }


def _compute_bank_position(asset, txs, as_of, fx_fn):
    """Daily accrual for bank wealth products.

    ``amount`` on buy/sell is the principal moved. Interest paid is recorded
    separately as an ``interest`` transaction and reduces unpaid accrual.
    """
    rate = float(asset.get('annual_rate') or 0)
    mode = asset.get('interest_mode') or 'simple'
    maturity = parse_date(asset['maturity_date']) if asset.get('maturity_date') else None
    if not txs:
        return _bank_result(asset, as_of, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, fx_fn,
                            ['银行理财暂无交易记录'])

    first_day = min(parse_date(t['trade_date']) for t in txs)
    end = parse_date(as_of)
    events = {}
    for tx in txs:
        events.setdefault(parse_date(tx['trade_date']), []).append(tx)

    principal = 0.0
    accrued = 0.0
    interest_paid = 0.0
    fees = 0.0
    cost_native = 0.0
    cost_cny = 0.0
    realized_cny = 0.0
    warnings = ['银行理财未设置年化收益率'] if rate <= 0 else []

    day = first_day
    accrued_today = 0.0
    while day <= end:
        for tx in events.get(day, []):
            amount = float(tx['amount'] or 0)
            fee = float(tx['fee'] or 0)
            fx = float(tx['fx_rate'] or fx_fn(tx['currency'], tx['trade_date']) or 1)
            tx_type = tx['type']
            if tx_type == 'buy':
                principal += amount
                cost_native += amount + fee
                cost_cny += (amount + fee) * fx
            elif tx_type == 'sell':
                principal -= amount
                if principal < -1e-9:
                    warnings.append(f'{tx["trade_date"]} 赎回本金超过当前本金')
                    principal = 0.0
                cost_native = max(cost_native - amount, 0.0)
                cost_cny = max(cost_cny - amount * fx, 0.0)
                fees += fee
                realized_cny -= fee * fx
            elif tx_type == 'interest':
                interest_paid += amount
                accrued = max(accrued - amount, 0.0)
                realized_cny += (amount - fee) * fx
            elif tx_type == 'fee':
                fees += amount
                realized_cny -= amount * fx
            else:
                warnings.append(f'银行理财忽略交易类型 {tx_type}')

        accrued_today = 0.0
        if principal > 0 and (maturity is None or day <= maturity):
            if mode == 'compound':
                accrued_today = (principal + accrued) * rate / 365.0
            else:
                accrued_today = principal * rate / 365.0
            accrued += accrued_today
        day += timedelta(days=1)

    value = principal + max(accrued, 0.0)
    day_pnl = accrued_today if day > end else 0.0
    result = _bank_result(asset, as_of, principal, accrued, interest_paid, fees,
                          cost_native, cost_cny, fx_fn, warnings, value=value,
                          realized_cny=realized_cny, day_pnl=day_pnl)
    return result


def _bank_result(asset, as_of, principal, accrued, interest_paid, fees,
                 cost_native, cost_cny, fx_fn, warnings, value=None,
                 realized_cny=None, day_pnl=0.0):
    fx_latest = float(fx_fn(asset.get('currency') or 'CNY', as_of) or 1)
    value = value if value is not None else principal + max(accrued, 0.0)
    realized_cny = realized_cny if realized_cny is not None else interest_paid - fees
    pnl_cny = value * fx_latest - cost_cny + realized_cny
    return {
        'asset_id': asset['id'],
        'symbol': asset['symbol'],
        'name': asset['name'],
        'market': asset['market'],
        'asset_class': asset['asset_class'],
        'currency': asset['currency'],
        'valuation_mode': 'accrual',
        'units': principal,
        'principal': principal,
        'accrued_interest': accrued,
        'interest_paid': interest_paid,
        'annual_rate': asset.get('annual_rate'),
        'interest_mode': asset.get('interest_mode') or 'simple',
        'cost_native': principal,
        'cost_cny': cost_cny or principal * fx_latest,
        'price': 1.0,
        'price_date': iso(as_of),
        'priced': True,
        'market_value_native': value,
        'market_value_cny': value * fx_latest,
        'realized_native': interest_paid - fees,
        'realized_cny': realized_cny,
        'pnl_native': value - principal + interest_paid - fees,
        'pnl_cny': pnl_cny,
        'pnl_pct': (pnl_cny / cost_cny) if cost_cny else None,
        'day_pnl_cny': day_pnl * fx_latest,
        'fees_cny': fees * fx_latest,
        'income_cny': interest_paid * fx_latest,
        'fx_rate': fx_latest,
        'has_position': principal > 1e-9 or accrued > 1e-9,
        'warnings': warnings,
    }


def load_accounts(conn):
    return {r['id']: dict(r) for r in conn.execute('SELECT * FROM wm_account').fetchall()}


def build_positions(conn, as_of=None):
    as_of = parse_date(as_of or datetime.now(SHANGHAI_TZ).date())
    assets = [dict(r) for r in conn.execute(
        'SELECT * FROM wm_asset ORDER BY asset_class, market, symbol'
    ).fetchall()]
    accounts = load_accounts(conn)
    txs_by_asset = {}
    for r in conn.execute(
        'SELECT * FROM wm_transaction WHERE trade_date <= ? ORDER BY trade_date, id',
        (iso(as_of),),
    ).fetchall():
        txs_by_asset.setdefault(r['asset_id'], []).append(dict(r))

    fx_cache = {}

    def fx_fn(currency, on_date):
        key = (currency or 'CNY', iso(on_date))
        if key not in fx_cache:
            fx_cache[key] = fx_rate(conn, currency, on_date)
        return fx_cache[key]

    positions = []
    for asset in assets:
        txs = txs_by_asset.get(asset['id'], [])
        if not txs:
            continue
        price = latest_price(conn, asset['id'], as_of)
        pos = compute_position(asset, txs, as_of, price=price, fx_fn=fx_fn)
        currency = asset.get('currency') or 'CNY'
        if currency != 'CNY' and fx_cache.get((currency, iso(as_of))) is None:
            pos['fx_missing'] = True
            pos['warnings'].append('缺少汇率，暂按 1:1 估值')
        else:
            pos['fx_missing'] = False
        account_id = None
        for tx in reversed(txs):
            if tx.get('account_id'):
                account_id = tx['account_id']
                break
        pos['account_id'] = account_id
        pos['account_name'] = accounts.get(account_id, {}).get('name') if account_id else ''
        pos['lot_size'] = asset.get('lot_size', 1)
        pos['allow_fraction'] = bool(asset.get('allow_fraction', 1))
        pos['quote_source'] = asset.get('quote_source')
        positions.append(pos)

    total = sum(p['market_value_cny'] for p in positions if p['has_position'])
    for p in positions:
        p['weight'] = (p['market_value_cny'] / total) if total and p['has_position'] else 0.0
    return positions, total


def _allocation(positions, key):
    buckets = {}
    for p in positions:
        if not p['has_position']:
            continue
        name = p.get(key) or '未分类'
        buckets[name] = buckets.get(name, 0.0) + p['market_value_cny']
    total = sum(buckets.values())
    return [
        {'name': k, 'value': round(v, 2), 'percent': (v / total * 100) if total else 0.0}
        for k, v in sorted(buckets.items(), key=lambda x: -x[1])
    ]


def net_invested_cny(conn, as_of):
    """Principal put in minus principal taken out (dividends excluded)."""
    total = 0.0
    for r in conn.execute(
        'SELECT type, amount, fee, fx_rate FROM wm_transaction WHERE trade_date <= ?',
        (iso(as_of),),
    ).fetchall():
        fx = float(r['fx_rate'] or 1)
        if r['type'] == 'buy':
            total += (float(r['amount'] or 0) + float(r['fee'] or 0)) * fx
        elif r['type'] == 'sell':
            total -= (float(r['amount'] or 0) - float(r['fee'] or 0)) * fx
    return total


def build_summary(conn, as_of=None, range_days=365):
    as_of = parse_date(as_of or datetime.now(SHANGHAI_TZ).date())
    positions, total = build_positions(conn, as_of)
    held = [p for p in positions if p['has_position']]
    total_cost = sum(p['cost_cny'] for p in positions)
    total_pnl = sum(p['pnl_cny'] for p in positions)
    realized = sum(p['realized_cny'] for p in positions)
    today_pnl = sum(p['day_pnl_cny'] for p in held)
    invested = net_invested_cny(conn, as_of)

    flows = external_cash_flows(conn, as_of)
    if total > 0:
        flows.append((as_of, total))
    mwr = xirr(flows)

    first_tx = conn.execute('SELECT MIN(trade_date) AS d FROM wm_transaction').fetchone()
    first_date = first_tx['d'] if first_tx and first_tx['d'] else None
    if first_date:
        start = max(parse_date(first_date), as_of - timedelta(days=range_days)) if range_days else parse_date(first_date)
    else:
        start = as_of - timedelta(days=range_days or 365)
    history = build_history(conn, start, as_of)
    twr = time_weighted_return(history)
    max_dd = max_drawdown(history)

    last_sync = conn.execute('SELECT * FROM wm_sync_log ORDER BY id DESC LIMIT 1').fetchone()
    pending = conn.execute(
        "SELECT COUNT(*) AS c FROM wm_dca_order WHERE status = 'awaiting_price'"
    ).fetchone()['c']
    failed = conn.execute(
        "SELECT COUNT(*) AS c FROM wm_dca_order WHERE status = 'failed'"
    ).fetchone()['c']

    return {
        'as_of': iso(as_of),
        'total_value': round(total, 2),
        'total_cost': round(total_cost, 2),
        'total_pnl': round(total_pnl, 2),
        'total_pnl_pct': (total_pnl / invested) if invested else None,
        'realized_pnl': round(realized, 2),
        'today_pnl': round(today_pnl, 2),
        'net_invested': round(invested, 2),
        'xirr': mwr,
        'twr': twr,
        'max_drawdown': max_dd,
        'position_count': len(held),
        'priced_count': sum(1 for p in held if p['priced']),
        'unpriced': [p['symbol'] for p in held if not p['priced']],
        'allocations': {
            'asset_class': _allocation(held, 'asset_class'),
            'market': _allocation(held, 'market'),
            'currency': _allocation(held, 'currency'),
            'account': _allocation(held, 'account_name'),
        },
        'first_tx_date': first_date,
        'last_sync': dict(last_sync) if last_sync else None,
        'pending_orders': pending,
        'failed_orders': failed,
    }


def build_history(conn, start, end):
    start = parse_date(start)
    end = parse_date(end)
    assets = [dict(r) for r in conn.execute(
        'SELECT * FROM wm_asset'
    ).fetchall()]
    txs_by_asset = {}
    first_tx_day = None
    for r in conn.execute(
        'SELECT * FROM wm_transaction WHERE trade_date <= ? ORDER BY trade_date, id',
        (iso(end),),
    ).fetchall():
        txs_by_asset.setdefault(r['asset_id'], []).append(dict(r))
        if first_tx_day is None:
            first_tx_day = parse_date(r['trade_date'])

    price_maps = {}
    for a in assets:
        price_maps[a['id']] = {
            r['price_date']: {'close': float(r['close']), 'prev_close': r['prev_close']}
            for r in conn.execute(
                'SELECT price_date, close, prev_close FROM wm_price WHERE asset_id = ? AND price_date <= ?',
                (a['id'], iso(end)),
            ).fetchall()
        }

    fx_maps = {}
    for r in conn.execute(
        'SELECT rate_date, currency, cny_rate FROM wm_fx_rate WHERE rate_date <= ?',
        (iso(end),),
    ).fetchall():
        fx_maps.setdefault(r['currency'], {})[r['rate_date']] = float(r['cny_rate'])

    states = {}
    for a in assets:
        txs = txs_by_asset.get(a['id'], [])
        if not txs:
            continue
        states[a['id']] = {
            'asset': a,
            'txs_by_date': {},
            'units': 0.0,
            'cost_native': 0.0,
            'cost_cny': 0.0,
            'realized_cny': 0.0,
            'principal': 0.0,
            'accrued': 0.0,
            'interest_paid': 0.0,
            'fees': 0.0,
            'last_price': None,
            'last_prev_close': None,
            'last_fx': None,
        }
        for tx in txs:
            states[a['id']]['txs_by_date'].setdefault(tx['trade_date'], []).append(tx)

    points = []
    last_fx = {}
    day = min(start, first_tx_day) if first_tx_day else start
    while day <= end:
        ds = iso(day)
        for currency, mapping in fx_maps.items():
            if ds in mapping:
                last_fx[currency] = mapping[ds]
            elif currency not in last_fx and mapping:
                prior = [d for d in mapping if d <= ds]
                last_fx[currency] = mapping[max(prior)] if prior else None

        total_value = 0.0
        total_cost = 0.0
        total_pnl = 0.0
        total_net_flow = 0.0

        for asset_id, st in states.items():
            a = st['asset']
            currency = a['currency'] or 'CNY'

            for tx in st['txs_by_date'].get(ds, []):
                typ = tx['type']
                amount = float(tx['amount'] or 0)
                fee = float(tx['fee'] or 0)
                fx = float(tx['fx_rate'] or last_fx.get(currency) or 1)
                if a.get('valuation_mode') == 'accrual':
                    if typ == 'buy':
                        st['principal'] += amount
                        st['cost_native'] += amount + fee
                        st['cost_cny'] += (amount + fee) * fx
                        total_net_flow += (amount + fee) * fx
                    elif typ == 'sell':
                        st['principal'] = max(st['principal'] - amount, 0.0)
                        st['cost_native'] = max(st['cost_native'] - amount, 0.0)
                        st['cost_cny'] = max(st['cost_cny'] - amount * fx, 0.0)
                        st['fees'] += fee
                        total_net_flow -= (amount - fee) * fx
                    elif typ == 'interest':
                        st['interest_paid'] += amount
                        st['accrued'] = max(st['accrued'] - amount, 0.0)
                        st['realized_cny'] += (amount - fee) * fx
                        total_net_flow -= (amount - fee) * fx
                    elif typ == 'fee':
                        st['fees'] += amount
                        st['realized_cny'] -= amount * fx
                    continue

                if typ == 'buy':
                    qty = float(tx['units'] or 0) or (amount / float(tx['price']) if tx['price'] else 0)
                    st['units'] += qty
                    st['cost_native'] += amount + fee
                    st['cost_cny'] += (amount + fee) * fx
                    total_net_flow += (amount + fee) * fx
                elif typ == 'sell':
                    qty = float(tx['units'] or 0) or (amount / float(tx['price']) if tx['price'] else 0)
                    qty = min(qty, st['units'])
                    if st['units'] > 0:
                        removed_native = st['cost_native'] * qty / st['units']
                        removed_cny = st['cost_cny'] * qty / st['units']
                    else:
                        removed_native = removed_cny = 0.0
                    st['realized_cny'] += (amount - fee) * fx - removed_cny
                    st['cost_native'] -= removed_native
                    st['cost_cny'] -= removed_cny
                    st['units'] -= qty
                    total_net_flow -= (amount - fee) * fx
                elif typ in INCOME_TYPES:
                    st['realized_cny'] += (amount - fee) * fx
                    total_net_flow -= (amount - fee) * fx
                elif typ == 'reinvest':
                    qty = float(tx['units'] or 0) or ((amount - fee) / float(tx['price']) if tx['price'] else 0)
                    st['units'] += qty
                    st['cost_native'] += amount
                    st['cost_cny'] += amount * fx
                elif typ == 'fee':
                    st['realized_cny'] -= amount * fx
                elif typ == 'split':
                    st['units'] *= _split_units(tx)

            fx_now = last_fx.get(currency)
            if fx_now is None:
                fx_now = 1.0
            if a.get('valuation_mode') == 'accrual':
                rate = float(a.get('annual_rate') or 0)
                maturity = a.get('maturity_date')
                if st['principal'] > 0 and rate > 0 and (not maturity or ds <= maturity):
                    if (a.get('interest_mode') or 'simple') == 'compound':
                        st['accrued'] += (st['principal'] + st['accrued']) * rate / 365.0
                    else:
                        st['accrued'] += st['principal'] * rate / 365.0
                value = st['principal'] + max(st['accrued'], 0.0)
                st['last_fx'] = fx_now
                total_value += value * fx_now
                total_cost += (st['cost_cny'] or st['principal'] * fx_now)
                total_pnl += value * fx_now - (st['cost_cny'] or st['principal'] * fx_now) + st['interest_paid'] - st['fees']
                continue

            price_map = price_maps.get(asset_id, {})
            if ds in price_map:
                st['last_price'] = price_map[ds]['close']
                st['last_prev_close'] = price_map[ds]['prev_close']
            price_for_value = st['last_price']
            if price_for_value is None and st['units'] > 0:
                price_for_value = st['cost_native'] / st['units']
            value_native = st['units'] * (price_for_value or 0.0)
            value_cny = value_native * fx_now
            st['last_fx'] = fx_now
            total_value += value_cny
            total_cost += st['cost_cny']
            total_pnl += value_cny + st['realized_cny'] - st['cost_cny']

        points.append({
            'date': ds,
            'value': round(total_value, 2),
            'cost': round(total_cost, 2),
            'pnl': round(total_pnl, 2),
            'net_flow': round(total_net_flow, 2),
        })
        day += timedelta(days=1)
    return [p for p in points if p['date'] >= iso(start)]


def time_weighted_return(points):
    product = 1.0
    prev = None
    for p in points:
        if prev is not None and prev['value'] > 0:
            r = (p['value'] - prev['value'] - p['net_flow']) / prev['value']
            if r > -1:
                product *= (1 + r)
        prev = p
    if prev is None:
        return None
    return product - 1


def max_drawdown(points):
    peak = None
    worst = 0.0
    for p in points:
        value = p['value']
        if peak is None or value > peak:
            peak = value
        if peak and peak > 0:
            dd = value / peak - 1
            if dd < worst:
                worst = dd
    return worst


def plan_position(conn, plan, as_of=None):
    """Position built only from transactions generated by one DCA plan."""
    as_of = parse_date(as_of or datetime.now(SHANGHAI_TZ).date())
    asset_row = conn.execute('SELECT * FROM wm_asset WHERE id = ?', (plan['asset_id'],)).fetchone()
    if asset_row is None:
        return None
    asset = dict(asset_row)
    txs = [dict(r) for r in conn.execute(
        """SELECT t.* FROM wm_transaction t
           JOIN wm_dca_order o ON o.id = t.dca_order_id
           WHERE o.plan_id = ? AND t.trade_date <= ?
           ORDER BY t.trade_date, t.id""",
        (plan['id'], iso(as_of)),
    ).fetchall()]
    if not txs:
        return None
    price = latest_price(conn, asset['id'], as_of)
    return compute_position(asset, txs, as_of, price=price,
                            fx_fn=lambda c, d: fx_rate(conn, c, d) or 1.0)
