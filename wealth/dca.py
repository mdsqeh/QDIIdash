"""DCA plan generation and settlement engine."""
import calendar
import math
import threading
from datetime import date, timedelta

from . import prices
from .cash import apply_transaction_cash
from .db import connect, iso, now_iso, today_str
from .portfolio import parse_date, plan_position, xirr

LOCK = threading.Lock()
MAX_BACKFILL_PER_RUN = 500
PRICE_WAIT_DAYS = 14


def _month_days(year, month):
    return calendar.monthrange(year, month)[1]


def next_due(plan, after):
    """First scheduled date strictly after ``after`` (or start_date)."""
    start = parse_date(plan['start_date'])
    after = parse_date(after)
    freq = plan['frequency']
    if freq == 'daily':
        return max(after + timedelta(days=1), start)

    if freq in ('weekly', 'biweekly'):
        weekday = int(plan['weekday'] if plan['weekday'] is not None else 0)
        step = 14 if freq == 'biweekly' else 7
        cursor = start
        while cursor.weekday() != weekday:
            cursor += timedelta(days=1)
        while cursor <= after:
            cursor += timedelta(days=step)
        return cursor

    day_of_month = int(plan['day_of_month'] or 1)
    cursor = max(after + timedelta(days=1), start)
    year, month = cursor.year, cursor.month
    while True:
        candidate = date(year, month, min(day_of_month, _month_days(year, month)))
        if candidate >= start and candidate > after:
            return candidate
        month += 1
        if month > 12:
            month = 1
            year += 1


def _is_open(conn, market, day):
    if market in ('CN', 'FUND_CN'):
        return prices.is_cn_trading_day(conn, day)
    if market in ('US', 'HK'):
        return day.weekday() < 5
    return True


def resolve_trade_date(conn, asset, due, roll_rule='next'):
    due = parse_date(due)
    market = asset.get('market') or 'CN'
    if roll_rule == 'skip' and not _is_open(conn, market, due):
        return None
    direction = -1 if roll_rule == 'prev' else 1
    for _ in range(40):
        if _is_open(conn, market, due):
            return due
        due += timedelta(days=direction)
    return due


def generate_orders(conn, today=None, plan_id=None):
    today = parse_date(today or today_str())

    sql = "SELECT * FROM wm_dca_plan WHERE status = 'active'"
    params = []
    if plan_id:
        sql += ' AND id = ?'
        params.append(plan_id)
    plans = [dict(r) for r in conn.execute(sql, params).fetchall()]

    created = 0
    skipped = 0
    calendar_ready = False
    for plan in plans:
        asset_row = conn.execute('SELECT * FROM wm_asset WHERE id = ?', (plan['asset_id'],)).fetchone()
        asset = dict(asset_row) if asset_row else {'market': 'CN'}
        if asset['market'] in ('CN', 'FUND_CN') and not calendar_ready:
            try:
                prices.ensure_cn_calendar(conn)
            except Exception:
                pass
            calendar_ready = True
        start = parse_date(plan['start_date'])
        end = parse_date(plan['end_date']) if plan.get('end_date') else None
        cursor = parse_date(plan['last_generated_date']) if plan.get('last_generated_date') else start - timedelta(days=1)
        if not plan.get('catch_up') and cursor < today - timedelta(days=1):
            # Do not materialize missed periods when catch-up is disabled; jump
            # straight to the current period so the order list stays clean.
            cursor = today - timedelta(days=1)
            conn.execute(
                'UPDATE wm_dca_plan SET last_generated_date = ? WHERE id = ?',
                (iso(cursor), plan['id']),
            )
        count = 0
        window_done = False
        while count < MAX_BACKFILL_PER_RUN:
            due = next_due(plan, cursor)
            if end and due > end:
                window_done = True
                break
            if due > today:
                break
            # Daily plans mean one investment per trading day: skip closed days
            # instead of rolling each weekend/holiday into the next session.
            if plan['frequency'] == 'daily' and not _is_open(conn, asset['market'], due):
                conn.execute(
                    'UPDATE wm_dca_plan SET last_generated_date = ? WHERE id = ?',
                    (iso(due), plan['id']),
                )
                cursor = due
                count += 1
                continue
            trade_date = resolve_trade_date(conn, asset, due, plan.get('roll_rule') or 'next')
            is_skipped = trade_date is None
            message = None
            if is_skipped:
                message = '非交易日，按计划跳过'
                skipped += 1

            if not is_skipped:
                cur = conn.execute(
                    """INSERT OR IGNORE INTO wm_dca_order
                       (plan_id, due_date, trade_date, amount, amount_currency, status, created_at)
                       VALUES (?, ?, ?, ?, ?, 'awaiting_price', ?)""",
                    (plan['id'], iso(due), iso(trade_date), plan['amount'],
                     plan.get('amount_currency') or 'CNY', now_iso()),
                )
                if cur.rowcount:
                    created += 1
            else:
                conn.execute(
                    """INSERT OR IGNORE INTO wm_dca_order
                       (plan_id, due_date, trade_date, amount, amount_currency, status, message, created_at)
                       VALUES (?, ?, ?, ?, ?, 'skipped', ?, ?)""",
                    (plan['id'], iso(due), iso(trade_date or due), plan['amount'],
                     plan.get('amount_currency') or 'CNY', message, now_iso()),
                )
            conn.execute(
                'UPDATE wm_dca_plan SET last_generated_date = ? WHERE id = ?',
                (iso(due), plan['id']),
            )
            cursor = due
            count += 1
        if count >= MAX_BACKFILL_PER_RUN:
            print(f'定投计划 {plan["name"]} 单次补记达到 {MAX_BACKFILL_PER_RUN} 期上限，剩余期数将在下次任务继续', flush=True)
        if end and end <= today and window_done:
            conn.execute("UPDATE wm_dca_plan SET status = 'finished' WHERE id = ?", (plan['id'],))
        conn.commit()
    return {'created': created, 'skipped': skipped, 'plans': len(plans)}


def _amount_in_asset_currency(conn, amount, amount_currency, asset_currency, on_date):
    amount_currency = (amount_currency or 'CNY').upper()
    asset_currency = (asset_currency or 'CNY').upper()
    amount_rate = 1.0 if amount_currency == 'CNY' else prices.get_fx_for_date(
        conn, amount_currency, on_date)
    asset_rate = 1.0 if asset_currency == 'CNY' else prices.get_fx_for_date(
        conn, asset_currency, on_date)
    if not amount_rate or not asset_rate:
        raise ValueError(f'缺少 {on_date} 的汇率（{amount_currency}/{asset_currency}）')
    return amount * amount_rate / asset_rate, asset_rate


def _insert_dca_transaction(conn, order, plan, asset, trade_date, amount, fee, units, price,
                            currency, fx_rate_value, note):
    cash_adjusted = 1 if plan.get('account_id') else 0
    cur = conn.execute(
        """INSERT INTO wm_transaction
           (asset_id, account_id, type, trade_date, units, price, amount, fee, currency,
            fx_rate, cash_adjusted, source, dca_order_id, note, created_at)
           VALUES (?, ?, 'buy', ?, ?, ?, ?, ?, ?, ?, ?, 'dca', ?, ?, ?)""",
        (asset['id'], plan.get('account_id'), iso(trade_date), units, price, amount, fee,
         currency, fx_rate_value, cash_adjusted, order['id'], note, now_iso()),
    )
    tx_id = cur.lastrowid
    apply_transaction_cash(conn, {
        'account_id': plan.get('account_id'), 'type': 'buy', 'currency': currency,
        'amount': amount, 'fee': fee, 'trade_date': trade_date,
    })
    return tx_id


def _fail_order(conn, order_id, message):
    conn.execute(
        "UPDATE wm_dca_order SET status = 'failed', message = ?, settled_at = ? WHERE id = ? AND status = 'awaiting_price'",
        (str(message)[:500], now_iso(), order_id),
    )


def settle_orders(conn, today=None):
    today = parse_date(today or today_str())
    rows = conn.execute(
        """SELECT o.*, p.name AS plan_name, p.asset_id, p.account_id, p.fee_rate, p.fee_min
           FROM wm_dca_order o JOIN wm_dca_plan p ON p.id = o.plan_id
           WHERE o.status = 'awaiting_price' AND o.trade_date <= ?
           ORDER BY o.trade_date, o.id""",
        (iso(today),),
    ).fetchall()
    filled = 0
    waiting = 0
    failed = 0
    skipped = 0

    for order in rows:
        order = dict(order)
        asset_row = conn.execute('SELECT * FROM wm_asset WHERE id = ?', (order['asset_id'],)).fetchone()
        if asset_row is None:
            _fail_order(conn, order['id'], '资产不存在')
            failed += 1
            conn.commit()
            continue
        asset = dict(asset_row)
        plan = dict(conn.execute('SELECT * FROM wm_dca_plan WHERE id = ?', (order['plan_id'],)).fetchone())
        trade_date = parse_date(order.get('trade_date') or order['due_date'])

        try:
            if asset.get('valuation_mode') == 'accrual':
                amount, fx_value = _amount_in_asset_currency(
                    conn, float(order['amount']), order['amount_currency'], asset['currency'], trade_date)
                tx_id = _insert_dca_transaction(
                    conn, order, plan, asset, trade_date, amount, 0.0, amount, 1.0,
                    asset['currency'], fx_value, f'{plan["name"]} 自动定投')
                conn.execute(
                    """UPDATE wm_dca_order SET status = 'filled', price_date = ?, trade_date = ?,
                       units = ?, price = 1, fee = 0, fx_rate = ?, transaction_id = ?, settled_at = ?
                       WHERE id = ? AND status = 'awaiting_price'""",
                    (iso(trade_date), iso(trade_date), amount, fx_value, tx_id, now_iso(), order['id']),
                )
                conn.commit()
                filled += 1
                continue

            price_row = conn.execute(
                """SELECT price_date, close FROM wm_price
                   WHERE asset_id = ? AND price_date >= ? ORDER BY price_date LIMIT 1""",
                (asset['id'], iso(trade_date)),
            ).fetchone()
            if price_row is None:
                if (today - trade_date).days > PRICE_WAIT_DAYS:
                    _fail_order(conn, order['id'], f'等待价格超过 {PRICE_WAIT_DAYS} 天')
                    failed += 1
                    conn.commit()
                else:
                    waiting += 1
                continue

            price_date = parse_date(price_row['price_date'])
            price = float(price_row['close'])
            if (price_date - trade_date).days > PRICE_WAIT_DAYS:
                _fail_order(conn, order['id'], f'行情/净值延迟超过 {PRICE_WAIT_DAYS} 天')
                failed += 1
                conn.commit()
                continue

            if plan.get('frequency') == 'daily':
                duplicate = conn.execute(
                    """SELECT 1 FROM wm_dca_order
                       WHERE plan_id = ? AND status = 'filled' AND price_date = ? AND id != ?""",
                    (plan['id'], iso(price_date), order['id']),
                ).fetchone()
                if duplicate:
                    conn.execute(
                        """UPDATE wm_dca_order SET status = 'skipped',
                           message = '同一交易日已有一笔定投，跳过（假期/净值延迟）',
                           settled_at = ? WHERE id = ? AND status = 'awaiting_price'""",
                        (now_iso(), order['id']),
                    )
                    conn.commit()
                    skipped += 1
                    continue

            amount_asset, fx_value = _amount_in_asset_currency(
                conn, float(order['amount']), order['amount_currency'], asset['currency'], price_date)
            if amount_asset <= 0 or price <= 0:
                raise ValueError('金额或价格无效')

            fee_rate = float(plan.get('fee_rate') or 0)
            fee_min = float(plan.get('fee_min') or 0)
            if asset['market'] == 'FUND_CN':
                fee = amount_asset - amount_asset / (1 + fee_rate) if fee_rate else 0.0
                net = amount_asset - fee
                units = net / price
                tx_amount = net
            elif bool(asset.get('allow_fraction', 1)):
                fee = max(amount_asset * fee_rate, fee_min)
                available = amount_asset - fee
                if available <= 0:
                    raise ValueError('金额不足以支付费用')
                units = available / price
                tx_amount = amount_asset - fee
            else:
                lot = float(asset.get('lot_size') or 1)
                fee = max(amount_asset * fee_rate, fee_min)
                available = amount_asset - fee
                units = math.floor(available / price / lot) * lot
                if units <= 0:
                    raise ValueError(
                        f'金额不足最小交易单位（{lot:g} 股/份，约需 {lot * price:.2f} {asset["currency"]}）'
                    )
                gross = units * price
                fee = max(gross * fee_rate, fee_min)
                if gross - fee <= 0:
                    raise ValueError('成交金额不足以支付费用')
                tx_amount = gross
                units = units

            tx_id = _insert_dca_transaction(
                conn, order, plan, asset, price_date, tx_amount, fee, units, price,
                asset['currency'], fx_value, f'{plan["name"]} 自动定投')
            conn.execute(
                """UPDATE wm_dca_order SET status = 'filled', trade_date = ?, price_date = ?,
                   units = ?, price = ?, fee = ?, fx_rate = ?, transaction_id = ?, settled_at = ?
                   WHERE id = ? AND status = 'awaiting_price'""",
                (iso(price_date), iso(price_date), units, price, fee, fx_value,
                 tx_id, now_iso(), order['id']),
            )
            conn.commit()
            filled += 1
        except prices.PriceError as exc:
            # A transient FX/provider failure should not permanently fail an
            # order; keep waiting until the normal price-wait window expires.
            conn.rollback()
            if (today - trade_date).days > PRICE_WAIT_DAYS:
                _fail_order(conn, order['id'], str(exc))
                failed += 1
            else:
                waiting += 1
            conn.commit()
        except Exception as exc:
            conn.rollback()
            _fail_order(conn, order['id'], str(exc))
            conn.commit()
            failed += 1

    return {'filled': filled, 'waiting': waiting, 'failed': failed, 'skipped': skipped}


def run_dca(conn=None, today=None, plan_id=None):
    if not LOCK.acquire(blocking=False):
        return {'skipped': True}
    own = conn is None
    conn = conn or connect()
    try:
        generated = generate_orders(conn, today=today, plan_id=plan_id)
        settled = settle_orders(conn, today=today)
        return {**generated, **settled}
    finally:
        if own:
            conn.close()
        LOCK.release()


def preview(conn, data, limit=12):
    """Preview due dates and backfill count without writing anything."""
    asset = dict(conn.execute('SELECT * FROM wm_asset WHERE id = ?', (data['asset_id'],)).fetchone())
    if asset['market'] in ('CN', 'FUND_CN'):
        try:
            prices.ensure_cn_calendar(conn)
        except Exception:
            pass
    plan = {
        'id': 0,
        'asset_id': asset['id'],
        'frequency': data.get('frequency') or 'monthly',
        'weekday': data.get('weekday'),
        'day_of_month': data.get('day_of_month'),
        'start_date': data.get('start_date') or today_str(),
        'end_date': data.get('end_date'),
        'roll_rule': data.get('roll_rule') or 'next',
        'amount': float(data.get('amount') or 0),
        'amount_currency': data.get('amount_currency') or 'CNY',
        'catch_up': 1 if data.get('catch_up', True) else 0,
    }
    today = parse_date(today_str())
    start = parse_date(plan['start_date'])
    if start > today or not plan['catch_up']:
        backfill = 0
    else:
        cursor = start - timedelta(days=1)
        backfill = 0
        while True:
            due = next_due(plan, cursor)
            if (plan['end_date'] and due > parse_date(plan['end_date'])) or due >= today:
                break
            if plan['frequency'] == 'daily' and not _is_open(conn, asset['market'], due):
                cursor = due
                continue
            backfill += 1
            cursor = due
            if backfill > 1000:
                break

    due_dates = []
    if plan['catch_up']:
        cursor = start - timedelta(days=1)
    else:
        cursor = max(start, today) - timedelta(days=1)
    while len(due_dates) < limit:
        due = next_due(plan, cursor)
        if plan['end_date'] and due > parse_date(plan['end_date']):
            break
        if plan['frequency'] == 'daily' and not _is_open(conn, asset['market'], due):
            trade_date = None
        else:
            trade_date = resolve_trade_date(conn, asset, due, plan['roll_rule'])
        due_dates.append({
            'due_date': iso(due),
            'trade_date': iso(trade_date) if trade_date else None,
            'amount': plan['amount'],
            'amount_currency': plan['amount_currency'],
        })
        cursor = due

    return {
        'asset': {'id': asset['id'], 'symbol': asset['symbol'], 'name': asset['name'],
                  'currency': asset['currency']},
        'backfill_count': backfill,
        'due_dates': due_dates,
    }


def plan_stats(conn, plan, as_of=None):
    as_of = parse_date(as_of or today_str())
    counts = {r['status']: r['c'] for r in conn.execute(
        'SELECT status, COUNT(*) AS c FROM wm_dca_order WHERE plan_id = ? GROUP BY status',
        (plan['id'],),
    ).fetchall()}
    pos = plan_position(conn, plan, as_of)
    invested = 0.0
    flows = []
    if pos:
        for r in conn.execute(
            """SELECT t.trade_date, t.amount, t.fee, t.fx_rate FROM wm_transaction t
               JOIN wm_dca_order o ON o.id = t.dca_order_id
               WHERE o.plan_id = ? AND t.trade_date <= ? ORDER BY t.trade_date""",
            (plan['id'], iso(as_of)),
        ).fetchall():
            cash = (float(r['amount'] or 0) + float(r['fee'] or 0)) * float(r['fx_rate'] or 1)
            invested += cash
            flows.append((parse_date(r['trade_date']), -cash))
        value = pos['market_value_cny']
        if value > 0:
            flows.append((as_of, value))
    else:
        value = 0.0

    cursor = parse_date(plan['last_generated_date']) if plan.get('last_generated_date') else parse_date(plan['start_date']) - timedelta(days=1)
    upcoming = next_due(plan, cursor) if plan.get('status') == 'active' else None
    asset_info = conn.execute(
        'SELECT symbol, name, market FROM wm_asset WHERE id = ?', (plan['asset_id'],)
    ).fetchone()
    if upcoming and plan.get('frequency') == 'daily':
        market = asset_info['market'] if asset_info else 'CN'
        for _ in range(40):
            if _is_open(conn, market, upcoming):
                break
            upcoming += timedelta(days=1)
        if plan.get('end_date') and upcoming > parse_date(plan['end_date']):
            upcoming = None
    return {
        **{k: plan[k] for k in ('id', 'name', 'asset_id', 'amount', 'amount_currency',
                                'frequency', 'weekday', 'day_of_month', 'start_date',
                                'end_date', 'roll_rule', 'fee_rate', 'fee_min',
                                'execution_mode', 'catch_up', 'status', 'note')},
        'asset_symbol': pos['symbol'] if pos else (asset_info['symbol'] if asset_info else None),
        'asset_name': pos['name'] if pos else (asset_info['name'] if asset_info else None),
        'filled_count': counts.get('filled', 0),
        'waiting_count': counts.get('awaiting_price', 0),
        'failed_count': counts.get('failed', 0),
        'skipped_count': counts.get('skipped', 0),
        'invested': round(invested, 2),
        'value': round(value, 2),
        'pnl': round(value - invested, 2),
        'pnl_pct': ((value - invested) / invested) if invested else None,
        'xirr': xirr(flows),
        'next_due': iso(upcoming) if upcoming else None,
        'last_generated_date': plan.get('last_generated_date'),
    }


def pause_plan(conn, plan_id):
    conn.execute("UPDATE wm_dca_plan SET status = 'paused' WHERE id = ? AND status = 'active'", (plan_id,))


def resume_plan(conn, plan_id):
    # Resume from tomorrow so a long pause does not create a burst of backfilled orders.
    conn.execute(
        "UPDATE wm_dca_plan SET status = 'active', last_generated_date = ? WHERE id = ? AND status = 'paused'",
        ((parse_date(today_str()) - timedelta(days=1)).isoformat(), plan_id),
    )


def skip_order(conn, order_id):
    conn.execute(
        "UPDATE wm_dca_order SET status = 'skipped', message = '手动跳过', settled_at = ? WHERE id = ? AND status IN ('awaiting_price', 'failed')",
        (now_iso(), order_id),
    )


def retry_order(conn, order_id):
    conn.execute(
        "UPDATE wm_dca_order SET status = 'awaiting_price', message = NULL, settled_at = NULL WHERE id = ? AND status IN ('failed', 'skipped')",
        (order_id,),
    )
