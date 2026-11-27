"""Core wealth-management tests (no network)."""
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta
from unittest import mock
from zoneinfo import ZoneInfo

from wealth.db import SHANGHAI_TZ, connect, now_iso, today_str
from wealth import dca, portfolio, prices
from wealth.schema import init_wealth_db


class WealthTestBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, 'wealth-test.db')
        init_wealth_db(self.db_path)
        self.conn = connect(self.db_path)
        # Stop ensure_cn_calendar() from hitting the network in tests.  With no
        # min bound, is_cn_trading_day() falls back to weekdays.
        self.conn.execute(
            "INSERT INTO wm_meta(key, value) VALUES ('cn_calendar_max', '2099-01-01')"
        )
        self.conn.commit()

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()

    def add_asset(self, symbol='TEST', market='MANUAL', asset_class='stock',
                  currency='CNY', quote_source='manual', valuation_mode='price',
                  **kwargs):
        fields = {
            'symbol': symbol, 'market': market, 'asset_class': asset_class,
            'name': kwargs.pop('name', symbol), 'currency': currency,
            'quote_source': quote_source, 'valuation_mode': valuation_mode,
            'is_active': 1, 'created_at': now_iso(),
        }
        fields.update(kwargs)
        columns = ', '.join(fields)
        placeholders = ', '.join('?' for _ in fields)
        cur = self.conn.execute(
            f'INSERT INTO wm_asset ({columns}) VALUES ({placeholders})',
            tuple(fields.values()),
        )
        self.conn.commit()
        return cur.lastrowid

    def add_price(self, asset_id, day, close, prev_close=None):
        self.conn.execute(
            """INSERT INTO wm_price(asset_id, price_date, close, prev_close, source, fetched_at)
               VALUES (?, ?, ?, ?, 'manual', ?)""",
            (asset_id, day, close, prev_close, now_iso()),
        )
        self.conn.execute(
            'UPDATE wm_asset SET last_price = ?, last_price_date = ? WHERE id = ?',
            (close, day, asset_id),
        )
        self.conn.commit()

    def add_tx(self, asset_id, type_, day, units=0, price=0, amount=0, fee=0,
               currency='CNY', fx_rate=1.0, split_ratio=None, account_id=None):
        cur = self.conn.execute(
            """INSERT INTO wm_transaction
               (asset_id, account_id, type, trade_date, units, price, amount, fee, currency,
                fx_rate, split_ratio, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual', ?)""",
            (asset_id, account_id, type_, day, units, price, amount, fee, currency,
             fx_rate, split_ratio, now_iso()),
        )
        self.conn.commit()
        return cur.lastrowid

    def add_account(self, name, currency='CNY', broker=None):
        cur = self.conn.execute(
            'INSERT INTO wm_account (name, broker, currency, created_at) VALUES (?, ?, ?, ?)',
            (name, broker, currency, now_iso()),
        )
        self.conn.commit()
        return cur.lastrowid

    def add_plan(self, asset_id, amount=1000, amount_currency='CNY',
                 frequency='monthly', start_date='2026-01-01', **kwargs):
        fields = {
            'name': kwargs.pop('name', '测试定投'),
            'asset_id': asset_id,
            'amount': amount,
            'amount_currency': amount_currency,
            'frequency': frequency,
            'start_date': start_date,
            'roll_rule': kwargs.pop('roll_rule', 'next'),
            'fee_rate': kwargs.pop('fee_rate', 0),
            'fee_min': kwargs.pop('fee_min', 0),
            'execution_mode': 'auto',
            'catch_up': kwargs.pop('catch_up', 1),
            'status': 'active',
            'created_at': now_iso(),
        }
        if 'weekday' in kwargs:
            fields['weekday'] = kwargs.pop('weekday')
        if 'day_of_month' in kwargs:
            fields['day_of_month'] = kwargs.pop('day_of_month')
        if 'end_date' in kwargs:
            fields['end_date'] = kwargs.pop('end_date')
        fields.update(kwargs)
        columns = ', '.join(fields)
        placeholders = ', '.join('?' for _ in fields)
        cur = self.conn.execute(
            f'INSERT INTO wm_dca_plan ({columns}) VALUES ({placeholders})',
            tuple(fields.values()),
        )
        self.conn.commit()
        return cur.lastrowid


class DueDateTests(WealthTestBase):
    def test_monthly_clamps_to_month_end(self):
        plan = {'start_date': '2026-01-31', 'frequency': 'monthly',
                'day_of_month': 31, 'weekday': None}
        self.assertEqual(dca.next_due(plan, '2026-01-01'), date(2026, 1, 31))
        self.assertEqual(dca.next_due(plan, '2026-01-31'), date(2026, 2, 28))
        self.assertEqual(dca.next_due(plan, '2026-02-28'), date(2026, 3, 31))

    def test_weekly_and_biweekly(self):
        weekly = {'start_date': '2026-01-01', 'frequency': 'weekly',
                  'weekday': 0, 'day_of_month': None}
        self.assertEqual(dca.next_due(weekly, '2026-01-01'), date(2026, 1, 5))
        self.assertEqual(dca.next_due(weekly, '2026-01-05'), date(2026, 1, 12))
        biweekly = dict(weekly, frequency='biweekly')
        self.assertEqual(dca.next_due(biweekly, '2026-01-05'), date(2026, 1, 19))

    def test_daily(self):
        plan = {'start_date': '2026-01-01', 'frequency': 'daily',
                'weekday': None, 'day_of_month': None}
        self.assertEqual(dca.next_due(plan, '2026-01-01'), date(2026, 1, 2))


class MathsTests(WealthTestBase):
    def test_xirr_one_year(self):
        result = portfolio.xirr([
            (date(2025, 1, 1), -1000.0),
            (date(2026, 1, 1), 1100.0),
        ])
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result, 0.10, places=6)

    def test_positions_split_by_account(self):
        asset_id = self.add_asset()
        account_a = self.add_account('账户A')
        account_b = self.add_account('账户B')
        self.add_tx(asset_id, 'buy', '2026-01-02', units=10, price=10,
                    amount=100, account_id=account_a)
        self.add_tx(asset_id, 'buy', '2026-01-03', units=5, price=20,
                    amount=100, account_id=account_b)
        self.add_price(asset_id, '2026-01-05', 30)
        positions, total = portfolio.build_positions(self.conn, '2026-01-05')
        self.assertEqual(len(positions), 2)
        by_account = {p['account_name']: p for p in positions}
        self.assertAlmostEqual(by_account['账户A']['units'], 10.0)
        self.assertAlmostEqual(by_account['账户A']['pnl_cny'], 200.0)
        self.assertAlmostEqual(by_account['账户B']['units'], 5.0)
        self.assertAlmostEqual(by_account['账户B']['pnl_cny'], 50.0)
        self.assertAlmostEqual(total, 450.0)

    def test_account_cash_is_included_in_total_assets(self):
        account_id = self.add_account('美元现金', currency='USD')
        today = datetime.now(SHANGHAI_TZ).date().isoformat()
        self.conn.execute(
            """INSERT INTO wm_account_balance
               (account_id, balance_date, amount, currency, source, created_at)
               VALUES (?, ?, ?, 'USD', 'test', ?)""",
            (account_id, today, 100, now_iso()),
        )
        self.conn.execute(
            """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
               VALUES (?, 'USD', 7.0, 'test')""",
            (today,),
        )
        self.conn.commit()
        cash_rows, cash_total = portfolio.account_cash(self.conn, today)
        self.assertAlmostEqual(cash_total, 700.0)
        self.assertEqual(cash_rows[0]['account_name'], '美元现金')
        summary = portfolio.build_summary(self.conn, today)
        self.assertAlmostEqual(summary['securities_value'], 0.0)
        self.assertAlmostEqual(summary['cash_value'], 700.0)
        self.assertAlmostEqual(summary['total_value'], 700.0)
        cash_alloc = [a for a in summary['allocations']['asset_class'] if a['name'] == 'cash']
        self.assertEqual(len(cash_alloc), 1)
        self.assertAlmostEqual(cash_alloc[0]['value'], 700.0)

    def test_manual_account_cash_upsert(self):
        from wealth.routes import _upsert_account_cash
        account_id = self.add_account('手工现金')
        _upsert_account_cash(self.conn, account_id, 100, '2026-11-16', 'CNY')
        _upsert_account_cash(self.conn, account_id, 200, '2026-11-17', 'CNY')
        self.conn.commit()
        rows, total = portfolio.account_cash(self.conn, '2026-11-16')
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(total, 100.0)
        rows, total = portfolio.account_cash(self.conn, '2026-11-17')
        self.assertAlmostEqual(total, 200.0)
        _upsert_account_cash(self.conn, account_id, 250, '2026-11-17', 'CNY')
        self.conn.commit()
        rows, total = portfolio.account_cash(self.conn, '2026-11-17')
        self.assertAlmostEqual(total, 250.0)
        self.assertEqual(rows[0]['account_id'], account_id)
        self.assertEqual(rows[0]['currency'], 'CNY')

    def test_multi_currency_cash_on_same_account_date(self):
        from wealth.routes import _upsert_account_cash
        account_id = self.add_account('多币种现金')
        _upsert_account_cash(self.conn, account_id, 200, '2026-11-17', 'CNY')
        _upsert_account_cash(self.conn, account_id, 100, '2026-11-17', 'USD')
        self.conn.execute(
            """INSERT INTO wm_fx_rate (rate_date, currency, cny_rate, source)
               VALUES ('2026-11-17', 'USD', 7.0, 'test')"""
        )
        self.conn.commit()
        rows, total = portfolio.account_cash(self.conn, '2026-11-17')
        self.assertEqual(len(rows), 2)
        self.assertAlmostEqual(total, 900.0)
        self.assertEqual({r['currency'] for r in rows}, {'CNY', 'USD'})

    def test_buy_and_sell_adjust_account_cash(self):
        from wealth.routes import _create_transaction, _upsert_account_cash
        from wealth.cash import reverse_transaction_cash
        account_id = self.add_account('交易现金')
        asset_id = self.add_asset()
        _upsert_account_cash(self.conn, account_id, 1000, '2026-11-16', 'CNY')
        buy_id = _create_transaction(self.conn, {
            'asset_id': asset_id, 'account_id': account_id, 'type': 'buy',
            'trade_date': '2026-11-16', 'units': 10, 'price': 10,
            'amount': 100, 'fee': 5, 'currency': 'CNY',
        })
        rows, total = portfolio.account_cash(self.conn, '2026-11-16')
        self.assertAlmostEqual(total, 895.0)
        self.assertEqual(
            self.conn.execute('SELECT cash_adjusted FROM wm_transaction WHERE id = ?',
                              (buy_id,)).fetchone()['cash_adjusted'], 1)

        _create_transaction(self.conn, {
            'asset_id': asset_id, 'account_id': account_id, 'type': 'sell',
            'trade_date': '2026-11-16', 'units': 2, 'price': 10,
            'amount': 20, 'fee': 1, 'currency': 'CNY',
        })
        rows, total = portfolio.account_cash(self.conn, '2026-11-16')
        self.assertAlmostEqual(total, 914.0)

        buy = self.conn.execute('SELECT * FROM wm_transaction WHERE id = ?',
                                (buy_id,)).fetchone()
        reverse_transaction_cash(self.conn, buy)
        rows, total = portfolio.account_cash(self.conn, '2026-11-16')
        self.assertAlmostEqual(total, 1019.0)

    def test_imported_transaction_does_not_touch_cash(self):
        from wealth.routes import _create_transaction, _upsert_account_cash
        account_id = self.add_account('导入现金')
        asset_id = self.add_asset()
        _upsert_account_cash(self.conn, account_id, 1000, '2026-11-16', 'CNY')
        tx_id = _create_transaction(self.conn, {
            'asset_id': asset_id, 'account_id': account_id, 'type': 'buy',
            'trade_date': '2026-11-16', 'units': 10, 'price': 10,
            'amount': 100, 'fee': 0, 'currency': 'CNY',
        }, source='import', import_hash='test-import-hash', affect_cash=False)
        rows, total = portfolio.account_cash(self.conn, '2026-11-16')
        self.assertAlmostEqual(total, 1000.0)
        self.assertEqual(
            self.conn.execute('SELECT cash_adjusted FROM wm_transaction WHERE id = ?',
                              (tx_id,)).fetchone()['cash_adjusted'], 0)

    def test_cash_fields_validation(self):
        from wealth.routes import _cash_fields
        self.assertEqual(_cash_fields({}), (None, None))
        self.assertEqual(_cash_fields({'cash_amount': ''}), (None, None))
        amount, balance_date = _cash_fields(
            {'cash_amount': '12.5', 'cash_date': '2026-11-16'})
        self.assertAlmostEqual(amount, 12.5)
        self.assertEqual(balance_date, '2026-11-16')
        with self.assertRaises(ValueError):
            _cash_fields({'cash_amount': 'abc'})
        with self.assertRaises(ValueError):
            _cash_fields({'cash_amount': 1, 'cash_date': '2999-01-01'})

    def test_moving_average_and_realized(self):
        asset_id = self.add_asset()
        self.add_tx(asset_id, 'buy', '2026-01-01', units=10, price=10, amount=100)
        self.add_tx(asset_id, 'buy', '2026-01-02', units=10, price=20, amount=200)
        self.add_tx(asset_id, 'sell', '2026-01-03', units=5, price=30, amount=150)
        self.add_price(asset_id, '2026-01-05', 30)
        positions, total = portfolio.build_positions(self.conn, '2026-01-05')
        pos = positions[0]
        self.assertAlmostEqual(pos['units'], 15.0)
        self.assertAlmostEqual(pos['avg_cost_native'], 15.0)
        self.assertAlmostEqual(pos['cost_native'], 225.0)
        self.assertAlmostEqual(pos['realized_cny'], 75.0)
        self.assertAlmostEqual(pos['market_value_cny'], 450.0)
        self.assertAlmostEqual(pos['pnl_cny'], 300.0)
        self.assertAlmostEqual(total, 450.0)

        summary = portfolio.build_summary(self.conn, '2026-01-05', 365)
        alloc = summary['allocations']['asset_class']
        self.assertEqual(len(alloc), 1)
        self.assertAlmostEqual(alloc[0]['percent'], 100.0)

        # Deactivating an asset must not hide an existing holding.
        self.conn.execute('UPDATE wm_asset SET is_active = 0 WHERE id = ?', (asset_id,))
        self.conn.commit()
        positions, total = portfolio.build_positions(self.conn, '2026-01-05')
        self.assertEqual(len(positions), 1)
        self.assertAlmostEqual(total, 450.0)

    def test_missing_fx_is_flagged(self):
        asset_id = self.add_asset(symbol='USD1', currency='USD')
        self.add_tx(asset_id, 'buy', '2026-01-02', units=1, price=10, amount=10,
                    currency='USD', fx_rate=1.0)
        positions, _ = portfolio.build_positions(self.conn, '2026-01-02')
        self.assertTrue(positions[0]['fx_missing'])
        self.assertTrue(any('汇率' in w for w in positions[0]['warnings']))

    def test_bank_compound_and_maturity(self):
        asset_id = self.add_asset(
            symbol='BANK2', market='BANK', asset_class='bank_wm',
            quote_source='accrual', valuation_mode='accrual',
            annual_rate=0.0365, interest_mode='compound', maturity_date='2026-01-05',
        )
        self.add_tx(asset_id, 'buy', '2026-01-01', amount=10000)
        positions, _ = portfolio.build_positions(self.conn, '2026-01-05')
        pos = positions[0]
        self.assertAlmostEqual(pos['accrued_interest'], 5.001, places=3)
        self.assertAlmostEqual(pos['day_pnl_cny'], 1.0004, places=3)
        positions, _ = portfolio.build_positions(self.conn, '2026-01-10')
        pos = positions[0]
        self.assertAlmostEqual(pos['accrued_interest'], 5.001, places=3)
        self.assertAlmostEqual(pos['day_pnl_cny'], 0.0)

    def test_history_starts_with_existing_position(self):
        asset_id = self.add_asset()
        self.add_tx(asset_id, 'buy', '2025-01-01', units=10, price=10, amount=100)
        self.add_price(asset_id, '2025-01-01', 10)
        self.add_price(asset_id, '2026-01-02', 20)
        points = portfolio.build_history(self.conn, '2026-01-01', '2026-01-02')
        self.assertEqual(len(points), 2)
        self.assertAlmostEqual(points[0]['value'], 100.0)
        self.assertAlmostEqual(points[1]['value'], 200.0)

    def test_bank_accrual_and_interest_payment(self):
        asset_id = self.add_asset(
            symbol='BANK1', market='BANK', asset_class='bank_wm',
            quote_source='accrual', valuation_mode='accrual',
            annual_rate=0.0365, interest_mode='simple',
        )
        self.add_tx(asset_id, 'buy', '2026-01-01', amount=10000)
        positions, _ = portfolio.build_positions(self.conn, '2026-01-11')
        pos = positions[0]
        self.assertAlmostEqual(pos['principal'], 10000.0)
        self.assertAlmostEqual(pos['accrued_interest'], 11.0)
        self.assertAlmostEqual(pos['market_value_cny'], 10011.0)
        self.assertAlmostEqual(pos['pnl_cny'], 11.0)

        self.add_tx(asset_id, 'interest', '2026-01-11', amount=10)
        positions, _ = portfolio.build_positions(self.conn, '2026-01-11')
        pos = positions[0]
        self.assertAlmostEqual(pos['accrued_interest'], 1.0)
        self.assertAlmostEqual(pos['interest_paid'], 10.0)
        self.assertAlmostEqual(pos['realized_cny'], 10.0)
        self.assertAlmostEqual(pos['pnl_cny'], 11.0)


class DcaTests(WealthTestBase):
    def _add_order(self, plan_id, due_date, trade_date, amount=1000,
                   status='awaiting_price'):
        cur = self.conn.execute(
            """INSERT INTO wm_dca_order
               (plan_id, due_date, trade_date, amount, amount_currency, status, created_at)
               VALUES (?, ?, ?, ?, 'CNY', ?, ?)""",
            (plan_id, due_date, trade_date, amount, status, now_iso()),
        )
        self.conn.commit()
        return cur.lastrowid

    def test_pending_amount_sync_scope(self):
        asset_id = self.add_asset(symbol='000001', market='FUND_CN', asset_class='fund')
        plan_id = self.add_plan(asset_id, amount=1000, start_date='2026-01-01')
        current = self._add_order(plan_id, '2026-01-05', '2026-01-05')
        future = self._add_order(plan_id, '2026-01-06', '2026-01-06')
        past = self._add_order(plan_id, '2026-01-02', '2026-01-02')
        filled = self._add_order(plan_id, '2026-01-07', '2026-01-05', status='filled')
        failed = self._add_order(plan_id, '2026-01-08', '2026-01-05', status='failed')

        now = datetime(2026, 1, 5, 10, 0, tzinfo=SHANGHAI_TZ)
        eligible = dca.pending_orders_for_amount_change(self.conn, plan_id, now=now)
        self.assertEqual({order['id'] for order in eligible}, {current, future})
        self.assertEqual(dca.update_pending_order_amounts(
            self.conn, plan_id, 500, 'CNY', now=now), 2)
        self.conn.commit()

        amounts = {row['id']: row['amount'] for row in self.conn.execute(
            'SELECT id, amount FROM wm_dca_order').fetchall()}
        self.assertEqual(amounts[current], 500)
        self.assertEqual(amounts[future], 500)
        self.assertEqual(amounts[past], 1000)
        self.assertEqual(amounts[filled], 1000)
        self.assertEqual(amounts[failed], 1000)

    def test_pending_amount_sync_respects_fund_cutoff(self):
        asset_id = self.add_asset(symbol='000001', market='FUND_CN', asset_class='fund')
        plan_id = self.add_plan(asset_id, amount=1000, start_date='2026-01-01')
        order_id = self._add_order(plan_id, '2026-01-05', '2026-01-05')

        before = datetime(2026, 1, 5, 14, 59, tzinfo=SHANGHAI_TZ)
        after = datetime(2026, 1, 5, 15, 0, tzinfo=SHANGHAI_TZ)
        self.assertEqual(len(dca.pending_orders_for_amount_change(
            self.conn, plan_id, now=before)), 1)
        self.assertEqual(dca.pending_orders_for_amount_change(
            self.conn, plan_id, now=after), [])
        self.assertEqual(dca.update_pending_order_amounts(
            self.conn, plan_id, 500, 'CNY', now=after), 0)
        self.conn.commit()
        order = self.conn.execute(
            'SELECT amount FROM wm_dca_order WHERE id = ?', (order_id,)
        ).fetchone()
        self.assertEqual(order['amount'], 1000)

    def test_generation_is_idempotent_and_settles(self):
        asset_id = self.add_asset()
        self.add_price(asset_id, '2026-01-02', 10)
        self.add_price(asset_id, '2026-01-05', 11, prev_close=10)
        plan_id = self.add_plan(asset_id, amount=1000, start_date='2026-01-01')

        first = dca.generate_orders(self.conn, today='2026-01-05', plan_id=plan_id)
        self.assertEqual(first['created'], 1)
        second = dca.generate_orders(self.conn, today='2026-01-05', plan_id=plan_id)
        self.assertEqual(second['created'], 0)
        count = self.conn.execute('SELECT COUNT(*) AS c FROM wm_dca_order').fetchone()['c']
        self.assertEqual(count, 1)

        settled = dca.settle_orders(self.conn, today='2026-01-05')
        self.assertEqual(settled['filled'], 1)
        order = self.conn.execute('SELECT * FROM wm_dca_order').fetchone()
        self.assertEqual(order['status'], 'filled')
        self.assertAlmostEqual(order['price'], 10.0)
        self.assertAlmostEqual(order['units'], 100.0)
        tx = self.conn.execute('SELECT * FROM wm_transaction WHERE dca_order_id = ?', (order['id'],)).fetchone()
        self.assertIsNotNone(tx)
        self.assertEqual(tx['source'], 'dca')

        plan = self.conn.execute('SELECT * FROM wm_dca_plan WHERE id = ?', (plan_id,)).fetchone()
        stats = dca.plan_stats(self.conn, dict(plan), '2026-01-05')
        self.assertEqual(stats['filled_count'], 1)
        self.assertAlmostEqual(stats['invested'], 1000.0)
        self.assertAlmostEqual(stats['value'], 1100.0)
        self.assertAlmostEqual(stats['pnl'], 100.0)

    def test_summary_plan_run_rate_and_actual(self):
        asset_id = self.add_asset()
        self.add_price(asset_id, '2026-01-05', 10)
        self.add_plan(asset_id, amount=100, frequency='daily',
                      start_date='2026-01-05')
        self.add_plan(asset_id, amount=2000, frequency='monthly',
                      start_date='2026-01-01', day_of_month=5)
        self.add_plan(asset_id, amount=500, frequency='monthly',
                      start_date='2026-01-01', day_of_month=5, status='paused')
        dca.generate_orders(self.conn, today='2026-01-05')
        dca.settle_orders(self.conn, today='2026-01-05')

        day = dca.summary(self.conn, 'day', as_of='2026-01-05')
        self.assertAlmostEqual(day['plan']['cny'], 100 + 2000 * 12 / 252, places=2)
        self.assertAlmostEqual(day['actual']['cny'], 2100.0)
        self.assertAlmostEqual(day['series'][-1]['actual_cny'], 2100.0)
        self.assertAlmostEqual(day['plan']['by_currency']['CNY'],
                               100 + 2000 * 12 / 252, places=2)

        month = dca.summary(self.conn, 'month', as_of='2026-01-05')
        self.assertAlmostEqual(month['plan']['cny'], 100 * 252 / 12 + 2000, places=2)
        self.assertAlmostEqual(month['actual']['cny'], 2100.0)

    def test_summary_uses_fx_and_flags_missing(self):
        asset_id = self.add_asset()
        self.add_plan(asset_id, amount=100, amount_currency='USD',
                      frequency='monthly', start_date='2026-01-01')
        self.conn.execute(
            "INSERT INTO wm_fx_rate(rate_date, currency, cny_rate, source) "
            "VALUES ('2026-01-05', 'USD', 7.2, 'test')")
        self.conn.commit()
        result = dca.summary(self.conn, 'month', as_of='2026-01-05')
        self.assertAlmostEqual(result['plan']['cny'], 720.0)
        self.assertAlmostEqual(result['plan']['by_currency']['USD'], 100.0)
        self.assertFalse(result['plan']['fx_missing'])

        self.conn.execute('DELETE FROM wm_fx_rate')
        self.conn.commit()
        result = dca.summary(self.conn, 'month', as_of='2026-01-05')
        self.assertTrue(result['plan']['fx_missing'])
        self.assertIn('USD', result['plan']['missing_currencies'])

    def test_summary_filters_by_account(self):
        asset_id = self.add_asset()
        self.add_price(asset_id, '2026-01-05', 10)
        account_a = self.add_account('账户A')
        account_b = self.add_account('账户B')
        self.add_plan(asset_id, amount=1000, frequency='monthly',
                      start_date='2026-01-01', day_of_month=5, account_id=account_a)
        self.add_plan(asset_id, amount=2000, frequency='monthly',
                      start_date='2026-01-01', day_of_month=5, account_id=account_b)
        self.add_plan(asset_id, amount=500, frequency='monthly',
                      start_date='2026-01-01', day_of_month=5)
        dca.generate_orders(self.conn, today='2026-01-05')
        dca.settle_orders(self.conn, today='2026-01-05')

        all_accounts = dca.summary(self.conn, 'month', as_of='2026-01-05')
        self.assertAlmostEqual(all_accounts['plan']['cny'], 3500.0)
        self.assertAlmostEqual(all_accounts['actual']['cny'], 3500.0)

        account_a_summary = dca.summary(
            self.conn, 'month', as_of='2026-01-05', account_id=account_a)
        self.assertEqual(account_a_summary['account_id'], account_a)
        self.assertAlmostEqual(account_a_summary['plan']['cny'], 1000.0)
        self.assertAlmostEqual(account_a_summary['actual']['cny'], 1000.0)
        self.assertAlmostEqual(account_a_summary['series'][-1]['actual_cny'], 1000.0)

        account_b_summary = dca.summary(
            self.conn, 'month', as_of='2026-01-05', account_id=account_b)
        self.assertAlmostEqual(account_b_summary['plan']['cny'], 2000.0)
        self.assertAlmostEqual(account_b_summary['actual']['cny'], 2000.0)

        unassigned = dca.summary(
            self.conn, 'month', as_of='2026-01-05', account_id='none')
        self.assertAlmostEqual(unassigned['plan']['cny'], 500.0)
        self.assertAlmostEqual(unassigned['actual']['cny'], 500.0)

    def test_dca_buy_reduces_account_cash(self):
        from wealth.routes import _upsert_account_cash
        asset_id = self.add_asset()
        account_id = self.add_account('定投账户')
        self.add_price(asset_id, '2026-01-02', 10)
        plan_id = self.add_plan(asset_id, amount=1000, start_date='2026-01-01',
                                account_id=account_id)
        _upsert_account_cash(self.conn, account_id, 2000, '2026-01-01', 'CNY')
        self.conn.commit()
        dca.generate_orders(self.conn, today='2026-01-02', plan_id=plan_id)
        dca.settle_orders(self.conn, today='2026-01-02')
        rows, total = portfolio.account_cash(self.conn, '2026-01-02')
        self.assertAlmostEqual(total, 1000.0)
        tx = self.conn.execute(
            'SELECT cash_adjusted FROM wm_transaction WHERE dca_order_id IS NOT NULL'
        ).fetchone()
        self.assertEqual(tx['cash_adjusted'], 1)

    def test_fund_fee_and_share_math(self):
        asset_id = self.add_asset(
            symbol='000001', market='FUND_CN', asset_class='fund',
            quote_source='fund_em',
        )
        self.add_price(asset_id, '2026-01-02', 2.0)
        plan_id = self.add_plan(asset_id, amount=1000, start_date='2026-01-01',
                                fee_rate=0.015)
        dca.generate_orders(self.conn, today='2026-01-02', plan_id=plan_id)
        dca.settle_orders(self.conn, today='2026-01-02')
        order = self.conn.execute('SELECT * FROM wm_dca_order').fetchone()
        self.assertEqual(order['status'], 'filled')
        expected_fee = 1000 - 1000 / 1.015
        self.assertAlmostEqual(order['fee'], expected_fee, places=6)
        self.assertAlmostEqual(order['units'], (1000 - expected_fee) / 2.0, places=6)
        self.assertAlmostEqual(order['units'] * 2.0 + order['fee'], 1000.0, places=6)

    def test_daily_plan_skips_closed_days(self):
        asset_id = self.add_asset(market='CN', asset_class='stock')
        for day, price in [('2026-01-02', 10), ('2026-01-05', 11), ('2026-01-06', 12)]:
            self.add_price(asset_id, day, price)
        plan_id = self.add_plan(asset_id, frequency='daily', start_date='2026-01-01',
                                roll_rule='next')
        dca.generate_orders(self.conn, today='2026-01-06', plan_id=plan_id)
        due_dates = [r['due_date'] for r in self.conn.execute(
            'SELECT due_date FROM wm_dca_order ORDER BY due_date').fetchall()]
        # 2026-01-01 is a Thursday holiday only if the CN calendar says so; with
        # the weekday fallback in tests it is generated, but the weekend 3rd/4th
        # is never rolled into Monday.
        self.assertIn('2026-01-02', due_dates)
        self.assertIn('2026-01-05', due_dates)
        self.assertIn('2026-01-06', due_dates)
        self.assertNotIn('2026-01-03', due_dates)
        self.assertNotIn('2026-01-04', due_dates)

    def test_us_market_holidays_are_closed(self):
        # Thanksgiving 2026-11-26; Independence Day observed 2026-07-03;
        # Christmas 2026-12-25 are all full-day closures.
        self.assertFalse(prices.is_us_trading_day(date(2026, 11, 26)))
        self.assertFalse(prices.is_us_trading_day(date(2026, 7, 3)))
        self.assertFalse(prices.is_us_trading_day(date(2026, 12, 25)))
        self.assertTrue(prices.is_us_trading_day(date(2026, 11, 27)))

    def test_us_plan_rolls_over_holiday(self):
        self.assertEqual(
            dca.resolve_trade_date(self.conn, {'market': 'US'}, date(2026, 11, 26), 'next'),
            date(2026, 11, 27))
        self.assertIsNone(
            dca.resolve_trade_date(self.conn, {'market': 'US'}, date(2026, 11, 26), 'skip'))

    def test_preview_does_not_count_today_as_backfill(self):
        today = datetime.now(SHANGHAI_TZ).date()
        asset_id = self.add_asset()
        preview = dca.preview(self.conn, {
            'asset_id': asset_id, 'amount': 100, 'frequency': 'monthly',
            'day_of_month': today.day, 'start_date': today.isoformat(), 'catch_up': 1,
        })
        self.assertEqual(preview['backfill_count'], 0)
        self.assertEqual(preview['due_dates'][0]['due_date'], today.isoformat())

    def test_no_catchup_jumps_to_current_period(self):
        asset_id = self.add_asset()
        for day, price in [('2026-01-05', 10), ('2026-01-06', 11), ('2026-01-07', 12),
                           ('2026-01-08', 13), ('2026-01-09', 14)]:
            self.add_price(asset_id, day, price)
        plan_id = self.add_plan(asset_id, frequency='daily', start_date='2026-01-05',
                                end_date='2026-01-09', catch_up=0)
        result = dca.generate_orders(self.conn, today='2026-01-07', plan_id=plan_id)
        self.assertEqual(result['created'], 1)
        due_dates = [r['due_date'] for r in self.conn.execute(
            'SELECT due_date FROM wm_dca_order ORDER BY due_date').fetchall()]
        self.assertEqual(due_dates, ['2026-01-07'])
        plan = self.conn.execute('SELECT * FROM wm_dca_plan WHERE id = ?', (plan_id,)).fetchone()
        self.assertEqual(plan['status'], 'active')

    def test_past_end_plan_backfills_and_finishes(self):
        asset_id = self.add_asset()
        for day, price in [('2026-01-05', 10), ('2026-01-06', 11), ('2026-01-07', 12),
                           ('2026-01-08', 13), ('2026-01-09', 14)]:
            self.add_price(asset_id, day, price)
        plan_id = self.add_plan(asset_id, frequency='daily', start_date='2026-01-05',
                                end_date='2026-01-09')
        result = dca.generate_orders(self.conn, today='2026-01-10', plan_id=plan_id)
        self.assertEqual(result['created'], 5)
        plan = self.conn.execute('SELECT * FROM wm_dca_plan WHERE id = ?', (plan_id,)).fetchone()
        self.assertEqual(plan['status'], 'finished')
        settled = dca.settle_orders(self.conn, today='2026-01-10')
        self.assertEqual(settled['filled'], 5)

    def test_backfill_is_capped(self):
        asset_id = self.add_asset(market='CRYPTO', asset_class='crypto')
        self.add_price(asset_id, '2026-01-02', 10)
        plan_id = self.add_plan(asset_id, frequency='daily', start_date='2020-01-01')
        result = dca.generate_orders(self.conn, today='2026-01-05', plan_id=plan_id)
        self.assertEqual(result['created'], dca.MAX_BACKFILL_PER_RUN)


class FxTests(WealthTestBase):
    def test_missing_fx_raises_instead_of_defaulting_to_one(self):
        original_rate = prices.fx_rate
        original_ensure = prices.ensure_fx
        original_latest = prices.fx_latest
        prices.fx_rate = lambda conn, currency, on_date: None
        prices.ensure_fx = lambda *args, **kwargs: None

        def offline(currencies):
            raise RuntimeError('offline')

        prices.fx_latest = offline
        try:
            with self.assertRaises(prices.PriceError):
                prices.get_fx_for_date(self.conn, 'USD', '2026-01-02')
        finally:
            prices.fx_rate = original_rate
            prices.ensure_fx = original_ensure
            prices.fx_latest = original_latest

    def test_latest_fx_is_stored_for_valuation(self):
        original_rate = prices.fx_rate
        original_ensure = prices.ensure_fx
        original_latest = prices.fx_latest
        prices.fx_rate = lambda conn, currency, on_date: None
        prices.ensure_fx = lambda *args, **kwargs: None
        prices.fx_latest = lambda currencies: {'USD': 7.1}
        try:
            rate = prices.get_fx_for_date(self.conn, 'USD', today_str())
            self.assertAlmostEqual(rate, 7.1)
            stored = self.conn.execute(
                "SELECT cny_rate FROM wm_fx_rate WHERE currency = 'USD' "
                'ORDER BY rate_date DESC LIMIT 1'
            ).fetchone()
            self.assertAlmostEqual(stored['cny_rate'], 7.1)
        finally:
            prices.fx_rate = original_rate
            prices.ensure_fx = original_ensure
            prices.fx_latest = original_latest

    def test_dca_usd_conversion_uses_fx(self):
        original = prices.get_fx_for_date
        prices.get_fx_for_date = lambda conn, currency, on_date: 7.1
        try:
            amount, fx = dca._amount_in_asset_currency(
                self.conn, 100, 'USD', 'USD', '2026-01-02')
            self.assertAlmostEqual(amount, 100.0)
            self.assertAlmostEqual(fx, 7.1)
            amount, fx = dca._amount_in_asset_currency(
                self.conn, 710, 'CNY', 'USD', '2026-01-02')
            self.assertAlmostEqual(amount, 100.0)
            self.assertAlmostEqual(fx, 7.1)
        finally:
            prices.get_fx_for_date = original


class ImportAndParsingTests(WealthTestBase):
    def test_import_fx_preflight_caches_missing_and_recent_fallback(self):
        from wealth.routes import _resolve_import_fx
        today = datetime.now(SHANGHAI_TZ).date()
        recent = (today - timedelta(days=3)).isoformat()
        original_ensure = prices.ensure_fx_range
        original_rate = prices.fx_rate
        prices.ensure_fx_range = lambda *args, **kwargs: None
        prices.fx_rate = lambda conn, currency, day: None
        try:
            _, failed, _ = _resolve_import_fx(
                self.conn, [{'currency': 'USD', 'trade_date': recent}])
            self.assertIn(('USD', recent), failed)
        finally:
            prices.fx_rate = original_rate
            prices.ensure_fx_range = original_ensure

        prices.ensure_fx_range = lambda *args, **kwargs: None
        prices.fx_rate = lambda conn, currency, day: 7.1 if str(day) == today.isoformat() else None
        try:
            fx, failed, warnings = _resolve_import_fx(
                self.conn, [{'currency': 'USD', 'trade_date': recent}])
            self.assertEqual(fx[('USD', recent)], 7.1)
            self.assertFalse(failed)
            self.assertTrue(warnings)
        finally:
            prices.fx_rate = original_rate
            prices.ensure_fx_range = original_ensure

    def test_fund_nav_js_parsing(self):
        stamp = int(datetime(2026, 1, 2, tzinfo=SHANGHAI_TZ).timestamp() * 1000)
        text = (
            'var fS_name = "测试基金";\n'
            'var Data_netWorthTrend = [{"x": %d, "y": 1.234, "equityReturn": 0}];\n'
        ) % stamp
        name, rows = prices.parse_fund_nav_js(text)
        self.assertEqual(name, '测试基金')
        self.assertEqual(rows, [('2026-01-02', 1.234)])

    def test_binance_bar_date_uses_open_time(self):
        original = prices._binance_get
        calls = []

        def fake(path):
            if calls:
                return []
            calls.append(path)
            return [[1735689600000, '0', '0', '0', '100', '0', 1735775999999]]

        prices._binance_get = fake
        try:
            rows = prices.binance_klines('BTCUSDT', '2025-01-01', '2025-01-01')
        finally:
            prices._binance_get = original
        self.assertEqual(rows, [('2025-01-01', 100.0)])

    def test_fund_currency_override(self):
        from wealth.routes import _create_asset_record
        # A USD share class can keep the Eastmoney NAV source: its NAV is USD.
        fund, created = _create_asset_record(self.conn, {
            'symbol': '000044', 'market': 'FUND_CN', 'asset_class': 'fund',
            'name': '嘉实美国成长股票美元现汇', 'quote_source': 'fund_em',
        })
        self.assertTrue(created)
        self.assertEqual(fund['currency'], 'USD')
        self.assertEqual(fund['quote_source'], 'fund_em')
        # A CNY share class cannot use a USD asset currency with the CNY NAV.
        fund, created = _create_asset_record(self.conn, {
            'symbol': '000043', 'market': 'FUND_CN', 'asset_class': 'fund',
            'name': '嘉实美国成长股票人民币', 'currency': 'USD', 'quote_source': 'fund_em',
        })
        self.assertTrue(created)
        self.assertEqual(fund['currency'], 'USD')
        self.assertEqual(fund['quote_source'], 'manual')

    def test_fund_currency_inference(self):
        cases = {
            '嘉实美国成长股票美元现汇': 'USD',
            '嘉实美国成长股票人民币': 'CNY',
            '广发纳斯达克100ETF联接美元(QDII)A': 'USD',
            '南方亚洲美元收益债券(QDII)A(人民币)': 'CNY',
            '南方亚洲美元收益债券(QDII)A(美元现汇)': 'USD',
            '银华美元债精选债券(QDII)A': 'CNY',
            '工银全球股票(QDII)港币': 'HKD',
        }
        for name, expected in cases.items():
            with self.subTest(name=name):
                self.assertEqual(prices.fund_currency(name), expected)

    def test_fund_nav_currency_must_match_asset(self):
        asset_id = self.add_asset(
            symbol='000044', market='FUND_CN', asset_class='fund',
            name='嘉实美国成长股票美元现汇', currency='USD',
            quote_source='fund_em', lot_size=1, allow_fraction=1)
        asset = dict(self.conn.execute(
            'SELECT * FROM wm_asset WHERE id = ?', (asset_id,)).fetchone())
        original = prices.fund_nav
        prices.fund_nav = lambda code: (
            '嘉实美国成长股票美元现汇', [('2026-11-17', 5.29)])
        try:
            self.assertEqual(prices.sync_asset(self.conn, asset), 1)
            self.conn.execute(
                'UPDATE wm_asset SET currency = ? WHERE id = ?', ('CNY', asset_id))
            self.conn.commit()
            asset['currency'] = 'CNY'
            with self.assertRaises(prices.PriceError):
                prices.sync_asset(self.conn, asset)
        finally:
            prices.fund_nav = original

    def test_ghostfolio_top_level_asset_profiles(self):
        from wealth.routes import _ghostfolio_rows
        payload = {
            'assetProfiles': [{
                'symbol': 'AAPL', 'name': 'Apple Inc.', 'assetClass': 'EQUITY',
                'currency': 'USD', 'dataSource': 'YAHOO',
            }],
            'activities': [
                {'type': 'BUY', 'date': '2026-01-02', 'symbol': 'AAPL',
                 'currency': 'USD', 'quantity': 1, 'unitPrice': 100,
                 'dataSource': 'YAHOO'},
                {'type': 'BUY', 'date': '2026-01-02', 'symbol': 'GF_000044',
                 'currency': 'USD', 'quantity': 1, 'unitPrice': 5,
                 'dataSource': 'MANUAL'},
            ],
        }
        _, rows = _ghostfolio_rows(payload)
        self.assertEqual(rows[0]['name'], 'Apple Inc.')
        self.assertEqual(rows[0]['asset_class'], 'stock')
        self.assertEqual(rows[0]['market'], 'US')
        self.assertEqual(rows[1]['asset_class'], 'fund')
        self.assertEqual(rows[1]['market'], 'MANUAL')

    def test_ghostfolio_prepare_metadata(self):
        from wealth.routes import _prepare_import
        asset_id = self.add_asset(symbol='AAPL', market='US', asset_class='other',
                                  currency='USD', name='AAPL')
        payload = {
            'accounts': [],
            'assetProfiles': [{
                'symbol': 'AAPL', 'name': 'Apple Inc.', 'assetClass': 'EQUITY',
                'currency': 'USD', 'dataSource': 'YAHOO',
            }],
            'activities': [{
                'type': 'BUY', 'date': '2026-01-02', 'symbol': 'AAPL',
                'currency': 'USD', 'quantity': 1, 'unitPrice': 100,
                'dataSource': 'YAHOO',
            }],
        }
        prepared = _prepare_import(self.conn, payload, dry_run=True)
        meta = prepared['metadata'][('AAPL', 'US')]
        self.assertEqual(meta['name'], 'Apple Inc.')
        self.assertEqual(meta['asset_class'], 'stock')
        self.assertEqual(len(prepared['rows']), 1)
        self.assertEqual(asset_id, self.conn.execute(
            "SELECT id FROM wm_asset WHERE symbol='AAPL'").fetchone()['id'])

    def test_ghostfolio_market_guess(self):
        from wealth.routes import _guess_market, _create_asset_record
        self.assertEqual(_guess_market('AAPL', 'USD', 'YAHOO', 'EQUITY'), 'US')
        self.assertEqual(_guess_market('600519', 'CNY', 'YAHOO', 'EQUITY'), 'CN')
        self.assertEqual(_guess_market('000001', 'CNY', 'YAHOO', 'MUTUALFUND'), 'FUND_CN')
        self.assertEqual(_guess_market('600519.SS', 'CNY', 'YAHOO', 'EQUITY'), 'CN')
        self.assertEqual(_guess_market('000001.SZ', 'CNY', 'YAHOO', 'MUTUALFUND'), 'FUND_CN')
        self.assertEqual(_guess_market('0700.HK', 'HKD', 'YAHOO', 'EQUITY'), 'HK')
        self.assertEqual(_guess_market('bitcoin', 'USD', 'COINGECKO', 'CRYPTO'), 'CRYPTO')
        self.assertEqual(_guess_market('CASH', 'CNY', 'MANUAL', 'CASH'), 'MANUAL')
        cn_usd, created = _create_asset_record(self.conn, {
            'symbol': '600519', 'market': 'CN', 'currency': 'USD',
            'name': '美元计价测试', 'quote_source': 'eastmoney',
        })
        self.assertTrue(created)
        self.assertEqual(cn_usd['currency'], 'USD')
        self.assertEqual(cn_usd['quote_source'], 'manual')

    def test_ghostfolio_symbol_normalization(self):
        from wealth.routes import _ghostfolio_rows, _create_asset_record
        payload = {'activities': [
            {'type': 'BUY', 'date': '2026-01-02', 'symbol': '0700.HK', 'currency': 'HKD',
             'quantity': 100, 'unitPrice': 300, 'dataSource': 'YAHOO',
             'assetProfile': {'symbol': '0700.HK', 'name': '腾讯', 'assetClass': 'EQUITY', 'currency': 'HKD'}},
            {'type': 'BUY', 'date': '2026-01-02', 'symbol': '600519.SS', 'currency': 'CNY',
             'quantity': 100, 'unitPrice': 1500, 'dataSource': 'YAHOO',
             'assetProfile': {'symbol': '600519.SS', 'name': '贵州茅台', 'assetClass': 'EQUITY', 'currency': 'CNY'}},
        ]}
        _, rows = _ghostfolio_rows(payload)
        self.assertEqual((rows[0]['market'], rows[0]['symbol']), ('HK', '0700'))
        self.assertEqual((rows[1]['market'], rows[1]['symbol']), ('CN', '600519'))

        cn, _ = _create_asset_record(self.conn, {
            'symbol': '600519.SS', 'market': 'CN', 'currency': 'CNY', 'name': '贵州茅台',
        })
        self.assertEqual(cn['symbol'], '600519')
        hk, _ = _create_asset_record(self.conn, {
            'symbol': '0700.HK', 'market': 'HK', 'currency': 'HKD', 'name': '腾讯',
        })
        self.assertEqual(hk['symbol'], '0700')


class SchemaMigrationTests(unittest.TestCase):
    def test_old_balance_table_allows_multiple_currencies(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'old-schema.db')
            conn = connect(path)
            conn.executescript("""
                CREATE TABLE wm_account (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    name       TEXT NOT NULL,
                    broker     TEXT,
                    currency   TEXT NOT NULL DEFAULT 'CNY',
                    is_active  INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    UNIQUE(name)
                );
                CREATE TABLE wm_account_balance (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id   INTEGER NOT NULL REFERENCES wm_account(id),
                    balance_date TEXT NOT NULL,
                    amount       REAL NOT NULL,
                    currency     TEXT NOT NULL DEFAULT 'CNY',
                    source       TEXT,
                    created_at   TEXT NOT NULL,
                    UNIQUE(account_id, balance_date)
                );
            """)
            conn.execute(
                "INSERT INTO wm_account (name, currency, created_at) VALUES ('旧账户', 'CNY', '2026-01-01')")
            conn.execute(
                """INSERT INTO wm_account_balance
                   (account_id, balance_date, amount, currency, source, created_at)
                   VALUES (1, '2026-01-01', 100, 'CNY', 'old', '2026-01-01')""")
            conn.commit()
            conn.close()

            init_wealth_db(path)
            conn = connect(path)
            try:
                conn.execute(
                    """INSERT INTO wm_account_balance
                       (account_id, balance_date, amount, currency, source, created_at)
                       VALUES (1, '2026-01-01', 50, 'USD', 'test', '2026-01-01')""")
                conn.commit()
                rows = conn.execute(
                    'SELECT currency, amount FROM wm_account_balance ORDER BY currency'
                ).fetchall()
                self.assertEqual([(r['currency'], r['amount']) for r in rows],
                                 [('CNY', 100.0), ('USD', 50.0)])
                columns = {r['name'] for r in conn.execute(
                    'PRAGMA table_info(wm_transaction)').fetchall()}
                self.assertIn('cash_adjusted', columns)
            finally:
                conn.close()


class RoutesApiTests(unittest.TestCase):
    def setUp(self):
        from flask import Flask
        from wealth.routes import register
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, 'wealth-api-test.db')
        self._old_password = os.environ.pop('WM_PASSWORD', None)
        self.app = Flask(__name__)
        self.app.config['TESTING'] = True
        self.app.config['SECRET_KEY'] = 'test'
        self.app.config['WM_DB_PATH'] = self.db_path
        register(self.app)
        self.client = self.app.test_client()

    def tearDown(self):
        if self._old_password is not None:
            os.environ['WM_PASSWORD'] = self._old_password
        self._tmp.cleanup()

    def _create_asset(self):
        response = self.client.post('/api/wm/assets', json={
            'symbol': 'API-CASH', 'market': 'MANUAL', 'asset_class': 'stock',
            'name': 'API 现金测试', 'currency': 'CNY', 'quote_source': 'manual',
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        return response.get_json()['id']

    def test_account_multi_currency_api(self):
        today = today_str()
        response = self.client.post('/api/wm/accounts', json={
            'name': 'API 多币种', 'currency': 'CNY',
            'cash_balances': [
                {'currency': 'CNY', 'amount': 1000, 'balance_date': today},
                {'currency': 'USD', 'amount': 100, 'balance_date': today},
            ],
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        account_id = response.get_json()['id']

        accounts = self.client.get('/api/wm/accounts').get_json()
        account = next(a for a in accounts if a['id'] == account_id)
        self.assertEqual({b['currency']: b['amount'] for b in account['cash_balances']},
                         {'CNY': 1000.0, 'USD': 100.0})

        response = self.client.put(f'/api/wm/accounts/{account_id}', json={
            'name': 'API 多币种', 'currency': 'CNY', 'is_active': True,
            'cash_balances': [
                {'currency': 'CNY', 'amount': 500, 'balance_date': today},
                {'currency': 'USD', 'amount': 50, 'balance_date': today},
                {'currency': 'HKD', 'amount': 20, 'balance_date': today},
            ],
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        accounts = self.client.get('/api/wm/accounts').get_json()
        account = next(a for a in accounts if a['id'] == account_id)
        self.assertEqual({b['currency']: b['amount'] for b in account['cash_balances']},
                         {'CNY': 500.0, 'USD': 50.0, 'HKD': 20.0})

    def test_transaction_api_adjusts_and_restores_cash(self):
        asset_id = self._create_asset()
        today = today_str()
        response = self.client.post('/api/wm/accounts', json={
            'name': 'API 交易现金', 'currency': 'CNY',
            'cash_balances': [{'currency': 'CNY', 'amount': 1000, 'balance_date': today}],
        })
        account_id = response.get_json()['id']

        response = self.client.post('/api/wm/transactions', json={
            'asset_id': asset_id, 'account_id': account_id, 'type': 'buy',
            'trade_date': today, 'units': 10, 'price': 10, 'amount': 100,
            'fee': 5, 'currency': 'CNY',
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        tx_id = response.get_json()['id']
        account = next(a for a in self.client.get('/api/wm/accounts').get_json()
                       if a['id'] == account_id)
        self.assertAlmostEqual(account['cash_balances'][0]['amount'], 895.0)

        response = self.client.put(f'/api/wm/transactions/{tx_id}', json={
            'asset_id': asset_id, 'account_id': account_id, 'type': 'buy',
            'trade_date': today, 'units': 20, 'price': 10, 'amount': 200,
            'fee': 5, 'currency': 'CNY',
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        tx_id = response.get_json()['id']
        account = next(a for a in self.client.get('/api/wm/accounts').get_json()
                       if a['id'] == account_id)
        self.assertAlmostEqual(account['cash_balances'][0]['amount'], 795.0)

        response = self.client.delete(f'/api/wm/transactions/{tx_id}')
        self.assertEqual(response.status_code, 200, response.get_json())
        account = next(a for a in self.client.get('/api/wm/accounts').get_json()
                       if a['id'] == account_id)
        self.assertAlmostEqual(account['cash_balances'][0]['amount'], 1000.0)

    def _position(self, asset_id, account_id):
        positions = self.client.get('/api/wm/positions').get_json()['positions']
        return next(p for p in positions
                    if p['asset_id'] == asset_id and p['account_id'] == account_id)

    def _adjust_transactions(self, asset_id):
        rows = self.client.get(
            f'/api/wm/transactions?asset_id={asset_id}').get_json()['transactions']
        return [row for row in rows if row['type'] == 'adjust']

    def _create_account(self, name='持仓修正账户', cash=1000):
        response = self.client.post('/api/wm/accounts', json={
            'name': name, 'currency': 'CNY',
            'cash_balances': [{'currency': 'CNY', 'amount': cash,
                               'balance_date': today_str()}],
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        return response.get_json()['id']

    def test_position_adjust_api_updates_without_cash(self):
        asset_id = self._create_asset()
        account_id = self._create_account()
        today = today_str()
        response = self.client.post('/api/wm/transactions', json={
            'asset_id': asset_id, 'account_id': account_id, 'type': 'buy',
            'trade_date': today, 'units': 10, 'price': 10, 'amount': 100,
            'fee': 0, 'currency': 'CNY',
        })
        self.assertEqual(response.status_code, 201, response.get_json())

        response = self.client.post('/api/wm/positions/adjust', json={
            'asset_id': asset_id, 'account_id': account_id,
            'units': 12, 'cost_native': 120,
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertTrue(response.get_json()['changed'])
        position = self._position(asset_id, account_id)
        self.assertAlmostEqual(position['units'], 12.0)
        self.assertAlmostEqual(position['cost_native'], 120.0)
        self.assertAlmostEqual(position['avg_cost_native'], 10.0)
        account = next(a for a in self.client.get('/api/wm/accounts').get_json()
                       if a['id'] == account_id)
        self.assertAlmostEqual(account['cash_balances'][0]['amount'], 900.0)
        self.assertEqual(len(self._adjust_transactions(asset_id)), 1)

        history = self.client.get(
            f'/api/wm/history?start={today}&end={today}').get_json()['points']
        self.assertAlmostEqual(history[-1]['value'], 120.0)

        # A second correction updates the same adjustment row.
        response = self.client.post('/api/wm/positions/adjust', json={
            'asset_id': asset_id, 'account_id': account_id,
            'units': 15, 'cost_native': 150,
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(len(self._adjust_transactions(asset_id)), 1)
        position = self._position(asset_id, account_id)
        self.assertAlmostEqual(position['units'], 15.0)
        self.assertAlmostEqual(position['cost_native'], 150.0)

        # Restoring the original totals removes the correction entirely.
        response = self.client.post('/api/wm/positions/adjust', json={
            'asset_id': asset_id, 'account_id': account_id,
            'units': 10, 'cost_native': 100,
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertTrue(response.get_json()['removed'])
        self.assertEqual(len(self._adjust_transactions(asset_id)), 0)
        position = self._position(asset_id, account_id)
        self.assertAlmostEqual(position['units'], 10.0)
        self.assertAlmostEqual(position['cost_native'], 100.0)

    def test_position_adjust_api_works_for_dca_source(self):
        asset_id = self._create_asset()
        account_id = self._create_account('定投修正账户')
        today = today_str()
        response = self.client.post('/api/wm/transactions', json={
            'asset_id': asset_id, 'account_id': account_id, 'type': 'buy',
            'trade_date': today, 'units': 10, 'price': 10, 'amount': 100,
            'fee': 0, 'currency': 'CNY',
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        tx_id = response.get_json()['id']
        conn = connect(self.db_path)
        conn.execute("UPDATE wm_transaction SET source = 'dca' WHERE id = ?", (tx_id,))
        conn.commit()
        conn.close()

        response = self.client.put(f'/api/wm/transactions/{tx_id}', json={})
        self.assertEqual(response.status_code, 409, response.get_json())

        response = self.client.post('/api/wm/positions/adjust', json={
            'asset_id': asset_id, 'account_id': account_id,
            'units': 20, 'cost_native': 200,
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        position = self._position(asset_id, account_id)
        self.assertAlmostEqual(position['units'], 20.0)
        self.assertAlmostEqual(position['cost_native'], 200.0)

        # The correction participates in sell validation.
        response = self.client.post('/api/wm/transactions', json={
            'asset_id': asset_id, 'account_id': account_id, 'type': 'sell',
            'trade_date': today, 'units': 15, 'price': 10, 'amount': 150,
            'fee': 0, 'currency': 'CNY',
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        position = self._position(asset_id, account_id)
        self.assertAlmostEqual(position['units'], 5.0)
        self.assertAlmostEqual(position['cost_native'], 50.0)

    def test_position_adjust_api_bank_principal(self):
        response = self.client.post('/api/wm/assets', json={
            'symbol': 'BANK-ADJ', 'market': 'BANK', 'asset_class': 'bank_wm',
            'name': '银行修正', 'currency': 'CNY', 'quote_source': 'accrual',
            'annual_rate': 0.02,
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        asset_id = response.get_json()['id']
        account_id = self._create_account('银行修正账户', cash=10000)
        today = today_str()
        response = self.client.post('/api/wm/transactions', json={
            'asset_id': asset_id, 'account_id': account_id, 'type': 'buy',
            'trade_date': today, 'units': 1000, 'price': 1, 'amount': 1000,
            'fee': 10, 'currency': 'CNY',
        })
        self.assertEqual(response.status_code, 201, response.get_json())

        response = self.client.post('/api/wm/positions/adjust', json={
            'asset_id': asset_id, 'account_id': account_id, 'units': 1200,
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        position = self._position(asset_id, account_id)
        self.assertAlmostEqual(position['principal'], 1200.0)
        self.assertAlmostEqual(position['cost_native'], 1200.0)
        account = next(a for a in self.client.get('/api/wm/accounts').get_json()
                       if a['id'] == account_id)
        self.assertAlmostEqual(account['cash_balances'][0]['amount'], 8990.0)

    def test_dca_plan_amount_change_can_sync_pending_order(self):
        asset_id = self._create_asset()
        today = today_str()
        response = self.client.post('/api/wm/dca/plans', json={
            'asset_id': asset_id, 'amount': 1000, 'amount_currency': 'CNY',
            'frequency': 'monthly', 'day_of_month': int(today[8:10]),
            'start_date': today, 'catch_up': True,
        })
        self.assertEqual(response.status_code, 201, response.get_json())
        plan_id = response.get_json()['id']

        conn = connect(self.db_path)
        try:
            order = conn.execute(
                'SELECT amount, status FROM wm_dca_order WHERE plan_id = ?',
                (plan_id,),
            ).fetchone()
            self.assertEqual(order['status'], 'awaiting_price')
            self.assertAlmostEqual(order['amount'], 1000.0)
        finally:
            conn.close()

        response = self.client.put(f'/api/wm/dca/plans/{plan_id}', json={'amount': 500})
        self.assertEqual(response.status_code, 409, response.get_json())
        self.assertTrue(response.get_json()['pending_action_required'])
        self.assertEqual(response.get_json()['pending_count'], 1)

        conn = connect(self.db_path)
        try:
            self.assertAlmostEqual(conn.execute(
                'SELECT amount FROM wm_dca_plan WHERE id = ?', (plan_id,)
            ).fetchone()['amount'], 1000.0)
            self.assertAlmostEqual(conn.execute(
                'SELECT amount FROM wm_dca_order WHERE plan_id = ?', (plan_id,)
            ).fetchone()['amount'], 1000.0)
        finally:
            conn.close()

        response = self.client.put(f'/api/wm/dca/plans/{plan_id}', json={
            'amount': 500, 'pending_action': 'future',
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()['pending_orders_updated'], 0)

        conn = connect(self.db_path)
        try:
            self.assertAlmostEqual(conn.execute(
                'SELECT amount FROM wm_dca_plan WHERE id = ?', (plan_id,)
            ).fetchone()['amount'], 500.0)
            self.assertAlmostEqual(conn.execute(
                'SELECT amount FROM wm_dca_order WHERE plan_id = ?', (plan_id,)
            ).fetchone()['amount'], 1000.0)
        finally:
            conn.close()

        response = self.client.put(f'/api/wm/dca/plans/{plan_id}', json={'amount': 700})
        self.assertEqual(response.status_code, 409, response.get_json())
        response = self.client.put(f'/api/wm/dca/plans/{plan_id}', json={
            'amount': 700, 'pending_action': 'update',
        })
        self.assertEqual(response.status_code, 200, response.get_json())
        self.assertEqual(response.get_json()['pending_orders_updated'], 1)

        conn = connect(self.db_path)
        try:
            self.assertAlmostEqual(conn.execute(
                'SELECT amount FROM wm_dca_plan WHERE id = ?', (plan_id,)
            ).fetchone()['amount'], 700.0)
            order = conn.execute(
                'SELECT id, amount FROM wm_dca_order WHERE plan_id = ?', (plan_id,)
            ).fetchone()
            self.assertAlmostEqual(order['amount'], 700.0)
            conn.execute(
                "UPDATE wm_dca_order SET status = 'failed', amount = 1000 WHERE id = ?",
                (order['id'],),
            )
            conn.commit()
        finally:
            conn.close()

        response = self.client.post(
            f"/api/wm/dca/orders/{order['id']}/retry",
            json={'use_plan_amount': True},
        )
        self.assertEqual(response.status_code, 200, response.get_json())
        conn = connect(self.db_path)
        try:
            retried = conn.execute(
                'SELECT amount, status FROM wm_dca_order WHERE id = ?', (order['id'],)
            ).fetchone()
            self.assertEqual(retried['status'], 'awaiting_price')
            self.assertAlmostEqual(retried['amount'], 700.0)
        finally:
            conn.close()


class YahooSourceTests(unittest.TestCase):
    def test_symbol_mapping(self):
        self.assertEqual(prices.yahoo_symbol('600519', 'CN'), '600519.SS')
        self.assertEqual(prices.yahoo_symbol('513000', 'CN'), '513000.SS')
        self.assertEqual(prices.yahoo_symbol('000001', 'CN'), '000001.SZ')
        self.assertEqual(prices.yahoo_symbol('159915', 'CN'), '159915.SZ')
        self.assertEqual(prices.yahoo_symbol('QQQM', 'US'), 'QQQM')

    def test_quote_parses_meta(self):
        node = {'meta': {
            'symbol': 'QQQM', 'regularMarketPrice': 250.5,
            'chartPreviousClose': 248.0, 'longName': 'Invesco NASDAQ 100 ETF',
        }}
        with mock.patch.object(prices, '_yahoo_chart', return_value=node):
            quote = prices.yahoo_quote('QQQM')
        self.assertEqual(quote['price'], 250.5)
        self.assertEqual(quote['prev_close'], 248.0)
        self.assertEqual(quote['name'], 'Invesco NASDAQ 100 ETF')

    def test_kline_maps_timestamps_to_exchange_dates(self):
        tz = ZoneInfo('America/New_York')
        stamps = [
            int(datetime(2026, 11, 20, 9, 30, tzinfo=tz).timestamp()),
            int(datetime(2026, 11, 23, 9, 30, tzinfo=tz).timestamp()),
        ]
        node = {
            'meta': {'exchangeTimezoneName': 'America/New_York'},
            'timestamp': stamps,
            'indicators': {'quote': [{'close': [100.0, 101.5]}]},
        }
        with mock.patch.object(prices, '_yahoo_chart', return_value=node):
            rows = prices.yahoo_kline('QQQM', '2026-11-20', '2026-11-24')
        self.assertEqual(rows, [('2026-11-20', 100.0), ('2026-11-23', 101.5)])


if __name__ == '__main__':
    unittest.main()
