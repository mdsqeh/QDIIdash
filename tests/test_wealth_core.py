"""Core wealth-management tests (no network)."""
import os
import tempfile
import unittest
from datetime import date, datetime, timedelta

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
               currency='CNY', fx_rate=1.0, split_ratio=None):
        cur = self.conn.execute(
            """INSERT INTO wm_transaction
               (asset_id, type, trade_date, units, price, amount, fee, currency,
                fx_rate, split_ratio, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'manual', ?)""",
            (asset_id, type_, day, units, price, amount, fee, currency,
             fx_rate, split_ratio, now_iso()),
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
        with self.assertRaises(ValueError):
            _create_asset_record(self.conn, {
                'symbol': '600519', 'market': 'CN', 'currency': 'USD',
            })

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


if __name__ == '__main__':
    unittest.main()
