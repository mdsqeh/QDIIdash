"""Tests for the QDII dashboard's persisted watchlist/ratings and holdings."""
import gc
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing

_TMP = tempfile.mkdtemp()
os.environ['DATA_DIR'] = _TMP
os.environ['WM_DB_PATH'] = os.path.join(_TMP, 'app-import.db')

from wealth.schema import init_wealth_db  # noqa: E402

import app as app_module  # noqa: E402


class DashboardTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, 'dashboard-test.db')
        app_module.DB_PATH = self.db_path
        app_module.app.config['WM_DB_PATH'] = self.db_path
        init_wealth_db(self.db_path)
        app_module.init_db()
        self.client = app_module.app.test_client()

    def tearDown(self):
        gc.collect()
        self._tmp.cleanup()

    def _conn(self, path=None):
        conn = sqlite3.connect(path or self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def add_fund(self, code, name=None):
        with closing(self._conn()) as conn, conn:
            conn.execute(
                'INSERT INTO funds (code, name, upd_date) VALUES (?, ?, ?)',
                (code, name or f'基金{code}', '2026-01-01'),
            )

    def add_asset(self, symbol, market, asset_class):
        with closing(self._conn()) as conn, conn:
            cur = conn.execute(
                """INSERT INTO wm_asset
                   (symbol, market, asset_class, name, currency, quote_source,
                    lot_size, allow_fraction, valuation_mode, is_active, created_at)
                   VALUES (?, ?, ?, ?, 'CNY', 'manual', 1, 1, 'price', 1, ?)""",
                (symbol, market, asset_class, symbol, '2026-01-01T00:00:00+08:00'),
            )
            return cur.lastrowid

    def add_tx(self, asset_id, tx_type='buy', units=100):
        with closing(self._conn()) as conn, conn:
            conn.execute(
                """INSERT INTO wm_transaction
                   (asset_id, type, trade_date, units, price, amount, fee,
                    currency, fx_rate, source, created_at)
                   VALUES (?, ?, '2026-01-01', ?, 1, ?, 0, 'CNY', 1,
                           'manual', '2026-01-01T00:00:00+08:00')""",
                (asset_id, tx_type, units, abs(units)),
            )

    def test_user_data_round_trip_and_merge(self):
        response = self.client.get('/api/fund-user-data')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {'watchlist': [], 'ratings': {}})

        response = self.client.post('/api/fund-user-data', json={
            'mode': 'merge',
            'watchlist': ['513100', '513100', '000001'],
            'ratings': {'513100': 5, '000001': 3, 'bad': 'x'},
        })
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data['watchlist'], ['513100', '000001'])
        self.assertEqual(data['ratings'], {'513100': 5, '000001': 3})

        response = self.client.post('/api/fund-user-data', json={
            'mode': 'merge',
            'watchlist': ['159509'],
            'ratings': {'513100': 2},
        })
        data = response.get_json()
        self.assertEqual(data['watchlist'], ['513100', '000001', '159509'])
        self.assertEqual(data['ratings']['513100'], 5)

        response = self.client.post('/api/fund-user-data', json={
            'mode': 'replace',
            'watchlist': ['159509'],
            'ratings': {'159509': 4},
        })
        data = response.get_json()
        self.assertEqual(data['watchlist'], ['159509'])
        self.assertEqual(data['ratings'], {'159509': 4})

    def test_watchlist_and_rating_item_endpoints(self):
        response = self.client.post('/api/fund-watchlist', json={
            'code': '513100', 'active': True,
        })
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json()['watchlist'], ['513100'])

        response = self.client.post('/api/fund-rating', json={
            'code': '513100', 'rating': 4,
        })
        self.assertEqual(response.get_json()['ratings'], {'513100': 4})

        response = self.client.post('/api/fund-watchlist', json={
            'code': '513100', 'active': False,
        })
        self.assertEqual(response.get_json()['watchlist'], [])

        response = self.client.post('/api/fund-rating', json={
            'code': '513100', 'rating': 0,
        })
        self.assertEqual(response.get_json()['ratings'], {})

        response = self.client.post('/api/fund-rating', json={
            'code': '513100', 'rating': 9,
        })
        self.assertEqual(response.status_code, 400)

    def test_holdings_only_include_qdii_funds_and_positive_positions(self):
        for code in ('513100', '000001', '000002', '161125', '600000'):
            self.add_fund(code)

        self.add_tx(self.add_asset('513100', 'CN', 'etf'))
        self.add_tx(self.add_asset('161125', 'CN', 'fund'))
        self.add_tx(self.add_asset('000001', 'FUND_CN', 'fund'))
        self.add_tx(self.add_asset('600000', 'CN', 'stock'))

        sold_id = self.add_asset('000002', 'FUND_CN', 'fund')
        self.add_tx(sold_id)
        self.add_tx(sold_id, tx_type='sell')

        self.add_tx(self.add_asset('159999', 'CN', 'etf'))

        response = self.client.get('/api/fund-holdings')
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.get_json(), {
            'codes': ['000001', '161125', '513100'],
        })

    def test_holdings_uses_configured_wealth_db(self):
        self.add_fund('513100')
        wealth_path = os.path.join(self._tmp.name, 'wealth-only.db')
        init_wealth_db(wealth_path)
        app_module.app.config['WM_DB_PATH'] = wealth_path
        with closing(self._conn(wealth_path)) as conn, conn:
            cur = conn.execute(
                """INSERT INTO wm_asset
                   (symbol, market, asset_class, name, currency, quote_source,
                    lot_size, allow_fraction, valuation_mode, is_active, created_at)
                   VALUES ('513100', 'CN', 'etf', '纳指ETF', 'CNY', 'manual',
                           1, 1, 'price', 1, '2026-01-01T00:00:00+08:00')"""
            )
            conn.execute(
                """INSERT INTO wm_transaction
                   (asset_id, type, trade_date, units, price, amount, fee,
                    currency, fx_rate, source, created_at)
                   VALUES (?, 'buy', '2026-01-01', 100, 1, 100, 0, 'CNY', 1,
                           'manual', '2026-01-01T00:00:00+08:00')""",
                (cur.lastrowid,),
            )

        response = self.client.get('/api/fund-holdings')
        self.assertEqual(response.get_json(), {'codes': ['513100']})


    def add_dlv_fund(self, code, name=None, ret_1y=None):
        with closing(self._conn()) as conn, conn:
            conn.execute(
                'INSERT INTO dlv_funds (code, name, ret_1y, upd_date) VALUES (?, ?, ?, ?)',
                (code, name or f'红利{code}', ret_1y, '2026-01-01'),
            )

    def test_dlv_table_columns_match_funds(self):
        with closing(self._conn()) as conn:
            def columns(table):
                return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            self.assertEqual(columns('dlv_funds'), columns('funds'))

    def test_dlv_funds_and_stats_are_separate_from_qdii(self):
        self.add_fund('513100')
        self.add_dlv_fund('512890', ret_1y=12.0)

        dlv = self.client.get('/api/dlv/funds').get_json()
        self.assertEqual([f['code'] for f in dlv['funds']], ['512890'])

        stats = self.client.get('/api/dlv/stats').get_json()
        self.assertEqual(stats['total'], 1)
        self.assertEqual(stats['pos_1y'], 1)

        qdii = self.client.get('/api/funds').get_json()
        self.assertEqual([f['code'] for f in qdii['funds']], ['513100'])


if __name__ == '__main__':
    unittest.main()
