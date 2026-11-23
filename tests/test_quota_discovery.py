"""Dynamic Nasdaq-100 fund-list discovery tests (no network)."""
import gc
import os
import sqlite3
import tempfile
import unittest

from quota.discovery import (
    company_name_from_fund,
    company_short_name,
    is_nasdaq100,
    normalize_base_name,
    parse_currency,
    parse_share_class,
    sync_nasdaq100_funds,
)
from quota.schema import init_quota_db
from quota.service import connect


class QuotaDiscoveryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, 'quota-discovery.db')
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """CREATE TABLE funds (
                   code TEXT PRIMARY KEY, name TEXT NOT NULL, ftype TEXT,
                   market TEXT, upd_date TEXT NOT NULL)"""
            )
            conn.executemany(
                'INSERT INTO funds (code, name, ftype, market, upd_date) '
                'VALUES (?, ?, ?, ?, ?)',
                [
                    ('016452', '南方纳斯达克100指数发起(QDII)A', '指数型-海外股票', '场外', '2026-01-01'),
                    ('016453', '南方纳斯达克100指数发起(QDII)C', '指数型-海外股票', '场外', '2026-01-01'),
                    ('021000', '南方纳斯达克100指数发起(QDII)I', '指数型-海外股票', '场外', '2026-01-01'),
                    ('270042', '广发纳斯达克100ETF联接人民币(QDII)A', '指数型-海外股票', '场外', '2026-01-01'),
                    ('000055', '广发纳斯达克100ETF联接美元(QDII)A', '指数型-海外股票', '场外', '2026-01-01'),
                    ('006480', '广发纳斯达克100ETF联接美元(QDII)C', '指数型-海外股票', '场外', '2026-01-01'),
                    ('019441', '万家纳斯达克100指数发起式(QDII)A', '指数型-海外股票', '场外', '2026-01-01'),
                    ('159513', '纳斯达克100ETF大成', '指数型-海外股票', '场内', '2026-01-01'),
                    ('900001', '招商纳斯达克100指数(QDII)A', '指数型-海外股票', '场外', '2026-01-01'),
                    ('900002', '南方沪深300指数(QDII)A', '指数型-海外股票', '场外', '2026-01-01'),
                ],
            )
        init_quota_db(self.db_path)

    def tearDown(self):
        gc.collect()
        self._tmp.cleanup()

    def test_homepage_filter_and_share_class_parsing(self):
        self.assertTrue(is_nasdaq100('南方纳斯达克100指数发起(QDII)A', '指数型-海外股票'))
        self.assertTrue(is_nasdaq100('Nasdaq 100 Index Fund', 'QDII'))
        self.assertFalse(is_nasdaq100('南方沪深300指数(QDII)A', '指数型-海外股票'))
        self.assertEqual(parse_share_class('南方纳斯达克100指数发起(QDII)I'), 'I')
        self.assertEqual(parse_share_class('广发纳斯达克100ETF联接美元(QDII)C'), 'C')
        self.assertEqual(parse_share_class('国泰纳斯达克100指数'), 'A')
        self.assertEqual(parse_currency('广发纳斯达克100ETF联接美元(QDII)A'), 'USD')
        self.assertEqual(parse_currency('易方达纳斯达克100ETF联接(QDII-LOF)A(人民币)'), 'CNY')
        self.assertEqual(
            normalize_base_name('易方达纳斯达克100ETF联接(QDII-LOF)A(人民币)'),
            normalize_base_name('易方达纳斯达克100ETF联接(QDII-LOF)C(美元现汇)'),
        )
        self.assertEqual(
            company_name_from_fund('南方纳斯达克100指数发起(QDII)A'), '南方'
        )
        self.assertEqual(
            company_name_from_fund('华泰柏瑞纳斯达克100ETF发起式联接(QDII)A'),
            '华泰柏瑞',
        )
        self.assertEqual(
            company_name_from_fund('华夏基金纳斯达克100ETF联接(QDII)A'), '华夏'
        )
        self.assertEqual(company_name_from_fund('纳斯达克100ETF大成'), '')
        self.assertEqual(company_short_name('华泰柏瑞'), '华泰柏瑞基金')

    def test_sync_adds_all_share_classes_and_keeps_manual_limit(self):
        result = sync_nasdaq100_funds(self.db_path)
        self.assertFalse(result['skipped'])
        self.assertEqual(result['discovered'], 8)
        self.assertEqual(result['inserted'], 4)
        self.assertEqual(result['updated'], 4)

        conn = connect(self.db_path)
        rows = conn.execute(
            "SELECT code, share_class, currency, csrc_code FROM quota_fund "
            "WHERE discovery_source = 'homepage' ORDER BY code"
        ).fetchall()
        by_code = {row['code']: row for row in rows}
        self.assertEqual(
            set(by_code),
            {
                '016452', '016453', '021000', '270042', '000055',
                '006480', '019441', '900001',
            },
        )
        self.assertEqual(by_code['016453']['share_class'], 'C')
        self.assertEqual(by_code['000055']['currency'], 'USD')
        self.assertEqual(by_code['021000']['csrc_code'], '016452')
        self.assertEqual(by_code['016453']['csrc_code'], '016452')
        self.assertEqual(by_code['006480']['csrc_code'], '270042')
        self.assertNotIn('159513', by_code)
        self.assertNotIn('900002', by_code)

        company = conn.execute(
            """SELECT c.short_name FROM quota_fund f
               JOIN quota_company c ON c.id = f.company_id
               WHERE f.code = '900001'"""
        ).fetchone()
        self.assertEqual(company['short_name'], '招商基金')

        manual = conn.execute(
            """SELECT l.amount FROM quota_limit l
               JOIN quota_fund f ON f.id = l.fund_id
               WHERE f.code = '021000' AND l.channel_type = 'direct'
                 AND l.is_manual = 1"""
        ).fetchone()
        self.assertEqual(manual['amount'], 200.0)
        conn.close()

        again = sync_nasdaq100_funds(self.db_path)
        self.assertEqual(again['discovered'], 8)
        self.assertEqual(again['inserted'], 0)
        self.assertEqual(again['updated'], 8)

    def test_sync_creates_company_for_new_fund(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                'INSERT INTO funds (code, name, ftype, market, upd_date) '
                'VALUES (?, ?, ?, ?, ?)',
                (
                    '900003', '新锐纳斯达克100指数(QDII)A',
                    '指数型-海外股票', '场外', '2026-01-01',
                ),
            )

        result = sync_nasdaq100_funds(self.db_path)
        self.assertEqual(result['discovered'], 9)
        self.assertEqual(result['inserted'], 5)
        self.assertEqual(result['companies_created'], 1)

        conn = connect(self.db_path)
        company = conn.execute(
            "SELECT * FROM quota_company WHERE short_name = '新锐基金'"
        ).fetchone()
        self.assertIsNotNone(company)
        self.assertEqual(company['full_name'], '新锐基金')
        fund = conn.execute(
            "SELECT * FROM quota_fund WHERE code = '900003'"
        ).fetchone()
        self.assertEqual(fund['company_id'], company['id'])
        self.assertEqual(fund['csrc_code'], '900003')
        conn.close()

        again = sync_nasdaq100_funds(self.db_path)
        self.assertEqual(again['companies_created'], 0)
        self.assertEqual(again['inserted'], 0)


if __name__ == '__main__':
    unittest.main()
