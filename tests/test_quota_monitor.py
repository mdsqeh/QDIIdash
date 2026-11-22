"""Quota-monitoring parser and read-model tests (no network)."""
import gc
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timezone

from quota.parser import is_relevant_title, is_share_class_relevant, parse_announcement
from quota.schema import FUNDS, init_quota_db
from quota.service import connect, get_fund_detail, set_manual_limit


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='seconds')


class QuotaParserTests(unittest.TestCase):
    def test_title_filter(self):
        self.assertTrue(is_relevant_title(
            '关于调整南方纳斯达克100指数发起式证券投资基金（QDII）申购、定投及转换转入业务金额限制的公告'
        ))
        self.assertFalse(is_relevant_title('关于嘉实纳斯达克100ETF联接（QDII）暂停赎回业务的公告'))
        self.assertFalse(is_relevant_title('华夏纳斯达克100ETF联接（QDII）2026年第3季度报告'))
        self.assertFalse(is_relevant_title('关于旗下部分基金新增华源证券为申购、赎回代办券商的公告'))

    def test_share_class_filter(self):
        fund_a = [item for item in FUNDS if item['code'] == '015299'][0]
        self.assertFalse(is_share_class_relevant(
            '关于调整华夏纳斯达克100ETF联接（QDII）C类基金份额申购限额的公告', fund_a
        ))
        self.assertTrue(is_share_class_relevant(
            '关于调整华夏纳斯达克100ETF联接（QDII）A类、C类基金份额申购限额的公告', fund_a
        ))

    def test_channel_specific_parse(self):
        fund = [item for item in FUNDS if item['code'] == '015299'][0]
        text = (
            '自2026年8月3日起，单个投资者通过本公司直销电子交易平台单日累计申购申请'
            '华夏纳斯达克100ETF发起式联接（QDII）A（人民币）（015299）的金额各类别'
            '均应不超过人民币100元。本基金各销售币种在本公司直销柜台及代销机构仍暂停'
            '办理申购及定期定额申购业务。'
        )
        limits = parse_announcement(
            '关于在基金管理人直销电子交易平台调整华夏纳斯达克100ETF发起式联接（QDII）人民币申购业务上限的公告',
            text, fund, '2026-07-31',
        )
        direct_amounts = [
            item for item in limits
            if item['channel_type'] == 'direct' and item['status'] == 'amount_cap'
        ]
        self.assertEqual(direct_amounts[0]['amount'], 100.0)
        self.assertTrue(any(
            item['channel_type'] == 'agency' and item['status'] == 'paused'
            for item in limits
        ))


class QuotaServiceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, 'quota-test.db')
        init_quota_db(self.db_path)
        self.conn = connect(self.db_path)
        self.fund = self.conn.execute(
            "SELECT * FROM quota_fund WHERE code = '015299'"
        ).fetchone()

    def tearDown(self):
        self.conn.close()
        gc.collect()
        self._tmp.cleanup()

    def _add_announcement(self, title, date='2026-01-01'):
        cur = self.conn.execute(
            """INSERT INTO quota_announcement
               (fund_id, source_type, source_url, external_id, title, published_at,
                fetched_at, text, parse_status, parse_confidence, is_read, created_at)
               VALUES (?, 'csrc', 'http://example.test/a', ?, ?, ?, ?, '', 'parsed',
                       0.9, 0, ?)""",
            (self.fund['id'], title, title, date, _now(), _now()),
        )
        self.conn.commit()
        return cur.lastrowid

    def _add_limit(self, announcement_id, channel, status, amount, effective):
        self.conn.execute(
            """INSERT INTO quota_limit
               (fund_id, announcement_id, channel_type, channel_name, status, amount,
                currency, limit_text, business_type, effective_from, effective_to,
                confidence, is_manual, note, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, 'CNY', '', 'subscription', ?, NULL,
                       0.9, 0, NULL, ?, ?)""",
            (
                self.fund['id'], announcement_id, channel,
                '直销' if channel == 'direct' else ('代销机构' if channel == 'agency' else '全渠道'),
                status, amount, effective, _now(), _now(),
            ),
        )
        self.conn.commit()

    def test_newer_all_channel_overrides_channel_rows(self):
        ann = self._add_announcement('调整申购限额', '2026-01-01')
        self._add_limit(ann, 'direct', 'amount_cap', 100, '2026-01-01')
        ann2 = self._add_announcement('暂停申购', '2026-02-01')
        self._add_limit(ann2, 'all', 'paused', None, '2026-02-01')

        detail = get_fund_detail(self.conn, '015299')
        self.assertEqual(detail['fund']['direct']['status'], 'paused')
        self.assertEqual(detail['fund']['agency']['status'], 'paused')
        self.assertIsNone(detail['fund']['all'])

    def test_specific_channel_newer_than_all_wins(self):
        ann = self._add_announcement('暂停申购', '2026-01-01')
        self._add_limit(ann, 'all', 'paused', None, '2026-01-01')
        ann2 = self._add_announcement('调整直销限额', '2026-02-01')
        self._add_limit(ann2, 'direct', 'amount_cap', 100, '2026-02-01')

        detail = get_fund_detail(self.conn, '015299')
        self.assertEqual(detail['fund']['direct']['status'], 'amount_cap')
        self.assertEqual(detail['fund']['direct']['amount'], 100.0)
        self.assertEqual(detail['fund']['agency']['status'], 'paused')

    def test_latest_direct_pause_beats_older_amount(self):
        ann = self._add_announcement('调整直销限额', '2026-07-31')
        self._add_limit(ann, 'direct', 'amount_cap', 100, '2026-07-31')
        ann2 = self._add_announcement('暂停直销申购', '2026-08-07')
        self._add_limit(ann2, 'direct', 'paused', None, '2026-08-07')

        detail = get_fund_detail(self.conn, '015299')
        self.assertEqual(detail['fund']['direct']['status'], 'paused')

    def test_manual_override_and_clear(self):
        ann = self._add_announcement('调整申购限额', '2026-01-01')
        self._add_limit(ann, 'direct', 'amount_cap', 100, '2026-01-01')

        set_manual_limit(self.conn, '015299', {
            'channel_type': 'direct',
            'status': 'amount_cap',
            'amount': 50,
            'note': 'App 实测',
        })
        detail = get_fund_detail(self.conn, '015299')
        self.assertTrue(detail['fund']['direct']['is_manual'])
        self.assertEqual(detail['fund']['direct']['amount'], 50.0)

        set_manual_limit(self.conn, '015299', {
            'channel_type': 'direct',
            'clear': True,
        })
        detail = get_fund_detail(self.conn, '015299')
        self.assertFalse(detail['fund']['direct']['is_manual'])
        self.assertEqual(detail['fund']['direct']['amount'], 100.0)


if __name__ == '__main__':
    unittest.main()
