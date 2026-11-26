"""Quota-monitoring parser and read-model tests (no network)."""
import gc
import os
import sqlite3
import tempfile
import unittest
from datetime import date, datetime, timezone

from quota.parser import (
    _extract_amount,
    is_relevant_title,
    is_share_class_relevant,
    parse_announcement,
)
import quota.report as quota_report
from quota.report import build_daily_report, is_trade_day
from quota.schema import FUNDS, init_quota_db
from quota.service import (
    connect,
    get_fund_detail,
    list_fund_summaries,
    set_manual_limit,
)


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
        self.assertFalse(is_relevant_title(
            '天弘基金管理有限公司关于调整旗下部分基金份额的最低申购金额限制和最低赎回份额、最低持有份额限制的公告'
        ))
        self.assertFalse(is_relevant_title(
            '关于新增华泰证券股份有限公司为建信旗下部分交易型开放式指数证券投资基金申购赎回代理机构的公告'
        ))

    def test_agency_addition_is_not_a_limit(self):
        fund = {'code': '012751', 'share_class': 'A', 'currency': 'USD'}
        limits = parse_announcement(
            '关于新增华泰证券股份有限公司为建信旗下部分交易型开放式指数证券投资基金申购赎回代理机构的公告',
            '自2026年11月4日起，以上证券公司将作为下述产品的申购赎回代理机构。',
            fund,
            '2026-11-04',
        )
        self.assertEqual(limits, [])

    def test_share_class_filter(self):
        fund_a = [item for item in FUNDS if item['code'] == '015299'][0]
        self.assertFalse(is_share_class_relevant(
            '关于调整华夏纳斯达克100ETF联接（QDII）C类基金份额申购限额的公告', fund_a
        ))
        self.assertTrue(is_share_class_relevant(
            '关于调整华夏纳斯达克100ETF联接（QDII）A类、C类基金份额申购限额的公告', fund_a
        ))
        fund_i = [item for item in FUNDS if item['code'] == '021000'][0]
        self.assertTrue(is_share_class_relevant(
            '关于调整南方纳斯达克100指数发起式证券投资基金（QDII）I类基金份额申购限额的公告',
            fund_i,
        ))
        self.assertFalse(is_share_class_relevant(
            '关于调整南方纳斯达克100指数发起式证券投资基金（QDII）A类基金份额申购限额的公告',
            fund_i,
        ))
        fund_d = {'share_class': 'D', 'currency': 'CNY'}
        self.assertFalse(is_share_class_relevant(
            '关于调整某基金A类基金份额申购限额的公告', fund_d
        ))
        self.assertTrue(is_share_class_relevant(
            '关于调整某基金D类基金份额申购限额的公告', fund_d
        ))
        fund_usd = {'share_class': 'A', 'currency': 'USD'}
        self.assertFalse(is_share_class_relevant(
            '关于调整某基金A类人民币份额申购限额的公告', fund_usd
        ))
        self.assertTrue(is_share_class_relevant(
            '关于调整某基金A类美元份额申购限额的公告', fund_usd
        ))
        self.assertTrue(is_share_class_relevant(
            '关于调整某基金申购限额的公告', fund_usd
        ))

    def test_temporary_holiday_suspension_filter(self):
        title = (
            '关于旗下部分基金2026年11月26日暂停申购、赎回、转换、'
            '定期定额投资业务的提示性公告'
        )
        self.assertFalse(is_relevant_title(title))
        fund = {'code': '161130', 'share_class': 'A', 'currency': 'CNY'}
        text = (
            '根据境外主要投资市场休市安排，旗下部分基金2026年11月26日将暂停'
            '申购、赎回、转换、定期定额投资业务，并于2026年11月27日恢复办理。'
        )
        self.assertEqual(parse_announcement(title, text, fund, '2026-11-23'), [])
        self.assertFalse(is_relevant_title(
            '关于嘉实纳斯达克100ETF联接（QDII）2026年1月19日暂停申购、'
            '赎回及定期定额投资业务的公告'
        ))
        self.assertTrue(is_relevant_title(
            '关于广发纳斯达克100ETF联接（QDII）继续暂停申购业务的提示性公告'
        ))

    def test_amount_extraction_ignores_total_fund_size(self):
        self.assertIsNone(_extract_amount('基金总规模不超过人民币24亿元', 'CNY')[0])
        self.assertEqual(
            _extract_amount('累计金额限制调整为5元（不含5元）', 'CNY')[0], 5.0
        )
        self.assertEqual(
            _extract_amount('单日申购上限为10万美元', 'USD')[0], 100000.0
        )

    def test_share_class_table_scope(self):
        text = (
            '下属分级基金的交易代码 016056 016055 016058 016057 024237 '
            '下属分级基金是否暂停大额申购、转换转入、定期定额投资业务否否否否是 '
            '下属分级基金的限制申购金额（单位：元）100 '
            '2025年11月18日起，投资人通过代销机构申购本基金I类份额单日累计金额'
            '应不超过100元。'
        )
        fund_a = {'code': '016056', 'share_class': 'A', 'currency': 'USD'}
        self.assertEqual(
            parse_announcement('调整大额申购业务的公告', text, fund_a, '2025-11-18'),
            [],
        )
        fund_i = {'code': '024237', 'share_class': 'I', 'currency': 'CNY'}
        limits = parse_announcement(
            '调整大额申购业务的公告', text, fund_i, '2025-11-18'
        )
        self.assertTrue(any(
            item['channel_type'] == 'agency'
            and item['status'] == 'amount_cap'
            and item['amount'] == 100.0
            for item in limits
        ))

    def test_share_class_limit_table(self):
        text = (
            '下属分级基金的交易代码 012751 539001 012753 012752 023422 '
            '该分级基金是否暂停大额申购、定期定额投资是是是是是 '
            '下属分级基金的限制申购金额（单位：人民币元） - 10,000.00 - 10,000.00 - '
            '自2026年8月13日起，对人民币份额的申购业务进行限制。'
        )
        title = '建信纳斯达克100指数型证券投资基金（QDII）在直销渠道暂停大额申购、定期定额投资公告'
        fund_cny = {'code': '539001', 'share_class': 'A', 'currency': 'CNY'}
        limits = parse_announcement(title, text, fund_cny, '2026-08-12')
        self.assertEqual(len(limits), 1)
        self.assertEqual(limits[0]['channel_type'], 'direct')
        self.assertEqual(limits[0]['status'], 'amount_cap')
        self.assertEqual(limits[0]['amount'], 10000.0)
        self.assertEqual(limits[0]['effective_from'], '2026-08-13')
        fund_usd = {'code': '012751', 'share_class': 'A', 'currency': 'USD'}
        limits = parse_announcement(title, text, fund_usd, '2026-08-12')
        self.assertEqual(len(limits), 1)
        self.assertEqual(limits[0]['channel_type'], 'direct')
        self.assertEqual(limits[0]['status'], 'normal')
        self.assertIsNone(limits[0]['amount'])

        text = (
            '下属基金份额的交易代码 018966 018967 018969 018968 021773 '
            '该基金份额是否暂停上述业务是是是是是 '
            '金额单位人民币元人民币元美元美元人民币元 '
            '下属基金份额的限制申购金额 10.00 10.00 2.00 2.00 10.00 '
            '自2026年09月14日起，限制金额为1000人民币元。'
            '自2026年09月22日起，单日累计申购金额不应超过10人民币元。'
        )
        title = '关于汇添富纳斯达克100ETF发起式联接（QDII）调整直销机构大额申购业务限制金额的公告'
        fund_cny = {'code': '018966', 'share_class': 'A', 'currency': 'CNY'}
        limits = parse_announcement(title, text, fund_cny, '2026-09-22')
        self.assertEqual(limits[0]['channel_type'], 'direct')
        self.assertEqual(limits[0]['amount'], 10.0)
        self.assertEqual(limits[0]['effective_from'], '2026-09-22')
        self.assertFalse(any(item['channel_type'] == 'agency' for item in limits))
        fund_usd = {'code': '018968', 'share_class': 'A', 'currency': 'USD'}
        limits = parse_announcement(title, text, fund_usd, '2026-09-22')
        self.assertEqual(limits[0]['amount'], 2.0)

    def test_large_pause_without_amount_is_unknown(self):
        fund = {'code': '012751', 'share_class': 'A', 'currency': 'USD'}
        limits = parse_announcement(
            '暂停大额申购公告',
            '自2026年8月13日起暂停大额申购、定期定额投资业务。',
            fund,
            '2026-08-12',
        )
        self.assertEqual(limits[0]['status'], 'unknown')

    def test_full_pause_beats_amount_in_text(self):
        fund = {'code': '018043', 'share_class': 'A', 'currency': 'CNY'}
        limits = parse_announcement(
            '天弘纳斯达克100指数型发起式证券投资基金（QDII）A类基金份额、C类基金份额暂停申购及定期定额投资业务的公告',
            '自2026年9月21日起暂停申购。此前单日累计申购金额不超过100元。',
            fund,
            '2026-09-18',
        )
        self.assertEqual(limits[0]['status'], 'paused')
        self.assertIsNone(limits[0]['amount'])

    def test_share_class_table_with_units(self):
        text = (
            '下属分级基金的交易代码 012751 539001 012753 012752 023422 '
            '该分级基金是否暂停大额申购、定期定额投资是是是是是 '
            '下属分级基金的限制申购金额 1 万美元 10 元 1 万美元 10 元 10 元 '
            '自2026年8月20日起，对人民币份额和美元份额的申购业务进行限制。'
        )
        title = '建信纳斯达克100指数型证券投资基金（QDII）暂停大额申购、定期定额投资公告'
        fund_usd = {'code': '012751', 'share_class': 'A', 'currency': 'USD'}
        limits = parse_announcement(title, text, fund_usd, '2026-08-19')
        self.assertEqual(limits[0]['channel_type'], 'all')
        self.assertEqual(limits[0]['amount'], 10000.0)
        self.assertEqual(limits[0]['effective_from'], '2026-08-20')
        fund_cny = {'code': '539001', 'share_class': 'A', 'currency': 'CNY'}
        limits = parse_announcement(title, text, fund_cny, '2026-08-19')
        self.assertEqual(limits[0]['channel_type'], 'all')
        self.assertEqual(limits[0]['amount'], 10.0)

    def test_code_table_scopes_share_classes(self):
        text = (
            '下属分级基金的交易代码 019736 019737 '
            '该分级基金是否暂停大额申购、定期定额投资是是 '
            '下属分级基金的限制申购金额（单位：人民币元） 10.00 10.00 '
            '自2026年9月28日起，本基金每一类基金份额单日申购限制金额调整为10元。'
        )
        title = '宝盈纳斯达克100指数型发起式证券投资基金（QDII）调整大额申购、定期定额投资业务限制金额的公告'
        fund_cny = {'code': '019736', 'share_class': 'A', 'currency': 'CNY'}
        limits = parse_announcement(title, text, fund_cny, '2026-09-24')
        self.assertEqual(limits[0]['status'], 'amount_cap')
        self.assertEqual(limits[0]['amount'], 10.0)
        fund_usd = {'code': '019738', 'share_class': 'A', 'currency': 'USD'}
        limits = parse_announcement(title, text, fund_usd, '2026-09-24')
        self.assertEqual(limits[0]['status'], 'unknown')
        self.assertIsNone(limits[0]['amount'])

    def test_single_fund_code_is_not_a_share_class_table(self):
        text = (
            '本基金（基金代码：019441）取消总规模上限。'
            '本基金A类份额和C类份额代销渠道单日累计申购金额限制为10元；'
            '直销渠道单日累计申购金额限制为100元。'
        )
        title = '关于万家纳斯达克100指数型发起式证券投资基金（QDII）取消总规模上限的公告'
        fund_c = {'code': '019442', 'share_class': 'C', 'currency': 'CNY'}
        limits = parse_announcement(title, text, fund_c, '2026-09-28')
        by_channel = {item['channel_type']: item for item in limits}
        self.assertEqual(by_channel['agency']['amount'], 10.0)
        self.assertEqual(by_channel['direct']['amount'], 100.0)

    def test_default_channel_applies_to_channel_less_announcement(self):
        fund = {
            'code': '021000',
            'share_class': 'I',
            'currency': 'CNY',
            'default_channel': 'direct',
        }
        text = '下属基金份额的代码 021000 该基金份额的限制金额 200 元。'
        title = '关于调整南方纳斯达克100指数发起式证券投资基金（QDII）I类基金份额申购金额限制的公告'
        limits = parse_announcement(title, text, fund, '2026-07-20')
        self.assertEqual(limits[0]['channel_type'], 'direct')
        self.assertEqual(limits[0]['amount'], 200.0)

    def test_explicit_agency_channel_beats_default_channel(self):
        fund = {
            'code': '021000',
            'share_class': 'I',
            'currency': 'CNY',
            'default_channel': 'direct',
        }
        text = '本基金I类基金份额在代销渠道单日申购限额为100元。'
        title = '关于调整南方纳斯达克100指数发起式证券投资基金（QDII）I类基金份额申购金额限制的公告'
        limits = parse_announcement(title, text, fund, '2026-07-20')
        self.assertEqual(limits[0]['channel_type'], 'agency')
        self.assertEqual(limits[0]['amount'], 100.0)

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

    def _add_announcement_for(self, fund, title, date='2026-01-01'):
        cur = self.conn.execute(
            """INSERT INTO quota_announcement
               (fund_id, source_type, source_url, external_id, title, published_at,
                fetched_at, text, parse_status, parse_confidence, is_read, created_at)
               VALUES (?, 'csrc', 'http://example.test/a', ?, ?, ?, ?, '', 'parsed',
                       0.9, 0, ?)""",
            (fund['id'], title, title, date, _now(), _now()),
        )
        self.conn.commit()
        return cur.lastrowid

    def _add_announcement(self, title, date='2026-01-01'):
        return self._add_announcement_for(self.fund, title, date)

    def _add_limit_for(self, fund, announcement_id, channel, status, amount,
                       effective, currency='CNY'):
        self.conn.execute(
            """INSERT INTO quota_limit
               (fund_id, announcement_id, channel_type, channel_name, status, amount,
                currency, limit_text, business_type, effective_from, effective_to,
                confidence, is_manual, note, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, '', 'subscription', ?, NULL,
                       0.9, 0, NULL, ?, ?)""",
            (
                fund['id'], announcement_id, channel,
                '直销' if channel == 'direct' else ('代销机构' if channel == 'agency' else '全渠道'),
                status, amount, currency, effective, _now(), _now(),
            ),
        )
        self.conn.commit()

    def _add_limit(self, announcement_id, channel, status, amount, effective):
        self._add_limit_for(
            self.fund, announcement_id, channel, status, amount, effective,
        )

    def test_i_share_uses_parent_csrc_code_and_default_direct_channel(self):
        row = self.conn.execute(
            "SELECT id, csrc_code, default_channel FROM quota_fund WHERE code = '021000'"
        ).fetchone()
        self.assertEqual(row['csrc_code'], '016452')
        self.assertEqual(row['default_channel'], 'direct')
        manual = self.conn.execute(
            """SELECT 1 FROM quota_limit
               WHERE fund_id = ? AND is_manual = 1""",
            (row['id'],),
        ).fetchone()
        self.assertIsNone(manual)

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

    def test_last_announcement_uses_current_limit(self):
        ann = self._add_announcement('调整申购限额', '2026-01-01')
        self._add_limit(ann, 'all', 'amount_cap', 10, '2026-01-01')
        self._add_announcement('仅 I 类份额调整，不适用本基金', '2026-02-01')

        detail = get_fund_detail(self.conn, '015299')
        self.assertEqual(detail['fund']['last_announcement_at'], '2026-01-01')
        self.assertFalse(detail['fund']['changed_today'])

    def test_all_channel_limit_is_included_in_limited_filter(self):
        ann = self._add_announcement('调整申购限额', '2026-01-01')
        self._add_limit(ann, 'all', 'amount_cap', 200, '2026-01-01')

        filtered, summary, _ = list_fund_summaries(
            self.conn, search='015299', status='limited'
        )
        self.assertEqual([item['code'] for item in filtered], ['015299'])
        self.assertEqual(summary['paused'], 0)

    def test_all_channel_pause_is_included_in_paused_filter_and_summary(self):
        ann = self._add_announcement('暂停申购', '2026-01-01')
        self._add_limit(ann, 'all', 'paused', None, '2026-01-01')

        filtered, summary, _ = list_fund_summaries(
            self.conn, search='015299', status='paused'
        )
        self.assertEqual([item['code'] for item in filtered], ['015299'])
        self.assertEqual(summary['paused'], 1)

    def test_purchasable_summary_uses_max_channel_and_splits_currency(self):
        ann = self._add_announcement('调整申购限额', '2026-01-01')
        self._add_limit(ann, 'direct', 'amount_cap', 100, '2026-01-01')
        self._add_limit(ann, 'agency', 'amount_cap', 5, '2026-01-01')

        fund_i = self.conn.execute(
            "SELECT * FROM quota_fund WHERE code = '021000'"
        ).fetchone()
        ann_i = self._add_announcement_for(fund_i, '调整申购限额', '2026-01-01')
        self._add_limit_for(fund_i, ann_i, 'direct', 'amount_cap', 200, '2026-01-01')

        self.conn.execute(
            """INSERT INTO quota_fund
               (company_id, code, name, share_class, currency, created_at)
               VALUES (?, 'TESTUSD', '测试美元基金', 'A', 'USD', ?)""",
            (self.fund['company_id'], _now()),
        )
        self.conn.commit()
        fund_usd = self.conn.execute(
            "SELECT * FROM quota_fund WHERE code = 'TESTUSD'"
        ).fetchone()
        ann_usd = self._add_announcement_for(fund_usd, '调整申购限额', '2026-01-01')
        self._add_limit_for(
            fund_usd, ann_usd, 'all', 'amount_cap', 1, '2026-01-01',
            currency='USD',
        )

        _, summary, _ = list_fund_summaries(self.conn)
        cny = summary['purchasable']['CNY']
        usd = summary['purchasable']['USD']
        self.assertEqual(cny['amount'], 300.0)
        self.assertEqual(cny['limited_count'], 2)
        self.assertEqual(usd['amount'], 1.0)
        self.assertEqual(usd['limited_count'], 1)

    def test_purchasable_summary_counts_unlimited_separately(self):
        ann = self._add_announcement('恢复申购', '2026-01-01')
        self._add_limit(ann, 'all', 'normal', None, '2026-01-01')

        _, summary, _ = list_fund_summaries(self.conn)
        cny = summary['purchasable']['CNY']
        self.assertEqual(cny['amount'], 0.0)
        self.assertEqual(cny['unlimited_count'], 1)
        self.assertEqual(cny['limited_count'], 0)

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


class QuotaReportTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self._tmp.name, 'quota-report.db')
        init_quota_db(self.db_path)
        self.conn = connect(self.db_path)

    def tearDown(self):
        self.conn.close()
        gc.collect()
        self._tmp.cleanup()

    def _add_limit(self, code, channel, status, amount, effective='2026-01-01'):
        fund = self.conn.execute(
            "SELECT * FROM quota_fund WHERE code = ?", (code,)
        ).fetchone()
        self.conn.execute(
            """INSERT INTO quota_limit
               (fund_id, announcement_id, channel_type, channel_name, status, amount,
                currency, limit_text, business_type, effective_from, effective_to,
                confidence, is_manual, note, created_at, updated_at)
               VALUES (?, NULL, ?, ?, ?, ?, ?, '', 'subscription', ?, NULL,
                       0.9, 0, NULL, ?, ?)""",
            (fund['id'], channel, channel, status, amount, fund['currency'],
             effective, _now(), _now()),
        )
        self.conn.commit()

    def test_report_lists_purchasable_funds_and_totals(self):
        self._add_limit('015299', 'direct', 'amount_cap', 100)
        report = build_daily_report(self.conn)
        self.assertIn('可购最大总额：¥100', report)
        self.assertIn('可购买基金（1 只）', report)
        self.assertIn('｜ 直销 ¥100', report)

    def test_unlimited_makes_total_unlimited(self):
        self._add_limit('015299', 'all', 'normal', None)
        report = build_daily_report(self.conn)
        self.assertIn('可购最大总额：¥不限', report)
        self.assertIn('全渠道 不限', report)

    def test_paused_fund_is_excluded(self):
        self._add_limit('015299', 'all', 'paused', None)
        report = build_daily_report(self.conn)
        self.assertIn('可购最大总额：无', report)
        self.assertIn('可购买基金：暂无', report)

    def test_is_trade_day_uses_calendar_with_weekday_fallback(self):
        today = datetime.now(quota_report.SHANGHAI_TZ).date()
        quota_report._TRADE_DAYS['day'] = today
        quota_report._TRADE_DAYS['days'] = {'2026-11-26'}
        self.assertTrue(is_trade_day(date(2026, 11, 26)))
        self.assertFalse(is_trade_day(date(2026, 11, 25)))  # weekday but not in set
        quota_report._TRADE_DAYS['days'] = None
        self.assertTrue(is_trade_day(date(2026, 11, 26)))   # Thursday fallback
        self.assertFalse(is_trade_day(date(2026, 11, 28)))  # Saturday fallback


if __name__ == '__main__':
    unittest.main()
