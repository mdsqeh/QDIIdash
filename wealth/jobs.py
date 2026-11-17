"""Scheduler hooks for the wealth module."""
import threading
from datetime import timedelta

from . import dca, prices
from .db import connect, today_str
from .portfolio import parse_date

SYNC_LOCK = threading.Lock()


def _sync(markets=None, sources=None, full=False):
    if not SYNC_LOCK.acquire(blocking=False):
        return {'skipped': True}
    conn = connect()
    try:
        result = prices.sync_all(conn, full=full, markets=markets, sources=sources)
        settled = dca.settle_orders(conn)
        return {**result, **settled}
    except Exception as exc:
        print(f'[wealth] 行情同步失败: {exc}', flush=True)
        return {'error': str(exc)}
    finally:
        conn.close()
        SYNC_LOCK.release()


def job_sync_cn():
    return _sync(markets=['CN'])


def job_sync_fund():
    return _sync(markets=['FUND_CN'])


def job_sync_us():
    return _sync(markets=['US', 'HK'])


def job_sync_crypto():
    return _sync(markets=['CRYPTO'])


def job_sync_fx():
    conn = connect()
    try:
        currencies = [r['currency'] for r in conn.execute(
            "SELECT DISTINCT currency FROM wm_asset WHERE currency != 'CNY'"
        ).fetchall()]
        for currency in currencies:
            prices.ensure_fx(conn, currency, parse_date(today_str()) - timedelta(days=7),
                             today_str())
        conn.commit()
    except Exception as exc:
        print(f'[wealth] 汇率同步失败: {exc}', flush=True)
    finally:
        conn.close()


def job_generate_dca():
    return dca.run_dca()


def job_settle_dca():
    conn = connect()
    try:
        return dca.settle_orders(conn)
    finally:
        conn.close()


def startup_catchup():
    """Run once at process start so downtime does not lose DCA periods."""
    try:
        result = dca.run_dca()
        print(f'[wealth] 定投启动检查: {result}', flush=True)
    except Exception as exc:
        print(f'[wealth] 定投启动检查失败: {exc}', flush=True)


def register_wealth_jobs(scheduler):
    scheduler.add_job(job_sync_cn, 'cron', day_of_week='mon-fri', hour=15, minute=10,
                      id='wm_sync_cn', replace_existing=True)
    scheduler.add_job(job_sync_fund, 'cron', hour=21, minute=30,
                      id='wm_sync_fund', replace_existing=True)
    scheduler.add_job(job_sync_us, 'cron', hour=7, minute=0,
                      id='wm_sync_us', replace_existing=True)
    scheduler.add_job(job_sync_crypto, 'cron', minute=5, hour='*/2',
                      id='wm_sync_crypto', replace_existing=True)
    scheduler.add_job(job_sync_fx, 'cron', hour=9, minute=0,
                      id='wm_sync_fx', replace_existing=True)
    scheduler.add_job(job_generate_dca, 'cron', hour=0, minute=10,
                      id='wm_dca_generate', replace_existing=True)
    scheduler.add_job(job_settle_dca, 'cron', minute='*/30',
                      id='wm_dca_settle', replace_existing=True)
