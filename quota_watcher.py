"""QDII 日申购额度监控模块 — 高频检测额度变动、记录每日快照并发送通知。"""
import math
import sqlite3
from datetime import datetime, timezone, timedelta

import akshare as ak
import requests


SHANGHAI_TZ = timezone(timedelta(hours=8))
UNLIMITED_LIMIT = 1_000_000_000


def _clean_text(value):
    if value is None:
        return ''
    text = str(value).strip()
    if not text or text.lower() in ('nan', 'none'):
        return ''
    return text


def _to_number(value):
    """把额度文本转成数值；'不限'、空值、NaN 返回 None。"""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    text = str(value).strip().replace(',', '')
    if not text or text.lower() in ('nan', 'none', '不限'):
        return None
    try:
        number = float(text)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(number) else number


def _format_number(number):
    if number is None:
        return ''
    if number == int(number):
        return str(int(number))
    return ('%.4f' % number).rstrip('0').rstrip('.')


def _parse_limit(raw_number, raw_text=None):
    """返回 (数值, 展示文本)，统一 1000/1000.0 这类格式差异。"""
    number = _to_number(raw_number)
    if number is None:
        number = _to_number(raw_text)
    text = _clean_text(raw_text)
    if number is not None and number >= UNLIMITED_LIMIT:
        return number, '不限'
    if number is not None:
        text = _format_number(number)
    return number, text


def _limit_changed(old_number, old_text, new_number, new_text):
    if old_number is not None or new_number is not None:
        return old_number != new_number
    return old_text != new_text


def check_quotas(db_path, webhook_url=None, webhook_secret=None):
    """检测 QDII 基金日申购额度变化，写入每日快照和变动事件。

    Returns:
        list[dict]: 本次检测到的变动记录
    """
    # 1. 获取最新申购数据
    try:
        df = ak.fund_purchase_em()
    except Exception as e:
        print(f"❌ 额度数据采集失败: {e}", flush=True)
        return []

    if df.empty:
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA busy_timeout=5000")

        # 2. 获取所有 QDII 基金代码
        funds = {
            row['code']: row['name']
            for row in conn.execute("SELECT code, name FROM funds").fetchall()
        }
        if not funds:
            return []

        # 3. 过滤 QDII 基金的最新数据
        sub = df[df['基金代码'].astype(str).isin(funds)]
        if sub.empty:
            return []

        now = datetime.now(timezone.utc).isoformat()
        today = datetime.now(SHANGHAI_TZ).date().isoformat()
        changes = []

        for _, row in sub.iterrows():
            code = str(row['基金代码'])
            name = funds[code]
            new_status = _clean_text(row.get('申购状态'))
            raw_limit = row.get('日累计限定金额')
            new_limit, new_text = _parse_limit(raw_limit, raw_limit)

            # 以每日快照的最后一条作为旧值，避免增量采集覆盖 funds 后漏掉变动。
            last = conn.execute(
                """SELECT daily_limit, daily_limit_text, purchase_status
                   FROM quota_daily WHERE code = ?
                   ORDER BY trade_date DESC LIMIT 1""",
                (code,)
            ).fetchone()

            has_status_history = False
            has_limit_history = False
            old_limit = old_text = old_status = None
            if last is not None:
                old_limit, old_text = _parse_limit(
                    last['daily_limit'], last['daily_limit_text']
                )
                old_status = _clean_text(last['purchase_status'])
                has_status_history = True
                has_limit_history = True
            else:
                # 兼容升级前只有变动事件、还没有每日快照的基金。
                last_limit = conn.execute(
                    """SELECT new_value FROM quota_changes
                       WHERE code = ? AND field = 'daily_limit'
                       ORDER BY detected_at DESC, id DESC LIMIT 1""",
                    (code,)
                ).fetchone()
                last_status = conn.execute(
                    """SELECT new_value FROM quota_changes
                       WHERE code = ? AND field = 'purchase_status'
                       ORDER BY detected_at DESC, id DESC LIMIT 1""",
                    (code,)
                ).fetchone()
                if last_limit is not None:
                    old_limit, old_text = _parse_limit(last_limit['new_value'])
                    has_limit_history = True
                if last_status is not None:
                    old_status = _clean_text(last_status['new_value'])
                    has_status_history = True

            if has_status_history and old_status != new_status:
                changes.append((code, name, 'purchase_status', old_status, new_status, now))
            if has_limit_history and _limit_changed(old_limit, old_text, new_limit, new_text):
                changes.append((code, name, 'daily_limit', old_text, new_text, now))

            # 4. 写入当天快照（当天多次检测只更新最新值）
            conn.execute(
                """INSERT INTO quota_daily
                       (code, trade_date, daily_limit, daily_limit_text,
                        purchase_status, first_seen_at, last_seen_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(code, trade_date) DO UPDATE SET
                       daily_limit      = excluded.daily_limit,
                       daily_limit_text = excluded.daily_limit_text,
                       purchase_status  = excluded.purchase_status,
                       last_seen_at     = excluded.last_seen_at""",
                (code, today, new_limit, new_text, new_status, now, now)
            )

            # 5. 同步 funds 当前值
            conn.execute(
                "UPDATE funds SET purchase_status = ?, daily_limit = ? WHERE code = ?",
                (new_status, new_limit, code)
            )

        if changes:
            conn.executemany(
                """INSERT INTO quota_changes (code, field, old_value, new_value, detected_at)
                   VALUES (?, ?, ?, ?, ?)""",
                [(c, f, o, n, t) for c, _, f, o, n, t in changes]
            )

        conn.commit()

        # 6. 发送 webhook
        if changes and webhook_url:
            _send_webhook(webhook_url, webhook_secret, changes)

        if changes:
            print(f"📢 额度变动: {len(changes)} 条", flush=True)
            for c, name, field, old, new, ts in changes:
                label = '日申购限额' if field == 'daily_limit' else '申购状态'
                print(f"  {c} {name} {label}: {old} → {new}", flush=True)
        else:
            print("✅ 额度无变动", flush=True)

        return [
            {'code': c, 'name': n, 'field': f, 'old_value': o, 'new_value': nv, 'detected_at': t}
            for c, n, f, o, nv, t in changes
        ]

    finally:
        conn.close()


def _send_webhook(url, secret, changes):
    """发送 webhook 通知。"""
    lines = []
    for c, name, field, old, new, _ in changes:
        label = '日申购限额' if field == 'daily_limit' else '申购状态'
        lines.append(f"基金 {c}（{name}）{label}：{old} → {new}")

    message = "QDII 额度变动\n" + "\n".join(lines)

    try:
        resp = requests.post(
            url,
            json={"message": message, "secret": secret},
            timeout=10
        )
        if resp.ok:
            print(f"  ✅ Webhook 发送成功 ({resp.status_code})", flush=True)
        else:
            print(f"  ⚠️ Webhook 发送失败 ({resp.status_code})", flush=True)
    except Exception as e:
        print(f"  ⚠️ Webhook 请求失败: {e}", flush=True)
