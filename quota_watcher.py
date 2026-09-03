"""QDII 日申购额度监控模块 — 高频检测额度变动并发送通知。"""
import sqlite3
from datetime import datetime, timezone

import akshare as ak
import requests


def check_quotas(db_path, webhook_url=None, webhook_secret=None):
    """检测 QDII 基金日申购额度变化，记录变动并发送通知。

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
        # 2. 获取所有 QDII 基金代码
        qdii_codes = set(
            row['code'] for row in conn.execute("SELECT code FROM funds").fetchall()
        )
        if not qdii_codes:
            return []

        # 3. 过滤 QDII 基金的最新数据
        sub = df[df['基金代码'].astype(str).isin(qdii_codes)]
        if sub.empty:
            return []

        now = datetime.now(timezone.utc).isoformat()
        changes = []

        for _, row in sub.iterrows():
            code = str(row['基金代码'])
            new_status = str(row.get('申购状态', '') or '')

            raw_limit = row.get('日累计限定金额')
            if raw_limit is not None and not (isinstance(raw_limit, float) and raw_limit != raw_limit):
                new_limit_raw = raw_limit
                new_limit = str(raw_limit)
            else:
                new_limit_raw = None
                new_limit = ''

            db_row = conn.execute(
                "SELECT name, purchase_status, daily_limit FROM funds WHERE code = ?",
                (code,)
            ).fetchone()
            if db_row is None:
                continue

            name = db_row['name']
            old_status = str(db_row['purchase_status'] or '')
            old_limit_raw = db_row['daily_limit']
            old_limit = str(old_limit_raw) if old_limit_raw is not None else ''

            if old_status != new_status:
                changes.append((code, name, 'purchase_status', old_status, new_status, now))

            if old_limit != new_limit:
                changes.append((code, name, 'daily_limit', old_limit, new_limit, now))

        if not changes:
            return []

        # 4. 写入 quota_changes 表
        conn.executemany(
            """INSERT INTO quota_changes (code, field, old_value, new_value, detected_at)
               VALUES (?, ?, ?, ?, ?)""",
            [(c, f, o, n, t) for c, _, f, o, n, t in changes]
        )
        conn.commit()

        # 5. 更新 funds 表中的值
        updated_codes = set(c[0] for c in changes)
        for code in updated_codes:
            fund_row = sub[sub['基金代码'].astype(str) == code].iloc[0]
            new_status = str(fund_row.get('申购状态', '') or '')
            raw_limit = fund_row.get('日累计限定金额')
            if raw_limit is not None and not (isinstance(raw_limit, float) and raw_limit != raw_limit):
                new_limit_val = raw_limit
            else:
                new_limit_val = None
            conn.execute(
                "UPDATE funds SET purchase_status = ?, daily_limit = ? WHERE code = ?",
                (new_status, new_limit_val, code)
            )
        conn.commit()

        # 6. 发送 webhook
        if webhook_url:
            _send_webhook(webhook_url, webhook_secret, changes)

        print(f"📢 额度变动: {len(changes)} 条", flush=True)
        for c, name, field, old, new, ts in changes:
            label = '日申购限额' if field == 'daily_limit' else '申购状态'
            print(f"  {c} {name} {label}: {old} → {new}", flush=True)

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
