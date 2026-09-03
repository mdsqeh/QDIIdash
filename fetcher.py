"""
AKShare 数据采集引擎 — 从东方财富（天天基金）获取 QDII 基金数据。

独立于 Web 框架，可在后台线程中调用 fetch_all() 获取全量数据。
返回 list[dict]，可直接存入 SQLite 或序列化为 JSON。
"""
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import StringIO
import time
import requests
import pandas as pd
import akshare as ak
from bs4 import BeautifulSoup


def _find_col(df, suffix, qdii_codes=None):
    """在 DataFrame 列中查找以指定后缀结尾的列，优先选非空值最多的。"""
    candidates = [c for c in df.columns if c.endswith(suffix)]
    if not candidates:
        return None
    if qdii_codes is None or len(candidates) == 1:
        return candidates[0]
    qdii_mask = df['基金代码'].isin(qdii_codes)
    return max(
        candidates,
        key=lambda c: df.loc[qdii_mask, c].notna().sum() + (df.loc[qdii_mask, c] != '').sum()
    )


def _fetch_fund_extra(codes):
    """单线程获取 QDII 基金的成立日期和区间涨跌幅（AKShare 内部 V8 不支持并发）。

    涨跌幅使用分红再投资法（复权单位净值法）计算，比简单累计净值比值更准确。
    """
    def get_one(code):
        try:
            # 1. 累计净值走势 — 用于 est_date 和 dividend 检测
            info_acc = ak.fund_open_fund_info_em(symbol=code, indicator='累计净值走势', period='成立来')
            acc_nav = info_acc['累计净值'].astype(float)
            acc_dates = pd.to_datetime(info_acc['净值日期'])
            last_date = acc_dates.iloc[-1]

            # 2. 单位净值走势 — 用于分红再投资计算
            info_unit = ak.fund_open_fund_info_em(symbol=code, indicator='单位净值走势', period='成立来')
            unit_nav = info_unit['单位净值'].astype(float)
            unit_dates = pd.to_datetime(info_unit['净值日期'])

            # 合并两个序列（只取共有日期，避免 ffill 导致的假 gap 变化）
            merged = pd.DataFrame({'净值日期': acc_dates, 'acc_nav': acc_nav})
            merged_unit = pd.DataFrame({'净值日期': unit_dates, 'unit_nav': unit_nav})
            merged = pd.merge(merged, merged_unit, on='净值日期', how='inner')
            merged = merged.sort_values('净值日期').reset_index(drop=True)

            merged['gap'] = merged['acc_nav'] - merged['unit_nav']
            merged['gap_change'] = merged['gap'].diff()

            est_date = str(merged['净值日期'].iloc[0].date())

            # 分红再投资法计算 ret_1y / ret_3y
            def reinvested_ret(start_date, end_date):
                mask = (merged['净值日期'] >= start_date) & (merged['净值日期'] <= end_date)
                sub = merged[mask]
                sub = sub.dropna(subset=['unit_nav'])
                if len(sub) < 2:
                    return None

                start_unit = sub['unit_nav'].iloc[0]
                if pd.isna(start_unit) or start_unit <= 0:
                    return None

                units = 1.0
                for i in range(1, len(sub)):
                    gc = sub['gap_change'].iloc[i]
                    u = sub['unit_nav'].iloc[i]
                    if pd.notna(gc) and gc > 0.8 and pd.notna(u) and u > 0:
                        div_per_unit = gc
                        total_div = div_per_unit * units
                        new_units = total_div / u
                        units += new_units

                final_val = sub['unit_nav'].iloc[-1] * units
                return round((final_val / start_unit - 1) * 100, 2)

            cutoff_1y = last_date - pd.Timedelta(days=365)
            if merged['净值日期'].iloc[0] <= cutoff_1y:
                ret_1y = reinvested_ret(cutoff_1y, last_date)
            else:
                ret_1y = None

            cutoff_2y = last_date - pd.Timedelta(days=730)
            if merged['净值日期'].iloc[0] <= cutoff_2y:
                ret_2y = reinvested_ret(cutoff_2y, last_date)
            else:
                ret_2y = None

            cutoff_3y = last_date - pd.Timedelta(days=1095)
            if merged['净值日期'].iloc[0] <= cutoff_3y:
                ret_3y = reinvested_ret(cutoff_3y, last_date)
            else:
                ret_3y = None

            cutoff_4y = last_date - pd.Timedelta(days=1461)
            if merged['净值日期'].iloc[0] <= cutoff_4y:
                ret_4y = reinvested_ret(cutoff_4y, last_date)
            else:
                ret_4y = None

            cutoff_5y = last_date - pd.Timedelta(days=1826)
            if merged['净值日期'].iloc[0] <= cutoff_5y:
                ret_5y = reinvested_ret(cutoff_5y, last_date)
            else:
                ret_5y = None

            cutoff_10y = last_date - pd.Timedelta(days=3653)
            if merged['净值日期'].iloc[0] <= cutoff_10y:
                ret_10y = reinvested_ret(cutoff_10y, last_date)
            else:
                ret_10y = None

            # total_ret 和 ret_ann 仍用累计净值法计算（从成立→现在，兼容）
            first_acc = merged['acc_nav'].iloc[0]
            last_acc = merged['acc_nav'].iloc[-1]
            total_ret = round((last_acc / first_acc - 1) * 100, 2) if first_acc > 0 else None

            years = (last_date - merged['净值日期'].iloc[0]).days / 365.25
            if years > 0 and first_acc > 0:
                ret_ann = round((pow(last_acc / first_acc, 1.0 / years) - 1) * 100, 2)
            else:
                ret_ann = None

            # 最新日增长率（取最后两期单位净值计算）
            if len(merged) >= 2 and pd.notna(merged['unit_nav'].iloc[-2]) and merged['unit_nav'].iloc[-2] > 0:
                dv = merged['unit_nav'].iloc[-1] / merged['unit_nav'].iloc[-2]
                daily_change = round((dv - 1) * 100, 2)
            else:
                daily_change = None

            return code, {
                'est_date': est_date,
                'ret_1y': ret_1y,
                'ret_2y': ret_2y,
                'ret_3y': ret_3y,
                'ret_4y': ret_4y,
                'ret_5y': ret_5y,
                'ret_10y': ret_10y,
                'ret_ann': ret_ann,
                'total_ret': total_ret,
                'daily_change': daily_change,
            }
        except Exception:
            return code, None

    results = {}
    total = len(codes)
    for i, code in enumerate(codes, 1):
        if i % 10 == 1 or i == total:
            print(f"  📊 净值历史 [{i}/{total}] 正在采集 {code}...", flush=True)
        code, data = get_one(code)
        results[code] = data
    print(f"  ✅ 净值历史完成，共 {total} 只", flush=True)
    return results


def _parse_fee(val):
    """从 '1.20%（每年）' 或长文本中提取费率。优先匹配标准格式。"""
    import re
    if val in ('---', '--', '', None):
        return None
    # 标准格式：X.XX%（每年）
    m = re.search(r'([\d.]+)%（每年）', str(val))
    if m:
        return float(m.group(1))
    # 非标准格式：取第一个带小数点的百分比，费率不可能超过30%
    m = re.search(r'([\d]+\.[\d]+)%', str(val))
    if m:
        v = float(m.group(1))
        return v if v < 30 else None
    return None


def _fetch_fund_fees(codes):
    """并发获取基金运作费用（管理费、托管费、销售服务费）。

    自建请求替代 akshare fund_fee_em：
    - fund_fee_em 内部裸 requests.get 无 timeout/UA/重试，10 并发易触发
      东财 fundf10 限流（连接被挂起不响应），最终 180s 总超时整体丢弃。
    - 这里自带 UA/Referer/timeout/重试，降并发并限速；单只失败不影响其他，
      部分成功照常返回，结束时打印成功/失败统计。
    """
    req_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://fundf10.eastmoney.com/",
    }

    def parse_fee_page(html):
        """从 fundf10 费率页解析"运作费用"表，返回 (mgmt, cust, sale)。"""
        soup = BeautifulSoup(html, features="html.parser")
        for h4 in soup.find_all(name="h4", class_="t"):
            if h4.get_text(strip=True) != "运作费用":
                continue
            table = h4.find_next("table")
            if table is None:
                break
            df = pd.read_html(StringIO(str(table)))[0]
            if df.empty:
                break
            row = df.iloc[0]
            mgmt = cust = sale = None
            for i in range(0, len(row), 2):
                label = str(row.iloc[i])
                val = str(row.iloc[i + 1])
                if '管理费' in label:
                    mgmt = _parse_fee(val)
                elif '托管费' in label:
                    cust = _parse_fee(val)
                elif '销售服务费' in label:
                    sale = _parse_fee(val)
            return mgmt, cust, sale
        return None, None, None

    def get_one(code):
        url = f"https://fundf10.eastmoney.com/jjfl_{code}.html"
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=req_headers, timeout=10)
                resp.raise_for_status()
                mgmt, cust, sale = parse_fee_page(resp.text)
                return code, {'mgmt_fee': mgmt, 'cust_fee': cust, 'sale_fee': sale}
            except Exception:
                if attempt < 2:
                    time.sleep(0.5 * (attempt + 1))
        return code, {}

    results = {}
    failures = 0
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(get_one, code): code for code in codes}
        for future in as_completed(futures):
            code, data = future.result()
            results[code] = data
            if not data:
                failures += 1
    print(f"  ✅ 费率完成：成功 {len(results) - failures} 只，失败 {failures} 只", flush=True)
    return results


def _fetch_cumulative_returns(codes):
    """并发获取基金的官方累计收益率（天天基金 LJSYLZS 接口）。

    该 API 返回分红再投资后的复权收益率，比简单地用
    (累计净值/1.0 - 1) 更准确——后者会低估分红再投资的复利效应。

    返回 dict[str, float]: code → 累计收益率（百分比，如 876.3 表示 876.3%）。
    """
    url = "https://api.fund.eastmoney.com/pinzhong/LJSYLZS"
    req_headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://fund.eastmoney.com/",
    }

    def get_one(code):
        try:
            params = {"fundCode": code, "indexcode": "000300", "type": "se"}
            resp = requests.get(url, params=params, headers=req_headers, timeout=10)
            data = resp.json()
            if data.get("Data") and data["Data"]:
                series = data["Data"][0]["data"]
                if series:
                    return code, series[-1][1]
            return code, None
        except Exception:
            return code, None

    results = {}
    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(get_one, code): code for code in codes}
        for future in as_completed(futures):
            code, ret = future.result()
            if ret is not None:
                results[code] = ret
    return results


def _fetch_incremental(codes):
    """增量采集：只更新单位净值、日增长率、折溢价率、申购状态、日额度。

    调用 fetcher.py 的已有函数，返回 list[dict]，每项只含 5 个可更新字段 + code。
    """
    codes_str = list(map(str, codes))
    codes_set = set(codes_str)

    # 1. 场外基金最新净值（单位净值 + 累计净值）
    print("  📡 增量：获取场外基金最新净值...", flush=True)
    all_daily = ak.fund_open_fund_daily_em()
    nav_col = _find_col(all_daily, '单位净值', codes_set)
    acc_nav_col = _find_col(all_daily, '累计净值', codes_set)
    if nav_col:
        open_nav = all_daily[all_daily['基金代码'].astype(str).isin(codes_set)][
            ['基金代码', nav_col, acc_nav_col]
        ].copy()
        open_nav.rename(columns={'基金代码': 'code', nav_col: 'nav', acc_nav_col: 'acc_nav'}, inplace=True)
    else:
        open_nav = pd.DataFrame(columns=['code', 'nav', 'acc_nav'])

    # 2. 交易所基金净值（补齐场内基金的 nav）
    print("  📡 增量：获取场内基金净值...", flush=True)
    exchange_prices = {}
    exchange_changes = {}
    exch_codes = set()
    try:
        exch_df = ak.fund_exchange_rank_em()
        exch_sub = exch_df[exch_df['基金代码'].astype(str).isin(codes_set)][
            ['基金代码', '单位净值']
        ].copy()
        exch_sub.rename(columns={'基金代码': 'code', '单位净值': 'nav_ex'}, inplace=True)
        exch_codes = set(exch_sub['code'].tolist())
        open_nav = pd.concat([open_nav, exch_sub[['code', 'nav_ex']]], ignore_index=True)
    except Exception:
        pass

    # 3. 场内实时行情（折溢价率 + 日增长率）
    print("  📡 增量：获取场内实时行情...", flush=True)
    try:
        etf_df = ak.fund_etf_spot_em()
        etf_df['代码'] = etf_df['代码'].astype(str)
        for _, row in etf_df[etf_df['代码'].isin(codes_set)].iterrows():
            code = str(row['代码'])
            try:
                exchange_prices[code] = float(row['最新价'])
                exchange_changes[code] = float(row['涨跌幅'])
            except (ValueError, TypeError):
                pass
    except Exception:
        pass
    try:
        lof_df = ak.fund_lof_spot_em()
        lof_df['代码'] = lof_df['代码'].astype(str)
        for _, row in lof_df[lof_df['代码'].isin(codes_set)].iterrows():
            code = str(row['代码'])
            if code not in exchange_prices:  # ETF 优先
                try:
                    exchange_prices[code] = float(row['最新价'])
                    exchange_changes[code] = float(row['涨跌幅'])
                except (ValueError, TypeError):
                    pass
    except Exception:
        pass

    # 4. 申购状态 & 日额度
    print("  📡 增量：获取申购状态...", flush=True)
    purchase_data = _fetch_fund_purchase_status(codes)

    # 5. 构建结果（所有 QDII 基金都出现在结果里，缺失字段为 None）
    nav_map = open_nav.set_index('code')['nav'].to_dict() if not open_nav.empty else {}
    exch_nav_map = open_nav.set_index('code')['nav_ex'].to_dict() if 'nav_ex' in open_nav.columns else {}

    results = []
    for code in codes:
        code_s = str(code)
        nav = nav_map.get(code_s)
        if nav is None and code_s in exch_codes:
            nav = exch_nav_map.get(code_s)
        nav = pd.to_numeric(nav, errors='coerce') if nav is not None else None

        daily_change = exchange_changes.get(code_s)
        if daily_change is not None:
            daily_change = round(float(daily_change), 2)

        premium_discount = None
        if code_s in exchange_prices and nav is not None and float(nav) > 0:
            mp = exchange_prices[code_s]
            premium_discount = round((mp / float(nav) - 1) * 100, 2)

        purchase = purchase_data.get(code_s, {})
        purchase_status = purchase.get('purchase_status')
        daily_limit_raw = purchase.get('daily_limit')
        # 保持与 fetch_all() 一致的 daily_limit 类型（None 或原始值）
        daily_limit = daily_limit_raw if daily_limit_raw is not None else None

        results.append({
            'code': code_s,
            'nav': float(nav) if nav is not None else None,
            'daily_change': daily_change,
            'premium_discount': premium_discount,
            'purchase_status': purchase_status,
            'daily_limit': daily_limit,
        })

    print(f"  ✅ 增量采集完成，共 {len(results)} 条", flush=True)
    return results


def _fetch_fund_purchase_status(codes):
    """获取基金申购状态和日累计限定金额。

    使用 fund_purchase_em 接口（天天基金申购状态页面），一次性获取全量数据再过滤。
    返回 dict[str, dict]: code → {purchase_status, daily_limit}
    """
    try:
        df = ak.fund_purchase_em()
        sub = df[df['基金代码'].isin(codes)][['基金代码', '申购状态', '日累计限定金额']].copy()
        sub.rename(columns={'基金代码': 'code', '申购状态': 'purchase_status', '日累计限定金额': 'daily_limit'}, inplace=True)
        return {row['code']: {'purchase_status': row['purchase_status'], 'daily_limit': row['daily_limit']}
                for _, row in sub.iterrows()}
    except Exception:
        return {}


def _fetch_exchange_prices(codes):
    """获取场内基金（ETF/LOF）的实时交易价格和涨跌幅。

    用 AKShare 实时行情接口获取最新成交价和涨跌幅。
    ThreadPoolExecutor + timeout 防止网络请求挂死。
    返回 (dict[str, float], dict[str, float]): code → market_price, code → change_pct
    """
    prices = {}
    changes = {}
    codes_str = set(str(c) for c in codes)  # 统一为字符串，避免类型不匹配

    def _fetch(name, func):
        try:
            df = func()
            df['代码'] = df['代码'].astype(str)
            sub = df[df['代码'].isin(codes_str)]
            result_prices = {}
            result_changes = {}
            for _, row in sub.iterrows():
                try:
                    price = float(row['最新价'])
                    if price > 0:
                        result_prices[str(row['代码'])] = price
                except (ValueError, TypeError):
                    pass
                try:
                    chg = float(row['涨跌幅'])
                    result_changes[str(row['代码'])] = chg
                except (ValueError, TypeError, KeyError):
                    pass
            return result_prices, result_changes
        except Exception:
            return {}, {}

    with ThreadPoolExecutor(max_workers=2) as executor:
        etf_future = executor.submit(_fetch, 'ETF', ak.fund_etf_spot_em)
        lof_future = executor.submit(_fetch, 'LOF', ak.fund_lof_spot_em)

        for future, label in [(etf_future, 'ETF'), (lof_future, 'LOF')]:
            try:
                result_prices, result_changes = future.result(timeout=30)
                prices.update(result_prices)
                changes.update(result_changes)
            except TimeoutError:
                print(f"⏰ {label} 实时行情请求超时（30s），跳过")
            except Exception:
                pass

    return prices, changes




def fetch_all():
    """全量采集 QDII 基金数据，返回 list[dict]。

    返回字段: code, name, ftype, nav, acc_nav, ret_1y, ret_3y, ret_ann, est_date, upd_date
    """
    print("📡 开始采集 QDII 基金数据...", flush=True)

    all_funds_df = ak.fund_name_em()
    qdii_df = all_funds_df[all_funds_df['基金类型'].astype(str).str.contains('QDII|海外股票', na=False)].copy()
    qdii_df.rename(columns={'基金代码': 'code', '基金简称': 'name', '基金类型': 'ftype'}, inplace=True)
    qdii_codes = qdii_df['code'].unique().tolist()
    print(f"  ✅ 基金列表: 共 {len(qdii_codes)} 只 QDII 基金", flush=True)

    # 1. 开放式基金最新净值
    print("  📡 获取场外基金最新净值...", flush=True)
    all_daily_df = ak.fund_open_fund_daily_em()
    nav_col = _find_col(all_daily_df, '单位净值', qdii_codes)
    acc_nav_col = _find_col(all_daily_df, '累计净值', qdii_codes)
    open_nav = all_daily_df[all_daily_df['基金代码'].isin(qdii_codes)][
        ['基金代码', nav_col, acc_nav_col]
    ].copy()
    open_nav.rename(columns={'基金代码': 'code', nav_col: 'nav', acc_nav_col: 'acc_nav'}, inplace=True)

    # 2. 交易所基金数据（ETF/LOF）
    print("  📡 获取场内基金排名...", flush=True)
    exch_sub = pd.DataFrame()
    try:
        exch_df = ak.fund_exchange_rank_em()
        exch_sub = exch_df[exch_df['基金代码'].isin(qdii_codes)][
            ['基金代码', '单位净值', '累计净值', '近1年', '近3年', '成立来', '成立日期', '日期']
        ].copy()
        exch_sub.rename(columns={
            '基金代码': 'code', '单位净值': 'nav_ex', '累计净值': 'acc_nav_ex',
            '近1年': 'ret_1y_ex', '近3年': 'ret_3y_ex', '成立来': 'ret_ann_ex',
            '成立日期': 'est_date', '日期': 'upd_date'
        }, inplace=True)
    except Exception:
        pass

    # 3. 开放式基金排行榜（含涨跌幅）
    print("  📡 获取场外基金排行榜...", flush=True)
    rank_sub = pd.DataFrame()
    try:
        rank_df = ak.fund_open_fund_rank_em()
        rank_sub = rank_df[rank_df['基金代码'].isin(qdii_codes)][
            ['基金代码', '近1年', '近3年', '成立来']
        ].copy()
        rank_sub.rename(columns={
            '基金代码': 'code', '近1年': 'ret_1y_rank', '近3年': 'ret_3y_rank', '成立来': 'ret_ann_rank'
        }, inplace=True)
    except Exception:
        pass

    # 记录场内基金代码（ETF/LOF）
    exch_codes = set(exch_sub['code'].unique()) if not exch_sub.empty else set()
    print(f"  ✅ 场内基金 {len(exch_codes)} 只", flush=True)

    # 合并
    final_df = pd.merge(qdii_df, open_nav, on='code', how='left')
    if not rank_sub.empty:
        final_df = pd.merge(final_df, rank_sub, on='code', how='left')
    if not exch_sub.empty:
        final_df = pd.merge(final_df, exch_sub, on='code', how='left')

    # 空字符串 → pd.NA
    for col in ['nav', 'acc_nav']:
        if col in final_df.columns:
            final_df[col] = final_df[col].replace('', pd.NA)

    # 净值历史补充（成立日期 + 涨跌幅计算）
    print("  📡 获取净值历史（逐只，最慢环节）...", flush=True)
    extra_data = _fetch_fund_extra(qdii_codes)

    # 费用数据获取（自建请求：UA/Referer/timeout/重试，降并发防限流）
    print("  📡 获取基金费率...", flush=True)
    fee_data = _fetch_fund_fees(qdii_codes)

    # 填充 est_date
    final_df['est_date'] = final_df['code'].map(lambda c: extra_data[c]['est_date'] if extra_data.get(c) else None)

    # nav/acc_nav：open_nav 优先 → exchange 补缺
    if not exch_sub.empty:
        final_df['nav'] = final_df['nav'].fillna(final_df['nav_ex'])
        final_df['acc_nav'] = final_df['acc_nav'].fillna(final_df['acc_nav_ex'])

    # ret_1y/ret_3y：交易所排名（净值回报）→ 场外排名 → extra_data 兜底
    # 场内基金用交易所排名数据（基于净值计算，非交易价格）
    if not rank_sub.empty:
        final_df['ret_1y'] = pd.to_numeric(final_df['ret_1y_rank'], errors='coerce')
        final_df['ret_3y'] = pd.to_numeric(final_df['ret_3y_rank'], errors='coerce')
    if not exch_sub.empty:
        m = final_df['code'].isin(exch_codes)
        final_df.loc[m, 'ret_1y'] = pd.to_numeric(final_df.loc[m, 'ret_1y_ex'], errors='coerce')
        final_df.loc[m, 'ret_3y'] = pd.to_numeric(final_df.loc[m, 'ret_3y_ex'], errors='coerce')
    final_df['ret_1y'] = final_df['ret_1y'].fillna(
        final_df['code'].map(lambda c: extra_data[c]['ret_1y'] if extra_data.get(c) else None))
    final_df['ret_3y'] = final_df['ret_3y'].fillna(
        final_df['code'].map(lambda c: extra_data[c]['ret_3y'] if extra_data.get(c) else None))

    for yr in ['ret_2y', 'ret_4y', 'ret_5y', 'ret_10y']:
        final_df[yr] = final_df['code'].map(lambda c, y=yr: extra_data[c][y] if extra_data.get(c) else None)

    # total_ret：优先使用官方累计收益率 API（分红再投资的复权收益率），
    # 回退到 NAV 历史计算值
    print("  📡 获取官方累计收益率...", flush=True)
    cum_ret_data = {}
    pool = ThreadPoolExecutor(max_workers=1)
    future = pool.submit(_fetch_cumulative_returns, qdii_codes)
    try:
        cum_ret_data = future.result(timeout=180)
    except Exception:
        print("  ⏰ 累计收益率获取超时，跳过", flush=True)
        future.cancel()
    finally:
        pool.shutdown(wait=False)
    final_df['total_ret'] = final_df['code'].map(lambda c: cum_ret_data.get(c))
    nav_total_ret = final_df['code'].map(lambda c: extra_data[c]['total_ret'] if extra_data.get(c) else None)
    final_df['total_ret'] = final_df['total_ret'].fillna(nav_total_ret)

    # 填充费用数据
    for fee_key in ['mgmt_fee', 'cust_fee', 'sale_fee']:
        final_df[fee_key] = final_df['code'].map(lambda c: fee_data[c][fee_key] if fee_data.get(c) else None)

    # 申购状态和日累计限额
    print("  📡 获取申购状态...", flush=True)
    purchase_data = _fetch_fund_purchase_status(qdii_codes)
    final_df['purchase_status'] = final_df['code'].map(
        lambda c: purchase_data[c]['purchase_status'] if purchase_data.get(c) else None)
    final_df['daily_limit'] = final_df['code'].map(
        lambda c: purchase_data[c]['daily_limit'] if purchase_data.get(c) else None)

    # 日增长率：来自净值历史计算的最近一期涨跌
    final_df['daily_change'] = final_df['code'].map(
        lambda c: extra_data[c]['daily_change'] if extra_data.get(c) else None)

    # 清理辅助列
    drop_cols = [c for c in final_df.columns if c.endswith(('_ex', '_rank')) or c.startswith('src') or '拼音' in c]
    final_df.drop(columns=drop_cols, inplace=True, errors='ignore')

    # 标记场内/场外
    final_df['market'] = final_df['code'].apply(lambda c: '场内' if c in exch_codes else '场外')

    # 场内基金折溢价计算
    print("  📡 获取场内实时行情...", flush=True)
    exchange_prices, exchange_changes = _fetch_exchange_prices(exch_codes)
    final_df['premium_discount'] = None
    mask_ex = final_df['market'] == '场内'
    final_df.loc[mask_ex, 'premium_discount'] = final_df.loc[mask_ex].apply(
        lambda r: round((exchange_prices[r['code']] / float(r['nav']) - 1) * 100, 2)
        if r['code'] in exchange_prices
           and pd.notna(r['nav'])
           and float(r['nav']) > 0
        else None,
        axis=1
    )

    # 场内基金用实时涨跌幅覆盖日增长率
    if exchange_changes:
        final_df.loc[mask_ex, 'daily_change'] = final_df.loc[mask_ex, 'code'].map(exchange_changes)

    # 补充缺失列
    final_df['upd_date'] = date.today().isoformat()

    # ret_ann 从 total_ret 重新计算（upd_date 已就绪）
    for idx, row in final_df.iterrows():
        tr = row.get('total_ret')
        est = row.get('est_date')
        upd = row.get('upd_date')
        if tr is not None and est and upd and pd.notna(tr):
            try:
                years = (pd.to_datetime(upd) - pd.to_datetime(est)).days / 365.25
                if years > 0:
                    ann = (pow(1 + tr / 100, 1.0 / years) - 1) * 100
                    final_df.at[idx, 'ret_ann'] = round(ann, 2)
            except Exception:
                pass

    # 保留所有 QDII 基金，缺失字段留空

    print("  🔄 数据处理中...", flush=True)

    # 确保涨跌幅为 float，None 保留
    for col in ['ret_1y', 'ret_2y', 'ret_3y', 'ret_4y', 'ret_5y', 'ret_10y', 'ret_ann', 'total_ret', 'daily_change']:
        if col in final_df.columns:
            final_df[col] = pd.to_numeric(final_df[col], errors='coerce')

    # NaN → None（避免 JSON 序列化出 NaN）
    records = final_df.to_dict(orient='records')
    for r in records:
        for k, v in r.items():
            if isinstance(v, float) and v != v:
                r[k] = None
            elif v == '':
                r[k] = None

    print(f"✅ 采集完成，共 {len(records)} 条有效记录", flush=True)
    return records
