/* Personal wealth management UI. Vanilla JS, same-origin API. */
(() => {
'use strict';

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const MARKET_LABELS = {
    CN: 'A股', US: '美股', HK: '港股', FUND_CN: '场外基金',
    CRYPTO: '加密货币', BANK: '银行理财', MANUAL: '手动资产', CASH: '现金',
};
const CLASS_LABELS = {
    stock: '股票', etf: '场内基金', fund: '基金', crypto: '加密货币',
    bank_wm: '银行理财', other: '其他', cash: '现金',
};
const TYPE_LABELS = {
    buy: '买入', sell: '卖出', dividend: '分红', reinvest: '分红再投',
    interest: '利息', fee: '费用', split: '拆分',
};
const SOURCE_LABELS = { manual: '手动', import: '导入', dca: '定投' };
const STATUS_LABELS = {
    awaiting_price: '等价格', filled: '已入账', skipped: '已跳过', failed: '失败',
};
const WEEKDAYS = ['周一', '周二', '周三', '周四', '周五', '周六', '周日'];

const state = {
    summary: null,
    positions: [],
    assets: [],
    accounts: [],
    plans: [],
    orders: [],
    orderTotal: 0,
    transactions: [],
    txMeta: { total: 0, page: 1, per_page: 100 },
    history: { points: [] },
    status: null,
    range: '1y',
    tab: 'overview',
    txPage: 1,
    orderFilter: '',
    charts: {},
    planPreview: null,
};

/* ── formatting ─────────────────────────────────────────────── */
function numberOrNull(value) {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
}

function fmtNum(value, digits = 2) {
    const n = numberOrNull(value);
    if (n === null) return '—';
    return n.toLocaleString('zh-CN', {
        minimumFractionDigits: digits, maximumFractionDigits: digits,
    });
}

function fmtMoney(value, digits = 2) {
    const n = numberOrNull(value);
    if (n === null) return '—';
    return '¥' + n.toLocaleString('zh-CN', {
        minimumFractionDigits: digits, maximumFractionDigits: digits,
    });
}

function fmtCcy(value, currency = 'CNY', digits = 2) {
    const n = numberOrNull(value);
    if (n === null) return '—';
    const symbol = { CNY: '¥', USD: '$', HKD: 'HK$' }[currency] || '';
    return symbol + n.toLocaleString('zh-CN', {
        minimumFractionDigits: digits, maximumFractionDigits: digits,
    });
}

function fmtPct(value, digits = 2) {
    const n = numberOrNull(value);
    if (n === null) return '—';
    return (n * 100).toFixed(digits) + '%';
}

function fmtSignedPct(value, digits = 2) {
    const n = numberOrNull(value);
    if (n === null) return '—';
    return (n >= 0 ? '+' : '') + (n * 100).toFixed(digits) + '%';
}

function fmtXirr(value, digits = 2) {
    const n = numberOrNull(value);
    // Annualizing a few days of returns produces absurd numbers; hide them.
    if (n === null || Math.abs(n) > 100) return '—';
    return fmtSignedPct(n, digits);
}

function fmtCompact(value) {
    const n = numberOrNull(value);
    if (n === null) return '—';
    if (Math.abs(n) >= 1e8) return (n / 1e8).toFixed(2) + '亿';
    if (Math.abs(n) >= 1e4) return (n / 1e4).toFixed(1) + '万';
    return n.toFixed(0);
}

function todayIso() {
    const parts = new Intl.DateTimeFormat('en-CA', {
        timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    }).formatToParts(new Date());
    const part = type => parts.find(p => p.type === type)?.value || '';
    return `${part('year')}-${part('month')}-${part('day')}`;
}

function esc(value) {
    return String(value ?? '').replace(/[&<>"']/g, c => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
    }[c]));
}

function pnlClass(value) {
    const n = numberOrNull(value) || 0;
    return n > 0 ? 'pos' : n < 0 ? 'neg' : '';
}

function typeTag(type) {
    const cls = type === 'buy' || type === 'reinvest' ? 'tag-buy'
        : type === 'sell' ? 'tag-sell' : '';
    return `<span class="tag ${cls}">${esc(TYPE_LABELS[type] || type)}</span>`;
}

function sourceTag(source) {
    if (source === 'dca') return '<span class="tag tag-dca">定投</span>';
    return `<span class="tag">${esc(SOURCE_LABELS[source] || source || '手动')}</span>`;
}

function statusTag(status) {
    return `<span class="tag tag-${esc(status)}">${esc(STATUS_LABELS[status] || status)}</span>`;
}

/* ── API ────────────────────────────────────────────────────── */
async function api(path, options = {}) {
    const init = { credentials: 'same-origin', ...options };
    if (init.body && !(init.body instanceof FormData) && typeof init.body !== 'string') {
        init.headers = { 'Content-Type': 'application/json', ...(init.headers || {}) };
        init.body = JSON.stringify(init.body);
    }
    const response = await fetch(path, init);
    if (response.status === 401) {
        location.href = '/wealth/login?next=' + encodeURIComponent(location.pathname);
        throw new Error('未登录');
    }
    const contentType = response.headers.get('content-type') || '';
    const data = contentType.includes('application/json')
        ? await response.json().catch(() => ({ error: '响应解析失败' }))
        : await response.text();
    if (!response.ok) {
        const err = new Error((data && data.error) || `HTTP ${response.status}`);
        err.status = response.status;
        err.data = data;
        throw err;
    }
    return data;
}

function toast(message, kind = '') {
    const el = $('#toast');
    el.textContent = message;
    el.className = 'toast show ' + kind;
    clearTimeout(toast._timer);
    toast._timer = setTimeout(() => { el.className = 'toast ' + kind; }, 3200);
}

/* ── boot / refresh ─────────────────────────────────────────── */
async function boot() {
    bindEvents();
    await refreshAll();
    await loadTransactions(1);
    setTimeout(() => resizeCharts(), 80);
}

async function refreshAll() {
    try {
        const [summary, positions, assets, accounts, plans, orders, status] = await Promise.all([
            api('/api/wm/summary?range=' + encodeURIComponent(state.range)),
            api('/api/wm/positions'),
            api('/api/wm/assets'),
            api('/api/wm/accounts'),
            api('/api/wm/dca/plans'),
            api('/api/wm/dca/orders' + (state.orderFilter ? '?status=' + encodeURIComponent(state.orderFilter) : '')),
            api('/api/wm/status'),
        ]);
        state.summary = summary;
        state.positions = positions.positions || [];
        state.assets = assets || [];
        state.accounts = accounts || [];
        state.plans = plans || [];
        state.orders = orders.orders || [];
        state.orderTotal = orders.total || 0;
        state.status = status;
        renderAll();
        await loadHistory();
    } catch (err) {
        toast(err.message, 'error');
    }
}

async function loadHistory() {
    try {
        state.history = await api('/api/wm/history?range=' + encodeURIComponent(state.range));
        renderHistory();
        if (state.tab === 'reports') renderMonthly();
    } catch (err) {
        toast(err.message, 'error');
    }
}

async function loadOrders() {
    try {
        const query = state.orderFilter ? '?status=' + encodeURIComponent(state.orderFilter) : '';
        const data = await api('/api/wm/dca/orders' + query);
        state.orders = data.orders || [];
        state.orderTotal = data.total || 0;
        renderOrders();
    } catch (err) {
        toast(err.message, 'error');
    }
}

async function loadTransactions(page = 1) {
    try {
        const params = new URLSearchParams({ page: String(page), per_page: '100' });
        if ($('#txAssetFilter')?.value) params.set('asset_id', $('#txAssetFilter').value);
        if ($('#txAccountFilter')?.value) params.set('account_id', $('#txAccountFilter').value);
        if ($('#txTypeFilter')?.value) params.set('type', $('#txTypeFilter').value);
        if ($('#txStart')?.value) params.set('start', $('#txStart').value);
        if ($('#txEnd')?.value) params.set('end', $('#txEnd').value);
        const data = await api('/api/wm/transactions?' + params.toString());
        state.transactions = data.transactions || [];
        state.txMeta = { total: data.total || 0, page: data.page || page, per_page: data.per_page || 100 };
        state.txPage = state.txMeta.page;
        renderTransactions();
    } catch (err) {
        toast(err.message, 'error');
    }
}

/* ── rendering ──────────────────────────────────────────────── */
function renderAll() {
    renderMetrics();
    renderAssetFilters();
    renderAccountFilters();
    renderHoldings();
    renderAccounts();
    renderPlans();
    renderOrders();
    renderAssets();
    renderUpcoming();
    renderStatus();
    renderReports();
    renderAlloc();
}

function renderMetrics() {
    const s = state.summary;
    if (!s) return;
    $('#asOf').textContent = s.as_of || '-';
    $('#mTotal').textContent = fmtMoney(s.total_value);
    const fxMissing = state.positions.filter(p => p.fx_missing).length;
    const sub = [`证券 ${fmtMoney(s.securities_value)}`];
    if (Number(s.cash_value || 0) !== 0) sub.push(`现金 ${fmtMoney(s.cash_value)}`);
    sub.push(`${s.position_count || 0} 个持仓`);
    if (s.unpriced?.length) sub.push(`${s.unpriced.length} 个缺价`);
    if (fxMissing) sub.push(`${fxMissing} 个持仓缺汇率`);
    if (Number(s.cash_fx_missing || 0)) sub.push(`${s.cash_fx_missing} 项账户现金缺汇率`);
    $('#mTotalSub').textContent = sub.join(' · ');
    $('#mNet').textContent = fmtMoney(s.net_invested);
    $('#mPnl').textContent = fmtMoney(s.total_pnl);
    $('#mPnl').className = pnlClass(s.total_pnl);
    $('#mPnlSub').textContent = s.total_pnl_pct === null || s.total_pnl_pct === undefined
        ? '证券成本口径' : fmtSignedPct(s.total_pnl_pct) + ' / 证券净投入';
    $('#mToday').textContent = fmtMoney(s.today_pnl);
    $('#mToday').className = pnlClass(s.today_pnl);
    $('#mXirr').textContent = fmtXirr(s.xirr);
    $('#mXirr').className = pnlClass(s.xirr);
}

function renderAssetFilters() {
    const select = $('#txAssetFilter');
    if (select) {
        const current = select.value;
        select.innerHTML = '<option value="">全部资产</option>' + state.assets
            .map(a => `<option value="${a.id}">${esc(a.name)} (${esc(a.symbol)})</option>`)
            .join('');
        if (current) select.value = current;
    }
}

function fillAccountFilter(select) {
    if (!select) return;
    const current = select.value;
    const options = state.accounts.map(a =>
        `<option value="${a.id}">${esc(a.name)}${a.is_active ? '' : '（停用）'}</option>`);
    select.innerHTML = '<option value="">全部账户</option>' + options.join('') +
        '<option value="none">无账户</option>';
    if (current) select.value = current;
}

function renderAccountFilters() {
    fillAccountFilter($('#txAccountFilter'));
    fillAccountFilter($('#holdingsAccountFilter'));
}

function renderHoldings() {
    const body = $('#holdingsBody');
    const accountFilter = $('#holdingsAccountFilter')?.value || '';
    let rows = state.positions.filter(p => p.has_position);
    if (accountFilter) {
        rows = rows.filter(p => accountFilter === 'none'
            ? !p.account_id
            : String(p.account_id) === accountFilter);
    }
    const viewTotal = rows.reduce((sum, p) => sum + (Number(p.market_value_cny) || 0), 0);
    $('#holdingsHint').textContent = rows.length
        ? `${rows.length} 个持仓，合计 ${fmtMoney(viewTotal)}`
        : '';
    if (!rows.length) {
        body.innerHTML = '<tr><td colspan="11"><div class="empty">还没有持仓。先添加资产，再记一笔买入。</div></td></tr>';
        return;
    }
    body.innerHTML = rows.map(p => {
        const bank = p.valuation_mode === 'accrual';
        const sub = bank
            ? `年化 ${fmtNum((p.annual_rate || 0) * 100, 2)}% · 已计提 ${fmtMoney(p.accrued_interest)}`
            : `${esc(MARKET_LABELS[p.market] || p.market)} · ${esc(CLASS_LABELS[p.asset_class] || p.asset_class)}`;
        const priceText = bank ? '—' : fmtNum(p.price, 4);
        const priceSub = bank ? '按日计提' : (p.price_date ? `<span class="muted">${esc(p.price_date)}</span>` : '<span class="neg">缺价格</span>');
        return `<tr>
            <td><div class="asset-cell"><span class="name">${esc(p.name)}</span><span class="code">${esc(p.symbol)}</span></div><div class="muted" style="font-size:.68rem">${sub}</div></td>
            <td>${esc(p.account_name || '无账户')}</td>
            <td>${esc(MARKET_LABELS[p.market] || p.market)}</td>
            <td class="num">${bank ? fmtMoney(p.principal) : fmtNum(p.units, 4)}</td>
            <td class="num">${bank ? '—' : fmtNum(p.avg_cost_native, 4)}</td>
            <td class="num">${priceText}<div style="font-size:.66rem">${priceSub}</div></td>
            <td class="num">${fmtMoney(p.market_value_cny)}${p.fx_missing ? '<div class="neg" style="font-size:.66rem">缺汇率</div>' : ''}</td>
            <td class="num ${pnlClass(p.pnl_cny)}">${fmtMoney(p.pnl_cny)}<div style="font-size:.66rem">${p.pnl_pct === null ? '' : fmtSignedPct(p.pnl_pct)}</div></td>
            <td class="num ${pnlClass(p.day_pnl_cny)}">${fmtMoney(p.day_pnl_cny)}</td>
            <td class="num">${fmtPct(p.weight)}</td>
            <td><div class="row-actions">
                <button class="btn btn-ghost btn-sm" data-action="edit-holding" data-asset="${p.asset_id}" data-account="${p.account_id ?? ''}">编辑</button>
                <button class="btn btn-ghost btn-sm" data-action="add-tx" data-asset="${p.asset_id}">加仓</button>
                ${bank ? '' : `<button class="btn btn-ghost btn-sm" data-action="asset-price" data-asset="${p.asset_id}">价格</button>`}
                ${p.quote_source && p.quote_source !== 'manual' && p.quote_source !== 'accrual' ? `<button class="btn btn-ghost btn-sm" data-action="backfill-asset" data-asset="${p.asset_id}">同步</button>` : ''}
            </div></td>
        </tr>`;
    }).join('');
}

function renderAccounts() {
    const body = $('#accountsBody');
    if (!body) return;
    const agg = new Map();
    for (const p of state.positions) {
        if (!p.has_position) continue;
        const key = p.account_id == null ? 'none' : String(p.account_id);
        const cur = agg.get(key) || { securities: 0, cost: 0, pnl: 0, count: 0 };
        cur.securities += Number(p.market_value_cny) || 0;
        cur.cost += Number(p.cost_cny) || 0;
        cur.pnl += Number(p.pnl_cny) || 0;
        cur.count += 1;
        agg.set(key, cur);
    }
    const rows = state.accounts.map(a => {
        const stats = agg.get(String(a.id)) || { securities: 0, pnl: 0, count: 0 };
        const canDelete = !a.tx_count && !a.plan_count;
        const balances = Array.isArray(a.cash_balances) ? a.cash_balances : [];
        let cashText = '—';
        if (balances.length) {
            cashText = balances.map(b => {
                const currency = b.currency || a.currency || 'CNY';
                const amount = Number(b.amount) || 0;
                const cny = Number(b.cny_value) || 0;
                return `<div class="${amount < 0 ? 'neg' : ''}">${fmtCcy(amount, currency)}`
                    + (currency !== 'CNY'
                        ? `<span class="muted" style="font-size:.66rem"> ≈ ${fmtMoney(cny)}</span>` : '')
                    + (b.fx_missing ? ' <span class="neg" style="font-size:.66rem">缺汇率</span>' : '')
                    + `</div>`;
            }).join('');
            if (balances.length > 1) {
                cashText += `<div class="muted" style="font-size:.66rem">合计 ${fmtMoney(a.cash_cny)}</div>`;
            } else if (balances[0].balance_date) {
                cashText += `<div class="muted" style="font-size:.64rem">${esc(balances[0].balance_date)}</div>`;
            }
        }
        return `<tr>
            <td><div class="asset-cell"><span class="name">${esc(a.name)}</span><span class="code">${a.tx_count || 0} 笔交易 · ${a.plan_count || 0} 个计划</span></div></td>
            <td>${esc(a.broker || '—')}</td>
            <td>${esc(a.currency || 'CNY')}</td>
            <td class="num">${cashText}</td>
            <td class="num">${fmtMoney(stats.securities)}</td>
            <td class="num ${pnlClass(stats.pnl)}">${fmtMoney(stats.pnl)}</td>
            <td class="num">${a.tx_count || 0}</td>
            <td class="num">${a.plan_count || 0}</td>
            <td>${a.is_active ? '<span class="tag tag-filled">启用</span>' : '<span class="tag tag-skipped">停用</span>'}</td>
            <td><div class="row-actions">
                <button class="btn btn-ghost btn-sm" data-action="edit-account" data-account="${a.id}">编辑</button>
                <button class="btn btn-ghost btn-sm" data-action="toggle-account" data-account="${a.id}" data-active="${a.is_active ? 1 : 0}">${a.is_active ? '停用' : '启用'}</button>
                <button class="btn btn-ghost btn-sm" data-action="account-transactions" data-account="${a.id}">交易</button>
                ${canDelete ? `<button class="btn btn-danger btn-sm" data-action="delete-account" data-account="${a.id}">删除</button>` : ''}
            </div></td>
        </tr>`;
    });
    const unassigned = agg.get('none');
    if (unassigned) {
        rows.push(`<tr>
            <td><div class="asset-cell"><span class="name">无账户</span><span class="code">未指定投资账户的交易</span></div></td>
            <td>—</td><td>CNY</td><td class="num">—</td>
            <td class="num">${fmtMoney(unassigned.securities)}</td>
            <td class="num ${pnlClass(unassigned.pnl)}">${fmtMoney(unassigned.pnl)}</td>
            <td class="num">—</td><td class="num">—</td><td>—</td>
            <td><div class="row-actions"><button class="btn btn-ghost btn-sm" data-action="account-transactions" data-account="none">交易</button></div></td>
        </tr>`);
    }
    body.innerHTML = rows.length
        ? rows.join('')
        : '<tr><td colspan="10"><div class="empty">还没有账户。</div></td></tr>';
    const totalCash = state.accounts.reduce((sum, a) => sum + (Number(a.cash_cny) || 0), 0);
    const totalSec = Array.from(agg.values()).reduce((sum, a) => sum + a.securities, 0);
    const active = state.accounts.filter(a => a.is_active).length;
    $('#accountsHint').textContent =
        `${state.accounts.length} 个账户（${active} 个启用） · 现金 ${fmtMoney(totalCash)} · 证券 ${fmtMoney(totalSec)}`;
}

function frequencyText(plan) {
    if (plan.frequency === 'daily') return '每个交易日';
    if (plan.frequency === 'weekly') return `每周${WEEKDAYS[plan.weekday ?? 0]}`;
    if (plan.frequency === 'biweekly') return `每两周${WEEKDAYS[plan.weekday ?? 0]}`;
    return `每月 ${plan.day_of_month || 1} 日`;
}

function renderPlans() {
    const wrap = $('#planList');
    if (!state.plans.length) {
        wrap.innerHTML = '<div class="empty">还没有定投计划。新建后会自动生成到期订单并记账。</div>';
        return;
    }
    wrap.innerHTML = state.plans.map(plan => {
        const paused = plan.status === 'paused';
        const finished = plan.status === 'finished';
        const counts = [];
        if (plan.waiting_count) counts.push(`<span class="tag tag-awaiting_price">待成交 ${plan.waiting_count}</span>`);
        if (plan.failed_count) counts.push(`<span class="tag tag-failed">失败 ${plan.failed_count}</span>`);
        if (plan.skipped_count) counts.push(`<span class="tag tag-skipped">跳过 ${plan.skipped_count}</span>`);
        return `<div class="plan-card ${paused ? 'paused' : ''}">
            <div class="plan-top">
                <div>
                    <h3>${esc(plan.name)}</h3>
                    <div class="plan-sub">${esc(plan.asset_name || '')} · ${esc(plan.symbol || '')}${plan.account_name ? ' · ' + esc(plan.account_name) : ''} · ${frequencyText(plan)}</div>
                </div>
                <span class="tag ${paused ? 'tag-skipped' : finished ? 'tag-filled' : 'tag-dca'}">${paused ? '已暂停' : finished ? '已结束' : '运行中'}</span>
            </div>
            <div class="plan-amount">${fmtCcy(plan.amount, plan.amount_currency || 'CNY')} <small>/ 期</small></div>
            <div class="plan-stats">
                <div><span>累计投入</span><b>${fmtMoney(plan.invested)}</b></div>
                <div><span>当前市值</span><b>${fmtMoney(plan.value)}</b></div>
                <div><span>估算盈亏</span><b class="${pnlClass(plan.pnl)}">${fmtMoney(plan.pnl)}</b></div>
                <div><span>XIRR</span><b class="${pnlClass(plan.xirr)}">${fmtXirr(plan.xirr)}</b></div>
                <div><span>已投期数</span><b>${plan.filled_count || 0}</b></div>
                <div><span>下次计划</span><b>${esc(plan.next_due || '—')}</b></div>
            </div>
            <div class="plan-foot">
                <div style="display:flex;gap:4px;flex-wrap:wrap">${counts.join('') || '<span class="muted">自动入账</span>'}</div>
                <div class="plan-actions">
                    ${paused ? `<button class="btn btn-ghost btn-sm" data-action="resume-plan" data-plan="${plan.id}">恢复</button>` : ''}
                    ${!paused && !finished ? `<button class="btn btn-ghost btn-sm" data-action="run-plan" data-plan="${plan.id}">立即执行</button>` : ''}
                    <button class="btn btn-ghost btn-sm" data-action="edit-plan" data-plan="${plan.id}">编辑</button>
                    ${!paused && !finished ? `<button class="btn btn-ghost btn-sm" data-action="pause-plan" data-plan="${plan.id}">暂停</button>` : ''}
                    <button class="btn btn-danger btn-sm" data-action="archive-plan" data-plan="${plan.id}">归档</button>
                </div>
            </div>
        </div>`;
    }).join('');
}

function renderOrders() {
    const body = $('#ordersBody');
    if (!state.orders.length) {
        body.innerHTML = '<tr><td colspan="9"><div class="empty">暂无执行记录</div></td></tr>';
        return;
    }
    body.innerHTML = state.orders.map(o => {
        const actions = [];
        if (o.status === 'awaiting_price' || o.status === 'failed') actions.push(`<button class="btn btn-ghost btn-sm" data-action="skip-order" data-order="${o.id}">跳过</button>`);
        if (o.status === 'failed' || o.status === 'skipped') actions.push(`<button class="btn btn-ghost btn-sm" data-action="retry-order" data-order="${o.id}">重试</button>`);
        return `<tr>
            <td>${esc(o.due_date)}</td>
            <td>${esc(o.trade_date || o.price_date || '—')}</td>
            <td><div class="asset-cell"><span class="name">${esc(o.plan_name)}</span><span class="code">${esc(o.symbol)} · ${esc(o.asset_name)}</span></div></td>
            <td class="num">${fmtCcy(o.amount, o.amount_currency)}</td>
            <td class="num">${o.price ? fmtNum(o.price, 4) : '—'}</td>
            <td class="num">${o.units ? fmtNum(o.units, 4) : '—'}</td>
            <td>${statusTag(o.status)}</td>
            <td class="muted" style="max-width:220px">${esc(o.message || '')}</td>
            <td><div class="row-actions">${actions.join('')}</div></td>
        </tr>`;
    }).join('');
}

function renderTransactions() {
    const body = $('#txBody');
    if (!state.transactions.length) {
        body.innerHTML = '<tr><td colspan="11"><div class="empty">暂无交易</div></td></tr>';
    } else {
        body.innerHTML = state.transactions.map(t => {
            const editable = t.source !== 'dca';
            return `<tr>
                <td>${esc(t.trade_date)}</td>
                <td><div class="asset-cell"><span class="name">${esc(t.asset_name)}</span><span class="code">${esc(t.symbol)}</span></div></td>
                <td>${typeTag(t.type)}</td>
                <td class="num">${t.units ? fmtNum(t.units, 4) : '—'}</td>
                <td class="num">${t.price ? fmtNum(t.price, 4) : '—'}</td>
                <td class="num">${fmtCcy(t.amount, t.currency)}</td>
                <td class="num">${fmtCcy(t.fee, t.currency)}</td>
                <td>${sourceTag(t.source)}</td>
                <td>${esc(t.account_name || '—')}</td>
                <td class="muted" style="max-width:200px">${esc(t.note || '')}</td>
                <td><div class="row-actions">
                    ${editable ? `<button class="btn btn-ghost btn-sm" data-action="edit-tx" data-tx="${t.id}">编辑</button>
                    <button class="btn btn-danger btn-sm" data-action="delete-tx" data-tx="${t.id}">删除</button>` : '<span class="muted">自动</span>'}
                </div></td>
            </tr>`;
        }).join('');
    }
    const meta = state.txMeta;
    const pages = Math.max(1, Math.ceil(meta.total / meta.per_page));
    $('#txPager').innerHTML = meta.total
        ? `<span>共 ${meta.total} 条 · 第 ${meta.page}/${pages} 页</span>
           <button class="btn btn-ghost btn-sm" data-page="${meta.page - 1}" ${meta.page <= 1 ? 'disabled' : ''}>上一页</button>
           <button class="btn btn-ghost btn-sm" data-page="${meta.page + 1}" ${meta.page >= pages ? 'disabled' : ''}>下一页</button>`
        : '';
}

function renderAssets() {
    const body = $('#assetsBody');
    if (!state.assets.length) {
        body.innerHTML = '<tr><td colspan="7"><div class="empty">还没有资产。添加股票、基金、加密货币或银行理财。</div></td></tr>';
        return;
    }
    body.innerHTML = state.assets.map(a => {
        const canDelete = !a.plan_count;
        const isAccrual = a.quote_source === 'accrual' || a.valuation_mode === 'accrual';
        const sourceLabel = { eastmoney: '东方财富', fund_em: '基金净值', binance: 'Binance', manual: '手动', accrual: '按日计提' }[a.quote_source] || a.quote_source;
        return `<tr>
            <td><div class="asset-cell"><span class="name">${esc(a.name)} ${a.is_active ? '' : '<span class="tag tag-skipped">停用</span>'}</span><span class="code">${esc(a.symbol)}</span></div></td>
            <td>${esc(MARKET_LABELS[a.market] || a.market)} / ${esc(CLASS_LABELS[a.asset_class] || a.asset_class)}</td>
            <td>${esc(sourceLabel)}</td>
            <td class="num">${a.last_price === null || a.last_price === undefined ? '—' : fmtNum(a.last_price, 4)}</td>
            <td>${esc(a.last_price_date || '—')}</td>
            <td class="muted" style="max-width:240px">${a.last_error ? `<span class="neg">${esc(a.last_error)}</span>` : `${a.tx_count || 0} 笔交易 · ${a.plan_count || 0} 个计划`}</td>
            <td><div class="row-actions">
                ${a.quote_source !== 'manual' && a.quote_source !== 'accrual' ? `<button class="btn btn-ghost btn-sm" data-action="backfill-asset" data-asset="${a.id}">同步历史</button>` : ''}
                ${isAccrual ? '' : `<button class="btn btn-ghost btn-sm" data-action="asset-price" data-asset="${a.id}">手工价格</button>`}
                <button class="btn btn-ghost btn-sm" data-action="edit-asset" data-asset="${a.id}">编辑</button>
                <button class="btn btn-ghost btn-sm" data-action="toggle-asset" data-asset="${a.id}" data-active="${a.is_active ? 1 : 0}">${a.is_active ? '停用' : '启用'}</button>
                ${canDelete ? `<button class="btn btn-danger btn-sm" data-action="delete-asset" data-asset="${a.id}" data-force="${a.tx_count ? 1 : 0}">删除</button>` : ''}
            </div></td>
        </tr>`;
    }).join('');
}

function renderUpcoming() {
    const box = $('#upcomingList');
    const upcoming = state.plans
        .filter(p => p.status === 'active' && p.next_due)
        .sort((a, b) => String(a.next_due).localeCompare(String(b.next_due)))
        .slice(0, 5);
    if (!upcoming.length) {
        box.innerHTML = '<div class="empty">暂无进行中的定投</div>';
        return;
    }
    box.innerHTML = upcoming.map(p => `
        <div class="list-row">
            <div class="left">
                <div class="title">${esc(p.name)}</div>
                <div class="sub">${esc(p.asset_name || '')} · ${frequencyText(p)}</div>
            </div>
            <div class="right">
                <div>${fmtCcy(p.amount, p.amount_currency || 'CNY')}</div>
                <div class="sub">${esc(p.next_due)}</div>
            </div>
        </div>`).join('');
}

function renderStatus() {
    const box = $('#statusBox');
    const s = state.status || {};
    const counts = s.counts || {};
    const last = s.last_sync;
    const logout = $('#logoutBtn');
    if (logout) logout.style.display = s.configured ? '' : 'none';
    const rows = [];
    if (counts.pending_orders) {
        rows.push(`<div class="list-row"><div class="left"><div class="title">等待价格的定投订单</div><div class="sub">净值/收盘价发布后自动入账</div></div><div class="right"><span class="tag tag-awaiting_price">${counts.pending_orders}</span></div></div>`);
    }
    if (counts.failed_orders) {
        rows.push(`<div class="list-row"><div class="left"><div class="title">失败的定投订单</div><div class="sub">可在定投页重试或跳过</div></div><div class="right"><span class="tag tag-failed">${counts.failed_orders}</span></div></div>`);
    }
    rows.push(`<div class="list-row"><div class="left"><div class="title">最近行情同步</div><div class="sub">${last ? esc((last.finished_at || last.started_at || '').replace('T', ' ')) : '尚未同步'}</div></div><div class="right">${last ? `<span class="tag ${last.status === 'done' ? 'tag-filled' : 'tag-failed'}">${esc(last.status)}</span><div class="sub">${last.count || 0} 个资产</div>` : '<span class="muted">—</span>'}</div></div>`);
    if (last?.message) {
        rows.push(`<div class="list-row"><div class="left"><div class="sub neg">${esc(last.message)}</div></div></div>`);
    }
    rows.push(`<div class="list-row"><div class="left"><div class="title">手动操作</div><div class="sub">定投生成 + 结算，或同步全部行情</div></div><div class="right" style="display:flex;gap:5px"><button class="btn btn-ghost btn-sm" data-action="run-dca">运行定投</button><button class="btn btn-ghost btn-sm" data-action="sync-all">同步行情</button></div></div>`);
    box.innerHTML = rows.join('');
}

function renderReports() {
    const s = state.summary;
    if (!s) return;
    $('#rXirr').textContent = fmtXirr(s.xirr);
    $('#rXirr').className = pnlClass(s.xirr);
    $('#rTwr').textContent = s.twr === null || s.twr === undefined ? '—' : fmtSignedPct(s.twr);
    $('#rTwr').className = pnlClass(s.twr);
    $('#rMdd').textContent = s.max_drawdown === null || s.max_drawdown === undefined ? '—' : fmtPct(s.max_drawdown);
    $('#rMdd').className = 'neg';
    $('#rRealized').textContent = fmtMoney(s.realized_pnl);
    $('#rRealized').className = pnlClass(s.realized_pnl);
}

/* ── charts ─────────────────────────────────────────────────── */
const CHART_COLORS = ['#f59e0b', '#60a5fa', '#34d399', '#a78bfa', '#f472b6', '#f87171', '#22d3ee', '#facc15'];

function getChart(id) {
    const el = document.getElementById(id);
    if (!el) return null;
    if (!state.charts[id]) state.charts[id] = echarts.init(el, null, { renderer: 'canvas' });
    return state.charts[id];
}

function renderAlloc() {
    const chart = getChart('allocChart');
    if (!chart || !state.summary) return;
    const key = $('#allocBy')?.value || 'asset_class';
    const data = (state.summary.allocations || {})[key] || [];
    const label = d => key === 'asset_class' ? (CLASS_LABELS[d.name] || d.name)
        : key === 'market' ? (MARKET_LABELS[d.name] || d.name)
        : d.name;
    const empty = !data.length;
    chart.setOption({
        color: CHART_COLORS,
        tooltip: {
            trigger: 'item',
            backgroundColor: '#16212d', borderColor: '#223041', textStyle: { color: '#e8eef5', fontSize: 12 },
            formatter: p => `${esc(p.name)}<br><b>${fmtMoney(p.value)}</b> · ${p.percent.toFixed(1)}%`,
        },
        legend: {
            bottom: 0, left: 'center', type: 'scroll',
            textStyle: { color: '#93a2b4', fontSize: 11 },
            itemWidth: 10, itemHeight: 10,
        },
        series: [{
            type: 'pie',
            radius: ['42%', '68%'],
            center: ['50%', '45%'],
            avoidLabelOverlap: true,
            itemStyle: { borderColor: '#121a24', borderWidth: 2 },
            label: { color: '#93a2b4', fontSize: 10, formatter: '{b}\n{d}%' },
            labelLine: { lineStyle: { color: '#223041' } },
            data: data.map(d => ({ name: label(d), value: d.value })),
        }],
        title: empty ? {
            text: '暂无持仓', left: 'center', top: '42%',
            textStyle: { color: '#65758a', fontSize: 12, fontWeight: 'normal' },
        } : undefined,
        graphic: empty ? [] : undefined,
    }, true);
}

function renderHistory() {
    const chart = getChart('historyChart');
    if (!chart) return;
    const points = state.history?.points || [];
    if (!points.length) {
        chart.setOption({
            title: { text: '暂无数据', left: 'center', top: 'middle', textStyle: { color: '#65758a', fontSize: 12, fontWeight: 'normal' } },
            xAxis: { show: false }, yAxis: { show: false }, series: [],
        }, true);
        return;
    }
    const dates = points.map(p => p.date);
    const values = points.map(p => p.value);
    const costs = points.map(p => p.cost);
    chart.setOption({
        color: ['#f59e0b', '#60a5fa'],
        tooltip: {
            trigger: 'axis', backgroundColor: '#16212d', borderColor: '#223041',
            textStyle: { color: '#e8eef5', fontSize: 12 },
            formatter: params => {
                const date = params[0]?.axisValue || '';
                return `${date}<br>` + params.map(p => `${p.marker}${p.seriesName}: <b>${fmtMoney(p.value)}</b>`).join('<br>');
            },
        },
        legend: { top: 0, right: 0, textStyle: { color: '#93a2b4', fontSize: 11 }, itemWidth: 12, itemHeight: 8 },
        grid: { left: 52, right: 16, top: 34, bottom: 30 },
        xAxis: {
            type: 'category', data: dates, boundaryGap: false,
            axisLine: { lineStyle: { color: '#223041' } },
            axisLabel: { color: '#65758a', fontSize: 10, hideOverlap: true },
            axisTick: { show: false },
        },
        yAxis: {
            type: 'value', scale: true,
            splitLine: { lineStyle: { color: '#1a2634' } },
            axisLabel: { color: '#65758a', fontSize: 10, formatter: v => fmtCompact(v) },
        },
        dataZoom: [{ type: 'inside', throttle: 50 }],
        series: [
            {
                name: '证券市值', type: 'line', data: values, smooth: true, showSymbol: false,
                lineStyle: { width: 2.2 }, areaStyle: { opacity: 0.12 },
                emphasis: { focus: 'series' },
            },
            {
                name: '证券成本', type: 'line', data: costs, smooth: true, showSymbol: false,
                lineStyle: { width: 1.2, type: 'dashed' }, emphasis: { focus: 'series' },
            },
        ],
    }, true);
}

function monthlyReturns(points) {
    const groups = new Map();
    for (const p of points) {
        const month = p.date.slice(0, 7);
        if (!groups.has(month)) groups.set(month, []);
        groups.get(month).push(p);
    }
    const result = [];
    for (const [month, arr] of groups) {
        const first = arr[0];
        const last = arr[arr.length - 1];
        const flows = arr.slice(1).reduce((sum, p) => sum + (Number(p.net_flow) || 0), 0);
        const ret = first.value > 0 ? (last.value - first.value - flows) / first.value : 0;
        result.push({ month, ret });
    }
    return result;
}

function renderMonthly() {
    const chart = getChart('monthlyChart');
    if (!chart) return;
    const points = state.history?.points || [];
    const data = monthlyReturns(points).slice(-24);
    if (!data.length) {
        chart.setOption({
            title: { text: '暂无数据', left: 'center', top: 'middle', textStyle: { color: '#65758a', fontSize: 12, fontWeight: 'normal' } },
            xAxis: { show: false }, yAxis: { show: false }, series: [],
        }, true);
        return;
    }
    chart.setOption({
        tooltip: {
            trigger: 'axis', backgroundColor: '#16212d', borderColor: '#223041',
            textStyle: { color: '#e8eef5', fontSize: 12 },
            formatter: params => {
                const p = params[0];
                const v = Number(p.value) || 0;
                return `${p.axisValue}<br><b class="${v >= 0 ? 'pos' : 'neg'}">${(v * 100).toFixed(2)}%</b>`;
            },
        },
        grid: { left: 52, right: 16, top: 22, bottom: 30 },
        xAxis: {
            type: 'category', data: data.map(d => d.month),
            axisLine: { lineStyle: { color: '#223041' } },
            axisLabel: { color: '#65758a', fontSize: 10, hideOverlap: true },
            axisTick: { show: false },
        },
        yAxis: {
            type: 'value',
            splitLine: { lineStyle: { color: '#1a2634' } },
            axisLabel: { color: '#65758a', fontSize: 10, formatter: v => (v * 100).toFixed(0) + '%' },
        },
        series: [{
            type: 'bar', data: data.map(d => ({
                value: d.ret,
                itemStyle: { color: d.ret >= 0 ? '#34d399' : '#f87171', borderRadius: [3, 3, 0, 0] },
            })),
            barMaxWidth: 26,
        }],
    }, true);
}

function resizeCharts() {
    Object.values(state.charts).forEach(chart => { try { chart.resize(); } catch (_) {} });
}

/* ── tabs / events ─────────────────────────────────────────── */
function switchTab(tab) {
    state.tab = tab;
    $$('.tab').forEach(btn => btn.classList.toggle('active', btn.dataset.tab === tab));
    $$('.panel').forEach(panel => panel.classList.toggle('active', panel.id === 'tab-' + tab));
    if (tab === 'overview') {
        renderAlloc();
        renderHistory();
        setTimeout(resizeCharts, 50);
    } else if (tab === 'reports') {
        renderReports();
        renderMonthly();
        setTimeout(resizeCharts, 50);
    } else if (tab === 'dca') {
        loadOrders();
    } else if (tab === 'transactions') {
        loadTransactions(state.txPage);
    }
}

function bindEvents() {
    $$('.tab').forEach(btn => btn.addEventListener('click', () => switchTab(btn.dataset.tab)));
    document.addEventListener('click', event => {
        const jump = event.target.closest('[data-tab-jump]');
        if (jump) { switchTab(jump.dataset.tabJump); return; }
        const pageBtn = event.target.closest('[data-page]');
        if (pageBtn) { loadTransactions(Number(pageBtn.dataset.page)); return; }
        const action = event.target.closest('[data-action]');
        if (action) handleAction(action);
    });

    $('#rangeBtns')?.addEventListener('click', event => {
        const btn = event.target.closest('button[data-range]');
        if (!btn) return;
        state.range = btn.dataset.range;
        $$('#rangeBtns button').forEach(b => b.classList.toggle('active', b === btn));
        refreshAll();
    });
    $('#allocBy')?.addEventListener('change', renderAlloc);
    $('#holdingsAccountFilter')?.addEventListener('change', renderHoldings);
    $('#syncBtn')?.addEventListener('click', () => syncAll());
    $('#logoutBtn')?.addEventListener('click', async () => {
        await fetch('/wealth/logout', { method: 'POST', credentials: 'same-origin' });
        location.href = '/wealth/login';
    });
    $('#addTxBtn')?.addEventListener('click', () => openTransactionModal());
    $('#addTxFromHoldings')?.addEventListener('click', () => openTransactionModal());
    $('#addPlanBtn')?.addEventListener('click', () => openPlanModal());
    $('#addAccountBtn')?.addEventListener('click', () => openAccountModal());
    $('#addAssetBtn')?.addEventListener('click', () => openAssetModal());
    $('#importBtn')?.addEventListener('click', openImportModal);
    $('#importCsvBtn')?.addEventListener('click', openCsvImportModal);
    $('#filterTx')?.addEventListener('click', () => loadTransactions(1));
    $('#orderStatusFilter')?.addEventListener('change', event => {
        state.orderFilter = event.target.value;
        loadOrders();
    });
    $('#modalClose')?.addEventListener('click', closeModal);
    $('#modalRoot')?.addEventListener('click', event => {
        if (event.target === $('#modalRoot')) closeModal();
    });
    document.addEventListener('keydown', event => {
        if (event.key === 'Escape') closeModal();
    });
    window.addEventListener('resize', () => resizeCharts());
}

async function handleAction(el) {
    const action = el.dataset.action;
    try {
        if (action === 'add-tx') {
            openTransactionModal(Number(el.dataset.asset));
        } else if (action === 'edit-holding') {
            await openHoldingEditor(
                Number(el.dataset.asset),
                el.dataset.account === '' ? null : Number(el.dataset.account));
        } else if (action === 'edit-tx') {
            let tx = state.transactions.find(t => String(t.id) === String(el.dataset.tx));
            if (!tx) tx = await api('/api/wm/transactions/' + el.dataset.tx);
            openTransactionModal(null, tx);
        } else if (action === 'delete-tx') {
            if (!confirm('删除这笔交易？持仓和盈亏会立即重算。')) return;
            await api('/api/wm/transactions/' + el.dataset.tx, { method: 'DELETE' });
            if ($('#modalRoot')?.classList.contains('open')) closeModal();
            toast('已删除', 'success');
            await refreshAll();
            await loadTransactions(state.txPage);
        } else if (action === 'asset-price') {
            openPriceModal(Number(el.dataset.asset));
        } else if (action === 'backfill-asset') {
            el.disabled = true;
            const data = await api('/api/wm/assets/' + el.dataset.asset + '/backfill', { method: 'POST' });
            toast(`同步完成，写入 ${data.count || 0} 条价格`, 'success');
            await refreshAll();
        } else if (action === 'edit-asset') {
            const asset = state.assets.find(a => String(a.id) === String(el.dataset.asset));
            if (asset) openAssetModal(asset);
        } else if (action === 'toggle-asset') {
            const active = el.dataset.active === '1';
            await api('/api/wm/assets/' + el.dataset.asset, { method: 'PUT', body: { is_active: !active } });
            toast(active ? '已停用' : '已启用', 'success');
            await refreshAll();
        } else if (action === 'delete-asset') {
            const asset = state.assets.find(a => String(a.id) === String(el.dataset.asset));
            const force = el.dataset.force === '1';
            const txCount = asset?.tx_count || 0;
            const msg = force
                ? `删除资产「${asset?.name || ''}」及其 ${txCount} 笔交易？\n\n交易记录和价格会一起删除，此操作不可恢复。`
                : `删除资产「${asset?.name || ''}」？`;
            if (!confirm(msg)) return;
            const data = await api(
                '/api/wm/assets/' + el.dataset.asset + (force ? '?force=1' : ''),
                { method: 'DELETE' });
            toast(data.deleted_transactions
                ? `已删除资产和 ${data.deleted_transactions} 笔交易`
                : '已删除', 'success');
            await refreshAll();
            await loadTransactions(state.txPage);
        } else if (action === 'edit-account') {
            const account = state.accounts.find(a => String(a.id) === String(el.dataset.account));
            if (account) openAccountModal(account);
        } else if (action === 'toggle-account') {
            const active = el.dataset.active === '1';
            await api('/api/wm/accounts/' + el.dataset.account, {
                method: 'PUT', body: { is_active: !active },
            });
            toast(active ? '账户已停用' : '账户已启用', 'success');
            await refreshAll();
        } else if (action === 'delete-account') {
            const account = state.accounts.find(a => String(a.id) === String(el.dataset.account));
            if (!confirm(`删除账户「${account?.name || ''}」？\n\n账户下的现金余额也会删除，此操作不可恢复。`)) return;
            await api('/api/wm/accounts/' + el.dataset.account, { method: 'DELETE' });
            toast('账户已删除', 'success');
            await refreshAll();
            await loadTransactions(state.txPage);
        } else if (action === 'account-transactions') {
            const select = $('#txAccountFilter');
            if (select) select.value = el.dataset.account;
            state.txPage = 1;
            switchTab('transactions');
        } else if (action === 'pause-plan') {
            await api(`/api/wm/dca/plans/${el.dataset.plan}/pause`, { method: 'POST' });
            toast('已暂停', 'success');
            await refreshAll();
        } else if (action === 'resume-plan') {
            await api(`/api/wm/dca/plans/${el.dataset.plan}/resume`, { method: 'POST' });
            toast('已恢复；从下一期开始，不补暂停期间', 'success');
            await refreshAll();
        } else if (action === 'run-plan') {
            el.disabled = true;
            const data = await api(`/api/wm/dca/plans/${el.dataset.plan}/run-now`, { method: 'POST' });
            if (data.skipped) toast('定投任务正在运行中，请稍后刷新', 'error');
            else toast(data.waiting ? '已提交，等待净值/价格' : '执行完成', 'success');
            await refreshAll();
        } else if (action === 'archive-plan') {
            if (!confirm('归档这个定投计划？已生成的历史订单会保留。')) return;
            await api('/api/wm/dca/plans/' + el.dataset.plan, { method: 'DELETE' });
            toast('已归档', 'success');
            await refreshAll();
        } else if (action === 'edit-plan') {
            const plan = state.plans.find(p => String(p.id) === String(el.dataset.plan));
            if (plan) openPlanModal(plan);
        } else if (action === 'skip-order') {
            await api(`/api/wm/dca/orders/${el.dataset.order}/skip`, { method: 'POST' });
            toast('已跳过', 'success');
            await refreshAll();
        } else if (action === 'retry-order') {
            await api(`/api/wm/dca/orders/${el.dataset.order}/retry`, { method: 'POST' });
            toast('已重试', 'success');
            await refreshAll();
        } else if (action === 'run-dca') {
            el.disabled = true;
            const data = await api('/api/wm/dca/run', { method: 'POST' });
            if (data.skipped) {
                toast('定投任务正在运行中，请稍后刷新', 'error');
            } else {
                toast(`定投检查完成：新生成 ${data.created || 0}，入账 ${data.filled || 0}，等待 ${data.waiting || 0}`, 'success');
            }
            await refreshAll();
        } else if (action === 'sync-all') {
            await syncAll();
        }
    } catch (err) {
        toast(err.message, 'error');
    } finally {
        el.disabled = false;
    }
}

async function syncAll() {
    const btn = $('#syncBtn');
    if (btn) { btn.disabled = true; btn.textContent = '同步中…'; }
    try {
        const data = await api('/api/wm/sync', { method: 'POST', body: { full: false } });
        toast(`同步完成：${data.ok || 0} 个资产${data.failed?.length ? `，${data.failed.length} 个失败` : ''}`, data.failed?.length ? 'error' : 'success');
        await refreshAll();
    } catch (err) {
        toast(err.message, 'error');
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = '同步行情'; }
    }
}

/* ── modal helpers ─────────────────────────────────────────── */
function openModal(title, html) {
    $('#modalTitle').textContent = title;
    $('#modalBody').innerHTML = html;
    $('#modalRoot').classList.add('open');
    $('#modalRoot').setAttribute('aria-hidden', 'false');
    const first = $('#modalBody input, #modalBody select');
    if (first) setTimeout(() => first.focus(), 30);
}

function closeModal() {
    $('#modalRoot').classList.remove('open');
    $('#modalRoot').setAttribute('aria-hidden', 'true');
    $('#modalBody').innerHTML = '';
    state.planPreview = null;
}

function formDataObject(form) {
    const obj = {};
    new FormData(form).forEach((value, key) => { obj[key] = value; });
    return obj;
}

function field(label, input, hint = '', extraClass = '') {
    return `<label class="field ${extraClass}"><span>${label}</span>${input}${hint ? `<small class="hint">${hint}</small>` : ''}</label>`;
}

function accountOptions(selected) {
    return '<option value="">无账户</option>' + state.accounts
        .filter(a => a.is_active || String(a.id) === String(selected))
        .map(a => `<option value="${a.id}" ${String(selected) === String(a.id) ? 'selected' : ''}>${esc(a.name)}</option>`)
        .join('');
}

function assetOptions(selected) {
    return state.assets
        .filter(a => a.is_active || String(a.id) === String(selected))
        .map(a => `<option value="${a.id}" ${String(selected) === String(a.id) ? 'selected' : ''}>${esc(a.name)} (${esc(a.symbol)})</option>`)
        .join('');
}

/* ── asset modal ───────────────────────────────────────────── */
function openAssetModal(asset = null) {
    const editing = Boolean(asset);
    const a = asset || {};
    const markets = ['CN', 'US', 'HK', 'FUND_CN', 'CRYPTO', 'BANK', 'MANUAL'];
    const classes = ['stock', 'etf', 'fund', 'crypto', 'bank_wm', 'other'];
    const sources = [['eastmoney', '东方财富'], ['fund_em', '基金净值'], ['binance', 'Binance'],
        ['manual', '手动价格'], ['accrual', '按日计提']];
    const html = `<form id="modalForm">
        <div class="form-grid">
            ${field('市场', `<select name="market" id="assetMarket" ${editing ? 'disabled' : ''}>${markets.map(m => `<option value="${m}" ${a.market === m ? 'selected' : ''}>${MARKET_LABELS[m]}</option>`).join('')}</select>`)}
            ${field('币种', `<select name="currency" id="assetCurrency" ${editing && (a.tx_count || a.plan_count) ? 'disabled' : ''}>${['CNY', 'USD', 'HKD'].map(c => `<option value="${c}" ${a.currency === c ? 'selected' : ''}>${c}</option>`).join('')}</select>`, editing && (a.tx_count || a.plan_count) ? '已有交易/计划，币种不能直接修改；需删除后重新添加' : '可手动选择；场外基金会按名称识别美元/港币份额，查询结果不会覆盖手选币种')}
            ${field('代码', `<input name="symbol" id="assetSymbol" value="${esc(a.symbol || '')}" ${editing ? 'readonly' : ''} placeholder="如 600519 / AAPL / 000001 / BTCUSDT">`)}
            ${field('名称', `<input name="name" id="assetName" value="${esc(a.name || '')}" placeholder="可留空自动查询">`, '', 'full')}
            ${field('类别', `<select name="asset_class" id="assetClass">${classes.map(c => `<option value="${c}" ${a.asset_class === c ? 'selected' : ''}>${CLASS_LABELS[c]}</option>`).join('')}</select>`)}
            ${field('行情源', `<select name="quote_source" id="assetSource">${sources.map(([v, l]) => `<option value="${v}" ${a.quote_source === v ? 'selected' : ''}>${l}</option>`).join('')}</select>`)}
            ${field('行情代码 / 交易对', `<input name="quote_key" id="assetQuoteKey" value="${esc(a.quote_key || '')}" placeholder="一般留空，自动解析">`)}
            ${field('最小交易单位', `<input name="lot_size" type="number" step="any" min="0" value="${a.lot_size ?? 1}">`)}
            ${field('允许碎股/碎份', `<select name="allow_fraction"><option value="1" ${a.allow_fraction ? 'selected' : ''}>是</option><option value="0" ${!a.allow_fraction ? 'selected' : ''}>否</option></select>`)}
            ${field('估值方式', `<select name="valuation_mode" id="assetValuation"><option value="price" ${a.valuation_mode !== 'accrual' ? 'selected' : ''}>按价格</option><option value="accrual" ${a.valuation_mode === 'accrual' ? 'selected' : ''}>银行理财按日计提</option></select>`)}
            <div class="full" id="bankFields" style="display:none">
                <div class="form-grid">
                    ${field('年化收益率 (%)', `<input name="annual_rate_pct" type="number" step="0.0001" min="0" value="${a.annual_rate ? (a.annual_rate * 100) : ''}" placeholder="如 2.5">`)}
                    ${field('计息方式', `<select name="interest_mode"><option value="simple" ${a.interest_mode !== 'compound' ? 'selected' : ''}>单利</option><option value="compound" ${a.interest_mode === 'compound' ? 'selected' : ''}>复利</option></select>`)}
                    ${field('到期日', `<input name="maturity_date" type="date" value="${esc(a.maturity_date || '')}">`)}
                </div>
            </div>
            ${field('备注', `<textarea name="note" placeholder="可选">${esc(a.note || '')}</textarea>`, '', 'full')}
        </div>
        <div class="form-actions">
            <button type="button" class="btn btn-ghost" id="lookupAsset">查询名称/现价</button>
            <button type="button" class="btn btn-ghost" data-close>取消</button>
            <button type="submit" class="btn btn-primary">${editing ? '保存' : '添加资产'}</button>
        </div>
    </form>`;
    openModal(editing ? '编辑资产' : '添加资产', html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);

    let currencyTouched = Boolean(editing);
    const fundCurrencyHint = name => {
        if (!name) return null;
        if (name.includes('人民币')) return 'CNY';
        if (name.includes('港币') || name.includes('港元')) return 'HKD';
        return /美元(?:现汇|现钞|汇|份额|[A-Za-z]|[（()）]|$)/.test(name) ? 'USD' : null;
    };
    let fundNavCurrency = editing && a.market === 'FUND_CN' ? fundCurrencyHint(a.name) : null;
    const nativeCurrency = market => market === 'FUND_CN'
        ? fundNavCurrency
        : ({ CN: 'CNY', US: 'USD', HK: 'HKD', CRYPTO: 'USD', BANK: 'CNY', MANUAL: 'CNY' }[market] || null);
    const applyCurrencySourceRule = () => {
        const market = $('#assetMarket')?.value;
        const currency = $('#assetCurrency')?.value;
        const native = nativeCurrency(market);
        const source = $('#assetSource');
        if (native && currency && currency !== native && source
                && !['manual', 'accrual'].includes(source.value)) {
            source.value = 'manual';
        }
    };
    const syncDefaults = () => {
        if (editing) return;
        currencyTouched = false;
        fundNavCurrency = null;
        const market = $('#assetMarket').value;
        $('#assetCurrency').value = nativeCurrency(market) || 'CNY';
        const source = { CN: 'eastmoney', US: 'eastmoney', HK: 'eastmoney', FUND_CN: 'fund_em', CRYPTO: 'binance', BANK: 'accrual', MANUAL: 'manual' };
        $('#assetSource').value = source[market] || 'manual';
        $('#assetValuation').value = market === 'BANK' ? 'accrual' : 'price';
        $('#assetClass').value = { FUND_CN: 'fund', CRYPTO: 'crypto', BANK: 'bank_wm' }[market] || 'stock';
        const fractionSelect = $('#modalForm [name=allow_fraction]');
        if (fractionSelect) fractionSelect.value = ['CN', 'HK'].includes(market) ? '0' : '1';
        const lotInput = $('#modalForm [name=lot_size]');
        if (lotInput) lotInput.value = ['CN', 'HK'].includes(market) ? 100 : 1;
        $('#bankFields').style.display = market === 'BANK' ? 'block' : 'none';
        $('#assetSymbol').placeholder = market === 'FUND_CN' ? '6 位基金代码' : market === 'CRYPTO' ? '如 BTC 或 BTCUSDT' : '代码';
        applyCurrencySourceRule();
    };
    $('#assetMarket')?.addEventListener('change', syncDefaults);
    syncDefaults();
    $('#assetCurrency')?.addEventListener('change', () => {
        currencyTouched = true;
        applyCurrencySourceRule();
    });
    $('#assetSource')?.addEventListener('change', applyCurrencySourceRule);
    applyCurrencySourceRule();
    const toggleBankFields = () => {
        $('#bankFields').style.display = $('#assetValuation').value === 'accrual' ? 'block' : 'none';
    };
    $('#assetValuation')?.addEventListener('change', toggleBankFields);
    toggleBankFields();

    $('#lookupAsset')?.addEventListener('click', async () => {
        const symbol = $('#assetSymbol').value.trim();
        const market = $('#assetMarket').value;
        if (!symbol) return toast('先填代码', 'error');
        const btn = $('#lookupAsset');
        btn.disabled = true;
        try {
            const data = await api(`/api/wm/lookup?symbol=${encodeURIComponent(symbol)}&market=${encodeURIComponent(market)}`);
            if (!data.found) throw new Error(data.error || '未找到');
            $('#assetName').value = data.name || $('#assetName').value;
            fundNavCurrency = data.currency || null;
            if (!currencyTouched && data.currency) $('#assetCurrency').value = data.currency;
            if (data.quote_source) $('#assetSource').value = data.quote_source;
            if (data.quote_key) $('#assetQuoteKey').value = data.quote_key;
            if (data.asset_class) $('#assetClass').value = data.asset_class;
            applyCurrencySourceRule();
            const selectedCurrency = $('#assetCurrency').value;
            let message = '已查询，确认后保存';
            if (currencyTouched && data.currency && data.currency !== selectedCurrency) {
                message = `已查询；保留手动币种 ${selectedCurrency}`;
            } else if (data.currency) {
                message = `已查询；识别币种 ${data.currency}`;
            }
            toast(message, 'success');
        } catch (err) {
            toast(err.message, 'error');
        } finally {
            btn.disabled = false;
        }
    });

    let submitting = false;
    $('#modalForm').addEventListener('submit', async event => {
        event.preventDefault();
        if (submitting) return;
        const data = formDataObject(event.target);
        if (data.annual_rate_pct) data.annual_rate = Number(data.annual_rate_pct) / 100;
        delete data.annual_rate_pct;
        data.allow_fraction = data.allow_fraction === '1';
        if (data.market === 'BANK' && !data.valuation_mode) data.valuation_mode = 'accrual';
        submitting = true;
        const submitBtn = event.target.querySelector('button[type=submit]');
        if (submitBtn) submitBtn.disabled = true;
        try {
            const url = editing ? '/api/wm/assets/' + a.id : '/api/wm/assets';
            const saved = await api(url, { method: editing ? 'PUT' : 'POST', body: data });
            const native = nativeCurrency(data.market);
            const currencyChanged = editing && data.currency && a.currency
                && data.currency !== a.currency;
            if (!editing && saved.existing) {
                toast('资产已存在，已保留原记录', 'error');
            } else if (currencyChanged) {
                toast('币种已更新；原价格记录已清除，请重新补价', 'success');
            } else if (!editing && native && data.currency !== native) {
                toast('资产已添加；非本币资产请手工补价', 'success');
            } else {
                toast(editing ? '已保存' : '资产已添加，正在拉取行情', 'success');
            }
            closeModal();
            await refreshAll();
            if (!editing && saved?.id) {
                setTimeout(() => refreshAll(), 2500);
            }
        } catch (err) {
            toast(err.message, 'error');
        } finally {
            submitting = false;
            if (submitBtn) submitBtn.disabled = false;
        }
    });
}

/* ── account modal ─────────────────────────────────────────── */
function openAccountModal(account = null) {
    const editing = Boolean(account);
    const a = account || {};
    const currencies = ['CNY', 'USD', 'HKD'];
    if (a.currency && !currencies.includes(a.currency)) currencies.push(a.currency);
    const originalBalances = Array.isArray(a.cash_balances) ? a.cash_balances : [];
    for (const balance of originalBalances) {
        if (balance.currency && !currencies.includes(balance.currency)) {
            currencies.push(balance.currency);
        }
    }
    const originalByCurrency = new Map(originalBalances.map(b => [b.currency, b]));
    const cashRow = (balance = {}, isNew = true) => {
        const currency = balance.currency || a.currency || 'CNY';
        return `<div class="cash-row" data-cash-row data-new="${isNew ? '1' : '0'}" style="display:grid;grid-template-columns:88px minmax(90px,1fr) 148px auto;gap:8px;align-items:end;margin-bottom:8px">
            <label class="field" style="margin:0"><span>币种</span>
                <select data-cash-currency ${isNew ? '' : 'disabled'}>${currencies.map(c => `<option value="${c}" ${c === currency ? 'selected' : ''}>${c}</option>`).join('')}</select>
            </label>
            <label class="field" style="margin:0"><span>金额</span>
                <input type="number" step="any" data-cash-amount value="${esc(balance.amount ?? '')}" placeholder="留空不修改">
            </label>
            <label class="field" style="margin:0"><span>余额日期</span>
                <input type="date" data-cash-date value="${esc(balance.balance_date || todayIso())}">
            </label>
            ${isNew ? '<button type="button" class="btn btn-ghost btn-sm" data-remove-cash style="margin-bottom:1px">移除</button>' : '<span></span>'}
        </div>`;
    };
    const initialBalances = originalBalances.length
        ? originalBalances
        : [{ currency: a.currency || 'CNY', amount: '', balance_date: todayIso() }];
    const html = `<form id="modalForm">
        <div class="full alert alert-info">投资账户对应 Ghostfolio 的券商/平台账户，不是登录账号。删除账户前需要先清空其交易和定投计划。</div>
        <div class="form-grid">
            ${field('账户名称', `<input name="name" value="${esc(a.name || '')}" required placeholder="如 招商证券 / IBKR">`, '', 'full')}
            ${field('平台 / 券商', `<input name="broker" value="${esc(a.broker || '')}" placeholder="可选">`)}
            ${field('默认币种', `<select name="currency">${currencies.map(c => `<option value="${c}" ${(a.currency || 'CNY') === c ? 'selected' : ''}>${c}</option>`).join('')}</select>`, '新增现金行的默认币种')}
            ${editing ? field('状态', `<select name="is_active"><option value="1" ${a.is_active ? 'selected' : ''}>启用</option><option value="0" ${!a.is_active ? 'selected' : ''}>停用</option></select>`) : ''}
        </div>
        <div class="full" style="margin-top:8px">
            <div style="display:flex;align-items:center;justify-content:space-between;gap:8px;margin-bottom:6px">
                <strong>现金余额</strong>
                <button type="button" class="btn btn-ghost btn-sm" id="addCashRow">+ 添加币种</button>
            </div>
            <div id="accountCashRows">${initialBalances.map(b => cashRow(b, !originalByCurrency.has(b.currency))).join('')}</div>
            <small class="hint">每个币种一行；留空不修改，填 0 清零；买入/卖出会按成交币种自动增减对应现金。</small>
        </div>
        <div class="form-actions">
            <button type="button" class="btn btn-ghost" data-close>取消</button>
            <button type="submit" class="btn btn-primary">${editing ? '保存' : '创建账户'}</button>
        </div>
    </form>`;
    openModal(editing ? '编辑账户' : '新建账户', html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);

    const rowsWrap = $('#accountCashRows');
    $('#addCashRow')?.addEventListener('click', () => {
        const used = new Set($$('[data-cash-currency]', rowsWrap).map(select => select.value));
        const next = currencies.find(currency => !used.has(currency));
        if (!next) return toast('CNY / USD / HKD 已全部添加', 'error');
        rowsWrap.insertAdjacentHTML('beforeend',
            cashRow({ currency: next, amount: '', balance_date: todayIso() }, true));
    });
    rowsWrap?.addEventListener('click', event => {
        const button = event.target.closest('[data-remove-cash]');
        if (button) button.closest('[data-cash-row]')?.remove();
    });
    if (!editing) {
        const defaultCurrency = $('#modalForm select[name=currency]');
        defaultCurrency?.addEventListener('change', () => {
            const row = rowsWrap?.querySelector('[data-cash-row][data-new="1"]');
            const amount = row?.querySelector('[data-cash-amount]');
            if (row && !String(amount?.value || '').trim()) {
                row.querySelector('[data-cash-currency]').value = defaultCurrency.value;
            }
        });
    }

    let submitting = false;
    $('#modalForm').addEventListener('submit', async event => {
        event.preventDefault();
        if (submitting) return;
        const data = formDataObject(event.target);
        if (editing) data.is_active = data.is_active === '1';
        else delete data.is_active;

        const cashBalances = [];
        const seen = new Set();
        for (const row of $$('[data-cash-row]', rowsWrap)) {
            const currency = row.querySelector('[data-cash-currency]').value;
            const amountRaw = String(row.querySelector('[data-cash-amount]').value ?? '').trim();
            const balanceDate = row.querySelector('[data-cash-date]').value || todayIso();
            if (!amountRaw) continue;
            const amount = Number(amountRaw);
            if (!Number.isFinite(amount)) {
                return toast('现金余额应为数字', 'error');
            }
            if (seen.has(currency)) {
                return toast(`币种 ${currency} 重复，请合并为一行`, 'error');
            }
            seen.add(currency);
            const old = originalByCurrency.get(currency);
            if (old && Number(old.amount) === amount && old.balance_date === balanceDate) continue;
            cashBalances.push({ currency, amount, balance_date: balanceDate });
        }
        if (cashBalances.length) data.cash_balances = cashBalances;
        delete data.cash_amount;
        delete data.cash_date;

        submitting = true;
        const submitBtn = event.target.querySelector('button[type=submit]');
        if (submitBtn) submitBtn.disabled = true;
        try {
            const url = editing ? '/api/wm/accounts/' + a.id : '/api/wm/accounts';
            await api(url, { method: editing ? 'PUT' : 'POST', body: data });
            toast(editing ? '账户已保存' : '账户已创建', 'success');
            closeModal();
            await refreshAll();
            await loadTransactions(state.txPage);
        } catch (err) {
            toast(err.message, 'error');
        } finally {
            submitting = false;
            if (submitBtn) submitBtn.disabled = false;
        }
    });
}

/* ── holding editor ─────────────────────────────────────────── */
async function openHoldingEditor(assetId, accountId = null) {
    const position = state.positions.find(p => Number(p.asset_id) === assetId
        && (p.account_id ?? null) === accountId);
    const params = new URLSearchParams({ asset_id: String(assetId), per_page: '500' });
    params.set('account_id', accountId === null ? 'none' : String(accountId));
    let data;
    try {
        data = await api('/api/wm/transactions?' + params.toString());
    } catch (err) {
        return toast(err.message, 'error');
    }
    const txs = data.transactions || [];
    const bank = position?.valuation_mode === 'accrual';
    const summary = position
        ? `<div class="muted" style="line-height:1.6;margin-top:8px">
             ${bank
                 ? `当前本金 ${fmtCcy(position.principal, position.currency)}`
                 : `当前数量 ${fmtNum(position.units, 4)} · 均价 ${fmtNum(position.avg_cost_native, 4)} · 成本 ${fmtCcy(position.cost_native, position.currency)}`}
             ${position.account_name ? ` · ${esc(position.account_name)}` : ' · 无账户'}
           </div>`
        : '';
    const rows = txs.length ? txs.map(t => {
        const editable = t.source !== 'dca';
        const units = t.type === 'split' ? '—' : (t.units ? fmtNum(t.units, 4) : '—');
        const price = bank || t.type === 'split' ? '—' : (t.price ? fmtNum(t.price, 4) : '—');
        const amount = t.type === 'split' ? '—' : fmtCcy(t.amount, t.currency);
        return `<tr>
            <td>${esc(t.trade_date)}</td>
            <td>${typeTag(t.type)}</td>
            <td class="num">${bank ? '—' : units}</td>
            <td class="num">${price}</td>
            <td class="num">${amount}</td>
            <td class="num">${fmtCcy(t.fee, t.currency)}</td>
            <td>${sourceTag(t.source)}</td>
            <td><div class="row-actions">${editable
                ? `<button class="btn btn-ghost btn-sm" data-action="edit-tx" data-tx="${t.id}">编辑</button>
                   <button class="btn btn-danger btn-sm" data-action="delete-tx" data-tx="${t.id}">删除</button>`
                : '<span class="muted">自动</span>'}</div></td>
        </tr>`;
    }).join('') : '<tr><td colspan="8"><div class="empty">没有找到该持仓的交易</div></td></tr>';
    const html = `<div>
        <div class="alert alert-info">持仓由交易自动计算。哪一笔填错了，直接编辑或删除它，保存后持仓和盈亏会立即重算；定投自动生成的交易请在定投页处理。</div>
        ${summary}
        <div class="table-wrap" style="margin-top:10px;max-height:46vh;overflow:auto">
            <table>
                <thead><tr><th>日期</th><th>类型</th><th class="num">数量</th><th class="num">价格</th><th class="num">金额</th><th class="num">费用</th><th>来源</th><th></th></tr></thead>
                <tbody>${rows}</tbody>
            </table>
        </div>
        <div class="form-actions">
            <button type="button" class="btn btn-ghost" data-close>关闭</button>
        </div>
    </div>`;
    openModal(`编辑持仓 · ${position?.name || '持仓'}`, html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);
}

/* ── transaction modal ─────────────────────────────────────── */
function openTransactionModal(assetId = null, tx = null) {
    if (!tx && !assetId && !state.assets.filter(a => a.is_active).length) {
        return toast('请先在“资产”页添加一个资产', 'error');
    }
    const editing = Boolean(tx);
    const selected = tx?.asset_id || assetId || state.assets.find(a => a.is_active)?.id;
    const types = ['buy', 'sell', 'dividend', 'reinvest', 'interest', 'fee', 'split'];
    const html = `<form id="modalForm">
        <div class="form-grid">
            ${field('资产', `<select name="asset_id" id="txAsset" ${editing ? 'disabled' : ''}>${assetOptions(selected)}</select>`, '', 'full')}
            ${field('类型', `<select name="type" id="txType">${types.map(t => `<option value="${t}" ${tx?.type === t ? 'selected' : ''}>${TYPE_LABELS[t]}</option>`).join('')}</select>`)}
            ${field('交易日期', `<input type="date" name="trade_date" value="${esc(tx?.trade_date || todayIso())}">`)}
            <div class="full" id="txQtyPrice" style="display:contents">
                ${field('数量 / 份额', `<input type="number" step="any" min="0" name="units" id="txUnits" value="${tx?.units ?? ''}">`)}
                ${field('价格 / 净值', `<input type="number" step="any" min="0" name="price" id="txPrice" value="${tx?.price ?? ''}">`)}
            </div>
            ${field('金额', `<input type="number" step="any" min="0" name="amount" id="txAmount" value="${tx?.amount ?? ''}">`, '', 'full')}
            ${field('费用', `<input type="number" step="any" min="0" name="fee" value="${tx?.fee ?? 0}">`)}
            ${field('账户', `<select name="account_id">${accountOptions(tx?.account_id)}</select>`)}
            <div class="full" id="txSplit" style="display:none">
                ${field('拆分比例', `<input type="number" step="any" min="0" name="split_ratio" value="${tx?.split_ratio ?? ''}" placeholder="2 表示 1 拆 2">`)}
            </div>
            ${field('备注', `<textarea name="note" placeholder="可选">${esc(tx?.note || '')}</textarea>`, '', 'full')}
        </div>
        <div class="alert alert-info" id="txHint"></div>
        <div class="form-actions">
            <button type="button" class="btn btn-ghost" data-close>取消</button>
            <button type="submit" class="btn btn-primary">${editing ? '保存' : '记账'}</button>
        </div>
    </form>`;
    openModal(editing ? '编辑交易' : '记一笔', html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);

    const asset = () => state.assets.find(a => String(a.id) === String($('#txAsset').value));
    const adjustForm = () => {
        const a = asset();
        const type = $('#txType').value;
        const bank = a?.valuation_mode === 'accrual';
        const qtyPrice = $('#txQtyPrice');
        const split = $('#txSplit');
        const units = $('#txUnits');
        const price = $('#txPrice');
        const amount = $('#txAmount');
        const hint = $('#txHint');
        if (bank) {
            $('#txType').innerHTML = ['buy', 'sell', 'interest', 'fee']
                .map(t => `<option value="${t}" ${type === t ? 'selected' : ''}>${TYPE_LABELS[t]}</option>`).join('');
            qtyPrice.style.display = 'none';
            split.style.display = 'none';
            amount.closest('.field').querySelector('span').textContent = '本金 / 金额';
            hint.textContent = '银行理财：买入/赎回填写本金金额；利息到账时记“利息”，会减少未计提余额。';
        } else if (type === 'split') {
            qtyPrice.style.display = 'none';
            split.style.display = 'block';
            amount.closest('.field').style.display = 'none';
            hint.textContent = '拆分比例：1 拆 2 填 2；只调整份额，不改变成本。';
        } else if (['dividend', 'interest', 'fee'].includes(type)) {
            qtyPrice.style.display = 'none';
            split.style.display = 'none';
            amount.closest('.field').style.display = 'flex';
            hint.textContent = '填写金额（税前/费用原值）和费用（如有）。';
        } else {
            qtyPrice.style.display = 'contents';
            split.style.display = 'none';
            amount.closest('.field').style.display = 'flex';
            if (type === 'sell') {
                amount.closest('.field').querySelector('span').textContent = '卖出金额（不含费用）';
                hint.textContent = '卖出收入 = 数量 × 价格；费用从卖出收入中扣除，按移动平均成本计算已实现盈亏。';
            } else if (type === 'reinvest') {
                amount.closest('.field').querySelector('span').textContent = '分红再投金额';
                hint.textContent = '分红再投会增加份额，不增加你的外部投入；金额按再投价格折算份额。';
            } else if (a?.market === 'FUND_CN') {
                amount.closest('.field').querySelector('span').textContent = '净申购金额（不含费用）';
                hint.textContent = '场外基金：份额 = 净申购金额 ÷ 净值。若填“申购金额”含费，可把费用单独填入费用栏。';
            } else {
                amount.closest('.field').querySelector('span').textContent = '成交金额（不含费用）';
                hint.textContent = '数量 × 价格 会自动填充金额；费用单独计入成本/卖出净收入。';
            }
        }
        if (bank) amount.closest('.field').style.display = 'flex';
    };
    $('#txAsset').addEventListener('change', adjustForm);
    $('#txType').addEventListener('change', adjustForm);
    adjustForm();

    let calcLock = false;
    const recalcFromQty = () => {
        if (calcLock) return;
        const u = Number($('#txUnits').value);
        const p = Number($('#txPrice').value);
        if (u > 0 && p > 0) {
            calcLock = true;
            $('#txAmount').value = (u * p).toFixed(6).replace(/\.?0+$/, '');
            calcLock = false;
        }
    };
    const recalcFromAmount = () => {
        if (calcLock) return;
        const a = Number($('#txAmount').value);
        const p = Number($('#txPrice').value);
        if (a > 0 && p > 0) {
            calcLock = true;
            $('#txUnits').value = (a / p).toFixed(8).replace(/\.?0+$/, '');
            calcLock = false;
        }
    };
    $('#txUnits').addEventListener('input', recalcFromQty);
    $('#txPrice').addEventListener('input', () => { recalcFromQty(); recalcFromAmount(); });
    $('#txAmount').addEventListener('input', recalcFromAmount);

    let submitting = false;
    $('#modalForm').addEventListener('submit', async event => {
        event.preventDefault();
        if (submitting) return;
        const data = formDataObject(event.target);
        submitting = true;
        const submitBtn = event.target.querySelector('button[type=submit]');
        if (submitBtn) submitBtn.disabled = true;
        try {
            if (editing) {
                await api('/api/wm/transactions/' + tx.id, { method: 'PUT', body: data });
                toast('已保存', 'success');
            } else {
                await api('/api/wm/transactions', { method: 'POST', body: data });
                toast('已记账', 'success');
            }
            closeModal();
            await refreshAll();
            await loadTransactions(state.txPage);
            setTimeout(() => refreshAll(), 2500);
        } catch (err) {
            toast(err.message, 'error');
        } finally {
            submitting = false;
            if (submitBtn) submitBtn.disabled = false;
        }
    });
}

/* ── manual price modal ────────────────────────────────────── */
function openPriceModal(assetId) {
    const asset = state.assets.find(a => String(a.id) === String(assetId));
    if (!asset) return;
    const html = `<form id="modalForm">
        <div class="alert alert-info">手动价格适合银行理财、私募、未上市资产，或行情源暂不可用时临时补价。</div>
        <div class="form-grid">
            ${field('日期', `<input type="date" name="price_date" value="${todayIso()}">`)}
            ${field('收盘价 / 净值', `<input type="number" step="any" min="0" name="close" required>`)}
            ${field('上一日价格（可选）', `<input type="number" step="any" min="0" name="prev_close">`, '用于计算今日盈亏；留空则自动取上一条价格。', 'full')}
        </div>
        <div class="form-actions">
            <button type="button" class="btn btn-ghost" data-close>取消</button>
            <button type="submit" class="btn btn-primary">保存价格</button>
        </div>
    </form>`;
    openModal(`更新价格 · ${asset.name}`, html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);
    $('#modalForm').addEventListener('submit', async event => {
        event.preventDefault();
        try {
            await api(`/api/wm/assets/${asset.id}/price`, { method: 'POST', body: formDataObject(event.target) });
            toast('价格已更新', 'success');
            closeModal();
            await refreshAll();
        } catch (err) {
            toast(err.message, 'error');
        }
    });
}

/* ── DCA plan modal ────────────────────────────────────────── */
function openPlanModal(plan = null) {
    if (!plan && !state.assets.filter(a => a.is_active).length) {
        return toast('请先添加资产', 'error');
    }
    const editing = Boolean(plan);
    const selectedAsset = plan?.asset_id || state.assets.find(a => a.is_active)?.id;
    const html = `<form id="modalForm">
        <div class="form-grid">
            ${field('资产', `<select name="asset_id" id="planAsset" ${editing ? 'disabled' : ''}>${assetOptions(selectedAsset)}</select>`, '', 'full')}
            ${field('计划名称', `<input name="name" value="${esc(plan?.name || '')}" placeholder="留空自动命名">`, '', 'full')}
            ${field('每期金额', `<input type="number" step="any" min="0" name="amount" value="${plan?.amount ?? 1000}" required>`)}
            ${field('金额币种', `<select name="amount_currency">${['CNY', 'USD', 'HKD'].map(c => `<option value="${c}" ${(plan?.amount_currency || 'CNY') === c ? 'selected' : ''}>${c}</option>`).join('')}</select>`)}
            ${field('频率', `<select name="frequency" id="planFrequency">${[['daily', '每交易日'], ['weekly', '每周'], ['biweekly', '每两周'], ['monthly', '每月']].map(([v, l]) => `<option value="${v}" ${(plan?.frequency || 'monthly') === v ? 'selected' : ''}>${l}</option>`).join('')}</select>`)}
            <div id="planWeekdayWrap">${field('星期', `<select name="weekday">${WEEKDAYS.map((w, i) => `<option value="${i}" ${plan?.weekday === i ? 'selected' : ''}>${w}</option>`).join('')}</select>`)}</div>
            <div id="planDomWrap">${field('每月日期', `<input type="number" min="1" max="31" name="day_of_month" value="${plan?.day_of_month ?? 1}">`, '遇到月末自动取当月最后一天')}</div>
            ${field('开始日期', `<input type="date" name="start_date" value="${esc(plan?.start_date || todayIso())}" ${editing ? 'readonly' : ''}>`)}
            ${field('结束日期（可选）', `<input type="date" name="end_date" value="${esc(plan?.end_date || '')}">`)}
            ${field('非交易日处理', `<select name="roll_rule">${[['next', '顺延到下一交易日'], ['prev', '提前到上一交易日'], ['skip', '跳过']].map(([v, l]) => `<option value="${v}" ${(plan?.roll_rule || 'next') === v ? 'selected' : ''}>${l}</option>`).join('')}</select>`, '日投默认按交易日投一次')}
            ${field('费率', `<input type="number" step="0.0001" min="0" name="fee_rate" value="${plan?.fee_rate ?? 0.0015}">`, '基金为申购费率；股票/ETF 为佣金率')}
            ${field('最低费用', `<input type="number" step="any" min="0" name="fee_min" value="${plan?.fee_min ?? 0}">`, 'A股佣金常见 5 元')}
            ${field('账户', `<select name="account_id">${accountOptions(plan?.account_id)}</select>`)}
            <div class="full">
                <label class="field"><span>历史期数</span>
                    <select name="catch_up"><option value="1" ${plan?.catch_up !== 0 ? 'selected' : ''}>补记（从开始日期起）</option><option value="0" ${plan?.catch_up === 0 ? 'selected' : ''}>不补记（只从最近一期开始）</option></select>
                    <small class="hint">开始日期早于今天时，会先预览需要补记的期数。</small>
                </label>
            </div>
            ${field('备注', `<textarea name="note" placeholder="可选">${esc(plan?.note || '')}</textarea>`, '', 'full')}
        </div>
        <div class="preview-box" id="planPreview" style="display:none"></div>
        <label class="field" id="planConfirmWrap" style="display:none;margin-top:10px">
            <span style="display:flex;align-items:center;gap:7px">
                <input type="checkbox" id="planConfirmBackfill" style="width:auto">
                我已确认补记上述历史期数
            </span>
        </label>
        <div class="form-actions">
            <button type="button" class="btn btn-ghost" id="previewPlan">预览期数</button>
            <button type="button" class="btn btn-ghost" data-close>取消</button>
            <button type="submit" class="btn btn-primary">${editing ? '保存' : '创建计划'}</button>
        </div>
    </form>`;
    openModal(editing ? '编辑定投计划' : '新建定投计划', html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);

    const adjustFrequency = () => {
        const freq = $('#planFrequency').value;
        $('#planWeekdayWrap').style.display = ['weekly', 'biweekly'].includes(freq) ? 'block' : 'none';
        $('#planDomWrap').style.display = freq === 'monthly' ? 'block' : 'none';
    };
    $('#planFrequency').addEventListener('change', adjustFrequency);
    adjustFrequency();

    const applyFeeDefaults = () => {
        if (editing) return;
        const a = state.assets.find(x => String(x.id) === String($('#planAsset').value));
        if (!a) return;
        const rate = $('#modalForm [name=fee_rate]');
        const min = $('#modalForm [name=fee_min]');
        if (a.market === 'FUND_CN') {
            rate.value = '0.0015';
            min.value = '0';
        } else if (a.market === 'CN' && ['stock', 'etf'].includes(a.asset_class)) {
            rate.value = '0.00025';
            min.value = '5';
        } else {
            rate.value = '0';
            min.value = '0';
        }
    };
    $('#planAsset').addEventListener('change', applyFeeDefaults);
    applyFeeDefaults();

    const collect = () => {
        const data = formDataObject($('#modalForm'));
        data.catch_up = $('#modalForm [name=catch_up]').value === '1';
        if (editing) data.asset_id = plan.asset_id;
        return data;
    };

    const renderPlanPreview = (preview) => {
        state.planPreview = preview;
        const box = $('#planPreview');
        box.style.display = 'block';
        const rows = (preview.due_dates || []).map(d =>
            `<div class="preview-row"><span>${esc(d.due_date)}</span><span>${d.trade_date ? '预计 ' + esc(d.trade_date) : '非交易日跳过'}</span></div>`
        ).join('');
        box.innerHTML = `<div class="preview-row"><b>预计补记 ${preview.backfill_count || 0} 期</b><span>${esc(preview.asset?.name || '')}</span></div>` +
            (preview.backfill_count > 0 ? `<div class="warn" style="padding:4px 0">开始日期在过去，创建后会立即生成这些订单并自动入账/等待价格。</div>` : '') +
            rows;
        const wrap = $('#planConfirmWrap');
        const checkbox = $('#planConfirmBackfill');
        const need = (preview.backfill_count || 0) > 0;
        if (wrap) wrap.style.display = need ? 'flex' : 'none';
        if (checkbox) checkbox.checked = false;
    };

    $('#previewPlan').addEventListener('click', async () => {
        const btn = $('#previewPlan');
        btn.disabled = true;
        try {
            renderPlanPreview(await api('/api/wm/dca/preview', { method: 'POST', body: collect() }));
        } catch (err) {
            toast(err.message, 'error');
        } finally {
            btn.disabled = false;
        }
    });

    let submitting = false;
    $('#modalForm').addEventListener('submit', async event => {
        event.preventDefault();
        if (submitting) return;
        const data = collect();
        if (state.planPreview?.backfill_count > 0) {
            if (!$('#planConfirmBackfill')?.checked) {
                return toast('请先预览并勾选确认补记历史期数', 'error');
            }
            data.confirm_backfill = true;
        }
        submitting = true;
        const submitBtn = event.target.querySelector('button[type=submit]');
        if (submitBtn) submitBtn.disabled = true;
        try {
            if (editing) {
                await api('/api/wm/dca/plans/' + plan.id, { method: 'PUT', body: data });
                toast('已保存，后续订单按新设置生成', 'success');
            } else {
                await api('/api/wm/dca/plans', { method: 'POST', body: data });
                toast('定投计划已创建', 'success');
            }
            closeModal();
            await refreshAll();
            await loadOrders();
        } catch (err) {
            if (err.status === 409 && err.data?.preview) {
                renderPlanPreview(err.data.preview);
                toast('该计划需要补记历史期数，请勾选确认后再提交', 'error');
            } else {
                toast(err.message, 'error');
            }
        } finally {
            submitting = false;
            if (submitBtn) submitBtn.disabled = false;
        }
    });
}

/* ── import modals ─────────────────────────────────────────── */
function openImportModal() {
    const html = `<div class="alert alert-info">
        从 Ghostfolio 导出的 JSON（含 activities）可直接导入。重复交易会按内容指纹跳过；LIABILITY 类型会忽略。
        导入后会为缺少行情的资产在后台拉取历史价格。
    </div>
    <label class="field"><span>Ghostfolio JSON 文件</span><input type="file" id="ghostFile" accept=".json,application/json"></label>
    <div class="preview-box" id="importPreview" style="display:none"></div>
    <div class="form-actions">
        <button type="button" class="btn btn-ghost" data-close>关闭</button>
        <button type="button" class="btn btn-ghost" id="ghostPreviewBtn">预览</button>
        <button type="button" class="btn btn-primary" id="ghostCommitBtn" disabled>确认导入</button>
    </div>`;
    openModal('导入 Ghostfolio JSON', html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);
    let parsed = null;

    const readFile = async () => {
        const file = $('#ghostFile').files[0];
        if (!file) throw new Error('请选择 JSON 文件');
        const text = await file.text();
        return JSON.parse(text);
    };

    $('#ghostPreviewBtn').addEventListener('click', async () => {
        try {
            parsed = await readFile();
            const data = await api('/api/wm/import/ghostfolio?dry_run=1', { method: 'POST', body: parsed });
            const box = $('#importPreview');
            box.style.display = 'block';
            box.innerHTML = `<div class="preview-row"><b>可导入 ${data.activities} 笔</b><span>新资产 ${data.new_assets?.length || 0} 个</span></div>
                <div class="preview-row"><span>重复跳过</span><span>${data.duplicates || 0}</span></div>
                <div class="preview-row"><span>账户余额</span><span>${data.balances || 0} 条</span></div>
                <div class="preview-row"><span>不支持/跳过</span><span>${data.skipped || 0}</span></div>
                ${data.errors?.length ? `<div class="warn">${data.errors.map(esc).join('<br>')}</div>` : ''}
                ${data.new_assets?.length ? `<div style="margin-top:6px">${data.new_assets.map(a => `<span class="tag">${esc(a.name)} ${esc(a.symbol)}</span>`).join(' ')}</div>` : ''}`;
            $('#ghostCommitBtn').disabled = false;
        } catch (err) {
            toast(err.message, 'error');
        }
    });

    $('#ghostCommitBtn').addEventListener('click', async () => {
        if (!parsed) return toast('先预览', 'error');
        const btn = $('#ghostCommitBtn');
        btn.disabled = true;
        try {
            const data = await api('/api/wm/import/ghostfolio?dry_run=0', { method: 'POST', body: parsed });
            const warningHtml = data.warnings?.length
                ? `<div class="muted" style="margin-top:4px">${data.warnings.map(esc).join('<br>')}</div>` : '';
            if (data.errors?.length) {
                toast(`导入 ${data.imported} 笔，${data.errors.length} 条失败`, 'error');
                const box = $('#importPreview');
                box.style.display = 'block';
                box.innerHTML = `<div class="preview-row"><b>导入 ${data.imported} 笔</b><span>失败 ${data.errors.length} 条</span></div>
                    <div class="warn">${data.errors.map(esc).join('<br>')}</div>${warningHtml}`;
                btn.disabled = false;
                await refreshAll();
                await loadTransactions(1);
                return;
            }
            toast(data.warnings?.length
                ? `导入完成：${data.imported} 笔；${data.warnings.length} 笔使用最新汇率`
                : `导入完成：${data.imported} 笔，新资产 ${data.new_assets} 个`
                  + (data.balances ? `，账户余额 ${data.balances} 条` : ''), 'success');
            closeModal();
            await refreshAll();
            await loadTransactions(1);
        } catch (err) {
            toast(err.message, 'error');
            btn.disabled = false;
        }
    });
}

function openCsvImportModal() {
    const html = `<div class="alert alert-info">
        CSV 列：<code>market,symbol,name,asset_class,type,trade_date,units,price,amount,fee,currency,account,split_ratio,note</code>。
        可先下载模板。金额为不含费用的成交额，费用单独填。
    </div>
    <label class="field"><span>CSV 文件</span><input type="file" id="csvFile" accept=".csv,text/csv"></label>
    <div class="preview-box" id="csvPreview" style="display:none"></div>
    <div class="form-actions">
        <a class="btn btn-ghost" href="/api/wm/import/csv/template">下载模板</a>
        <button type="button" class="btn btn-ghost" data-close>关闭</button>
        <button type="button" class="btn btn-ghost" id="csvPreviewBtn">预览</button>
        <button type="button" class="btn btn-primary" id="csvCommitBtn" disabled>确认导入</button>
    </div>`;
    openModal('导入 CSV', html);
    $('#modalBody [data-close]')?.addEventListener('click', closeModal);

    const send = async (dryRun) => {
        const file = $('#csvFile').files[0];
        if (!file) throw new Error('请选择 CSV 文件');
        const form = new FormData();
        form.append('file', file);
        return api('/api/wm/import/csv?dry_run=' + (dryRun ? '1' : '0'), { method: 'POST', body: form });
    };

    $('#csvPreviewBtn').addEventListener('click', async () => {
        try {
            const data = await send(true);
            const box = $('#csvPreview');
            box.style.display = 'block';
            box.innerHTML = `<div class="preview-row"><b>${data.rows} 行</b><span>${data.errors?.length || 0} 个错误</span></div>` +
                (data.errors?.length ? `<div class="warn">${data.errors.map(esc).join('<br>')}</div>` : '');
            $('#csvCommitBtn').disabled = data.errors?.length > 0;
        } catch (err) {
            toast(err.message, 'error');
        }
    });

    $('#csvCommitBtn').addEventListener('click', async () => {
        const btn = $('#csvCommitBtn');
        btn.disabled = true;
        try {
            const data = await send(false);
            const warningHtml = data.warnings?.length
                ? `<div class="muted" style="margin-top:4px">${data.warnings.map(esc).join('<br>')}</div>` : '';
            if (data.errors?.length) {
                toast(`导入 ${data.imported} 笔，${data.errors.length} 条失败`, 'error');
                const box = $('#csvPreview');
                box.style.display = 'block';
                box.innerHTML = `<div class="preview-row"><b>导入 ${data.imported} 笔</b><span>失败 ${data.errors.length} 条</span></div>
                    <div class="warn">${data.errors.map(esc).join('<br>')}</div>${warningHtml}`;
                btn.disabled = false;
                await refreshAll();
                await loadTransactions(1);
                return;
            }
            toast(data.warnings?.length
                ? `导入完成：${data.imported} 笔；${data.warnings.length} 笔使用最新汇率`
                : `导入完成：${data.imported} 笔`, 'success');
            closeModal();
            await refreshAll();
            await loadTransactions(1);
        } catch (err) {
            toast(err.message, 'error');
            btn.disabled = false;
        }
    });
}

document.addEventListener('DOMContentLoaded', boot);
})();
