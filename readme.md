# QDII 基金数据看板

Flask + SQLite 后端、纯前端展示的 QDII 基金数据看板。自动采集全市场 QDII 基金的多维度数据，支持搜索、筛选、排序、分页，并附带日申购额度监控与额度历史曲线。

- 访问路径：**根路径 `/`**（例如 `http://localhost:5000/`）
- 数据库：SQLite（单文件，默认在项目目录下 `qdii.db`）
- 采集引擎：AKShare（东方财富 / 天天基金）

---

## 目录

- [功能特性](#功能特性)
- [个人财富管理](#个人财富管理)
- [技术栈与目录结构](#技术栈与目录结构)
- [环境要求](#环境要求)
- [方式一：本地运行（推荐用于开发/自用）](#方式一本地运行推荐用于开发自用)
- [方式二：Docker Compose 部署](#方式二docker-compose-部署)
- [方式三：Ubuntu + Nginx + PM2 部署](#方式三ubuntu--nginx--pm2-部署)
- [环境变量](#环境变量)
- [定时任务](#定时任务)
- [数据库表](#数据库表)
- [API 接口](#api-接口)
- [Webhook 通知](#webhook-通知)
- [升级与数据迁移](#升级与数据迁移)
- [常见问题](#常见问题)
- [更新日志](#更新日志)

---

## 功能特性

- **QDII 基金列表**：代码、名称、类型、场内/场外标识、最新净值、累计净值、近 1/2/3/4/5/10 年涨跌幅、年化收益、总收益、管理费/托管费/销售费、申购状态、日累计限额、场内折溢价、日涨跌幅
- 搜索、排序、筛选（成立以来每年正收益 / 年化 > X%）、分页
- 自选 / 自评级（localStorage 持久化，支持导出、导入备份）
- CSV 导出、移动端自适应卡片布局
- 定时自动采集 + 首次启动自动全量采集
- **日申购额度监控**：交易日 9:00–19:00 每小时检测额度与申购状态变化，未读徽标提醒 + webhook 推送
- **额度历史曲线**：每日快照落库，点击基金「日额度」可查看 30 天 / 90 天 / 1 年 / 全部区间的阶梯走势

### 数据源

| 类型 | 数据源 | 采集引擎 | 更新时间（北京时间） |
|------|--------|----------|-------------------|
| QDII 基金 | 东方财富（天天基金） | AKShare | 每月 1 号 21:00 全量 / 每日 21:00 增量 |

---

## 个人财富管理

在 QDII 看板基础上增加的个人全资产账本，入口为 **`/wealth`**，与 QDII 共用同一个 Flask 进程和 `qdii.db`（新表统一使用 `wm_` 前缀，不修改原有 QDII 表）。

### 阶段状态

| 阶段 | 内容 | 状态 |
|---|---|---|
| P1 | 账本、迁移、单密码登录、Ghostfolio/CSV 导入导出、单元测试 | ✅ |
| P2 | 行情同步、历史回填、定投、银行理财计息 | ✅ |
| P3 | XIRR、TWR、最大回撤、月度收益、资产配置、文档 | ✅ |
| P4 | 账户管理、账户现金、按账户持仓/盈亏、导入数据质量 | ✅ |

### 功能

- **多资产**：A 股、港股、美股、场外基金、加密货币、银行理财、手动资产
- **资产币种**：默认按市场（A 股/场外基金/银行 CNY、美股 USD、港股 HKD、加密 USD），可在添加/编辑时手动改为 `CNY`/`USD`/`HKD`；非本币资产会自动使用手动价格，避免把本币行情当外币
- **投资账户**：账户管理（新增/改名/停用/删除未使用）、交易和定投按账户归属、账户现金余额、按账户查看持仓和盈亏、交易/持仓按账户筛选
- **账本**：买入 / 卖出 / 分红 / 分红再投 / 利息 / 费用 / 拆分，持仓与成本由交易流水实时计算（移动平均成本法）
- **资产删除**：没有定投计划的资产可删除；已有交易时需确认强制删除，会连同该资产的交易和价格一起删除
- **自动定投**：固定金额，支持每日 / 每周 / 每两周 / 每月；到期自动按交易日收盘价或基金净值入账，**无需确认**
- **银行理财**：按本金 + 日计提收益估值，支持单利/复利、到期日、利息到账记录
- **收益分析**：人民币视角的总资产、累计/今日盈亏、XIRR、TWR、最大回撤、资产走势、月度收益、资产配置
- **导入**：Ghostfolio JSON（保留账户、现金余额、资产名称/类别）、CSV；**导出**：JSON / CSV
- **默认账户**：首次初始化自带「默认账户」，可在账户页新增、改名、停用或删除未使用账户

> 总资产 = 证券市值 + 账户现金；XIRR、TWR、最大回撤和走势图目前为**证券口径**，不含账户现金。负现金余额计入总资产，但配置饼图只展示正现金余额。
>
> 从旧版本升级到 P4 后，建议重新导入一次 Ghostfolio JSON：已有交易会按重复跳过，但会补入账户余额并更新资产类别/名称。

> 本功能只做本地记账与行情展示，**不会向任何券商或交易所下单**。

### 访问密码

设置环境变量 `WM_PASSWORD` 后，外网访问 `/wealth` 需要先输入密码。未设置时仅允许本机（`127.0.0.1` / `::1`）访问，外网请求返回 503。

### 定投规则

- `daily`：每个交易日最多投一次；周末/节假日不生成订单，不把多天合并到下一个交易日。
- `weekly` / `biweekly` / `monthly`：按 `roll_rule` 处理非交易日（顺延 / 提前 / 跳过）。
- 订单状态：`awaiting_price`（等净值/收盘价）→ `filled`；异常时 `failed`，手动跳过为 `skipped`。
- 创建历史开始的计划时会先预览补记期数，确认后才生成；单次最多补记 500 期，剩余在下次任务继续。
- 场外基金费用：`fee = 金额 - 金额 / (1 + 费率)`，份额 `= (金额 - fee) / 净值`；股票/ETF 按佣金率和最低佣金计算，A 股按 100 股整手取整。
- 定时任务按**交易日收盘/净值**结算；「立即执行」允许使用当前可见的最新价格。

### 数据源

| 资产 | 行情源 |
|------|--------|
| A 股 / 港股 / 美股 | 东方财富 `push2.eastmoney.com`、`push2his.eastmoney.com` |
| 场外基金 | 天天基金 `fund.eastmoney.com/pingzhongdata/{code}.js` |
| 加密货币 | Binance `data-api.binance.vision` |
| 汇率 | `frankfurter.app`（历史）/ `open.er-api.com` / 东方财富 USDCNH/HKDCNH（最新兜底） |
| A 股交易日历 | 东方财富上证指数日 K |

> 本机网络若无法访问上述域名，资产仍可创建、使用手工价格；非人民币资产的记账需要汇率源可用。部署到可访问外网的 VPS 后行情会自动同步。

### 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `WM_PASSWORD` | 空 | 财富管理访问密码；为空时仅本机可访问 |
| `WM_SECRET_KEY` | 随机 | Flask session 密钥；不设置时每次重启会使登录失效 |
| `WM_DB_PATH` | `DATA_DIR/qdii.db` | 可选，财富模块单独的 SQLite 路径；一般不需要设置 |

> 以上变量均可写在项目根目录 `.env`（复制 `.env.example`）；`app.py` 启动时自动加载，系统环境变量优先。

### 定时任务（新增）

| 任务 | 触发时间 | 说明 |
|------|----------|------|
| `wm_sync_cn` | 周一至周五 15:10 | 同步 A 股/ETF 收盘价 |
| `wm_sync_fund` | 每日 21:30 | 同步场外基金净值 |
| `wm_sync_us` | 每日 07:00 | 同步美股/港股（等待上一交易日收盘） |
| `wm_sync_crypto` | 每 2 小时（第 5 分钟） | 同步加密货币日线 |
| `wm_sync_fx` | 每日 09:00 | 同步汇率 |
| `wm_dca_generate` | 每日 00:10 | 生成到期定投订单 |
| `wm_dca_settle` | 每 30 分钟 | 用已发布价格结算订单 |

启动时会在后台执行一次定投补跑，避免停机错过期数。

### 主要接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/wealth` | 财富管理页面（需登录） |
| GET | `/api/wm/status` | 状态、待处理订单、最近同步 |
| GET | `/api/wm/summary?range=1y` | 总资产、盈亏、XIRR、TWR、配置 |
| GET | `/api/wm/history?range=1y` | 每日资产/成本/净流入 |
| GET | `/api/wm/positions` | 持仓明细 |
| GET/POST | `/api/wm/assets` | 资产列表 / 新增 |
| PUT/DELETE | `/api/wm/assets/<id>` | 修改 / 删除（无交易/计划可改币种并清除旧价格；有交易需 `?force=1`；有定投计划时不可删） |
| POST | `/api/wm/assets/<id>/price` | 手工价格 |
| POST | `/api/wm/assets/<id>/backfill` | 回补历史行情 |
| GET/POST | `/api/wm/transactions` | 交易列表 / 记账（支持 `account_id` 筛选） |
| GET/PUT/DELETE | `/api/wm/transactions/<id>` | 单笔交易 |
| GET/POST | `/api/wm/accounts` | 账户列表（含交易/计划数、现金）/ 新建 |
| PUT/DELETE | `/api/wm/accounts/<id>` | 修改 / 删除未使用账户 |
| GET/POST | `/api/wm/dca/plans` | 定投计划 |
| PUT/DELETE | `/api/wm/dca/plans/<id>` | 修改 / 归档 |
| POST | `/api/wm/dca/plans/<id>/pause\|resume\|run-now` | 暂停 / 恢复 / 立即执行 |
| GET | `/api/wm/dca/orders` | 执行记录 |
| POST | `/api/wm/dca/orders/<id>/skip\|retry` | 跳过 / 重试 |
| POST | `/api/wm/dca/preview` | 预览期数与补记数量 |
| POST | `/api/wm/dca/run` | 手动触发生成+结算 |
| POST | `/api/wm/sync` | 同步全部行情 |
| POST | `/api/wm/import/ghostfolio?dry_run=1` | Ghostfolio JSON 预览/导入 |
| POST | `/api/wm/import/csv?dry_run=1` | CSV 预览/导入 |
| GET | `/api/wm/export?format=csv` | 导出账本 |

### 测试

```bash
python -m unittest tests.test_wealth_core -v
```

### 已知限制

- 第一版不支持组合级定投、智能/价值平均定投、历史回测。
- 总资产包含账户现金余额；XIRR、TWR、最大回撤和走势图目前是**证券口径**，不含现金。负现金余额计入总资产，但不进入配置饼图。
- 账户现金余额来自 Ghostfolio `accounts[].balances` 快照的最新一条，不是完整的现金流水账；现金只影响总资产和账户页，不参与 XIRR/TWR。
- 持仓按 `(账户, 资产)` 拆分；账户页提供现金、证券市值和盈亏，历史走势和 XIRR/TWR 仍是组合/证券级别，没有账户级时间序列。
- 有交易或定投计划的账户不能删除，只能停用；删除账户会同时删除其现金余额。
- 累计盈亏按移动平均成本法；分红再投会增加份额和成本，总收益看 XIRR/TWR。
- Ghostfolio 导出格式存在多个版本，导入器按常见 `activities` 结构做了兼容；建议先 `dry_run=1` 预览。
- CSV 导入不做内容去重（Ghostfolio 导入会按内容指纹去重），请勿把同一批交易重复导入同一数据库。
- XIRR 在极短周期（几天）内年化后可能非常大，这是数学结果，不代表真实长期收益。
- 港股每手股数因股票而异，默认 100 股，可在资产编辑中修改最小交易单位。
- 行情源不可用时不会伪造价格：订单会保持 `awaiting_price`，超过 14 天标记失败，可重试或手工补价。
- 非人民币资产缺少历史汇率时，手工/导入记账会报错，定投订单会等待；只有当天记账会用最新汇率兜底，不会按 1:1 静默入账。组合估值缺汇率时会标记「缺汇率」。
- 导入 Ghostfolio/CSV 时会先统一预取汇率，不会逐条等待网络；最近 7 天的记录在历史汇率不可用时可使用最新汇率（结果里会返回 `warnings`），更早的记录会跳过并在 `errors` 中列出。

---

## 技术栈与目录结构

| 层 | 技术 |
|----|------|
| 后端 | Python 3.10+ / Flask / APScheduler |
| 存储 | SQLite（WAL 模式） |
| 采集 | AKShare、pandas、requests、BeautifulSoup |
| 前端 | 原生 HTML/JS + ECharts（`static/vendor/echarts.min.js` 本地引入，无 CDN 依赖） |

```
fundx/
├── app.py                  # Flask 主应用：路由、定时任务、额度曲线计算
├── fetcher.py              # QDII 全量/增量采集引擎（AKShare）
├── quota_watcher.py        # 日额度变动检测 + webhook 推送
├── wealth/                 # 个人财富管理模块
│   ├── __init__.py         # 模块导出
│   ├── db.py               # 财富 DB 连接/时区
│   ├── schema.py           # wm_* 表结构
│   ├── portfolio.py        # 持仓/成本/估值/XIRR/TWR
│   ├── prices.py           # 行情、净值、汇率、交易日历
│   ├── dca.py              # 定投订单生成/结算
│   ├── jobs.py             # APScheduler 任务
│   └── routes.py           # /wealth 页面与 /api/wm/* 接口
├── templates/
│   ├── index.html          # QDII 单页前端（含全部样式与脚本）
│   ├── wealth.html         # 财富管理前端
│   └── wealth_login.html   # 财富管理登录页
├── static/
│   ├── wealth.css          # 财富管理样式
│   ├── wealth.js           # 财富管理前端逻辑
│   └── vendor/
│       └── echarts.min.js  # 本地 ECharts
├── tests/
│   └── test_wealth_core.py # 财富管理核心单元测试
├── requirements.txt        # Python 依赖
├── .env.example            # 环境变量模板（复制为 .env，.env 不提交）
├── Dockerfile              # 镜像构建
├── docker-compose.yml      # 容器编排（端口 5000、数据挂载 ./data）
├── ecosystem.config.js     # PM2 进程配置
├── cleanup.py              # 一次性清理脚本（历史遗留，可不使用）
└── readme.md
```

运行时自动生成：

```
qdii.db                  # SQLite 数据库（本地/PM2 部署，默认在项目根目录）
qdii.db-wal/-shm         # WAL 日志（SQLite 自动维护）
qdii.db.full_fetch_flag  # 最近一次全量采集日期标记
data/qdii.db             # Docker 部署的数据库（挂载到容器 /app/data）
logs/err.log、logs/out.log  # PM2 日志（仅 PM2 部署方式）
```

---

## 环境要求

- **Python ≥ 3.10**（推荐 3.11 / 3.12；Docker 镜像使用 3.12-slim）
- 可访问外网（需要请求天天基金接口）
- 时区建议设为 `Asia/Shanghai`（定时任务按北京时间触发）
- 部署方式二选一所需：
  - Docker 20.10+ / Docker Compose v2
  - 或 Ubuntu 20.04+ + Nginx + Node.js（仅用于 PM2）

---

## 方式一：本地运行（推荐用于开发/自用）

### 1. 获取代码

```bash
git clone <你的仓库地址> fundx
cd fundx
```

### 2. 创建虚拟环境并安装依赖

**Linux / macOS**

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**Windows（PowerShell / CMD）**

```powershell
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

> 首次安装 AKShare 会连带安装 pandas、numpy 等，耗时约 1–3 分钟。`fetcher.py` 还用到了 BeautifulSoup，已在 `requirements.txt` 中显式声明 `beautifulsoup4`。

### 3. 配置环境变量（推荐）

`app.py` 会通过 `python-dotenv` 自动读取**项目根目录**（与 `app.py` 同级）的 `.env`。系统环境变量优先于 `.env`。

```bash
# Linux / macOS
cp .env.example .env

# Windows PowerShell
Copy-Item .env.example .env
```

编辑 `.env`：

```env
WM_PASSWORD=你的财富管理密码
WM_SECRET_KEY=一段足够长的随机字符串
```

- `.env` 已在 `.gitignore` 中，不要提交真实密码。
- `WM_PASSWORD` 为空时，只允许本机（`127.0.0.1` / `::1`）访问 `/wealth`，外网返回 503。
- `WM_SECRET_KEY` 不设置时每次重启都会生成随机密钥，登录状态会失效。
- 本地 / PM2 部署可在 `.env` 里加 `DATA_DIR=./data`；Docker 部署固定使用 `/app/data`，不需要设置。
- 修改 `.env` 后需要重启进程（`python app.py` / PM2 / 容器）。

### 4. 启动

```bash
python app.py
```

浏览器打开 <http://localhost:5000/>。

首次启动时数据库为空，会**立即在后台线程触发一次全量采集**（全市场 QDII 约 300–500 只，每只都要请求历史净值，通常需要 **10–30 分钟**）。采集期间页面可正常打开，只是列表为空；采集完成后刷新即可看到数据。控制台会打印进度：

```
🚀 采集任务开始...
✅ 采集完成，耗时 812s，共 456 条
📅 定时任务已启动：每月1号 21:00 全量采集，每日 21:00 增量更新，额度监控交易日 9:00-19:00 每小时及 21:05
```

### 5. 指定数据目录（可选）

默认数据库写在 `app.py` 同级目录。想放到别处（例如 NAS 挂载盘）：

**Linux / macOS**

```bash
DATA_DIR=/var/lib/fundx python app.py
```

**Windows**

```powershell
$env:DATA_DIR="D:\fundx-data"; python app.py
```

也可以写在 `.env` 里：

```env
DATA_DIR=./data
```

### 6. 后台常驻（可选）

本地长期运行时可用 `nohup` 或 PM2，参考[方式三](#方式三ubuntu--nginx--pm2-部署)（PM2 部分与操作系统无关，但 `ecosystem.config.js` 里的解释器路径写的是 `./venv/bin/python3`，Windows 需改成 `./venv/Scripts/python.exe`）。

---

## 方式二：Docker Compose 部署

### 说明

- 容器内 Flask **单进程**运行，APScheduler 也在容器内，避免多进程导致定时任务重复触发
- SQLite 通过 volume 持久化到宿主机 `./data/qdii.db`，重建容器不丢数据
- 宿主机 `5000` 端口映射到容器 `5000`，启动后直接访问

### 1. 构建并启动

```bash
git clone https://github.com/mdsqeh/QDIIdash.git && cd QDIIdash

# 数据目录（compose 里挂载的是相对路径 ./data，不存在会自动创建）
mkdir -p data

# 创建 .env（与 docker-compose.yml 同目录），填写密码后再启动
cp .env.example .env
# 编辑 .env：至少设置 WM_PASSWORD 和 WM_SECRET_KEY

docker compose up -d --build

# 查看日志（首次会看到全量采集进度）
docker compose logs -f

# 停止并移除容器（数据保留在宿主机）
docker compose down
```

> Docker Compose 会自动读取 `docker-compose.yml` 同目录的 `.env`，用于 `docker-compose.yml` 里的 `${WM_PASSWORD:-}` 等变量替换；**不需要把 `.env` 复制进镜像**。
>
> 修改 `.env` 后执行 `docker compose up -d --force-recreate` 让容器重新读取。
>
> 不创建 `.env` 也能启动，但 `WM_PASSWORD` 为空时 `/wealth` 只允许本机访问，外网返回 503。

### 2. 访问与端口调整

启动后访问 <http://服务器IP:5000/>（本机即 <http://localhost:5000/>）。

`docker-compose.yml` 内容：

```yaml
services:
  fundx:
    build: .
    ports:
      - "5000:5000"          # 宿主机:容器
    restart: unless-stopped
    # 同目录的 .env 会被 Docker Compose 自动读取，用于下面的 ${...} 替换
    environment:
      - TZ=Asia/Shanghai
      - DATA_DIR=/app/data
      - WM_PASSWORD=${WM_PASSWORD:-}
      - WM_SECRET_KEY=${WM_SECRET_KEY:-}
      - WM_DB_PATH=${WM_DB_PATH:-}
    volumes:
      - ./data:/app/data     # 相对 compose 文件所在目录
```

- 宿主 5000 已被占用时，改左边即可，例如 `"8080:5000"`，访问 `http://服务器IP:8080/`（容器内端口不要改）
- 想用 Nginx 统一 80/443 入口，可把 `ports` 改成 `"127.0.0.1:5000:5000"` 只监听本机，再由宿主机 Nginx 反代到 `127.0.0.1:5000`（见方式三的 Nginx 配置）
- 若 Nginx 也跑在容器里，则把 fundx 加入同一 docker network，`proxy_pass` 写成 `http://fundx:5000`

> 数据目录用相对路径 `./data`，Windows / macOS / Linux 宿主都能直接用，无需预先创建绝对路径。

### 3. 首次从旧版本升级（旧库还在容器里）

旧版本把 `qdii.db` 写在容器内 `/app/qdii.db`。升级前**必须先 `stop` 而不是 `down`**，否则容器被删除后旧库就找不到了：

```bash
docker compose stop
cid=$(docker compose ps -aq fundx)
mkdir -p data
docker cp "$cid":/app/qdii.db      data/qdii.db
docker cp "$cid":/app/qdii.db-wal  data/qdii.db-wal 2>/dev/null || true
docker cp "$cid":/app/qdii.db-shm  data/qdii.db-shm 2>/dev/null || true
docker compose up -d --build
```

注意 `DATA_DIR=/app/data` 对应 compose 里的 volume `./data`，把库放到 `data/` 下才能被容器读到。

---

## 方式三：Ubuntu + Nginx + PM2 部署

### 前提条件

Ubuntu 20.04+ 服务器，一个域名（可选），已配置 DNS 解析。

### 第一步：安装基础环境

```bash
sudo apt update && sudo apt upgrade -y

# Python 与 venv
sudo apt install -y python3 python3-pip python3-venv

# Node.js（PM2 需要）
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Nginx
sudo apt install -y nginx

# PM2
sudo npm install -g pm2
```

### 第二步：上传代码并安装依赖

```bash
# 方式 A：从 Git 仓库拉取（推荐）
git clone <你的仓库地址> /home/ubuntu/qdii

# 方式 B：本地 scp 上传
# scp -r /本地路径/fundx ubuntu@服务器IP:/home/ubuntu/qdii

cd /home/ubuntu/qdii

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

mkdir -p logs data
```

### 第三步：配置 .env 并启动 PM2

`app.py` 会用 `python-dotenv` 自动读取项目根目录（`cwd` 所在目录）的 `.env`，因此 PM2 部署也不需要把密码写进 `ecosystem.config.js`。

```bash
cp .env.example .env
# 编辑 .env：至少设置 WM_PASSWORD 和 WM_SECRET_KEY
# - .env 已被 .gitignore 忽略
# - 系统环境变量优先于 .env
# - 若要把数据库放到 data/ 目录，可在 .env 里加 DATA_DIR=./data
```

`ecosystem.config.js` 已配置：单实例、自动重启、512M 内存上限、日志写入 `./logs`。

```bash
pm2 start ecosystem.config.js

# 修改 .env 后重启（让新进程重新加载）
pm2 restart qdii

# 开机自启
pm2 save
sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u ubuntu --hp /home/ubuntu
```

> `pip install -r requirements.txt` 会安装 `python-dotenv`；如果跳过了这一步，`.env` 不会被读取。
>
> Docker 部署不使用这里的 `DATA_DIR`（容器内固定 `/app/data`）。

### 第四步：配置 Nginx 反向代理

**重要：应用必须挂在根路径 `/`。** 前端所有接口都用绝对路径 `/api/...`，如果配成 `/qdii/` 子路径，接口会 404。

```nginx
server {
    listen 80;
    server_name qdii.yourdomain.com;   # 换成你的域名或服务器 IP

    access_log /var/log/nginx/qdii_access.log;
    error_log  /var/log/nginx/qdii_error.log;

    # 全量采集/额度曲线接口响应可能较慢，适当放宽超时
    proxy_read_timeout 120s;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

启用配置：

```bash
sudo ln -s /etc/nginx/sites-available/qdii /etc/nginx/sites-enabled/qdii
sudo nginx -t
sudo systemctl reload nginx
```

### 第五步：HTTPS（可选）

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d qdii.yourdomain.com
```

### 日常管理命令

```bash
# PM2
pm2 status                 # 运行状态
pm2 logs qdii              # 实时日志
pm2 restart qdii           # 重启（改代码后）
pm2 stop qdii              # 停止
pm2 delete qdii            # 移除

# Nginx
sudo nginx -t                    # 测试配置
sudo systemctl reload nginx      # 重载
```

### 验证部署

```bash
curl -s http://127.0.0.1:5000/api/stats   # 应返回 {"total": ..., "pos_1y": ..., ...}
curl -s http://127.0.0.1:5000/api/fetch/status
```

---

## 环境变量

`app.py` 启动时会用 `python-dotenv` 读取**项目根目录**（与 `app.py` 同级）的 `.env`。系统环境变量优先于 `.env`；模板见 `.env.example`，`.env` 已被 `.gitignore` 忽略。

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DATA_DIR` | `app.py` 所在目录 | SQLite 数据库存放目录，目录不存在会自动创建。Docker 部署时设为 `/app/data` |
| `TZ` | 系统时区 | 建议 `Asia/Shanghai`。容器内已在 Dockerfile 中设置 |
| `FLASK_ENV` | - | `ecosystem.config.js` 中设为 `production`，仅作标记，不影响行为 |
| `WM_PASSWORD` | 空 | 财富管理访问密码；为空时仅本机可访问 `/wealth` |
| `WM_SECRET_KEY` | 随机 | Flask session 密钥；建议固定，否则重启后登录失效 |
| `WM_DB_PATH` | `DATA_DIR/qdii.db` | 可选，财富模块单独使用一个 SQLite 文件 |

> 财富管理的页面、定投规则和接口说明见[个人财富管理](#个人财富管理)章节。

> 容器内监听端口固定为 `5000`（写在 `app.run(host='0.0.0.0', port=5000)`），不支持环境变量覆盖；宿主机端口在 compose 的 `ports` 里改。
> Webhook 地址与密钥是 `app.py` 中的 `WEBHOOK_URL` / `WEBHOOK_SECRET` 常量，需要改代码。

---

## 定时任务

APScheduler 在应用进程内运行（`Asia/Shanghai` 时区）：

| 任务 | 触发时间 | 说明 |
|------|----------|------|
| 全量采集 `monthly_fetch` | 每月 1 号 21:00 | 重建所有字段（净值、区间收益、费率、折溢价等），并写入当日全量标记 |
| 增量更新 `daily_incremental` | 每日 21:00 | 只更新 `nav`、`daily_change`、`premium_discount`、`purchase_status`、`daily_limit`；同时检测新基金并全量采集。**若当天已全量采集则自动跳过** |
| 额度检测 `quota_check` | 周一至周五 9:00–19:00 每小时整点 | 检测日累计限额与申购状态变化，写快照 + 变动事件 + webhook |
| 额度检测 `quota_check_after_fetch` | 周一至周五 21:05 | 增量更新后再校验一次额度 |

补充行为：

- 启动时若 `funds` 表为空 → 立即在后台线程执行一次全量采集
- 启动时若已有数据 → 跳过启动采集，等待定时任务
- 采集任务带进程内互斥锁，重复触发会直接跳过

---

## 数据库表

| 表 | 用途 |
|----|------|
| `funds` | 基金主表，每只基金一行（代码为主键），含净值、区间收益、费率、申购状态、日额度、折溢价等 |
| `fetch_log` | 采集日志：状态（running/done/failed）、起止时间、条数、错误信息 |
| `quota_changes` | 额度变动事件：基金代码、字段（`daily_limit`/`purchase_status`）、旧值、新值、检测时间、已读标记 |
| `quota_daily` | 每日额度快照（`code + trade_date` 主键），额度曲线的数据来源 |

表结构在 `app.py` 的 `init_db()` 中创建；升级时通过 `ALTER TABLE` 自动补列，**不需要手动迁移**。

财富管理使用同一数据库中的 `wm_asset`、`wm_transaction`、`wm_price`、`wm_fx_rate`、`wm_dca_plan`、`wm_dca_order`、`wm_account`、`wm_account_balance`、`wm_trade_calendar`、`wm_sync_log`、`wm_import_log` 等表，均由 `wealth/schema.py` 自动创建。

---

## API 接口

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/` | 前端页面 |
| GET | `/api/funds` | 基金列表。参数：`search`、`sort`、`order`、`page`、`per_page`、`exclude`、`filter=pos_1y\|ann_gt`、`ann_threshold` |
| GET | `/api/stats` | 统计：总数、近 1 年正收益数、年化超阈值数（`ann_threshold`，默认 20） |
| GET | `/api/fetch/status` | 最近一次采集状态 |
| GET | `/api/quota/changes` | 未读额度变动（用于 Tab 徽标，最多 50 条） |
| GET | `/api/quota/history` | 额度变动历史。参数：`date`（默认今天）、`is_read`（`0`/`1`） |
| GET | `/api/quota/fund/<code>` | 单只基金额度曲线。参数：`days=30\|90\|365\|all` |
| POST | `/api/quota/ack` | 全部标记已读 |
| POST | `/api/quota/ack-single` | 单条标记已读，body：`{"id": 123}` |

---

## Webhook 通知

额度变动时 POST 到 `WEBHOOK_URL`（默认 `http://127.0.0.1:3000/webhook/cme`），请求体：

```json
{
  "message": "QDII 额度变动\n基金 000001（某某基金）日申购限额：1000 → 500",
  "secret": "***"
}
```

- 默认地址是内网地址，部署在别的机器上时需要改成实际可达的地址
- 修改方式：改 `app.py` 顶部的 `WEBHOOK_URL` / `WEBHOOK_SECRET` 常量后重启
- 推送失败只打印日志，不影响数据入库

---

## 升级与数据迁移

```bash
# PM2 方式
cd /home/ubuntu/qdii
git pull
source venv/bin/activate && pip install -r requirements.txt   # 依赖有变化时
pm2 restart qdii

# Docker 方式
git pull
docker compose up -d --build
```

- 备份数据：直接复制 `qdii.db`（连同 `-wal`/`-shm`），或 `docker cp fundx:/app/data/qdii.db ./backup.db`
- 从旧版本（库在容器内）升级，见[方式二第 3 节](#3-首次从旧版本升级旧库还在容器里)

---

## 常见问题

**Q：页面能打开但列表是空的？**
首次启动正在后台全量采集，看控制台/日志是否有 `🚀 采集任务开始...`。全量采集通常 10–30 分钟。也可以用 `curl http://localhost:5000/api/fetch/status` 查看进度状态。

**Q：采集失败，日志里是网络错误 / 接口返回异常？**
天天基金接口偶发限流或字段变动。先确认服务器能访问外网，再升级 AKShare：`pip install -U akshare`。AKShare 版本过低会导致列名不匹配。

**Q：`sqlite3.OperationalError: database is locked`？**
说明有多个进程同时写库（比如 PM2 开了多实例，或同时跑了两个 `app.py`）。本项目必须**单进程**运行：PM2 保持 `instances: 1`，不要用 gunicorn 多 worker。

**Q：导入 Ghostfolio 后长时间没反应？**
导入会先统一预取汇率，不再逐条等待网络。若服务器访问不了 `frankfurter.app`，会尝试 `open.er-api.com` 和东方财富 USDCNH/HKDCNH 最新价；最近 7 天的记录可用最新汇率兜底（结果里的 `warnings`），更早的记录会跳过并在 `errors` 中列出，人民币记录不受影响。若之前已经有一次超时请求仍在后台运行，先重启进程清掉它，再重试导入：

```bash
pm2 restart qdii
# 或
docker compose restart fundx
```

可以在这台服务器上检查汇率源是否可达：

```bash
curl -I --max-time 10 'https://api.frankfurter.app/2026-11-12?from=USD&to=CNY'
curl -I --max-time 10 'https://open.er-api.com/v6/latest/USD'
```

大文件导入如果超过 Nginx 的 `proxy_read_timeout`（示例配置为 120s），建议分批导入或临时调大该值。重复导入会按内容指纹跳过已存在的交易。

**Q：资产查询出来的币种不对，可以手工改吗？**
可以。添加或编辑资产时可在币种下拉框手动选择 `CNY` / `USD` / `HKD`；查询结果不会覆盖已经手动选择的币种。非本币资产会自动切换到「手动价格」，避免把行情源的本币价格当成外币。无交易/定投计划的资产可以直接改币种（会清除旧价格记录）；已有交易或计划时币种不能改，需删除后重新添加（资产页支持强制删除）。

**Q：导入的资产怎么删除？**
在「资产」页点删除。资产没有定投计划时即可删除；如果已有交易，会二次确认并连同该资产的交易和价格一起删除。有关联定投计划的资产暂时不能删除，可先停用该资产。

**Q：账户现金余额从哪里来？**
Ghostfolio 导入时会读取每个账户的 `balances` 快照，在总资产和账户页使用**最新一条**余额；不是完整现金流水。没有余额的账户显示 0。
**Q：账户可以删除吗？**
没有交易和定投计划的账户可以删除；有交易的账户需要先删除资产/交易，或停用。删除账户会同时删除该账户的现金余额。

**Q：定时任务没按预期时间执行？**
检查服务器/容器时区。容器已设 `TZ=Asia/Shanghai`；PM2 方式下若系统时区是 UTC，需 `sudo timedatectl set-timezone Asia/Shanghai`。

**Q：额度监控没有推送？**
交易时段（周一至周五 9:00–19:00 整点）才检测；额度无变化不会推送；检查 `WEBHOOK_URL` 是否可达，日志里会有 `⚠️ Webhook 请求失败`。

**Q：端口 5000 被占用？**
Docker 部署改 compose 的 `ports` 左侧端口（如 `"8080:5000"`）；本地/PM2 部署改 `app.py` 末尾 `app.run(..., port=5000)`，并同步修改 Nginx 的 `proxy_pass`。

**Q：`ModuleNotFoundError: No module named 'bs4'`？**
`fetcher.py` 用到 BeautifulSoup，已声明在 `requirements.txt` 中。若环境是旧依赖，重新执行 `pip install -r requirements.txt` 或 `pip install beautifulsoup4`。

---

## 免责声明

- 数据仅供参考，不构成投资建议
- QDII 基金净值更新有 1–2 个工作日延迟
- 数据来源为公开接口，接口变动可能导致采集失败，请关注日志

---

## 更新日志

### 2026-11-17

- **个人财富管理**：新增 `/wealth` 全资产账本、自动定投、银行理财按日计提、Ghostfolio / CSV 导入导出、XIRR/TWR 与资产走势
- **`.env` 支持**：`app.py` 启动时通过 `python-dotenv` 读取项目根目录 `.env`；Docker Compose 和 PM2 部署均可从 `.env` 读取 `WM_PASSWORD`、`WM_SECRET_KEY` 等变量
- **Ghostfolio 导入优化**：导入前统一预取汇率，避免逐条等待；历史汇率不可用时用东方财富最新 USDCNH/HKDCNH 兜底，避免页面长时间无响应
- **资产币种手选**：所有资产支持手动选择 CNY/USD/HKD，查询不覆盖手选币种；非本币自动使用手动价格；无交易/计划可改币种，有交易/计划需删除重加；有交易的资产可确认后强制删除
- **P4 账户模块**：账户管理页、账户现金余额导入、按账户拆分持仓和盈亏、账户筛选、Ghostfolio 资产名称/类别解析与推断
- **依赖补全**：`requirements.txt` 增加 `python-dotenv`

### 2026-10-27

- **文档重写**：补齐安装部署全流程（本地 / Docker Compose / Ubuntu + Nginx + PM2）、环境变量、定时任务、API、FAQ
- **修正访问路径**：应用在根路径 `/` 提供服务，不再写成 `/qdii`；并说明不能部署在子路径（前端接口为绝对路径 `/api/...`）
- **Docker Compose 调整**：`expose` 改为 `ports: "5000:5000"`，宿主机可直接访问；数据卷由绝对路径 `/fundx/data` 改为相对路径 `./data`，跨平台可用
- **依赖补全**：`requirements.txt` 增加 `beautifulsoup4`（`fetcher.py` 实际使用）

### 2026-10-20

- **额度历史曲线**：新增 `quota_daily` 每日快照表，点击基金「日额度」或额度变动记录中的代码，可查看 30 天 / 90 天 / 1 年 / 全部的阶梯走势
- **额度监控调整**：变动检测改为对比每日快照，避免 21:00 增量更新覆盖 `funds` 后漏记；新增 21:05 检测任务
- **移除顶部额度横幅**：改为「额度变动」Tab 未读徽标
- **Docker 数据持久化**：SQLite 迁移到宿主机挂载目录，重建容器不再丢数据

### 2026-09-03

- **新增折溢价率显示**：前端表格新增"折溢价率"列（仅场内基金有值），同步加入 CSV 导出和排序
- **全量/增量更新重构**：
  - 全量采集由每日 21:00 改为每月 1 号 21:00（重建所有字段）
  - 新增每日 21:00 增量更新：只更新 nav、daily_change、premium_discount、purchase_status、daily_limit 五个动态字段，速度大幅提升
  - 增量任务同时检测并全量 fetch 新增基金，确保新基金最晚 24 小时内入库
  - 全量采集当日自动跳过增量更新（避免重复）
- **额度监控时间放宽**：交易日 9:00-19:00 每小时检测一次（原 9:00-15:00 每 30 分钟）
- **年化筛选交互优化**：点击统计块只切换筛选开关，点击"年化 > X%"文字才弹出阈值输入框
- **新增额度变动历史记录 Tab**：
  - 页面新增"额度变动"Tab，完整展示所有历史变动（不限数量、不限次数）
  - 支持按日期筛选，回溯查看历史
  - 单条记录可标记已读，或批量"全部已读"
  - 未读条数徽标实时显示在 Tab 按钮上
  - 额度变动横幅点击可直接跳转至额度变动 Tab
- **调度调整**：22:00 增量更新改至 21:00，与全量采集合并于同日则自动跳过增量
