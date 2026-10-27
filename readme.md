# QDII 基金数据看板

Flask + SQLite 后端、纯前端展示的 QDII 基金数据看板。自动采集全市场 QDII 基金的多维度数据，支持搜索、筛选、排序、分页，并附带日申购额度监控与额度历史曲线。

- 访问路径：**根路径 `/`**（例如 `http://localhost:5000/`）
- 数据库：SQLite（单文件，默认在项目目录下 `qdii.db`）
- 采集引擎：AKShare（东方财富 / 天天基金）

---

## 目录

- [功能特性](#功能特性)
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
├── templates/
│   └── index.html          # 单页前端（含全部样式与脚本）
├── static/
│   └── vendor/
│       └── echarts.min.js  # 本地 ECharts
├── requirements.txt        # Python 依赖
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

### 3. 启动

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

### 4. 指定数据目录（可选）

默认数据库写在 `app.py` 同级目录。想放到别处（例如 NAS 挂载盘）：

**Linux / macOS**

```bash
DATA_DIR=/var/lib/fundx python app.py
```

**Windows**

```powershell
$env:DATA_DIR="D:\fundx-data"; python app.py
```

### 5. 后台常驻（可选）

本地长期运行时可用 `nohup` 或 PM2，参考[方式三](#方式三ubuntu--nginx--pm2-部署)（PM2 部分与操作系统无关，但 `ecosystem.config.js` 里的解释器路径写的是 `./venv/bin/python3`，Windows 需改成 `./venv/Scripts/python.exe`）。

---

## 方式二：Docker Compose 部署

### 说明

- 容器内 Flask **单进程**运行，APScheduler 也在容器内，避免多进程导致定时任务重复触发
- SQLite 通过 volume 持久化到宿主机 `./data/qdii.db`，重建容器不丢数据
- 宿主机 `5000` 端口映射到容器 `5000`，启动后直接访问

### 1. 构建并启动

```bash
# 数据目录（compose 里挂载的是相对路径 ./data，不存在会自动创建）
mkdir -p data

docker compose up -d --build

# 查看日志（首次会看到全量采集进度）
docker compose logs -f

# 停止并移除容器（数据保留在宿主机）
docker compose down
```

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
    environment:
      - TZ=Asia/Shanghai
      - DATA_DIR=/app/data
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

### 第三步：配置并启动 PM2

`ecosystem.config.js` 已配置：单实例、自动重启、512M 内存上限、日志写入 `./logs`。若要把数据库放到 `data/` 目录，可在 `env` 里加 `DATA_DIR: './data'`。

```bash
pm2 start ecosystem.config.js

# 开机自启
pm2 save
sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u ubuntu --hp /home/ubuntu
```

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

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `DATA_DIR` | `app.py` 所在目录 | SQLite 数据库存放目录，目录不存在会自动创建。Docker 部署时设为 `/app/data` |
| `TZ` | 系统时区 | 建议 `Asia/Shanghai`。容器内已在 Dockerfile 中设置 |
| `FLASK_ENV` | - | `ecosystem.config.js` 中设为 `production`，仅作标记，不影响行为 |

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
  "secret": "addressTagPWD"
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
