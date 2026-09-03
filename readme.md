# QDII 基金数据看板

Flask + SQLite 后端、纯前端展示的 QDII 基金数据看板，能自动采集全市场 QDII 基金的多维度数据，并支持筛选、排序、分页和 CSV 导出。

### 数据源
| 类型 | 数据源 | 采集引擎 | 更新时间 (北京时间) |
|------|--------|----------|-------------------|
| QDII 基金 | 东方财富（天天基金） | AKShare | 每日 21:00 |

### 功能
- **QDII 基金**：代码、名称、类型、场内/场外标识、最新净值、累计净值、近1/2/3/4/5/10年涨跌幅、年化收益、总收益、管理费/托管费/销售费、申购状态、日累计限额、场内折溢价
- 搜索、排序、筛选（成立以来每年正收益 / 年化>X%）、分页、CSV 导出
- 自选/自评级（localStorage 持久化，支持导出/导入备份）
- 移动端自适应卡片布局
- 定时自动采集 + 启动时立即采集
- **日申购额度监控**：交易日 9:00-15:00 每 30 分钟检测 QDII 基金额度与申购状态变化，页面横幅提醒，webhook 推送通知

## 完整部署方案：Ubuntu + Nginx + PM2

### 前提条件
一个 Ubuntu 服务器（20.04+），一个域名（可选），已配置好 DNS 解析。

#### 第一步：安装基础环境
```
# 系统更新
sudo apt update && sudo apt upgrade -y

# 安装 Python 和 pip
sudo apt install -y python3 python3-pip python3-venv

# 安装 Node.js（PM2 需要）
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# 安装 Nginx
sudo apt install -y nginx

# 安装 PM2
sudo npm install -g pm2
```

#### 第二步：上传代码并安装依赖
```
# 方式 A：从 GitHub 拉取（推荐）
git clone <你的仓库地址> /home/ubuntu/qdii

# 方式 B：通过 scp 上传
# scp -r /本地路径/qdii ubuntu@服务器IP:/home/ubuntu/

cd /home/ubuntu/qdii

# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r requirements.txt

# 创建日志目录
mkdir -p logs
```

#### 第三步：配置 PM2
```
# 启动应用
pm2 start ecosystem.config.js

# 设置开机自启
pm2 save
sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u ubuntu --hp /home/ubuntu
```

#### 第四步：配置 Nginx 反向代理
```
server {
    listen 80;
    server_name qdii.yourdomain.com;  # 替换为你的域名

    access_log /var/log/nginx/qdii_access.log;
    error_log  /var/log/nginx/qdii_error.log;

    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### 日常管理命令
```
# PM2
pm2 status              # 查看运行状态
pm2 logs qdii           # 查看实时日志
pm2 restart qdii        # 重启应用（修改代码后）
pm2 stop qdii           # 停止应用

# Nginx
sudo nginx -t           # 测试配置
sudo systemctl reload nginx   # 重载配置
```

### 文件结构
```
/home/ubuntu/qdii/
├── app.py                  # Flask 主应用
├── fetcher.py              # QDII 采集引擎 (AKShare)
├── quota_watcher.py        # QDII 日额度变动监控 + Webhook 通知
├── qdii.db                 # SQLite 数据库（自动生成）
├── ecosystem.config.js     # PM2 进程配置
├── requirements.txt        # Python 依赖
├── readme.md
├── templates/
│   └── index.html          # 前端页面
├── venv/                   # Python 虚拟环境
└── logs/
    ├── err.log
    └── out.log
```

### 注意
- 数据仅供参考，不构成投资建议
- QDII 基金净值更新有 1-2 个工作日延迟

### 日申购额度监控

系统在交易日（周一至周五）9:00-15:00 每 30 分钟自动检测 QDII 基金的日申购限额和申购状态变化。

- 检测到变化后自动更新 `funds` 表并记录到 `quota_changes` 表
- 前端页面每 15 分钟轮询一次，顶部横幅展示变动详情（手动关闭后标记已读，不再重复提醒）
- 同时通过 webhook 推送消息到 `http://127.0.0.1:3000/webhook/cme`（密钥 `addressTagPWD`），如需修改可在 `app.py` 中调整 `WEBHOOK_URL` / `WEBHOOK_SECRET`

---

## 更新日志

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

