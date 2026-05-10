一个基于 AKShare 数据源、Flask + SQLite 后端、纯前端展示的 QDII 基金数据看板 ，能自动采集全市场 QDII 基金的多维度涨跌幅、费率、净值、申购状态和额度数据，并支持筛选、排序、分页和 CSV 导出。
## 完整部署方案：Ubuntu + Nginx + PM2
### 前提条件
一个 Ubuntu 服务器（20.04+），一个域名（可选），已配置好 DNS 解析。
第一步：安装基础环境
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
第二步：上传代码并安装依赖
# 方式 A：从 GitHub 拉取（推荐）
git clone <你的仓库地址> /home/ubuntu/qdii

# 方式 B：通过 scp 上传
# scp -r /本地路径/qdii ubuntu@服务器IP:/home/ubuntu/

cd /home/ubuntu/qdii

# 创建虚拟环境
python3 -m venv venv
source venv/bin/activate

# 安装依赖
pip install -r qdii/requirements.txt

# 创建日志目录
mkdir -p qdii/logs
第三步：配置 PM2
# 启动应用
pm2 start qdii/ecosystem.config.js

# 设置开机自启
pm2 save
sudo env PATH=$PATH:/usr/bin pm2 startup systemd -u ubuntu --hp /home/ubuntu
第四步：配置 Nginx 反向代理
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
日常管理命令
# PM2
pm2 status              # 查看运行状态
pm2 logs qdii           # 查看实时日志
pm2 restart qdii        # 重启应用（修改代码后）
pm2 stop qdii           # 停止应用

# Nginx
sudo nginx -t           # 测试配置
sudo systemctl reload nginx   # 重载配置
文件结构
/home/ubuntu/qdii/
├── qdii/
│   ├── app.py
│   ├── fetcher.py
│   ├── qdii.db              # 自动生成
│   ├── templates/
│   │   └── index.html
│   ├── requirements.txt
│   └── ecosystem.config.js
├── venv/
└── logs/
    ├── err.log
    └── out.log