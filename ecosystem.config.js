module.exports = {
  apps: [{
    name: 'qdii',
    script: 'app.py',
    cwd: __dirname,
    // app.py 会用 python-dotenv 读取本目录下的 .env；
    // 这里不要写死密码，避免把密钥提交到 Git。
    interpreter: './venv/bin/python3',
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '512M',
    env: {
      FLASK_ENV: 'production',
    },
    log_date_format: 'YYYY-MM-DD HH:mm:ss',
    error_file: './logs/err.log',
    out_file: './logs/out.log',
    merge_logs: true,
  }]
}
