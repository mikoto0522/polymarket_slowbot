# 服务器部署指南（Linux / systemd）

适用于 Ubuntu / Debian 等常见 Linux 服务器。

## 1. 上传项目

把整个 `polymarket_slowbot` 上传到服务器，例如：

```bash
scp -r ./polymarket_slowbot user@your-server:/opt/
```

登录服务器：

```bash
ssh user@your-server
cd /opt/polymarket_slowbot
```

## 2. 初始化环境

```bash
bash scripts/server_setup.sh
```

然后编辑 `.env`：

```bash
nano .env
```

至少确认这三项：

- `OPENAI_API_KEY`
- `OPENAI_BASE_URL=https://api.qnaigc.com`
- `OPENAI_MODEL=claude-4.5-opus`

## 3. 手动跑一次

```bash
bash scripts/server_run.sh
```

运行后看输出文件：

- 日报：`data/reports/daily_report_*.md`
- 审计：`data/reports/audit_bundle_*.jsonl`
- 程序日志：`data/logs/slowbot.log`

## 4. 配置每天自动运行（systemd）

```bash
bash scripts/install_systemd.sh
```

默认每天 UTC `01:00` 执行一次（可改 `scripts/install_systemd.sh` 里的 `OnCalendar`）。

## 5. 常用运维命令

```bash
# 立即执行一次
sudo systemctl start polymarket-slowbot.service

# 看定时器状态
sudo systemctl status polymarket-slowbot.timer

# 列出下次触发时间
systemctl list-timers | grep polymarket-slowbot

# 看 systemd 运行日志
tail -n 200 data/logs/systemd.log
tail -n 200 data/logs/systemd.err.log
```

## 6. 更新版本

上传新代码后，在项目目录执行：

```bash
bash scripts/server_setup.sh
sudo systemctl restart polymarket-slowbot.timer
```
