# Football Odds Monitor - Railway 初版

这是一个 Railway 可部署的 FastAPI 项目，用来测试：

1. iSports 当前主盘口接口：`/sport/football/odds/main`
2. iSports 盘口变化接口：`/sport/football/odds/main/changes`
3. Sportradar Push / Webhook 接收口：`/sportradar/push/events`、`/sportradar/push/statistics`

## 本地运行

```bash
pip install -r requirements.txt
copy .env.example .env
# 编辑 .env，填入 ISPORTS_API_KEY
uvicorn main:app --reload
```

本地测试：

- http://127.0.0.1:8000/health
- http://127.0.0.1:8000/isports/odds-main
- http://127.0.0.1:8000/isports/odds-changes

## Railway 部署

1. 把本仓库连接到 Railway。
2. Railway 新建 Project。
3. 选择 Deploy from GitHub repo。
4. 在 Railway Variables 里添加：

```text
ISPORTS_API_KEY=你的真实 iSports key
ISPORTS_BASE_URL=http://isports.feijing88.com
REQUEST_TIMEOUT=30
```

5. 部署成功后打开：

```text
https://你的项目.up.railway.app/health
https://你的项目.up.railway.app/isports/odds-main
https://你的项目.up.railway.app/isports/odds-changes
```

## Sportradar Push/Webhook 测试地址

如果供应商需要你提供接收地址，可以先给：

```text
https://你的项目.up.railway.app/sportradar/push/events
https://你的项目.up.railway.app/sportradar/push/statistics
```

收到的最近数据可以查看：

```text
https://你的项目.up.railway.app/debug/last-push-events
https://你的项目.up.railway.app/debug/last-push-statistics
```

## 注意

当前版本只是接通服务和接口测试，不是最终分析系统。后续还需要接数据库、定时任务、盘口解析和提醒规则。
