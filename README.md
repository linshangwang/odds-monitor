# Football Data Monitor - Railway 初版

这是一个 Railway 可部署的 FastAPI 项目，用来测试：

1. API-Football / API-SPORTS 的实时比赛、事件、技术统计接口
2. TheStatsAPI 的通用 REST 接口代理测试
3. 后续可扩展 iSports 盘口接口、Sportradar Push/Webhook 接收口

## 本地运行

```bash
pip install -r requirements.txt
copy .env.example .env
# 编辑 .env，填入 API_FOOTBALL_KEY / THESTATS_API_KEY
uvicorn main:app --reload
```

本地测试：

- http://127.0.0.1:8000/health
- http://127.0.0.1:8000/api-football/live
- http://127.0.0.1:8000/api-football/fixtures?date=2026-09-27
- http://127.0.0.1:8000/api-football/events?fixture=FIXTURE_ID
- http://127.0.0.1:8000/api-football/statistics?fixture=FIXTURE_ID
- http://127.0.0.1:8000/thestats/raw?path=/YOUR_ENDPOINT

## Railway Variables

在 Railway Variables 里添加：

```text
API_FOOTBALL_KEY=你的 API-Football / API-SPORTS key
API_FOOTBALL_BASE_URL=https://v3.football.api-sports.io
THESTATS_API_KEY=你的 TheStatsAPI key
THESTATS_BASE_URL=https://api.thestatsapi.com/api
REQUEST_TIMEOUT=30
```

如果以后补 iSports，再加：

```text
ISPORTS_API_KEY=你的 iSports key
ISPORTS_BASE_URL=http://isports.feijing88.com
```

## Railway 测试地址

部署成功后打开：

```text
https://你的项目.up.railway.app/health
https://你的项目.up.railway.app/api-football/live
https://你的项目.up.railway.app/api-football/fixtures?date=YYYY-MM-DD
```

拿到 fixture id 后再测：

```text
https://你的项目.up.railway.app/api-football/events?fixture=FIXTURE_ID
https://你的项目.up.railway.app/api-football/statistics?fixture=FIXTURE_ID
```

TheStatsAPI 因为 endpoint 路径需要按你账号文档来，所以先用通用测试口：

```text
https://你的项目.up.railway.app/thestats/raw?path=/你的接口路径
```

## Push/Webhook 测试地址

如果以后供应商需要你提供接收地址，可以先给：

```text
https://你的项目.up.railway.app/push/events
https://你的项目.up.railway.app/push/statistics
```

收到的最近数据可以查看：

```text
https://你的项目.up.railway.app/debug/last-push-events
https://你的项目.up.railway.app/debug/last-push-statistics
```

## 注意

当前版本只是接通服务和接口测试，不是最终分析系统。后续还需要接数据库、定时任务、事件/统计快照存储、变化检测和提醒规则。
