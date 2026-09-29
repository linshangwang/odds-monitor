# Football AI 影子分析 / Odds Monitor v0.7

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

## V4 兼容升级

- 固定时间轴：`Opening → T-24h → T-12h → T-6h → T-3h → T-1h → T-15m → Closing`。未真实采集的节点返回 `data_missing`，不使用当前赔率回填。
- 每个节点保存 1X2、亚洲让球、大小球，并在上游提供时保存 BTTS、主队进球数、客队进球数。
- `primary` 字段继续保留以兼容旧调用方，但内容改为基于完整公司数组计算的 `consensus_main_line`，不再机械取第一家公司。
- 显著跨档、异常价格或跨市场背离会触发基本面重新采集，并保存基本面版本、触发原因、变量变化、概率变化和最优盘口变化。
- Pure Fundamental Script 与盘口隔离；未知的战术/动机信息明确为 `data_missing`。
- 决策层检查模型概率、市场去水概率、Edge、EV、Script Coverage、Crowding、Line Movement、Lineup Confidence 和 Death Path；输入不足或无正优势时返回 `PASS`。

主要接口保持兼容，并新增：

```text
GET  /shadow/ai-packet?fixture=FIXTURE_ID
POST /shadow/evaluate
```

`/shadow/evaluate` 的 JSON 示例：

```json
{
  "fixture": 123,
  "model_probabilities": {"home": 0.50, "draw": 0.28, "away": 0.22},
  "script_coverage": {"home": 0.75, "draw": 0.40, "away": 0.20},
  "crowding": 0.45,
  "lineup_confidence": 0.90,
  "death_path": []
}
```

## Railway 持久化

在 Railway 为服务挂载 `/data` Volume，并设置：

```text
SNAPSHOT_STORE_PATH=/data/shadow_snapshots.json
```

存储格式向后兼容原 `fixtures` 快照；新增的 `fundamental_versions` 与其并列保存。

## Nami 可选数据源

Nami 只用于补充数据，不是主流程依赖。未配置、IP 未授权、限流、超时、非 JSON
响应或其他上游异常都会返回 `degraded=true` 和
`fallback=continue_without_nami`；服务健康状态、既有数据源和影子分析流程继续运行。
可通过 `NAMI_REQUEST_TIMEOUT` 单独限制等待时间，默认最多 10 秒。

## 测试

```bash
python -m unittest -v
```

真实密钥只放在 Railway Variables，禁止写入仓库或日志。
