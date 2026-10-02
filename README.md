# Football AI 影子分析 / Odds Monitor v0.9

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
- 决策层检查模型概率、市场去水概率、Edge、EV、Script Coverage、Crowding、Line Movement、Lineup Confidence 和 Death Path；输入不足或未达到风险门槛时返回 `PASS`。

默认最终推荐门槛可通过 Railway Variables 调整：

```text
MIN_EDGE=0.03
MIN_EV=0.03
MIN_SCRIPT_COVERAGE=0.60
HIGH_VARIANCE_MIN_SCRIPT_COVERAGE=0.40
MAX_CROWDING=0.80
MIN_LINEUP_CONFIDENCE=0.70
```

模型主胜、平局、客胜概率必须各自在 0–1 内且合计误差不超过 0.02。最佳候选仍需同时满足
Edge、EV、脚本覆盖率、拥挤度和阵容可信度门槛，且不存在 Death Path；任何一项缺失或不合格均明确记录原因并 `PASS`。

合格结果固定输出三层：

- `first_choice_high_consistency`：优先选择 Script Coverage 和基本面/盘口一致性最高的表达，允许赔率较低。
- `second_choice_higher_return`：仍通过全部标准门槛，但在其他候选中优先更高 EV。
- `high_variance_single`：Edge/EV 为正、覆盖率至少达到独立门槛，但未达到主推荐覆盖率，只能作为高博弈单关。

高博弈候选不会为了填充结果而进入第一或第二首选；Crowding、Lineup Confidence、Death Path
或关键字段出现硬性问题时，三层都清空并返回 `PASS`。

`/shadow/portfolio/evaluate` 接受 1–10 场完整赛前模型输入，逐场运行同一套新鲜度与决策闸门，
可传 `risk_preference=conservative|balanced|aggressive`。系统仍会返回从2腿到可用上限的
全部建议方案，并分别按稳健2腿、平衡3腿、进取型可用最大腿数标记推荐方案和理由；
这些是建议选择而不是硬性腿数限制。
每一档组合同时返回 `selection_audit`，列出入选标的和排除原因，包括对应档位无合格项、
相关组已占用及达到用户设置的腿数上限，便于直接审计系统建议。
再输出综合过关第一首选、综合过关次首选和高博弈单关。2 腿是偏稳健建议，3 腿是默认
平衡建议，4 腿以上作为可选扩展高波动方案；用户可用 `max_legs` 指定 2–10 的展示上限，
不是只能选择 2–3 腿。同一 `correlation_group` 最多一腿，同一比赛不能重复提交。
相关性筛选后不足两腿时直接 `PASS`，不会强行凑单。系统只计算组合展示赔率；由于亚洲盘走盘及跨比赛剩余相关性，
组合 EV 明确标记为 `data_missing`，不做错误的概率相乘。

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
能力检查使用当前已授权的足球实时数据 v5 日期赛程接口；Nami 的基础、实时、统计、
高阶和指数数据包分别授权，禁止使用未订阅数据包的接口推断密钥无效。

## 测试

```bash
python -m unittest -v
```

真实密钥只放在 Railway Variables，禁止写入仓库或日志。

## pang 赛前数据导入

系统可接收 `shadow_prematch_packet_v1` 数据包。导入只写入现有持久化文件，
不调用 API-Football 或 Nami，也不会覆盖其他比赛。相同比赛和节点重复导入时会更新该节点。

```text
POST /shadow/import-prematch-packets?token=SHADOW_ACCESS_TOKEN
GET  /shadow/import-status?token=SHADOW_ACCESS_TOKEN
GET  /shadow/data-source-health?token=SHADOW_ACCESS_TOKEN
POST /shadow/model/poisson?token=SHADOW_ACCESS_TOKEN
POST /shadow/model/fundamental-xg?token=SHADOW_ACCESS_TOKEN
POST /shadow/model/prematch-evaluate?token=SHADOW_ACCESS_TOKEN
POST /shadow/portfolio/evaluate?token=SHADOW_ACCESS_TOKEN
GET  /shadow/imported-prematch/{MATCH_UUID}?token=SHADOW_ACCESS_TOKEN
```

POST 请求可直接传一个数据包、数据包数组，或 `{ "packets": [...] }`。
外部 UUID 与原有数字 fixture ID 分开使用；缺失节点保留为 `data_missing`，不会用当前盘口反推。

持久化文件默认使用 gzip 压缩（路径和接口保持不变），也能继续读取旧版纯 JSON 文件。
GET 默认只返回 Consensus Main Line 和变化结果；仅在确实需要逐家公司报价时添加
`?include_companies=true`，需要完整阵容历史时添加 `?include_lineups=true`。
POST 支持 `Content-Encoding: gzip`，可直接发送压缩 JSON，
避免完整公司数组在网络传输中膨胀。

同步程序推荐使用请求头 `Authorization: Bearer ...` 或 `X-Shadow-Token: ...`，
避免把令牌放进 URL。可用 `X-Sync-Mode: incremental` 和 `X-Sync-Source: pang`
记录增量同步来源；`/shadow/import-status` 返回最后成功时间、请求大小和比赛数量。
节点导入按比赛、阶段、观测时间和内容指纹幂等合并；重复内容标记 `unchanged`，
较旧内容标记 `stale_skipped`，不会覆盖较新的节点。响应中的 `stage_counts`
分别报告 `inserted`、`updated`、`unchanged` 和 `stale_skipped`。

`/shadow/data-source-health` 汇总 pang 只读数据的新鲜度和 Nami 配置状态。
默认盘口新鲜度门槛为30分钟，可用 `EXTERNAL_DATA_STALE_SECONDS` 调整；超过门槛、
没有可用盘口或比赛已开赛时，AI数据包会加入对应原因并强制 `PASS`。

## 独立概率模型

`/shadow/model/poisson` 接受明确的主客队预期进球、输入置信度及来源说明，
输出比分分布、1X2、2.5大小球、BTTS，以及主客队 0.5/1.5/2.5/3.5/4.5
半球线 Team Total 概率。来源必须明确声明
`uses_market_odds=false`，禁止使用盘口反推模型；置信度低于0.6、数据过期、
比赛已开赛或最终决策字段不完整时继续返回 `PASS`。

```json
{
  "fixture": "MATCH_UUID",
  "home_expected_goals": 1.65,
  "away_expected_goals": 1.10,
  "input_confidence": 0.78,
  "provenance": {"source": "verified_team_metrics", "uses_market_odds": false},
  "script_coverage": {"home": 0.75, "draw": 0.45, "away": 0.25},
  "crowding": 0.35,
  "lineup_confidence": 0.85,
  "death_path": []
}
```

`/shadow/model/fundamental-xg` 在 Poisson 前增加可审计的 λ 生成层：使用联赛主客场
基准、球队同场景进攻率、对手同场景防守率、样本量、指标类型和明确阵容调整。
公式为 `球队进攻率 × 对手防守率 ÷ 联赛场景基准 × 调整系数`。调整系数限制在
0.8–1.2；支持 xG 或进球率，但进球率会获得较低的数据质量权重。该接口只生成
概率并固定返回 PASS，必须绑定新鲜比赛和完整决策字段后才能进入最终推荐。

`/shadow/model/prematch-evaluate` 将已导入比赛的最新真实盘口与独立基本面模型绑定，依次执行
基本面 xG、Poisson 概率、市场去水概率、Edge/EV 和最终风险门槛。它不使用赔率生成或修改
基本面；比赛不存在、盘口过期、已经开赛、模型置信度不足或决策字段不完整时均返回 `PASS`。
每次调用都会保存一条基本面版本，包含触发原因、变量变化、概率变化和最优盘口变化。

请求体在 `/shadow/model/fundamental-xg` 字段基础上增加：

```json
{
  "fixture": "MATCH_UUID",
  "script_coverage": {"home": 0.75, "draw": 0.45, "away": 0.25},
  "crowding": 0.35,
  "death_path": [],
  "revalidation_trigger": {"triggered": true, "reasons": ["significant_line_move"]},
  "fundamental_chain": {
    "result_utility": {"status": "available", "evidence": "verified competition state"}
  }
}
```

未提供的基本面链环节明确保存为 `data_missing`，不会根据盘口补写。

最终决策层会同时比较可获得的 1X2、亚洲让球、O/U、BTTS 和 Home/Away
Team Total，分别计算去水概率、Edge、EV 与 Script Coverage，再选择最佳盘口表达。
旧调用方继续可以只传平面的 `{home, draw, away}` 概率。

亚洲盘结算支持 0.25 递增线：四分之一盘自动拆成相邻两条半盘，整数盘保留走盘概率，
并在 EV 中分别计算全赢、半赢、走盘、半输和全输。模型同时输出净胜球、总进球、
主队进球和客队进球分布作为审计依据。不是 0.25 递增的异常盘口继续 `PASS`。

## 本地只读同步器

`sync_prematch.py` 只在本地运行。它可以读取本地 JSON/JSON.GZ，或通过 SSH 对 pang 上一个
已经存在的绝对路径执行固定的 `cat`/`gzip -cd`。程序不接受自定义远程命令，不向 pang
上传文件，也不创建远程程序或计划任务。上传 Railway 时使用 gzip、Bearer 请求头和增量模式。

先做不联网的检查：

```text
python sync_prematch.py --input bundle.json.gz --league "UEFA Nations League" --dry-run
```

使用已经配置好的 SSH 主机别名读取现有文件：

```text
python sync_prematch.py --ssh-host pang --remote-path /absolute/path/bundle.json.gz --league "UEFA Nations League" --dry-run
```

取消 `--dry-run` 才会上传 Railway。访问令牌只能通过本机环境变量
`SHADOW_ACCESS_TOKEN` 提供；程序不会把令牌放进 URL 或输出中。SSH 模式使用密钥或 agent，
不会把 pang 密码写入脚本。
