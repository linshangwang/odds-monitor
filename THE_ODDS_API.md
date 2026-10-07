# The Odds API 赛前八节点采集

版本 1.87.0 将 The Odds API 接入为候选主盘口源。只有真实历史快照、完整公司数组、时间审计和覆盖门槛全部通过时，数据才具有主参照资格。

固定时间轴：

```text
Opening → T-24h → T-12h → T-6h → T-3h → T-1h → T-30m → Closing
```

## 配置

真实密钥只能放在 Railway Variables 或本地未提交的 `.env`：

```text
THE_ODDS_API_KEY=...
THE_ODDS_API_REGIONS=fi,eu
THE_ODDS_API_OPENING_LOOKBACK_DAYS=7
THE_ODDS_API_OPENING_SCAN_HOURS=12
THE_ODDS_API_MAX_HISTORY_REQUESTS=48
THE_ODDS_API_MIN_1X2_BOOKMAKERS=5
THE_ODDS_API_MIN_AH_BOOKMAKERS=3
THE_ODDS_API_MIN_OU_BOOKMAKERS=3
```

若设置 `THE_ODDS_API_BOOKMAKERS`，它优先于地区参数。最多允许20家公司，避免无界额度消耗。

## 权限自检

受保护接口：

```text
GET /shadow/historical-odds-test
```

该接口先验证密钥，再用低成本的历史赛事接口验证付费历史权限。响应只显示状态与额度，不返回密钥。

## 采集一场比赛

受保护接口：

```text
POST /shadow/the-odds-api/collect-timeline
Authorization: Bearer <SHADOW_ACCESS_TOKEN>
Content-Type: application/json
```

示例请求体：

```json
{
  "fixture": "gnistan-inter-2026-10-07",
  "sport_key": "soccer_finland_veikkausliiga",
  "league": "Finland Veikkausliiga",
  "home_team": "IF Gnistan",
  "away_team": "Inter Turku",
  "home_aliases": ["Gnistan Helsinki", "Gnistan"],
  "away_aliases": ["FC Inter Turku", "Turku International"],
  "kickoff_utc": "2026-10-07T16:00:00Z",
  "regions": "fi,eu",
  "persist": true
}
```

采集器会：

1. 在开赛前窗口内扫描历史赛事快照；
2. 使用主客队、开赛时间和别名唯一匹配赛事；
3. 找到“前一快照不存在、后一快照首次出现”的证据后才建立 Opening；
4. 查询其余七个固定节点；
5. 保存公司级1X2、亚洲让球和大小球；
6. 自行计算 Consensus Main Line；
7. 通过统一导入器完成时间、顺序、质量和不可降级审计。

## 主参照门槛

默认每个节点要求：

- 1X2至少5家完整公司；
- 亚洲让球至少3家完整公司；
- 大小球至少3家完整公司；
- API历史快照距离目标节点不超过10分钟；
- Closing中的公司更新时间必须早于开赛；
- Opening必须有首次出现及前一时点缺失证据。

未达到门槛的数据仍可保存用于覆盖率研究，但不会成为正式决策数据路由。缺失节点保持 `data_missing`，不使用临场价格回填。

## 当前边界

- 第一阶段只把1X2、AH和O/U作为核心市场；BTTS与Team Total随后按单场附加市场接入。
- The Odds API只有盘口路径，不能提供真实 Money%、Bets% 或成交额。
- 历史接口调用会消耗付费额度；每次响应保存额度审计。
- 网页聚合数据继续作为同公司、同时间点的交叉验证源，不覆盖API原始快照。
