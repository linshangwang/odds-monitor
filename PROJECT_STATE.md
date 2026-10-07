# 项目接管状态

更新时间：2026-10-06 Asia Shanghai

## 当前基线

- 仓库：`linshangwang/odds-monitor`
- 分支：`main`
- 接管基线提交：`b7feddc`
- 本地候选版本：`1.87.0`（线上仍为 `1.86.0`，尚未部署）
- 测试基线：285 项通过
- 正式赛前时间轴：Opening、T-24h、T-12h、T-6h、T-3h、T-1h、T-30m、Closing
- 旧 T-15m 数据保留，但不得替代 T-30m。

## 已完成能力

- 完整赛前节点、时间戳和顺序审计。
- 多公司数组计算 Consensus Main Line，不机械依赖上游 primary 字段。
- Opening 来源验证和缺失节点 `data_missing` 规则。
- Pure Fundamental Script、基本面复核触发器和版本记录。
- 1X2、AH、O/U、BTTS、主客 TT 的市场结构。
- 模型概率、no-vig 概率、Edge、EV、Script Coverage、Crowding、Line Movement、Lineup Confidence、Death Path 和 PASS 门禁。
- pang 导入、持久化、数据新鲜度、发布验收与 Railway 健康检查。
- The Odds API 历史赛事唯一匹配、Opening 首次出现证明、八节点公司级
  1X2/AH/O-U 抓取、额度审计和统一数据包持久化。
- The Odds API 只有达到 1X2/AH/O-U 公司覆盖门槛后才能成为主参照；
  未达门槛的快照只保存用于审计，不进入正式决策路由。

## 当前真实阻塞

1. 线上健康接口显示 `market_source_blocked`，没有新鲜、可验证的赛前盘口快照，因此必须安全 `PASS`。
2. 本地与当前可访问环境没有配置 `THE_ODDS_API_KEY`，新采集链已通过模拟响应测试，
   但仍需在 Railway 使用真实付费历史权限完成芬超逐公司实测。
3. 缺少真实 Money%、Bets% 和成交额时，资金模块只能使用盘口路径代理，不得声称真实资金流向。
4. 若无可靠事件级 xG、xThreat、阵型和赛制目标数据，部分基本面链字段仍会保持 `partial` 或 `data_missing`。

## 下一批工程任务

1. 为真实资金字段建立独立 schema、来源等级和时间戳审计，严格区分真实资金与 Capital Pressure Proxy。
2. 将资金压力、盘口响应、市场接受度和 Expression Switch 固化进最终决策输出。
3. 为 `PriorityQuality`、`SelectionQuality` 和 Learning Card 增加内部校准记录；不得把赛后模块扩展成赛后推荐产品。
4. 建立欧国联八场冻结样本集，保存当时版本、盘口路径、最终选择与优先级，作为不可回填的回归测试夹具。
5. 对 MSCB、State Tree、IEH、TAC、TDD、LET、LPS 和 OCR 做 Champion 与 Challenger 消融框架。
6. 数据源恢复后先进行 Shadow 验收；满足新鲜度、完整公司数组、基本面链和阵容置信门槛后，才允许生成非 PASS 建议。

## 接管原则

后续开发以 `MODEL_RULES.md` 为规范来源。聊天记录用于追溯，不作为唯一运行依据。任何与规范冲突的旧代码或旧说明，应通过版本化迁移修正，不直接篡改历史记录。
