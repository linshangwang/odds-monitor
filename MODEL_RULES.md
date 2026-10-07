# 赛前分析与赛后复盘模型规范

本文件是项目的决策规则来源。代码、接口、报告和后续模型版本不得与这些规则冲突。任何规则调整都必须形成版本记录，并从调整后的比赛开始生效；禁止把新规则回填成旧比赛当时已经使用的规则。

## 一 最高优先级约束

1. 所有赛前判断只使用当时可获得的信息。赛果、赛后 xG、射门和事件不得进入赛前快照。
2. 缺失数据必须标记为 `data_missing`。禁止使用当前赔率反推 Opening 或其他历史节点。
3. 模型目标是提高选择质量、价格质量和排序质量，不承诺百分之百命中。
4. 胜负方向、净胜球方向、总进球方向和进球归属必须分别定价。
5. 盘口变化可触发基本面复核，但不能仅因市场与模型不同就改写基本面。
6. 资金方向不等于可买方向。真实资金、资金代理和盘口响应必须分开记录。
7. 没有足够优势时必须允许 `WAIT` 或 `PASS`，不得为了覆盖全部比赛强制推荐。

## 二 赛前运行顺序

固定时间轴：

`Opening → T-24h → T-12h → T-6h → T-3h → T-1h → T-30m → Closing`

赔率来源优先级：

1. The Odds API 的真实历史时间戳、完整公司数组和八节点快照，在通过公司覆盖、时间误差、Opening 首次出现证明及 Closing 赛前状态审计后，作为赔率主参照。
2. 博彩公司官网及可靠网页聚合记录用于同公司、同时间点交叉验证，不得覆盖 API 原始值。
3. The Odds API 某一市场或节点未达到门槛时，该市场单独降级为 `data_missing` 或外部 B/C 级证据；不得用完整的 1X2 推定 AH、O/U 同样完整。
4. The Odds API 的盘口路径仍然只是 `Capital Pressure Proxy`，不得称为真实 Money%、Bets% 或成交额。

每个节点执行：

1. 校验时间戳、来源、公司数组和数据完整度。
2. 独立生成或更新 Pure Fundamental Script。
3. 比较 1X2、AH、O/U；可得时同时比较 BTTS、主队 TT、客队 TT。
4. 检测显著变盘、跨市场背离、异常价格路径和公司共识变化。
5. 触发事实复核时，只根据新的伤停、首发、赛制、天气、场地、教练信息等更新基本面版本。
6. 生成比分分布、公平盘口、公平赔率、EV 和不确定性区间。
7. 比较所有可用表达，输出最优盘口或 `PASS`。

基本面链固定为：

`Result Utility → Tactical Risk Appetite → Rotation Quality → Execution Ability → Tactical Matchup → Game State Elasticity → First Goal State Transition → Open Game Beneficiary → Time Segment Strength → Goal Conversion`

## 三 盘口与资金语言

盘口变化必须输出四项：

- 资金压力：Home、Away、Over、Under；强、中、弱或 `data_missing`。
- 盘口响应：升档、不动、退档。
- 市场接受度：`Accepted`、`Partial`、`Resistance`、`Rejected`。
- 最优表达：原方向、平切、换浅盘、换深盘、反向价值或 `PASS`。

数据等级：

- A 级：真实 Money%、Bets%、成交额或可靠交易数据。
- B 级：完整多公司价格路径与盘口响应，只能称为 `Capital Pressure Proxy`。
- C 级：单一公司或单个收盘价格，不得推断真实资金。

市场响应判断：

- 资金同向且盘口同步升档：`Accepted Repricing`。
- 资金同向但盘口不动：`Resistance`。
- 资金同向但盘口反向退档：`Strong Resistance / Divergence`。
- 低水或降水本身不得写成真实资金流入。

重点拥堵风险结构：

`Crowding + Price Compression + Line Resistance + Cross Market Divergence`

只有证据完整时才可标记高风险。比赛最终输赢不得用于反向制造该标签。

## 四 最优表达与执行

方向正确不等于表达正确。系统必须横向比较同一剧本下的 1X2、不同 AH 档位、不同 O/U 档位、TT 和 BTTS。

综合执行评分以 Core、EV、Confidence、Script Coverage、Market Fit 和 Expression Risk 为主。MSCB 只作为低权重纠偏层，负向调整空间大于正向调整空间；它可以降级、等待、放弃或切换表达，但不能把负 EV 变成正 EV。

若 Core 足够强且模型优势明确，市场背离只能触发复核或降低执行等级，不能自行反转方向。发现新的客观事实后，才可更新 Core。

综合过关腿优先满足：

- Core 不低于 75。
- Confidence 不低于 70。
- Adjusted EV 不低于 3%。
- 剧本与市场至少达到 B+ 共振。
- 无明显 Resistance 或 Overshoot。
- Expression Risk 为低或中低。

赔率区间不是买入理由。胜率优先时可接受十进制 1.30 至 2.00，但仍必须满足正 EV 和剧本覆盖要求。

## 五 赛后复盘

赛后只评估赛前决策过程，不创造赛后理论。最终比分只记录，不反向污染赛前字段。

固定复盘对象：

- 最终冻结方向。
- 比赛优先级和资金分配顺序。
- 单关、组合、WAIT、PASS 的执行分层。
- 数据完整度与当时可见的盘口价值。
- 资金代理与盘口接受度是否被正确区分。
- 是否存在更好的盘口表达。

核心评价指标是 `PriorityQuality` 和 `SelectionQuality`，不是单场命中率。赢了不自动证明排序正确；输了也不自动证明决策错误。

错误归因仅允许使用当时输入和赛后过程数据进行模块诊断：

`DATA_ERROR`、`ABILITY_ERROR`、`LINEUP_ERROR`、`UTILITY_ERROR`、`STATE_ERROR`、`MARGIN_ERROR`、`GOAL_OWNERSHIP_ERROR`、`PRICE_ERROR`、`MARKET_ERROR`、`EVENT_SHOCK`、`FINISHING_VARIANCE`。

每场生成 Learning Card，但只有满足样本质量要求的比赛进入正式学习集。红牌、点球、乌龙、门将重大失误和极端终结偏差必须降低学习权重。

## 六 模型治理

- 当前正式版本为 Champion；任何新变量或权重先作为 Challenger 运行 Shadow。
- 新模块必须通过消融测试，证明对 Calibration、CLV、Process Accuracy 或风险指标有独立增益。
- 检查特征重复，防止同一事实被 Recent Process、Matchup、State Tree 和 Fair Pricing 重复加权。
- 快层可按比赛更新；中层至少按 20 至 50 个有效样本更新；慢层至少按 100 个有效样本调整。
- 新版本连续两个有效窗口显著恶化时回退到上一稳定版本。
- 系统当前不授权真实资金下注或自动下注。
