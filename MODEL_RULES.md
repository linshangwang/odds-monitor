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

全局分析主链固定为：

`PIT数据冻结 → 赛事/赛季/阶段识别 → Verified League DNA → 球队相对联赛残差 → 本场基本面链 → State Tree → 公平价格 → 盘口时间线 → 市场接受/拒绝 → 表达优化 → BET/WAIT/PASS`

League DNA 是统一模型中的正式先验层，不是独立预测模型：

- 每个联赛、赛季和竞赛阶段可以拥有不同的进球、大小球主线、让球深度、角球、节奏、主场、旅行、比赛状态弹性和市场微结构标签。
- 联赛标签描述相对全局或同级联赛基准的分布差异，只能调整先验、阈值和需要优先比较的市场，不得直接输出投注方向。
- 必须先计算联赛基准，再计算球队相对该联赛的残差；禁止把联赛均值和球队近期数据重复加权。
- 联赛高进球、高角球或低角球标签不等于对应市场存在价值。市场已经提高O/U或角球门槛时，仍须重新计算公平线、价格和EV。
- 联赛标签必须绑定 `league + season + phase + format_version`，不得把历史赛季或常规赛标签无条件外推到新赛季、争冠组、保级组或季后赛。
- 用户经验、单场观察或少量样本只能登记为 `LEAGUE_TAG_CANDIDATE`。具体标签只有完成 `LEAGUE_DNA.md` 的全部验证门槛并获用户确认后，才能成为 `VERIFIED_ACTIVE`。
- 未验证标签不得影响模型概率、评级、权重、EV或BET/WAIT/PASS。

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

新理论准入采用零例外硬门槛：

- 单场比赛无论结果多典型、过程多吻合，都不能产生、确认或加入任何正式模型新理论，也不能据此修改 Champion 的字段、权重、阈值或决策规则。
- 单场 Learning Card 只允许做三件事：检查既有规则是否被正确执行、记录已有模块的执行错误、提出标记为 `HYPOTHESIS_ONLY` 的待验证问题。
- `HYPOTHESIS_ONLY` 不得参与评分、评级、概率、EV、BET/WAIT/PASS、组合排序或历史样本重标注。
- 新理论只有在预先写明适用范围、输入字段、方向预期、失效条件和反证标准，并通过独立多场 PIT 样本、事件污染审计、样本外 Shadow、消融测试及全部预定验证门槛，且不存在未解决反例后，才可视为完成 100% 的既定验证流程。
- 完成验证流程仍不得自动并入 Champion；必须形成版本记录并获得用户明确确认。任一环节未完成，一律不得加入模型。
- 同一场比赛的赛后结果、过程统计和由此提出的解释，不能同时作为新理论的发现证据与验证证据。

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
- Challenger 的建立也必须来自预先登记的 `HYPOTHESIS_ONLY`，不得由单场赛果直接生成；单场只能触发研究任务，不能触发模型变更。
- 新模块必须通过消融测试，证明对 Calibration、CLV、Process Accuracy 或风险指标有独立增益。
- 检查特征重复，防止同一事实被 Recent Process、Matchup、State Tree 和 Fair Pricing 重复加权。
- 快层可按比赛更新；中层至少按 20 至 50 个有效样本更新；慢层至少按 100 个有效样本调整。
- 新版本连续两个有效窗口显著恶化时回退到上一稳定版本。
- 系统当前不授权真实资金下注或自动下注。

## 七 自动学习任务

- 自动学习只处理男子职业国内最高级别联赛，具体范围和排除项以 `AUTO_LEARNING_TASK.md` 为准。
- 每个赛前样本必须在开赛前冻结，并绑定模型、规则、League DNA、赔率节点和数据截止版本；已开赛比赛不得补做赛前冻结。
- 赛后只能复盘已有冻结样本。无冻结版本的比赛不得用于Champion学习或理论验证。
- 自动任务可以修复有明确原规则依据的 `IMPLEMENTATION_GAP`，但不得借实现修复改变正式规则含义或权重。
- 自动任务可以登记和验证 `HYPOTHESIS_ONLY`、`LEAGUE_TAG_CANDIDATE`，但不得自动将其并入Champion。
- 只有形成完整 `PROMOTION_CANDIDATE` 且获得用户对该具体晋级包的明确确认后，才能版本化修改Champion。
- 自动学习的首要目标是提高 `PriorityQuality`、`SelectionQuality`、价格质量与校准，不以短期命中率为优化目标。
