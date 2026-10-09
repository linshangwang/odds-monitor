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

机器执行必须在读取盘口快照、生成市场候选或运行Expression Optimizer之前，完成并冻结赔率无关的基本面估计、概率、Pure Fundamental Script及基本面链审计。冻结使用`fundamental_first_freeze_v1`，并由`fundamental_first_pipeline_audit_v1`验证市场处理前后脚本哈希一致。基本面不足或污染时仍可输出盘口诊断，但固定为`diagnostic_only`并强制`PASS`，不得产生BET或WAIT表达。

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

机器输出必须把赔率路径字段固定标为`Capital Pressure Proxy`并设置`is_real_money=false`。真实资金字段使用独立的`real_money_data`对象；没有带来源、时间戳和字段定义的A级数据时，Money%、Bet%和成交额全部保持`data_missing/null`，不得从赔率变化补算。

`Accepted Repricing`只描述资金压力与结构盘口同向时的市场响应，不是重定价成因证明。机器必须使用独立的`repricing_attribution_v1`记录成因状态：只有“通过审计的实质基本面版本变化”与“对应市场复核触发器”同时存在时，才可标记`fundamental_repricing_confirmed`；A级真实资金只能证明资金压力确实存在，不能单独证明盘口变化由该资金造成；其余情况必须保持`market_move_cause_unverified`、`likely_information_driven_unconfirmed`或`data_missing`。市场接受、真实资金和基本面调整不得合并为同一个结论。

A级真实资金包固定使用`real_money_v1`：必须绑定比赛ID、注册来源域名及允许的来源类型、可定位HTTP证据、已核实来源权威和方法、赛前`observed_at`、市场与具体盘口档位。Money%或Bet%必须覆盖该市场全部选项、各项为0至100且总和在容差内闭合；成交额如存在必须同时带非负数值和币种。跨比赛、过期、开赛后、未来时间、盘口档位不匹配或未注册来源不得参与方向判断。合格资金证据必须保存`evidence_hash`，且只提供资金压力，盘口响应仍由独立赔率时间线决定。

最终决策必须保存Home、Away、Over、Under四个方向的压力代理、盘口响应、接受度和证据依据。`Resistance`或`Rejected`不能覆盖Core并自动反向；系统只能在相同赛前剧本内切换到通过正EV、Script Coverage和风险门禁的更低阻力表达，否则输出`WAIT`或`PASS`。只有`BET`可以进入组合，`WAIT/PASS`在赛后不得按实际下注结算输赢。

重点拥堵风险结构：

`Crowding + Price Compression + Line Resistance + Cross Market Divergence`

只有证据完整时才可标记高风险。比赛最终输赢不得用于反向制造该标签。

## 四 最优表达与执行

方向正确不等于表达正确。系统必须横向比较同一剧本下的 1X2、不同 AH 档位、不同 O/U 档位、TT 和 BTTS。

跨场排序必须冻结`prematch_priority_vector_v1`，只允许使用赛前证据，并采用不设赛后拟合权重的字典序：基本面准入、基本面完整度、阵容可信度、剧本覆盖、盘口接受度、Edge、EV。基本面字段永远先于盘口和价格字段；赛果、结算、命中率、收益及赛后事件不得进入排序或改写历史名次。比赛参与排序与具体盘口表达排序必须分别保存，不能用某个高赔率表达抬高整场比赛的优先级。

普通组合评估必须把上述两层排序冻结为带内容哈希的`prematch_priority_board_v1`；自动学习周期必须冻结`learning_prematch_priority_board_v1`并绑定每场`freeze_id/freeze_hash`。高波动单关也必须服从同一基本面优先顺序，不得单独按EV或赔率排序。自动赛后`PriorityQuality`优先核对该场是否位于当时冻结的`core_top_three`及是否达到赛前B级门槛；缺失榜单或冻结评级时保持`data_missing/ungraded`，禁止根据赛果补排。

只有赛前`execution_action=BET`且存在完整候选表达的比赛可以占用优先级名次。WAIT、PASS和缺失表达的记录必须随榜单冻结在`non_participating_records`中，但不得进入核心前三、挤压可执行比赛，也不得通过高波动单关通道绕过执行门禁。

榜单完整性不得只依赖内容哈希。审计还必须核对名次连续且唯一、比赛榜与表达榜成员一致、冻结ID与冻结哈希完整、核心角色与名次一致、赛前时间有效、实际顺序能由冻结优先级向量重放，以及参与榜单的执行动作确为BET。重新计算哈希不能使语义错误合法化。

任一已持久化学习榜单未通过哈希或语义审计时，自动赛后证据复盘必须全局失败关闭，不得把损坏榜单解释为“榜单缺失”并退回旧评级路径。修复或恢复有效事实库前，该轮PriorityQuality不得生成有效标签。

`2.19.8`及以后生成的可执行冻结必须绑定有效优先级榜；缺失时`match_selection_quality`固定为`data_missing`，不得仅凭赛前字母评级判为通过。只有旧版本历史冻结允许兼容评级路径。Learning Card必须保存榜单ID、榜单哈希、冻结名次、角色及核心名次数，便于后续校准重放。

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

单关采用独立价格准入门槛：只允许十进制赔率不低于 1.75（即香港盘不低于 HK0.75）的候选进入“单关首选/次选”。低于该门槛时必须输出 `WAIT`，不得为了满足价格门槛临时切换到相反方向，也不得因赔率更高而降低 Edge、EV、Script Coverage、阵容可信度或风险门禁。综合过关仍使用其自身的稳健性与价值区间，不受此单关下限替代。

## 五 赛后复盘

赛后只评估赛前决策过程，不创造赛后理论。最终比分只记录，不反向污染赛前字段。

非大型赛事和未进入用户确认的大型联赛学习注册表的比赛一律标记为`MODEL_LEARNING_EXCLUDED`。这些比赛只允许执行`MARKET_LANGUAGE_ONLY`和`SETTLEMENT_ONLY`：可核对盘口语言与冻结选择是否兑现，但不得进入Learning Card、假设发现/验证、命中率校准、MSCB、League DNA、阈值修改或Champion晋级证据。即使同类信号多次出现，也不得自动提升为模型学习材料。

赛果进入自动复盘前必须由两个带证据定位、比分一致且HTTP权威域名独立的来源确认。The Odds API只可作为第二赛果来源：sport key、event id、主客队和身份哈希必须在开赛前写入冻结，赛后只允许访问该冻结身份对应的官方`scores`记录；事件ID、运动键、球队、响应状态或比分任一不一致均不得计为独立确认，禁止赛后模糊搜索补配比赛。

无人值守周期可自动完成`automatic_evidence_review`，但只能根据冻结契约和哈希绑定证据判断过程。双源赛果只证明最终比分，不证明事件路径；若红牌、点球、乌龙等事件序列没有两个独立权威来源，事件污染状态必须视为未知并归类`DATA_INSUFFICIENT`，选择输赢不得派生。`DATA_INSUFFICIENT`和`EVENT_CONTAMINATED`均不得进入Learning Card有效样本、多场失败信号或Champion晋级证据。

第二事件权威属于可选增强能力，不是自动学习启动硬依赖。当前保留的TheStatsAPI适配器如被启用，其`match_id`必须在开赛前通过UTC日期、双方球队和开赛时间唯一匹配并哈希冻结。赛后match detail必须复核同一ID、双方、开赛时间、完赛状态和比分；`timeline`还必须与API-Football在进球、红牌、点球和乌龙的类型、主客侧及分钟容差上形成同一`event_signature_hash`。仅有两个不同域名而没有相同事件签名不得视为事件双源核验。未配置或核验失败时，事件路径相关复盘固定为`DATA_INSUFFICIENT`且无学习效力，但不阻断赛前冻结、赔率节点和双源赛果归档。

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

每个已完成结构化复盘的冻结版本生成一张不可变 `Learning Card`，绑定 `freeze_hash`、`fact_hash` 和 `postmatch_hash`：

- `PriorityQuality` 只取自 `match_selection_quality` 的过程审计。
- `SelectionQuality` 只取自 `expression_audit` 与 `price_execution_audit`；任一失败即失败，两项都通过才通过，其余保持 `ungraded`。
- 最终比分不复制进卡片，赛果只以 `postmatch_hash` 作为隔离的审计引用，不参与上述标签。
- 只有赛前明确冻结的 `match_rating`、`market_rating` 才能进入相应校准分组；缺失评级必须排除，不得用赛果或赛后印象补造。
- 同一真实比赛只使用最新已结算冻结版本参与校准；历史版本保留审计但不得重复计样本。
- 达到最低样本量只表示内部过程校准可读，不自动调权、不登记理论、不修改 Champion。

错误归因仅允许使用当时输入和赛后过程数据进行模块诊断：

`DATA_ERROR`、`ABILITY_ERROR`、`LINEUP_ERROR`、`UTILITY_ERROR`、`STATE_ERROR`、`MARGIN_ERROR`、`GOAL_OWNERSHIP_ERROR`、`PRICE_ERROR`、`MARKET_ERROR`、`EVENT_SHOCK`、`FINISHING_VARIANCE`。

每场生成 Learning Card，但只有满足样本质量要求的比赛进入正式学习集。红牌、点球、乌龙、门将重大失误和极端终结偏差必须降低学习权重。

## 六 模型治理

- 当前正式版本为 Champion；任何新变量或权重先作为 Challenger 运行 Shadow。
- Challenger 的建立也必须来自预先登记的 `HYPOTHESIS_ONLY`，不得由单场赛果直接生成；单场只能触发研究任务，不能触发模型变更。
- 新模块必须通过消融测试，证明对 Calibration、自动学习T-1h价格质量、Process Accuracy 或风险指标有独立增益。
- 新理论登记时必须预先指定结构化联赛/市场范围及模块级消融计划。允许隔离的模块标识为 `MSCB`、`STATE_TREE`、`IEH`、`TAC`、`TDD`、`LET`、`LPS`、`OCR`；必须逐项使用规则定义的固定移除/中和干预，不能在赛后改名或换口径。
- 每个验证样本必须在开赛前同时锁定 Champion、Challenger 及预登记全部模块的消融输出、计算时间和证据定位。模块缺失、多余、开赛后计算或超出预登记联赛/市场范围时，样本无效。
- 已锁定验证样本的赛后证据只能由系统绑定已核实Postmatch与自动学习四节点终点`T-1h`快照自动派生。T-1h必须与冻结表达保持同一市场、选择和盘口档位，并达到公司覆盖门槛；调用方不得提交价格或证据引用，缺失或跨档时不得用Closing或当前赔率替代。普通赛前分析仍保留完整八节点。全部派生门槛通过后可以自动创建等待确认的Promotion Candidate，但不得自动激活Champion。
- 自动赛后过程审核只能使用冻结评级、冻结决策合同和赛前T-1h证据。A/B字母评级可满足B以上门槛，数字分数不得在没有预登记映射时换算字母等级；价格执行以同线T-1h隐含概率差审核。某一过程维度证据不足只能阻止该维度或整体过程定级，不得抹去其他已独立核验维度的校准资格。
- `support/counterexample` 只能由锁定后的前向 Brier 增益、模块消融增益和风险阈值派生；调用方提交的结论一律不参与验证标签。
- 前向 Brier 必须绑定赛前已冻结的实际`market + selection + line`：1X2使用`home/draw/away`，BTTS使用`yes/no`，AH、O/U、主队总进球和客队总进球使用`full_win/half_win/push/half_loss/full_loss`结算分布；四分之一盘必须保留半赢/半输，不得把非1X2市场借用胜平负概率或胜平负赛果评分。
- Champion市场分布必须由冻结的独立Poisson重放投影得出；Challenger和每个预登记模块消融必须在完全相同的市场、选择、盘口档位与类别空间内比较。旧版`legacy_1x2_brier`只可审计，不具备Champion晋级资格。
- Hypothesis必须包含可执行的`poisson_log_rate_adjustment_v1` Challenger规格。每个预登记消融模块必须且只能对应一个赔率无关的主/客队对数进球率贡献；全量Challenger为模块贡献之和，单项消融只移除该模块。单模块绝对增量不得超过0.25，组合绝对增量不得超过0.40，且任何模块不得为零贡献。
- 正式前向证据仅接受内置`builtin_preregistered_poisson_challenger@1`。运行器身份、规格哈希、冻结概率重放哈希、Hypothesis哈希、模块集合或计算时间任一不一致即拒绝；外部提交结果不得获得Promotion资格。
- 晋级的消融门禁必须逐模块达到预登记最低增益；匿名的单一 Ablation 向量不能证明具体模块有独立贡献。
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
- 自动登记只允许实例化事前存在的不可变因果/Challenger模板：模板必须早于全部发现样本，且联赛、市场、失败维度、消融计划和赔率无关干预唯一匹配。系统不得看见多场结果后临时生成模板；无匹配、晚登记或多模板冲突均失败关闭。
- 只有形成完整 `PROMOTION_CANDIDATE` 且获得用户对该具体晋级包的明确确认后，才能版本化修改Champion。
- 自动学习的首要目标是提高 `PriorityQuality`、`SelectionQuality`、价格质量与校准，不以短期命中率为优化目标。
- 自动生成的赛前概率必须带`learning_probability_replay_v1`契约。契约必须保存完整的独立输入、PIT观测时间、来源内容哈希、估计器输出与哈希、概率输出与模型哈希，以及覆盖全部内容的`replay_hash`；冻结时由服务端独立重算，不能只相信调用方提交的概率。
- 当前内置自动概率基线只允许使用同一赛前积分榜的主客场分项：联赛主客场进球基准、主队主场攻防率、客队客场攻防率及真实样本数。盘口、博彩公司概率、供应商赛果预测、赛后数据和后续节点均禁止进入该计算器。
- 自动赛前决策只能读取已经持久化且通过节点时间审计的盘口快照。刚抓取但未保存的当前赔率不得冒充Opening、T-12h、T-6h或T-1h；没有绑定节点哈希时只能`PASS`，不得冻结可执行BET/WAIT表达。
- 每日14:30治理周期是学习范围的唯一自动准入入口。未来24小时合格比赛必须先写入不可变`learning_admissions`；分钟级节点执行器只允许推进已准入比赛的Opening、T-12h、T-6h和T-1h版本，不得自行发现比赛、扩大赛事范围、执行赛后学习或修改Champion。
- 大型联赛注册表必须版本化并生成稳定内容哈希。准入记录绑定当时版本与哈希；节点执行前按当前注册表再次审核。注册表外比赛只能进入`market_language_frozen/market_language_settlements`隔离命名空间，永不产生模型、Hypothesis、League DNA或Champion效果。
- 自动节点执行必须具有稳定执行键、持久化租约和有上限的指数退避。活跃租约禁止重复抓取；失败不得每轮询无节制消耗供应商额度；过期租约必须可以恢复。调用方不得在正式写入时覆盖服务器时间。
- 每日14:30运行记录的`run_hash`必须从固定不可变字段重新计算验证，不能只检查字段存在。哈希不一致、不可变标记缺失或执行顺序异常一律视为运行证据无效。
- 学习数据健康必须独立报告已准入未冻结即开赛、节点超宽限、未解决失败、陈旧租约、赛后窗口过期和非大型观察待兑现；健康审计本身不得调用供应商、读取赛果优化模型或产生Champion效果。
- 非大型赛事赛后可以自动采集双源事实，但事实必须写入独立`market_language_postmatch_facts`。双源一致只授予`settlement_ready`，不得自动把盘口语言标为正确/错误，不得创建Learning Card、Hypothesis、League DNA或Champion证据。最终兑现必须绑定最新事实哈希。
- 当前JSON事实库只支持单写者进程。生产启动必须显式使用一个Uvicorn worker；配置为零、无效或多个写者时，无人值守学习授权和运维健康必须失败关闭。
- 大型联赛冻结样本超过36小时自动赛后窗口后不得静默删除或用赛后信息重建赛前版本。系统必须进入人工恢复队列；受保护恢复接口只允许重新采集事实，随后仍按哈希绑定复盘流程处理。
- 独立概率输入不完整、来源时间不合格、哈希不可重放或基本面链仍不足时，保留明确的`data_missing`及`PASS`。安全PASS仍可作为数据完整度样本审计，但不能被解释为已完成方向选择。
- 前向Champion/Challenger/模块消融运行只能接收重放审计为`ready`的冻结样本。内部运行请求必须绑定`probability_replay_hash`；返回的Champion概率必须逐项等于冻结Poisson基线，Challenger和消融才允许作为同一PIT输入上的比较输出。缺少重放契约或擅自改写Champion基线时，样本不得锁定或进入Promotion。
- 无人值守激活必须生成`automatic_learning_activation_plan_v1`。计划固定覆盖主数据源、赔率源、受保护访问、持久化与备份、单写者、工作线程、联赛注册表、节点恢复和14:30调度门禁，并逐项保存责任方、依赖、下一动作和验证方式。计划只用于审计，不得配置密钥、调用供应商、启动线程或绕过失败门禁；哈希正确但责任、动作或门禁顺序被改写时同样无效。
- `automatic_learning_activation_plan_v1`未通过时，`automatic_learning_execution_gate_v1`必须同时阻止14:30学习周期和固定节点学习执行器的全部写入，不得生成空的“已完成”日运行记录。独立的The Odds API观察名单发现与已登记盘口节点采集可以继续，但保持`MODEL_LEARNING_EXCLUDED`，不得借此推进冻结、Learning Card、Hypothesis或Champion。
