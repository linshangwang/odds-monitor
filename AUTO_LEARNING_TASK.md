# 顶级联赛自动学习任务

## 一、任务目标

建立可持续、可审计的自动闭环：

`顶级联赛赛程发现 → 赛前PIT分析 → 决策冻结 → 赛后事实复盘 → 既有规则执行检查 → 新假设登记 → 独立多场验证 → 晋级审查 → 优化未来赛前选择`

目标是提高赛前场次排序、盘口表达和价格选择质量，不追求用赛后结果解释每场比赛，也不授权自动下注。

## 二、比赛范围

只纳入男子职业国内最高级别联赛：

- `competition_type = domestic_league`
- `tier = 1`
- 常规赛、官方争冠组、保级组和顶级联赛季后赛可纳入，但必须分别标记阶段。
- 排除国内杯赛、超级杯、洲际赛事、国家队赛事、友谊赛、二级及以下联赛、预备队、青年赛事和女子赛事。
- 联赛名称中出现“甲级”不代表一定是最高级别，必须核实该国联赛层级。
- 自动发现只使用与通用赛事目标分离的`LEARNING_TOP_FLIGHT_LEAGUES`注册表；欧冠、欧联、欧协联和欧国联即使可做普通分析，也不得进入学习池。
- 注册表外赛事必须附带可定位的官方/高权威层级证据和`verification_status=verified`，不能只靠调用方声明`tier=1`。

## 三、持续运行窗口

自动学习周期固定每天上海时间`14:30`运行一次。服务内原始快照采集器仍可按自己的轮询频率保存数据，但不得据此额外启动学习写入周期。每日学习周期使用日期化`run_id`保证幂等，固定按以下顺序执行：

调度采用当日补跑语义：`14:30`之前不得提前执行；`14:30`之后由首个可用工作线程轮询触发，即使服务重启或短暂中断越过原定分钟也不能漏掉当日周期。每个响应记录`scheduled_at`和`trigger_delay_seconds`，同一日期的确定性`run_id`确保补跑或并发触发最多形成一次不可变写入。跨日后不把旧日期任务伪装为新鲜PIT运行。

`GET /shadow/learning/status`中的`automatic_learning_runtime`是无人值守运行硬门禁。API-Football、The Odds API、TheStatsAPI、安全令牌、`/data`持久化及备份、自动工作线程、顶级联赛注册表和14:30调度必须全部通过；缺一项只能视为实现就绪，不能宣称闭环正在运行。该状态只返回布尔检查和阻塞项，绝不返回凭据内容。

运行配置通过后仍须检查`automatic_learning_schedule`：系统以持久化`learning_runs`中的当日确定性run id、执行顺序、开始时间和run hash为完成证据。14:30后允许最多`max(2×轮询间隔, 600秒)`的首轮询宽限；超过宽限仍无合格记录，或记录结构无效，运维状态必须发出critical告警。内存里的“最后运行结果”不能代替持久化证据。

1. 过去36小时已经完赛且存在冻结快照的比赛。
2. 未来24小时尚未开赛的合格顶级联赛比赛。

先完成过去36小时样本的赛后事实采集、证据草稿和多场重复信号刷新，再发现未来24小时赛程并创建新的赛前样本。未来赛程发现不得先于赛后阶段调用。

`GET /shadow/learning/cycle-plan`生成当日只读执行计划：列出未来24小时全部合格候选，以及开赛至少2小时且仍在36小时窗口内的最新未结算冻结版本。本任务不设置每日比赛数、每日冻结数或完整历史赔率样本数上限。

`POST /shadow/learning/run`执行一次幂等周期。默认只预览；`apply=true`时必须携带唯一安全`run_id`，重复运行返回原记录。执行器先处理已提交的合格赛后复盘，再为未结算样本收集赛后事实并生成证据草稿，完成后才发现未来赛程、构建PIT数据包并冻结。单场失败只记录为rejected，不得污染其他场次。

学习任务的赛前赔率节点固定为`Opening → T-12h → T-6h → T-1h`。Opening只有在真实来源、观测时间和盘口内容通过审计时才可形成Opening版本；不得按时钟伪造。节点推进、当前节点出现新快照证据或新的基本面复核触发器时生成不可覆写的新版本；同节点且无新证据时跳过。错过的节点保持`data_missing`，不得用后续信息回填。普通赛前分析仍可保留更完整的八节点时间轴，两者不得混淆。

学习池不限制合格比赛数量，也暂不设置完整历史赔率样本额度上限。幂等、节点去重、时间审计和来源审计仍然生效，防止重复请求或伪造节点。

赛后自动采集先建立`learning_postmatch_facts`版本：单一来源保持`single_source_pending`，两个带证据定位且比分一致的独立来源才标记`settlement_eligible`。独立性同时检查来源标识、证据定位和HTTP域名；相同证据链接或同一域名的不同别名只能计为一个来源。事件证据使用同一独立性规则。The Odds API只可作为第二赛果来源：其sport key、event id、主客队和身份哈希必须在开赛前写入冻结，赛后官方`scores`响应必须逐项匹配冻结身份；不能用赛后模糊搜索补配比赛。随后生成不可变`learning_postmatch_drafts`证据草稿，绑定`freeze_hash`和`fact_hash`，整理实际状态路径、事件污染、冻结流程完整度和待复核项。`GET /shadow/learning/review-queue`并列提供冻结、事实和对应草稿；`POST /shadow/learning/review-draft`可幂等重建最新事实版本的草稿。

自动任务可以通过`POST /shadow/learning/complete-review`完成证据驱动的结构化复盘，但接口不接受调用方Process分类。六个复盘对象中任何`passed/failed`判断都必须包含绑定冻结或事实哈希的证据和完整理由，并明确`outcome_not_used_for_process_grade=true`。系统先从六项审计派生Process正确/错误，之后才独立结算冻结选择的win/loss；最终输赢永远不能反向改变Process等级。事件污染优先，证据不完整、事件路径未独立核实、盘口无法无歧义结算或走盘时统一进入`DATA_INSUFFICIENT`。

每日周期默认在最新双源结果事实和对应草稿均存在时执行`automatic_evidence_review`。自动复盘只审计赛前不可变契约及哈希绑定事件证据，不读取终场比分来判定六项过程质量。若事件序列未达到两个独立权威来源，事件污染状态视为未知，分类固定为`DATA_INSUFFICIENT`且`selection_outcome_audit=not_used`；该记录不得进入过程质量校准、多场研究提案或Champion验证证据。只有事件序列已独立核实且全部过程证据可判定时，才允许先派生Process等级、再单独结算冻结选择。

第二事件源固定采用TheStatsAPI的赛前身份与赛后timeline契约。赛前只能用UTC日期、主客队和开赛时间唯一匹配并冻结`match_id`；多匹配、无匹配或赛后才找到的ID都不合格。赛后match detail须核对同一比赛与比分，timeline须与API-Football的进球、红牌、点球、乌龙关键序列按类型、主客侧和分钟容差一致。双方事件证据必须绑定同一`event_signature_hash`，否则仍按单源处理。

TheStatsAPI赛前比赛列表按UTC比赛日批量抓取并在同一运行进程内短时复用；同日多场和并发冻结不得逐场重复请求。缓存只接受完整成功分页，失败、限速、schema异常或超过安全分页边界时不写缓存并保持身份`data_missing`。缓存不改变PIT截止时间，也不得把缓存中未唯一匹配的比赛强行绑定。

## 四、赛前阶段

### 1. 发现与筛选

- 赛程必须至少由一个官方或高权威来源确认。
- 比赛必须尚未开赛；已开赛比赛不得重新生成赛前分析。
- 先进行低成本数据完整度筛选，再决定是否进入正式学习池。
- 不限制合格比赛数量；同一比赛每个节点或实质新证据最多生成一个不可变版本。
- 样本不能只选择热门或最终BET场次；BET、WAIT、PASS都可进入学习池，避免选择偏差。

### 2. 数据与赔率

- 严格使用PIT信息，不读取实时比分、赛中事件或结果。
- 赔率只抓取本次正式分析需要的缺失节点；已经保存的节点不得重复请求。
- 不为未入选比赛抓取完整赔率时间线。
- 学习任务暂不设置完整历史赔率样本数或任务级额度保留线；仍记录每次请求、返回节点、提供商用量和失败原因，并通过已有节点去重避免无意义重复抓取。
- Opening、T-12h、T-6h和T-1h缺失时保持`data_missing`，禁止插值或用当前赔率回填。
- 自动公平概率只使用同一PIT积分榜可核实的主客场分项建立联赛基准、主队主场攻防率和客队客场攻防率；完整输入与来源内容哈希必须进入`learning_probability_replay_v1`。供应商赛果预测和任何盘口字段都不能替代独立模型输入。
- 刚抓取但尚未持久化并通过时间审计的当前赔率只能作为待采观测，不能驱动学习冻结。可执行表达必须绑定对应Opening/T-12h/T-6h/T-1h快照的时间和内容哈希。
- 赔率路径只能称为Capital Pressure Proxy，除非存在真实Money%/Bet%数据。
- 真实Money%/Bet%/成交额只有通过`real_money_v1`比赛绑定、来源注册、证据定位、方法、时间、盘口档位和百分比闭合审计后才可标记A级；否则保持`data_missing`或拒绝，禁止从赔率路径补造。

### 3. 固定分析链

`PIT数据冻结 → 赛事/赛季/阶段 → Verified League DNA → 球队联赛残差 → 基本面链 → State Tree → 公平线/概率 → 盘口时间线 → Accepted/Resistance → Expression Optimizer → BET/WAIT/PASS`

### 4. 冻结记录

每场必须在开赛前保存：

- 唯一比赛ID、联赛、赛季、阶段、开赛时间和数据截止时间。
- 模型版本、规则版本、League DNA版本和数据源。
- 完整基本面链与数据缺失项。
- State Tree主要状态与死亡路径。
- 各市场模型概率、公平线、市场价格、Edge和EV。
- 独立概率的输入哈希、估计器哈希、模型哈希和整包重放哈希；冻结前必须由服务端重算一致。缺失或不一致时只能保存明确的`PASS/data_missing`，不得保存可执行方向。
- Match Rating和逐市场Market Rating。
- 综合过关首选/次选、单关首选/次选及BET/WAIT/PASS。
- 可接受HK赔率区间和导致降级/放弃的条件。
- Home、Away、Over、Under四轴`Capital Pressure Proxy`、盘口响应、市场接受度和Expression Optimizer结果；真实资金缺失必须明确为`data_missing`。
- 最终`execution_action`必须是`BET/WAIT/PASS`；只有`BET`可进入组合，`WAIT/PASS`仅保留过程复盘，不按已执行投注结算。
- 当时已经存在的假设和不确定性，禁止赛后补写。

冻结记录写入`SNAPSHOT_STORE_PATH`内的`learning_frozen`持久化命名空间，保存后不得覆写；后续节点更新必须生成新版本。仓库中的`learning/frozen/`仅表示导出目录约定，不是线上运行时的第二份事实源。

## 五、赛后阶段

### 1. 事实层

- 只对已冻结比赛复盘。
- 赛果必须由两个带证据定位、且比分一致的独立来源确认后才可进入复盘就绪状态。
- 记录半场/全场比分、红牌、点球、乌龙、重大伤退、进球时间和可获得的射门/xG/角球等过程数据。
- 数据缺失保持`data_missing`，不得补猜。

### 2. 固定复盘对象

- 当时是否选对比赛，而非只看方向输赢。
- 基本面是否漏项或重复计权。
- State Tree是否覆盖实际路径。
- 盘口语言、Capital Pressure Proxy和市场接受度是否正确区分。
- Match Rating与Market Rating是否混淆。
- 是否存在比最终选择更低条件、更高覆盖的盘口表达。
- 价格区间和执行门槛是否合理。
- 结果由过程错误、表达错误、事件冲击还是正常终结方差造成。

### 3. 输出分类

每场必须区分：

- `PROCESS_CORRECT_RESULT_WIN`
- `PROCESS_CORRECT_RESULT_LOSS`
- `PROCESS_ERROR_RESULT_WIN`
- `PROCESS_ERROR_RESULT_LOSS`
- `EVENT_CONTAMINATED`
- `DATA_INSUFFICIENT`

赛后报告写入`SNAPSHOT_STORE_PATH`内的`learning_postmatch`持久化命名空间，并以`freeze_id`绑定冻结版本；仓库中的`learning/postmatch/`仅供人工导出。不得把“赢”直接写成模型正确，也不得把“输”直接写成模型错误。

结算接口必须同时提交结构化`review`，逐项覆盖：场次选择质量、基本面链、State Tree覆盖、盘口语言、市场表达和价格执行。`learning_disposition.result_backfit_used`与`champion_change_requested`必须明确为`false`。自动证据复盘还必须绑定最新`draft_hash`且由系统派生分类；调用方提交的分类字段会被忽略。单场发现的新想法最多标记为`HYPOTHESIS_ONLY`或`LEAGUE_TAG_CANDIDATE`；实现错误必须引用已有规则，并声明需要回归测试。缺少这些字段时拒绝结算，不能把只有比分的记录伪装成赛后复盘。

## 六、学习点治理

### A. 既有规则执行错误

如果复盘证明现有正式规则已经明确存在，但报告、排序或代码没有正确执行，可登记为`IMPLEMENTATION_GAP`。这类修正不是新理论，可以修复实现，但必须：

- 引用原规则位置。
- 说明错误发生在哪个决策层。
- 添加回归测试。
- 不改变规则本身的含义和权重。

### B. 新理论或新联赛标签

任何新想法一律登记为`HYPOTHESIS_ONLY`或`LEAGUE_TAG_CANDIDATE`：

- 单场不能确认。
- 不得参与正式评分、概率、EV、评级或推荐。
- 发现样本不能同时作为独立验证样本。
- 必须预登记适用范围、因果解释、输入、预期方向、失效条件、反证标准和所需样本窗口。

### C. 验证与晋级

自动任务可以持续收集独立多场样本并运行Shadow、样本外验证和消融测试，但只有全部门槛100%完成、没有未解决反例时，才能生成`PROMOTION_CANDIDATE`。

每个验证样本必须先通过`/shadow/learning/hypotheses/{id}/shadow-lock`在开赛前锁定Champion概率、Challenger概率、选择表达、入场价格、两者尾部风险，以及预登记全部模块的消融输出。模块消融范围限定为`MSCB`、`STATE_TREE`、`IEH`、`TAC`、`TDD`、`LET`、`LPS`、`OCR`；每项必须绑定固定干预、冻结哈希、计算时间和证据定位，模块缺失、多余或开赛后计算均拒绝。冻结样本不得早于假设登记，发现样本不得充当验证样本，比赛和市场必须落在预登记结构化范围内。赛后验证必须绑定该`lock_hash`和Closing价格证据。系统从不可变冻结、结算与Shadow锁自动计算冻结市场的类别Brier、CLV概率差、逐模块消融增益、Process Accuracy及尾部风险差，并按预登记阈值派生`support/counterexample`；调用方自报的验证结论、PIT状态或事件污染状态均不采信。类别空间必须与冻结表达完全一致：1X2为`home/draw/away`，BTTS为`yes/no`，AH/O-U/球队总进球为`full_win/half_win/push/half_loss/full_loss`；禁止用1X2替代其他市场评分。

前向队列由内置`builtin_preregistered_poisson_challenger@1`自动执行。Hypothesis登记时必须提供`poisson_log_rate_adjustment_v1`：为每个预登记模块声明有界、赔率无关的主客队对数进球率贡献；运行器在冻结Poisson基线上生成全量Challenger，并逐项移除模块形成消融输出。规格缺失、零贡献、越界、身份或哈希不一致时必须fail-closed，禁止临场或赛后临时选择参数。

`GET /shadow/learning/hypotheses/{id}/promotion-evidence`输出派生门槛。`create_promotion_candidate`禁止调用方提交自定义passed状态；九项门槛必须全部由账本证据达到passed，任何缺失、失败或反例都阻止候选生成。

`PROMOTION_CANDIDATE`仍不得自动进入Champion。必须向用户提交：

- 理论定义及适用范围。
- 发现样本与独立验证样本分离证明。
- 全部支持与反例。
- 消融、Calibration、CLV、Process Accuracy和风险影响。
- 建议权重、保护上限和回退条件。

只有用户对该具体晋级包明确确认后，才允许版本化进入Champion。

League DNA具备单独的哈希绑定确认入口`POST /shadow/learning/league-dna/{TAG_ID}/confirm`。请求必须引用最新`activation_hash`、记录确认人和可追溯确认凭据，并逐字提交系统要求的确认语句。自动学习任务、定时任务和任何无人值守流程禁止调用该入口；只有收到用户对该具体激活包的明确指令后才可调用。成功后记录固定为`VERIFIED_ACTIVE`、100分且`automatic_activation=false`，相同确认可幂等重试，其他确认不得覆写。

### D. 多场重复信号与前向验证队列

自动任务只从已经完成人工结构化复盘的样本生成重复信号，并按不同比赛去重；同一比赛的多个冻结版本只能保留最新已结算版本，不能冒充多场独立证据。

默认至少满足以下条件才建立`RESEARCH_PROPOSAL`：

- 同一联赛、同一市场至少5场不同比赛。
- 同一失败维度至少重复3次。
- 失败率至少25%。
- 失败维度只能来自Process、场次选择、表达或价格执行审计，不得使用最终输赢作为标签。

`RESEARCH_PROPOSAL`只说明存在值得研究的重复信号，不构成因果理论，固定`causal_claim_status=not_formulated`，不得由提案内容临时编造Hypothesis、调整权重或影响Champion。提案会保存全部支持冻结ID、上下文样本和不可变提案哈希。

允许的唯一自动登记路径是事前模板实例化。`POST /shadow/learning/hypothesis-templates`登记不可变模板，必须预先固定因果定义、联赛/市场/失败维度、验证样本下限、模块消融及赔率无关Challenger干预。模板的`registered_at`必须早于提案全部支持样本的结算时间；同一提案必须恰好匹配一个模板。无模板、模板晚于任一发现证据或多个模板冲突时，周期只记录blocked，不登记Hypothesis。成功实例化仍固定为`HYPOTHESIS_ONLY`/`LEAGUE_TAG_CANDIDATE`、`champion_effect=false`，随后仅使用登记后的新比赛做前向验证。

把提案转为`HYPOTHESIS_ONLY`时，必须明确写出可证伪定义、适用范围、预期方向、失败条件和反证标准；必须绑定最新提案哈希、全部且仅限提案支持样本，并预登记不少于正式治理下限的独立验证样本数及结构化联赛/市场范围。

`GET /shadow/learning/validation-queue`只列出假设登记之后生成、未参与发现、尚未开赛、联赛和市场范围匹配的新冻结比赛。同一真实比赛即使有多个冻结版本，也只能建立一次Shadow锁。系统只安排验证机会，不自动编造Champion、Challenger或Ablation概率；这些输出必须在开赛前实际计算并锁定。

内部Shadow运行器执行前必须重新审计冻结样本的`learning_probability_replay_v1`并绑定其`replay_hash`。没有可重放PIT输入的冻结样本保持阻塞，运行器不会收到请求；运行器返回的Champion概率必须等于冻结基线，不能通过改写Champion制造Challenger增益。

## 七、优化赛前选择方向

优化优先级固定为：

1. 场次选择质量。
2. 市场表达质量。
3. 可接受价格与执行时点。
4. 组合排序与相关性控制。
5. 概率校准。

禁止根据短期命中率直接调权。任何方向优化必须证明：

- 使用赛前可知变量。
- 相对当前Champion有独立样本外增益。
- 没有仅靠更深盘口或更低赔率制造表面胜率。
- 不显著增加尾部风险、拥堵风险或市场相关性。

`GET /shadow/learning/selection-quality`按联赛与市场汇总冻结样本的Process Accuracy、场次选择失败率、表达失败率和价格执行失败率。`EVENT_CONTAMINATED`与`DATA_INSUFFICIENT`不进入有效样本。最终赢/输不得作为优化目标；达到最低样本量也只能输出`HYPOTHESIS_ONLY_REVIEW_ALLOWED`研究信号。只有更早登记且唯一匹配的模板可把该信号实例化为无模型效力的Hypothesis；否则不会自动登记、调权或修改Champion。

每次完成可结算复盘后，周期在生成多场研究提案之前先刷新不可变 Learning Card。`POST /shadow/learning/quality-cards/refresh`按冻结版本保存哈希绑定卡片；`GET /shadow/learning/quality-calibration`只使用赛前明确冻结的评级与过程审计做`PriorityQuality`、`SelectionQuality`内部校准。同一比赛仅最新结算版本计入，缺失评级不补猜，比分与输赢不得进入质量标签。该报告无自动调权、自动理论登记或Champion效力。

## 八、每日汇报

仅在以下情况通知用户：

- 新增正式冻结赛前样本。
- 完成赛后复盘并发现明确的实现错误。
- 某个候选假设新增了有效独立证据或出现反例。
- 形成需要用户确认的`PROMOTION_CANDIDATE`。
- 数据源、额度、时间戳或比赛身份出现阻塞。

如果当天没有符合条件的比赛或没有新增有效信息，安静结束，不为凑数分析低级别赛事。
