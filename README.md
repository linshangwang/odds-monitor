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
组合方案会基于各腿已计算的单腿EV给出 `estimated_combined_ev`，并明确标记其依赖跨场独立性假设。
普通二项盘口可同时给出估算全中概率；含亚洲盘结算、走盘或半赢半输时不会机械相乘条件胜率，
而是明确返回 `data_missing`。
普通二项组合还会并列输出模型全中概率、市场去水组合概率、两者差值、庄家赔率盈亏平衡概率、
最弱一腿和风险警告；4腿以上会明确标记方差快速上升。
普通二项组合增加半优势压力测试：将每腿模型概率向对应市场去水概率收缩50%，重新计算组合
全中概率和EV，用于判断优势减弱后是否仍为正值。这只是透明的敏感性测试，不替代正式模型概率。
组合上下文同时汇总最低阵容置信度、最高拥挤度、盘口变化可用性和死亡路径状态。
`risk_adjusted_recommendation` 将压力结果正式接入最终建议：稳健模式只接受压力后仍为正EV的
组合，否则明确 PASS；平衡模式优先高一致性且通过压力测试的组合；进取模式可选择更高回报
组合，但未通过压力测试时必须附带脆弱性警告。

组合请求可传安全格式的 `portfolio_id`；未传时按排序后的比赛集合自动生成稳定ID。每次评估会
在Railway现有持久化文件中保存最近100个版本，包括触发原因、风险偏好、推荐签名及相对上一版
是否发生变化。历史通过受令牌保护的 `GET /shadow/portfolio/history/{portfolio_id}` 查询。
组合评估还可传 `stage`，支持 Opening、T-24h、T-12h、T-6h、T-3h、T-1h、T-15m、Closing。
历史接口固定返回完整八节点 `timeline`；没有执行组合评估的节点明确为 `data_missing`，后续建议
不会反向填充早期节点。未传阶段的评估独立列入 `manual_runs`。
每个保存版本还带有 `transition`：自动区分 Initial Recommendation、No Change、
Selection Change、Robustness Change、Risk Downgrade to PASS 和 Recovery from PASS，
并列出新增腿、移除腿以及决策来源和稳健性前后变化。
版本中的 `change_drivers` 按比赛保存基本面复核触发原因、盘口变化分类、PASS门槛、最佳盘口
及基本面版本号。没有事实证据时标记 `data_missing`，不会仅因推荐变化而推断基本面发生变化。

完整赛前评估新增 `fundamental_chain_audit`：available计1、partial计0.5、data_missing计0，
并校验五种比赛状态、首次进球双方路径、六个时间分段、开放局面受益方，以及
`Strength Edge / Goal Edge / Margin Edge` 三层是否分别给出。结构不完整会明确列入
`structural_issues` 并强制 PASS。响应同时提供精简的 `decision_summary`，便于前端直接展示。

外部盘口导入若触发显著变盘或跨市场背离，会写入持久化的基本面复核队列；同一比赛、节点和
数据版本自动去重。可通过受保护的 `GET /shadow/revalidation-queue` 查看 pending/revalidated/all，
无论盘口来自 pang 导入、手动快照还是自动 T-X 快照，只要触发复核条件都会进入同一去重队列；未触发的普通波动不会创建任务。
自动或手动快照完成事实复查但证据链仍不足时，任务保持 pending，同时记录尝试次数、时间、覆盖节点和仍需补充的证据；证据充足时才转为 revalidated。
队列视图和导入状态会分别统计 queued、awaiting_evidence 与 overdue，并给出下一动作，避免将数据源缺口误判成调度器未运行。
只有实际完成新的基本面版本计算后任务才会转为 `revalidated`，盘口变化本身不会改写基本面。
队列会结合 T-X 节点、触发类型和等待时间生成 normal/high/critical 优先级；等待超过30分钟的
pending 任务标记为 overdue，并在 `/shadow/import-status` 汇总待处理与逾期数量。
任务结案会保存所用基本面版本、证据状态、变化字段、概率变化和最优盘口变化；只有相对旧版本
发现真实事实变化时才标记 `Fundamental Confirmed`，首次建模不会被误判为基本面变化，否则归为
`Market-Only Move`。
每个基本面版本同时保存 `previous_version_number` 与 `recalculation_audit`，明确区分首次基线、
重复重算和真实基本面变化，并记录规范化的触发节点、原因、概率变化及最优盘口变化。
首次基线没有可比较的前序版本，因此 `changed_information` 保持为空，概率和最佳盘口变化状态
保持不可比较，不会把“首次生成”误报成“发生变化”。
并发重算写入时会在持久化锁内重新选择最新版本作为比较基准；若请求携带的上一版已过期，
`comparison_rebased_to_latest` 会明确标记，并重新计算概率差及最优盘口差，避免错位版本比较。

T-X 变盘比较现覆盖全部可用市场：1X2、AH、O/U、BTTS、主队进球数和客队进球数；每个市场
分别记录线路及价格变化。跨市场背离检查包括 1X2↔AH、O/U↔BTTS，以及双方1X2↔对应球队
进球数。可选市场缺失时保持 `data_missing`，不会用其他盘口推算补齐。
价格异动同时保存原始赔率差值和去水后的概率差值；同一盘口线下 no-vig 概率变化达到3%会触发
复核。盘口线已经变化时，两组价格不强行横向比较，明确标为 `line_changed`。
最终评估会从每个可比较选项的模型概率与市场 no-vig 概率计算绝对差值；最大偏差达到8%时
标记 `Model-Market Divergence`，同时保留对应市场、选择和偏差方向。没有可比较概率时保持
`data_missing`，不会仅凭盘口方向生成该分类。
该分类还必须通过三项可信度门槛：模型状态 ready、基本面链 decision_eligible、阵容置信度达到
系统最低值。任一项不满足时仍保留概率差供审计，但 `triggered=false`，不会输出伪背离信号。

`Pure Fundamental Script` 现在执行市场污染检查：基本面链禁止包含赔率、庄家、市场概率、
隐含概率、盘口快照或盘口变化字段，source/provenance 也不能以这些市场数据作为事实来源。
命中后在 `market_contaminated_sections` 列出具体路径，最终结论强制 PASS。

阵容置信度不再直接相信请求值：官方阵容、预计阵容、陈旧阵容和缺失阵容分别设置证据上限，
未来时间戳或无效时间戳不计入有效证据。模型和最终门槛统一使用 `effective_confidence`，原提交值、
上限、是否被压低及证据新鲜度保存在 `lineup_confidence_audit`。

`GET /shadow/snapshots` 在保留原 `snapshots` 数组的同时新增固定八节点
`complete_prematch_timeline`。没有采集到的历史节点以 `synthetic_placeholder=true`、
`timeline_status=data_missing`、`backfilled_from_current=false` 返回，并附时间轴覆盖统计。
最终下注门槛至少需要两个真实可用节点，并且最新节点必须完成与前序节点的比较；单点赔率仍可
计算模型概率、Edge和EV供审计，但会以 `line_movement_requires_two_real_comparable_stages`
强制 PASS。
“最新盘口”按固定 T-X 阶段顺序选择最接近开赛的真实节点，而非按数据库写入时间。较早阶段
即使稍后补传或修订，也不会覆盖 T-3h、T-1h、T-15m 或 Closing 的决策位置。
数据新鲜度也绑定该最新阶段，而不是所有记录中最大的写入时间；超过当前时间5分钟的快照标记
`invalid_timestamp` 并强制 PASS，防止时间漂移或错误时间戳污染临场判断。
导入时还会核对节点标签与开赛时间：各 T-X 节点采用明确允许误差，严重错位标记
`stage_timestamp_mismatch`。原始快照继续保存，但该节点不计入完整时间轴的 available 数量，
也不会成为“最新盘口”。缺少开赛时间时返回 `data_missing`，不虚构校验结果。
完整序列还必须随 Opening→Closing 单调向前；后续节点时间早于或等于前序有效节点时标记
`non_monotonic_stage_timestamp`。异常节点保留审计，但从共识变化、复核触发和最终决策中隔离。
导入幂等哈希现包含观测时间、目标时间和开赛时间；比赛信息、开赛时间、阵容历史或数据质量
单独发生修订时也会持久化并重跑时间校验，不再因赔率内容未变化而被忽略。
复核任务以实际盘口信号生成去重指纹，纯元数据修订不会重复排队；同一比赛同一节点出现新的
盘口信号时，新任务会将旧 pending 任务标为 `superseded`，队列只保留最新任务待处理。
队列达到保留上限时只裁剪最旧的 revalidated/superseded 历史，pending 永不静默淘汰；若 pending
本身超过500条，`/shadow/import-status` 返回 `over_capacity=true`，便于及时扩容或处理积压。
复核结案受当前评估阶段约束：例如使用 T-3h 数据重算时，只能关闭 T-3h 及更早任务；后来产生的
T-1h、T-15m 或 Closing 信号继续保持 pending，等待对应阶段的新事实复核。
`Likely Information-Driven` 只在变盘已触发、信息状态为 `suspected_unconfirmed` 且至少存在一个
证据引用时使用；“暂时找不到原因”不再被包装成信息盘。每次分类附 `classification_audit.basis`。
外部导入的 `information_search.evidence_refs` 会限制数量和长度，并校验结构化证据的来源、定位符与观察时间；未来时间或不可识别来源只进入审计，不参与信息盘分类。
批量导入采用单次加载、内存合并和一次原子持久化；任意数据包校验失败时整批不写入，避免大批量同步产生重复压缩开销或半批状态。
受保护的存储健康接口同时报告压缩前后体积、压缩率、比赛/快照/复核任务数量和容量预警；默认压缩文件达到 256MB 时进入 warning，可用 `SNAPSHOT_STORE_WARN_BYTES` 调整。
持久化备份是真正的上一已提交版本：首次写入建立基线，后续写入先保留旧主文件，再原子替换新主文件，避免逻辑错误同时覆盖主文件与回退点。
pang 导入的六类盘口均优先从完整公司数组重新计算 Consensus Main Line；只有对应公司数组不完整时才使用上游共识，并以 `upstream_consensus_fallback` 和 `consensus_audit` 明确标注，绝不把上游 primary 当作无条件真值。
Consensus 样本少于两家公司时仍可展示概率和价差，但不能形成最终建议，输出 `consensus_bookmaker_coverage_below_minimum` 并 PASS；阈值可通过 `MIN_CONSENSUS_BOOKMAKERS` 调整。
标记为 `upstream_consensus_fallback` 的盘口同样只用于展示与监控，不能形成最终建议；系统必须取得公司数组并自行重算后才能解除该 PASS 门槛。
系统同时保留价格离散度，并以各公司独立去水后的概率离散度作为最终门槛。任一选项的最大去水概率差超过默认 5% 时，标记 `dispersion_eligible=false` 并强制 PASS；阈值可通过 `MAX_CONSENSUS_NO_VIG_PROBABILITY_SPREAD` 调整。这样不会因为高赔率选项存在较大的表面价格差而误判市场分裂。
市场共识概率采用“每家公司独立去水 → 各结果概率取中位数 → 再归一化”，Edge/EV 优先读取该概率，而不是先合并赔率后统一去水；输出中通过 `market_no_vig_probability.method` 标明实际算法。
公司名称在计数前会进行大小写、首尾空格和连续空格归一化；同一公司重复记录先在公司内部取中位数，只能贡献一个公司样本，不能靠重复行绕过最低公司数门槛。
时间轴的概率变化、异常价格触发以及最终 Edge/EV 统一调用同一套公司级去水共识；每个节点同时记录当前与上一节点采用的概率算法，避免比较口径漂移。
十进制赔率变化只在完整去水概率不可得时作为后备异常信号；一旦概率可比较，就不会再因高赔率选项较大的表面赔率变化制造虚假复核任务。
跨盘口背离也优先比较公司级去水概率方向；盘口跨档或十进制赔率方向仅在概率不可比时使用，并通过 `cross_market_directional_signals` 记录每组实际信号。
盘口变化分类保留主分类与全部并存标签。已确认基本面变化优先，其后依次为模型—市场背离、跨盘口背离、疑似信息驱动和纯市场变化；`matched_classifications` 不会丢失同节点的其他有效信号。
任一 T-X 节点只与时间轴中严格更早的最近节点比较；补采或修正早期节点时不会引用 T-1h、Closing 等未来节点，Opening 永远不会从后续盘口反推变化。
早期节点被修正后，系统自动重算所有后续节点的盘口变化、背离信号、复核触发与分类审计；因此后续节点不会继续引用已被替换的旧盘口。
下游重算会排除时间戳或节点顺序审计为 invalid 的记录；这些节点保持 `data_missing`，既不参与前后比较，也不会生成基本面复核任务。
手动快照、自动 T-X 快照和 pang 导入现在统一保存 `stage_timing_audit` 与 `sequence_timing_audit`；本地采集路径不再绕过节点窗口和时间顺序校验。
所有最终评估入口统一执行 Line Movement 硬门槛：至少需要两个真实节点，且最新节点必须能与更早节点比较；否则清空最佳盘口、Edge 和 EV，并强制 PASS。
`/shadow/ai-packet` 只从通过时间与顺序审计的真实赛前节点选择首盘、最新盘和累计让球变化；FT、无效节点与占位节点不会进入这些字段。
`/shadow/imported-prematch/{fixture}` 使用相同的节点筛选与 Line Movement 门槛；pang 路径中的无效晚盘不会覆盖较早的有效当前盘口。
无盘口或时间审计无效的旧记录在原节点配置窗口内保持可重试；配置窗口结束后仍明确 `data_missing`，不会用后续盘口回填。
自动调度只在节点配置窗口内采集；错过窗口后该历史节点保持 `data_missing`，不会在下一个节点到来前用当前赔率“补采”或伪装成历史盘口。
多个盘口档位覆盖数与水位平衡度相同时，以全部有效报价的中位档位作为决胜基准，不再机械偏向绝对值更小的浅盘。
总完整度至少需达到0.6，且 Result Utility、Rotation Quality、Execution Ability、Goal Conversion
四个关键环节不得缺失。证据不足时仍生成概率供审计，但最终建议强制 PASS。
`available` 或 `partial` 不能只写状态：必须同时包含至少一个实质字段，例如证据、变量值或明确
结论；只有状态的空壳环节按 `data_missing` 计分。未知状态也不会计分，并分别列入
`unsubstantiated_sections` 和 `invalid_status_sections`。
四个关键基本面环节还必须提供 `source` 或 `provenance`，否则不能进入最终推荐。
所有可用/部分可用环节的 `observed_at` 或 `as_of` 会单独审计为时间戳覆盖率；由于赛季统计、
阵容消息和临场事件的合理时效不同，关键环节采用分类阈值：Result Utility 48小时、Rotation
Quality 24小时、Execution Ability 与 Goal Conversion 14天。关键时间戳缺失、无效、明显在
未来或超过对应时效时强制 PASS；非关键环节继续只做缺失审计。
关键内容还需满足最低结构：Result Utility包含双方数值型win/draw/loss；Rotation Quality
双方各至少4个指定质量维度；Execution Ability与Goal Conversion双方各至少一个数值指标。
不符合时返回 `critical_semantic_issues`，概率仍可审计但最终建议强制PASS。
所有数值统一拒绝NaN和Infinity；Result Utility必须满足win≥draw≥loss且存在实际差异；轮换
数值评分限制在0–1；执行能力和进球转化数值不得为负。

组合接口对外严格校验：请求体必须是对象，`max_legs` 必须为2–10的整数，风险偏好只能是
conservative、balanced、aggressive，复核原因必须是数组。异常输入返回明确的400/422，
不会因类型错误产生500；内部旧调用仍使用安全默认值并限制在2–10腿。

持久化写入采用同目录临时文件后原子替换。写盘或替换失败时不再只记录日志并假装保存成功，
而是清理临时文件并抛出统一的 `snapshot_store_write_failed`；只有原子替换完成才返回成功。
读取端只在文件确实不存在时初始化空库；现有文件解压失败、JSON损坏或根结构不是对象时返回
`snapshot_store_read_failed` 并停止后续写入，绝不把损坏文件当成空库覆盖。
每次成功写入还会以相同编码原子更新 `.bak` 滚动备份。主文件损坏时不会自动恢复或覆盖，
但可通过内部备份读取逻辑验证并用于人工恢复；备份不存在或损坏会返回独立错误。
受令牌保护的 `GET /shadow/store-health` 可检查主文件与备份是否存在、可读、gzip状态、大小、
版本、记录数量和SHA-256指纹。接口不返回比赛内容，也不会自动恢复或改写任何文件。
为防止Railway卷无限增长，每场基本面版本和每个组合历史默认各保留最近100条，可分别通过
`FUNDAMENTAL_VERSION_RETENTION`、`PORTFOLIO_RUN_RETENTION` 在10–1000范围调整。裁剪后版本号
继续单调递增，不会重新从保留条数开始编号。
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
所有外部数据源的 JSON 与文本诊断响应都会递归移除已配置的密钥值；即使上游回显请求参数，
接口响应和日志数据也不会返回真实凭据。
重叠凭据按长度从长到短处理，避免较短用户名先替换后仍残留较长密钥的部分内容。
网络客户端抛出的异常文本也执行相同脱敏，失败诊断不会成为绕过响应脱敏的旁路。
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
