# 项目接管状态

更新时间：2026-10-08 Asia Shanghai

## 规则版本记录

- 2026-10-08：新增“单场不得生成或确认新理论”的零例外门槛。单场赛后复盘只能核查既有规则、记录执行错误或登记 `HYPOTHESIS_ONLY`；任何新理论必须完成预登记、独立多场PIT验证、事件污染审计、样本外Shadow、消融测试、无未解决反例，并经用户明确确认后，才可进入Champion。此次规则从本次记录后生效，不回填旧比赛。
- 2026-10-08：建立League DNA规范。统一主链增加“赛事/赛季/阶段识别→Verified League DNA→球队相对联赛残差”；具体联赛经验标签只登记为候选，完成全部验证门槛并获用户确认前不进入Champion。
- 2026-10-08：建立男子职业国内顶级联赛自动学习任务，形成赛前冻结、赛后复盘、实现错误修复、候选假设验证和用户确认后晋级的完整闭环；任务不授权自动下注。

## 当前基线

- 仓库：`linshangwang/odds-monitor`
- 分支：`main`
- 接管基线提交：`b7feddc`
- 本地候选版本：`1.89.0`（线上为 `1.87.0`，本版本尚未部署）
- 测试基线：318 项通过
- GitHub发布候选：草稿PR `#1`，分支`codex/auto-learning-v1.89`；Railway仍只跟踪`main`，因此草稿PR不会触发生产部署。
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
- 顶级联赛自动学习存储已经实现：明确核实男子职业国内一级联赛范围、开赛前不可覆写冻结、赛后绑定冻结版本、发现样本与验证样本隔离、Promotion全部门槛及用户确认门禁。
- 自动学习API不会自动修改Champion；即使达到验证样本和全部审计门槛，也只生成`AWAITING_EXPLICIT_USER_CONFIRMATION`候选。
- The Odds API 已支持缺失节点增量采集：默认跳过任何已记录节点，全节点已有时不发外部请求；普通节点复用已保存event id，只有补Opening时才重新执行首次出现证明扫描；`data_missing`默认不无限重试。
- 自动学习已有独立顶级联赛注册表与24小时/36小时只读周期计划；洲际和国家队赛事无法混入学习池，注册表外联赛必须提供可定位的外部层级证据。学习接口在Shadow令牌缺失时全部 fail-closed。
- 赛后学习结算已增加结构化反倒推审计：六类固定复盘对象必须逐项记录，`result_backfit_used=false`和`champion_change_requested=false`为硬门槛；实现错误必须绑定既有规则位置与回归测试要求。
- 每日09:30（Asia/Shanghai）的线程Heartbeat已更新为调用新周期计划语义，并使用持久化命名空间、增量赔率规则和结构化赛后复盘门禁；无有效新增时保持静默。
- League DNA候选画像、训练/验证窗口、MagnitudeScore、动态EvidenceConfidence和激活候选已经持久化；完成全部验证也只能形成99分待确认包。赛前视图只接受100分、`VERIFIED_ACTIVE`且带用户确认记录的版本，候选无法伪装成正式先验。
- Selection Quality学习卡已按联赛与市场聚合Process Accuracy、场次选择、表达和价格执行质量；事件污染与数据不足样本排除，赛果输赢不作为优化目标。样本成熟后也只产生`HYPOTHESIS_ONLY_REVIEW_ALLOWED`信号，不自动调权或登记理论。
- The Odds API付费历史权限自检与时间线采集已经改为fail-closed；服务端未配置`SHADOW_ACCESS_TOKEN`时返回503，令牌错误返回401，避免公开消耗付费额度。
- 自动学习单次运行器已经实现：默认dry-run，`apply=true`必须提供安全且幂等的run_id；只处理当期计划内顶级联赛，自动构建PIT赛前数据包并冻结原始PASS/WAIT/BET决策，逐项错误隔离且永不自动登记理论或修改Champion。
- 赛后事实队列已经实现：到期样本自动采集最终比分、事件和关键统计并版本化；单一API-Football来源只能进入`single_source_pending`，至少两个带证据定位且比分一致的独立来源才可进入复盘就绪队列。结算必须提交最新已核实`fact_hash`且比分完全一致，直接结算接口也无法绕过。事实采集不自动生成Process分类，也不自动结算样本。

## 当前真实阻塞

1. 线上健康接口显示 `market_source_blocked`，没有新鲜、可验证的赛前盘口快照，因此必须安全 `PASS`。
2. 本地没有配置 `THE_ODDS_API_KEY`；Railway已配置并完成巴西甲真实多公司历史快照验证。自动任务必须通过线上受控采集，并继续遵守额度保留线。
3. 缺少真实 Money%、Bets% 和成交额时，资金模块只能使用盘口路径代理，不得声称真实资金流向。
4. 若无可靠事件级 xG、xThreat、阵型和赛制目标数据，部分基本面链字段仍会保持 `partial` 或 `data_missing`。

## 下一批工程任务

1. 为真实资金字段建立独立 schema、来源等级和时间戳审计，严格区分真实资金与 Capital Pressure Proxy。
2. 将资金压力、盘口响应、市场接受度和 Expression Switch 固化进最终决策输出。
3. 为 `PriorityQuality`、`SelectionQuality` 和 Learning Card 增加内部校准记录；不得把赛后模块扩展成赛后推荐产品。
4. 建立欧国联八场冻结样本集，保存当时版本、盘口路径、最终选择与优先级，作为不可回填的回归测试夹具。
5. 对 MSCB、State Tree、IEH、TAC、TDD、LET、LPS 和 OCR 做 Champion 与 Challenger 消融框架。
6. 数据源恢复后先进行 Shadow 验收；满足新鲜度、完整公司数组、基本面链和阵容置信门槛后，才允许生成非 PASS 建议。
7. 为MLS、巴西甲、阿根廷甲和挪威顶级联赛积累合格冻结发现样本后，按新League DNA存储登记候选并启动独立验证；在此之前不影响任何正式分析。
8. 部署并验收本地`1.89.0`自动学习执行器与治理接口；Railway当前未配置`SHADOW_ACCESS_TOKEN`，在明确发布前必须先安全配置。即使误先部署，学习与付费赔率接口也会保持fail-closed。

## 接管原则

后续开发以 `MODEL_RULES.md` 为规范来源。聊天记录用于追溯，不作为唯一运行依据。任何与规范冲突的旧代码或旧说明，应通过版本化迁移修正，不直接篡改历史记录。
