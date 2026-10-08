# 自动学习导出目录约定

线上与本地运行时的唯一事实源是`SNAPSHOT_STORE_PATH`。API分别写入`learning_admissions`、`learning_frozen`、`learning_postmatch`、`learning_hypotheses`和`learning_promotions`持久化命名空间；这些子目录仅用于经审计后的人工导出，不由服务并行写入，避免双写不一致。

- `frozen/`：不可覆写的赛前冻结记录；节点更新使用新版本。
- `postmatch/`：与冻结版本绑定的赛后复盘。
- `hypotheses/`：`HYPOTHESIS_ONLY`和`LEAGUE_TAG_CANDIDATE`登记。
- `validation/`：独立验证样本、Shadow与消融结果。
- `promotion/`：等待用户明确确认的`PROMOTION_CANDIDATE`。

本目录中的任何单场记录都不能直接修改Champion。

`learning_admissions`由每日14:30治理周期生成，是分钟级节点执行器继续工作的唯一自动授权。执行器不得用节点到期作为重新发现赛事或扩大学习池的理由；历史上已冻结但早于准入台账的比赛只获得向后兼容的节点续跑资格。

`learning_node_attempts`保存节点执行恢复状态。键由比赛、节点、上个冻结版本和证据哈希稳定生成；记录可从`running`转为`failed`或`completed`，业务冻结记录仍保持不可覆写。失败按配置退避，活跃租约防止重复供应商调用，历史尝试有界保留。该账本只证明执行过程，不是学习样本或Champion证据。

每日`learning_runs`记录必须带`immutable=true`，且`run_hash`能由固定运行摘要字段重新计算。非空但无法重算一致的旧记录不会被当作健康执行证据。

`SNAPSHOT_STORE_PATH`的滚动备份只用于受保护的人工灾难恢复。恢复必须先读取`/shadow/store-recovery-preview`取得绑定当前主库与备份字节指纹的令牌，再向`/shadow/store-recover`提交相同令牌和精确确认短语。恢复不会由14:30周期、分钟级执行器或任何自动线程触发；原损坏主库会隔离保留，备份原件不改写，恢复动作写入`snapshot_store_recovery_audit`，且不产生任何模型或Champion效果。

注册表外比赛的赛后事实写入`market_language_postmatch_facts`，不能写入`learning_postmatch_facts`。事实包只负责双源比分、事件和统计核验；即使达到`settlement_ready`，也不能自动评价盘口语言或产生任何学习效果。兑现记录必须绑定最新事实哈希，历史手工观察在没有自动事实包时仍需两条独立结果证据。

## 模型学习排除

系统学习池只接受注册表内的**大型男子职业国内顶级联赛**。仅仅属于某国`tier=1`或名称含“甲级/超级”不构成学习资格；非大型顶级联赛、小众联赛及杯赛/洲际赛事全部标记为`MODEL_LEARNING_EXCLUDED`。

被排除比赛可以保存两类独立记录：

- `MARKET_LANGUAGE_ONLY`：冻结盘口语言、Capital Pressure Proxy、Accepted/Resistance和赛前状态树，用于人工观察。
- `SETTLEMENT_ONLY`：赛后只结算冻结方向和检查盘口语言是否兑现。

这两类记录不得生成Learning Card、`HYPOTHESIS_ONLY`、联赛标签候选或验证样本，不得进入命中率校准、MSCB/League DNA调整、阈值修改、消融测试或Champion晋级证据。重复出现也不得自动转为学习样本；只有用户明确扩大大型联赛注册表后，未来尚未开赛的新样本才可取得学习资格，历史排除样本不追溯重标。
