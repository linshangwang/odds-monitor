# 自动学习导出目录约定

线上与本地运行时的唯一事实源是`SNAPSHOT_STORE_PATH`。API分别写入`learning_frozen`、`learning_postmatch`、`learning_hypotheses`和`learning_promotions`持久化命名空间；这些子目录仅用于经审计后的人工导出，不由服务并行写入，避免双写不一致。

- `frozen/`：不可覆写的赛前冻结记录；节点更新使用新版本。
- `postmatch/`：与冻结版本绑定的赛后复盘。
- `hypotheses/`：`HYPOTHESIS_ONLY`和`LEAGUE_TAG_CANDIDATE`登记。
- `validation/`：独立验证样本、Shadow与消融结果。
- `promotion/`：等待用户明确确认的`PROMOTION_CANDIDATE`。

本目录中的任何单场记录都不能直接修改Champion。
