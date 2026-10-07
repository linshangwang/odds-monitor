# League DNA：联赛专属标签与评分规范

## 一、定位

模型保留统一的大框架，但不同联赛不能共享完全相同的默认比赛环境。League DNA位于“赛事识别”之后、“球队与本场基本面”之前，为各模块提供经过验证的联赛先验。

League DNA回答的是：

> 在不知道本场具体球队差异以前，这个联赛、这个赛季、这个竞赛阶段通常处于什么比赛与市场环境？

它不回答：

> 本场应该买哪个方向？

## 二、固定分析结构

每场比赛按以下层级计算：

1. `Global Baseline`：全球同级赛事基础分布。
2. `Verified League Prior`：联赛、赛季、阶段和赛制的已验证偏移。
3. `Team Residual`：球队相对本联赛基准的长期与近期残差。
4. `Opponent Adjustment`：对手允许值和对手风格校正。
5. `Match Context Delta`：阵容、赛程、天气、场地、旅行、战意和赛制目标。
6. `Market Baseline`：该联赛常见Opening与当前盘口门槛。
7. `Price/EV`：模型公平线与市场价格比较。

概念式：

`本场估计 = 全局基准 + 已验证联赛先验 + 球队残差 + 对手校正 + 本场变化`

同一事实只能进入一个主字段。联赛均值已经进入Prior后，球队统计必须使用相对联赛均值的残差，防止重复计权。

## 三、联赛标签目录

### A. Goal Environment

- 每场进球、xG、射门、射正的均值与分布。
- O/U Opening主线分布：2、2.25、2.5、2.75、3、3.25等各档占比。
- BTTS、零封、0–0、1–0、1–1和3+球比赛比例。
- 上下半场进球占比、首球时间和末段进球比例。

### B. Handicap and Parity

- 主场优势。
- 强弱分层和常见AH Opening深度。
- 热门方赢球概率与Margin Conversion分布。
- 平局率、受让覆盖结构和强队领先后的控制倾向。

### C. Corner Environment

- 总角球、主客角球及角球差的均值、中位数和分布。
- 角球Opening主线分布。
- 落后、领先、红牌和比赛末段对角球产生率的影响。
- 传中、边路推进和压迫风格对角球的解释比例。

### D. Tempo and State Elasticity

- 转换频率、直接进攻、控球推进、低位防守和定位球占比。
- First Goal后比赛扩张或收缩程度。
- 领先方继续扩大、领先保护和落后方反扑能力。
- FGH、IEH、TAC、TDD、LET、LPS、LGH的联赛基线。

### E. Match Context

- 旅行距离、时区、气候、海拔、人工草和场地尺寸。
- 赛程密度、跨洲比赛、杯赛夹层和轮换习惯。
- 常规赛、争冠组、保级组、季后赛及两回合赛制差异。

### F. Discipline and Officiating

- 犯规、黄牌、红牌和点球基线。
- 裁判影响必须与联赛基线分开，避免把单一裁判特征错误写成联赛属性。

### G. Market Microstructure

- 主要公司的覆盖度和更新时间。
- 该联赛1X2、AH、O/U和角球常见主线及价格范围。
- Price Pressure转化为升档的弹性，即Line Elasticity。
- Closing前常见流动性、价格压缩和区域公司主线分裂。

## 四、标签评分

每个标签同时记录两个独立分数：

1. `MagnitudeScore`：相对基准的方向与幅度，范围 `-3` 至 `+3`。
   - `-3`：显著低于基准。
   - `0`：接近基准。
   - `+3`：显著高于基准。
2. `EvidenceConfidence`：验证流程完成度，范围0至100。

`MagnitudeScore`高不代表可以激活。只有验证流程全部完成、`EvidenceConfidence = 100`、不存在未解决反例并获得用户明确确认，状态才可进入 `VERIFIED_ACTIVE`。

标签状态：

- `LEAGUE_TAG_CANDIDATE`：经验或初步数据提出，完全不参与模型。
- `SHADOW_VALIDATION`：预登记后进行独立样本验证，只输出审计结果。
- `VERIFIED_ACTIVE`：完成全部验证并获用户确认，可有限度调整先验。
- `SUSPENDED`：新赛季、赛制变化或稳定性检测失败，停止参与模型。
- `RETIRED`：证据否定或已被新版本替代。

## 五、100%验证门槛

具体联赛标签不得由单场、少量比赛或直觉直接激活。必须全部满足：

1. 预先登记标签定义、适用联赛、赛季、阶段、指标、方向预期、失效条件和反证标准。
2. 使用严格PIT数据，禁止用赛后可见信息回填赛前特征。
3. 样本覆盖不同球队、主客、强弱档、月份和赛季阶段。
4. 剔除或单独标记红牌、极端天气、延期、重大早早点球等事件污染。
5. 与全局基准和至少一个可比联赛进行分布比较，而非只看原始均值。
6. 完成训练区间、独立验证区间和样本外Shadow。
7. 完成球队构成、升降级、赛制变化和博彩公司覆盖变化的稳健性检查。
8. 完成消融测试，证明标签具有独立增益且没有与球队特征重复计权。
9. 所有预定验证门槛通过，且不存在未解决反例。
10. 形成版本记录并获得用户明确确认。

任一条件未完成，标签保持候选或Shadow状态，不进入Champion。

## 六、用户示例的当前登记方式

以下仅登记为候选研究问题，不作为已验证事实，也不进入当前赛前评分：

| 联赛 | 候选标签 | 当前状态 | 需要验证的核心数据 |
|---|---|---|---|
| 美职联 MLS | O/U主线及进球环境可能显著高于部分联赛 | LEAGUE_TAG_CANDIDATE | 各赛季Opening O/U分布、实际进球/xG、球队与阶段分层 |
| 巴西甲 | 总角球环境可能偏高 | LEAGUE_TAG_CANDIDATE | 角球均值/中位数/分位数、角球Opening、比赛状态校正 |
| 阿根廷甲 | 总角球环境可能偏低 | LEAGUE_TAG_CANDIDATE | 同上，并校正赛制、阶段和球队风格 |
| 挪威顶级联赛 | O/U主线及Over环境可能偏高 | LEAGUE_TAG_CANDIDATE | Opening主线、no-vig Over概率、实际进球/xG和天气月份分层 |

## 七、如何进入具体比赛分析

最终报告增加“League DNA”段，但只展示 `VERIFIED_ACTIVE` 标签：

- 本联赛已验证基准。
- 本场两队相对联赛基准的残差。
- 市场是否已经把联赛特性计入Opening。
- League DNA对Goal、AH、Corner、State Tree和表达选择的具体影响。
- 标签版本、样本窗口和验证状态。

如果没有已验证标签，明确写：

`League DNA：data_missing / candidate_only，不参与本场模型。`

## 八、最重要的防误用规则

- “这个联赛经常大球”不能直接推出本场Over。
- “这个联赛角球多”不能直接推出角球大，因为市场门槛可能已经更高。
- 联赛标签是先验，不得覆盖明确的球队、阵容、战术和本场盘口证据。
- 不同市场分别建模：高进球联赛不自动等于高角球，高角球联赛也不自动等于高BTTS。
- 联赛特性必须随赛季和赛制更新，不得永久固化为刻板印象。

## 九、运行时存储与激活门禁

- `league_dna_candidates`保存与`LEAGUE_TAG_CANDIDATE`假设绑定的不可覆写画像，包括联赛/赛季/阶段、类别、市场、MagnitudeScore、指标、比较基准、模型影响、防重复计权规则及训练/验证窗口。
- 验证进行中时，EvidenceConfidence最多为90；完成全部假设晋级门槛后，`league_dna_activation_candidates`也只能达到99，并保持`champion_effect=false`。
- 赛前模型只读取`league_dna_active`中同时满足`status=VERIFIED_ACTIVE`、`EvidenceConfidence=100`及明确用户确认记录的标签。任一条件不满足，读取结果只能是`candidate_only`或`data_missing`。
- 自动任务和对外API没有把激活候选写入`league_dna_active`的路径。用户确认后仍须执行版本化迁移并保留确认引用、激活版本及回退条件。
- 冻结记录必须绑定运行时返回的League DNA版本；候选标签或调用方自报版本不能进入赛前模型。

相关受保护接口：

```text
POST /shadow/learning/league-dna
POST /shadow/learning/league-dna/{TAG_ID}/activation-candidate
GET  /shadow/learning/league-dna/status
```
