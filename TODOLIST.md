# DE 面试准备 TODOLIST — UK / NL Data Engineer (Mid + Senior 双线)

> 目标：3 个月内拿到 UK / NL Data Engineer offer，带签证赞助。
> **双线投递**：Senior 优先（UK £70-90k / NL €75-100k），Mid 兜底（UK £55-75k / NL €65-85k）。
> 起点：QA 背景（Alibaba → SenseTime → Data.ai → NVIDIA），自建 portfolio（iodp + download_channel + trtllm-ci 真实工作 + word2pdf）。
> 主路线：**领域知识（Data.ai 业务）+ 工程能力（iodp/download_channel 代码）+ 故事力（QA→DE 转型）**。

## Senior vs Mid — 投哪个

| 公司类型 | Senior 难度 | 投这个 |
|---|---|---|
| Scale-up（Wise / Revolut / Picnic / Mollie / Bol / Mendix） | 中 | **Senior** |
| Startup（< 200 人） | 低 | **Senior** |
| Tech-strong enterprise（Booking / Adyen / Spotify EU / Just Eat） | 中-高 | **两个都投**，简历 Senior 版优先 |
| 传统 enterprise（ING / Rabobank / ASML / TomTom） | 高 | **Mid**（Senior 卡 6-8 年生产年限） |
| FAANG EU（AWS / Google / Meta London / Amsterdam） | 极高 | **Mid 起步**（Senior 走 SDE-III ladder） |

**简历准备 2 版**：Mid 版 bullet 偏执行（built / tested / reduced %），Senior 版 bullet 偏决策（led / chose A over B / mentored N people / owned trade-off）。投 Senior 时简历 Senior 版，被 downlevel 到 Mid 不要拒（Booking Mid > 随便 startup Senior，长期看）。

---

## 0. 简历定位（先把这个定死，再开始复习）

> **底线**：title 不能改、authorship 不能虚构。但 **bullet 可以围绕真实 QA 工作里的"工程含量"来写** —— 你大概率严重低估了自己在 Data.ai 的真实工程量。EU 背调会问前 manager 你的 title 和职责，不会问每行 bullet 的措辞，所以**核心是 bullet 写得有数据工程味道、但 100% 经得起追问**。

### 0.1 角色映射（title 保持真实）

| 公司 | 真实角色 | 简历 title 写法 |
|---|---|---|
| Alibaba | QA | **QA Engineer** |
| SenseTime（商汤） | QA / 后台测试 | **QA Engineer**（**不要挂 iodp 到这里**） |
| Data.ai | QA for Download Channel 产品 | **QA Engineer — Download Channel data product**（title 不动，bullet 围绕数据工程含量重写，见 §0.4） |
| NVIDIA | trtllm-ci 自建 | 按真实 title 写。如果是 SWE 就 SWE，如果是 QA 写"QA Engineer, scope expanded to own CI infrastructure" |

### 0.2 项目区位

| 项目 | 简历区位 | 一句话定位 |
|---|---|---|
| trtllm-ci | NVIDIA 工作经历 bullet 下 | "Designed GitLab CI pipeline orchestrating 1000+ daily test jobs across multi-GPU clusters; reduced regression detection time from N to M." |
| download_channel | **混合**：Data.ai bullet 里以 QA 视角描述真实参与；**外加 Personal Projects 板块**列出你自己重写的 GitHub 实现，互相引用 | 见 §0.4 |
| iodp | **Personal Projects** 板块（**绝不挂公司名**） | "Self-built data + AI agent platform showcasing modern lakehouse stack (MSK + Glue Streaming + Iceberg + Athena + LangGraph + OpenSearch)." |
| word2pdf | 可放可不放，放的话归 Personal Projects | DE 岗位帮助有限，可砍 |

### 0.3 转型叙事（cover letter / LinkedIn About / 面试开场）

```
我从 QA 起家，在 Alibaba / SenseTime / Data.ai 6+ 年里贴近过电商、AI 视觉、移动 ad-tech 三个领域的核心数据产品。
Data.ai 期间我 own Download Channel 这条核心数据产品的 QA —— 写过 Python + SQL 测试框架做 data contract 校验,
深入了解窄→宽 schema 演进、restate 窗口、四象限口径设计。我亲眼看到团队里一位同事 QA→Dev 转型 own 了 pipeline
重建,这成了我自己转型的样板。
转 NVIDIA 后自建了 TensorRT-LLM CI 基础设施 —— 证明我能 own 复杂 infra。
过去 12 个月业余时间补 DE 工程,开源了 iodp(lakehouse + agent)和 download_channel(ETL pipeline,基于我在 Data.ai
的 domain 重新 from scratch 实现)两个项目,Terraform / Glue / Snowflake 每一行代码我都写过。
现在准备全职转 DE。
```

QA 出身在欧洲数据圈是优势（data contract / DQ-as-code / SLA 都是 QA mindset 的工程化），别藏。

### 0.4 download_channel 在简历上怎么"软挂"到 Data.ai

**核心思路**：title 不动，bullet 围绕你真实做过的 QA 工程工作 + 你对数据产品的深度认知。最后一行明确指向你的 GitHub portfolio —— **写明那是 personal re-implementation，不是 Data.ai 代码**。

```
QA Engineer — Download Channel data product            Data.ai (App Annie)   20XX–20XX

• Owned end-to-end QA for the Download Channel ETL pipeline (AWS Glue + Snowflake
  medallion architecture, processing N GB/day across 200+ countries × 1M+ apps).
• Built Python + SQL test framework validating data contracts across the
  narrow→wide schema migration (4-quadrant Featured × Paid matrix);
  caught X regressions pre-production.
• Authored data quality checks for restate-window correctness (trailing 7-day
  upserts) — collaborated closely with the engineering team that handled the
  QA→Dev transition for the pipeline rebuild.
• Deep domain expertise: channel attribution semantics, schema evolution patterns,
  vendor-side data delivery SLAs, MECE 4-quadrant taxonomy (paid_featured /
  unpaid_featured / paid_organic / unpaid_organic).
• Re-implemented the full ETL stack as a personal engineering project to
  demonstrate ownership beyond QA scope (full Terraform + Glue + Snowpipe +
  dbt + Airflow): github.com/<you>/download_channel
```

**这套写法的"四个安全锚"**：
1. Title = QA Engineer（背调一致）
2. Bullet 里的"测试框架 / DQ 检查 / 测试策略协作"= 你 QA 真做过的事，可以追问 ✅
3. "engineering team that handled the QA→Dev transition" = 实情，不是你
4. 最后一行明文写 "Re-implemented…as a personal engineering project" + GitHub 链接 = **完全无歧义**说明哪部分是公司哪部分是个人

**面试官追问 "Did you write this pipeline?" 的标准答案**：
> "At Data.ai I owned the QA side. I wrote test code, validated data contracts, worked closely with the engineers rebuilding the pipeline. I did NOT author the production ETL code. After leaving I rebuilt the whole stack from scratch as a portfolio project — that's the GitHub repo. Every line of Terraform, Glue, Snowflake SQL in there is mine."

诚实 + 主动 + domain depth + 有代码作证。**比硬扛说"是我写的"强 10 倍**。

### 0.5 自检：你 Data.ai 真实工程量盘点（写 bullet 之前先做）

回忆一下，下面这些事你做过几件？做过的就是 bullet 素材：

- [ ] 写过 Python 测试脚本（pytest / unittest）
- [ ] 写过 SQL 校验查询（行数对比 / 聚合校验 / referential integrity）
- [ ] 用过 Great Expectations / Soda / dbt tests / 自研 DQ 框架
- [ ] 写过 CI/CD 配置（Jenkins / GitLab / GitHub Actions）跑测试
- [ ] 做过 schema 兼容性测试 / contract testing
- [ ] 做过性能 / regression 自动化
- [ ] 写过测试数据生成器
- [ ] 主导过 bug triage / 跨团队协作过线上事故复盘
- [ ] 培训过新人 / mentor 过 junior QA
- [ ] 跟 PM / engineering / 客户 review 过 spec

**做过 5 件以上 = 你完全有资格自称 "QA Engineer with strong engineering toolkit"，不要再贬低自己了**。把这些事写进 Data.ai bullet，配合 §0.4 那段，简历的"data engineering 含量"立刻起来了。

### 0.6 Senior 版 bullet 改写法（同一件事换说法）

Senior 面试官扫简历看的不是技术词，是**决策动词** + **影响半径**。同一件事，三组改写：

| Mid 版（执行） | Senior 版（决策 + 半径） |
|---|---|
| Built Python + SQL test framework validating data contracts | **Led test strategy for narrow→wide schema migration; designed the 4-quadrant data contract framework adopted by the 3-person rebuild team; surfaced X regressions pre-prod** |
| Wrote DQ checks for restate-window correctness | **Owned data contract design for the 7-day restate window; chose row-level upsert verification over snapshot diff after benchmarking both; framework reused across 3 downstream products** |
| Designed GitLab CI for 1000+ daily test jobs | **Owned the architecture decision to consolidate fragmented per-team CI into a unified pipeline; reduced regression detection from N hours to M minutes; mentored 2 engineers on the new framework** |
| Re-implemented ETL stack as personal project | **Open-sourced a reference implementation of the Download Channel pipeline (Terraform + Glue + Snowflake + dbt + Airflow); used in interviews to discuss medallion architecture trade-offs and FinOps decisions** |

**改写公式**：
- 把 `Built X` → `Led / Owned / Designed / Chose X over Y because Z`
- 加一个**人数信号**："adopted by N-person team / mentored M juniors / unblocked K teams"
- 加一个**钱 / 时间 / 风险信号**："reduced cost by $X/mo / cut latency from Y to Z / prevented N% of regressions"
- 加一个**决策替代项**："chose A over B after benchmarking" —— Senior 面试就是考"你为什么没选 B"

⚠️ **不要为了 Senior bullet 编数字**。能讲清楚的小数字 > 编出来的大数字。被追问"你说的 3-person team 是哪 3 个？" 答不出来就崩了。

### 0.7 Senior 必备 STAR 故事清单（10 个）

Senior 面试 behavioral 占 30-40% 时间。这 10 个故事提前打磨好（写进 [STAR_STORIES.md](STAR_STORIES.md)），每个 90 秒讲完：

| # | 题型 | 用哪个项目素材 | 关键信号 |
|---|---|---|---|
| 1 | "Tell me about a time you led a technical decision" | Data.ai DQ 框架设计 / trtllm-ci 架构合并 | leadership / trade-off |
| 2 | "Tell me about a time you disagreed with a stakeholder" | Data.ai schema 口径争议 / NVIDIA test priority | influence without authority |
| 3 | "Tell me about your most painful production incident" | trtllm-ci 真实事故（必须真实，否则细节穿帮） | calm under fire / RCA discipline |
| 4 | "Tell me about a time you mentored someone" | NVIDIA 带新人 / Data.ai 培训测试规范 | leverage / patience |
| 5 | "Tell me about a time you optimized cost" | iodp FinOps 设计（生命周期 / DPU 选型 / Snowflake auto-suspend） | business sense |
| 6 | "Tell me about a time you owned something end-to-end" | trtllm-ci 自建 / download_channel portfolio | ownership |
| 7 | "Tell me about a time you said no" | 拒绝 hack 修复 / 拒绝跳过测试 | engineering principles |
| 8 | "Tell me about a time you migrated a system" | Data.ai 窄→宽 schema 迁移（你 QA 视角） | risk management |
| 9 | "Tell me about a time you failed" | 真实失败（必须真），关键是 reflection | growth mindset |
| 10 | "Why are you transitioning to DE?" | QA→DE 叙事（§0.3） | self-awareness |

**Senior bar raiser 看的是**：你能不能在 90 秒里讲出 (Situation 15s + Task 10s + **Action 50s,要包含 trade-off** + **Result 15s,要有数字**)。**Action 那段必须出现 "I chose X over Y because Z"** —— 这是最容易被 Mid 候选人忽略的环节。

---

## 1. 12 周复习计划

### Week 1-2：SQL 与数据建模（基本功）

- [ ] **SQL 进阶**（每天 1 小时刷 [DataLemur](https://datalemur.com) / [StrataScratch](https://stratascratch.com)，目标 Hard 题 30 道）
  - Window functions: `ROW_NUMBER / RANK / DENSE_RANK / LAG / LEAD / SUM(...) OVER`
  - CTE 嵌套 + recursive CTE
  - Pivot / unpivot（**对应 download_channel 窄→宽**）
  - 自连接（员工经理树、漏斗分析）
  - 时间序列（连续登录 N 天、活跃留存）
- [ ] **Snowflake SQL 特化**
  - `QUALIFY` 子句
  - `MATCH_RECOGNIZE`（模式识别）
  - `COPY INTO` / `STAGE` / `PIPE`
  - `STREAMS` + `TASKS` vs `DYNAMIC TABLE`
  - micro-partition + clustering keys + search optimization
- [ ] **数据建模**（读 Kimball *The Data Warehouse Toolkit* 第 1-7 章 + 第 11-14 章）
  - Star vs Snowflake schema
  - Fact 表种类（transaction / periodic snapshot / accumulating snapshot）
  - SCD Type 1/2/3/6（**面试高频**）
  - Conformed dimension
  - Bridge table / factless fact
  - 反范式 OBT（One Big Table）vs star schema 的取舍
- [ ] **能产出**：
  - 把 download_channel Gold 层重做一份 **Kimball 风格**：`dim_app / dim_country / dim_device / dim_date / fact_downloads_daily`，对比 PLAN.md 现在的预聚合宽表，能讲出两套各自的优劣
  - 写 [STAR_STORIES.md](STAR_STORIES.md)（自己建，后面用）

### Week 3-4：Spark / Glue 内功

- [ ] **Spark 核心机制**（读 *Learning Spark 2nd Edition* + Databricks 官方 perf doc）
  - Catalyst optimizer 4 阶段（Analysis / Logical / Physical / Code Gen）
  - DAG → Stage → Task 三级划分
  - Wide vs narrow transformation
  - Shuffle 三种实现（Hash / Sort / Tungsten）
  - **Skew 处理**：salting / AQE skew join / broadcast hint
  - **AQE**（Adaptive Query Execution）三件套：动态合并 partition、动态切换 join 策略、动态处理 skew
  - Cache vs persist vs checkpoint
  - Broadcast threshold 默认 10MB，调到多少 / 何时关闭
- [ ] **Glue 特化**
  - Glue Job vs Glue Streaming vs Glue Workflow vs Glue Trigger
  - Job bookmark 机制 / 失效条件 / 与 idempotency 的关系
  - DPU 类型（G.1X / G.2X / G.4X / G.025X）+ FinOps 选型
  - Glue Catalog vs Hive Metastore vs Iceberg REST catalog
- [ ] **能产出**：
  - 在 download_channel 上跑一份 **5-10 GB 真实造数据**（用 [dbldatagen](https://github.com/databrickslabs/dbldatagen) 或自己写 generator）
  - 记下：input rows / shuffle write / shuffle read / stage 时长 / DPU 用量 / 单次成本
  - 故意造一个 skew（某 country 占 80%）然后 fix 掉，记调优前后指标
  - 写到 download_channel/PERFORMANCE.md，**面试时直接 show**

### Week 5：Lakehouse 表格式（Iceberg / Delta / Hudi）

- [ ] **Iceberg**（你 iodp 已经用了，深挖）
  - Metadata 树形结构：catalog → table metadata.json → manifest list → manifest → data file
  - Snapshot / Time travel / Branching / Tagging
  - Hidden partitioning（vs Hive 显式分区）
  - Compaction（rewrite_data_files）+ snapshot expiration
  - Merge-on-read vs Copy-on-write
  - Position deletes vs equality deletes
  - **REST catalog 趋势**（vs Glue / HMS）
- [ ] **Delta Lake** 简单对比（懂概念即可，不深挖）
  - `_delta_log` 结构 + checkpoint
  - VACUUM / OPTIMIZE / Z-ORDER
- [ ] **Hudi** 简单对比
  - COW vs MOR
  - Timeline + commit metadata
- [ ] **能讲出来**：三家在 ACID、schema evolution、time travel、compaction 上的差异；为什么 Netflix 选 Iceberg、为什么 Databricks 死磕 Delta。

### Week 6：dbt（必补）

- [ ] **dbt 核心概念**（[dbt Learn](https://courses.getdbt.com) 免费课程过一遍，~10 小时）
  - models / sources / seeds / snapshots / tests / macros
  - materialization 4 种：view / table / incremental / ephemeral
  - incremental strategy: append / merge / delete+insert / insert_overwrite
  - 测试: unique / not_null / accepted_values / relationships + 自定义 generic test
  - dbt-utils 常用宏：`equal_rowcount / pivot / unpivot / surrogate_key`
  - `ref()` / `source()` / DAG / dbt docs
  - exposures / metrics / semantic layer（趋势）
- [ ] **能产出**：
  - 给 download_channel 加一份 **dbt 实现**（Snowflake adapter）
  - `models/staging/stg_dc_wide.sql` → `models/marts/fct_downloads_daily.sql` + `dim_*`
  - `tests/` 加 5-10 个 schema test + 1-2 个 singular test（数据契约：四象限和 = total）
  - **面试时这是大杀器**：你能 show 一份你自己写的 dbt project 跑在你自己部署的 Snowflake 上，比单纯说"我用过 dbt"高三档

### Week 7：Airflow（必补）

- [ ] **Airflow 核心**（[Astronomer Academy](https://academy.astronomer.io) 免费课）
  - DAG / Task / Operator / Sensor / Hook / XCom / TaskFlow API
  - Scheduler / Executor（LocalExecutor / Celery / Kubernetes）
  - Catchup / backfill / depends_on_past
  - SLA / on_failure_callback / retries / exponential backoff
  - Dynamic task mapping（2.3+）
  - Deferrable operator（trigger 机制，省 worker slot）
  - Datasets（2.4+，事件驱动调度）
- [ ] **常考 pitfall**
  - 为什么 `datetime.now()` 在 DAG 顶层是个大坑（每次 schedule 都重算）
  - top-level Python code 慢会阻塞 scheduler
  - XCom 不能传大数据
  - Idempotency: task 重跑应该幂等
- [ ] **能产出**：
  - 把 download_channel Glue Workflow 重写一份 **Airflow DAG**（用 `GlueJobOperator` 或 `BashOperator`）
  - docker-compose 本地跑通，截图放进 README
  - 不需要部署到生产，**目的是简历能写 Airflow 且面试能讲清楚**

### Week 8：流处理 + Kafka

- [ ] **Kafka 核心**
  - Topic / partition / offset / consumer group
  - At-most-once / at-least-once / exactly-once（KIP-98 transactional producer）
  - ISR / leader election / replication factor / min.insync.replicas
  - Compaction vs deletion retention
  - Schema Registry + Avro / Protobuf
  - **MSK Serverless** vs MSK Provisioned vs Confluent vs Redpanda 选型
- [ ] **流处理引擎对比**
  - Spark Structured Streaming：micro-batch、watermark、output mode（append/complete/update）
  - **Flink**（EU 市场 Flink 比 Spark Streaming 更主流，特别是 ING / Adyen / Bol）
    - Event time vs processing time
    - Windowing: tumbling / sliding / session
    - State backend: RocksDB / heap
    - Checkpoint + savepoint
    - CEP（complex event processing）
  - Kafka Streams 简单了解
- [ ] **能讲出来**：
  - 你 iodp 里的 Glue Streaming 是 Spark Structured Streaming，watermark 怎么设的、exactly-once 是怎么保证的（idempotent producer + checkpoint to S3）
  - 如果换 Flink 实现 iodp 会怎么改（state backend、checkpoint、event-time semantics）

### Week 9：AWS / Cloud 深度 + 成本

- [ ] **AWS 数据栈**
  - S3：multipart upload / versioning / replication / Intelligent-Tiering / Bucket Key
  - Glue: 上面已覆盖
  - Athena: Athena Engine v3 + Iceberg + workgroup + 成本控制
  - EMR vs Glue vs Databricks 选型
  - Step Functions vs Airflow
  - Lake Formation: row/column-level security
  - DataZone（新）+ Glue Data Quality（新）
- [ ] **FinOps 思维**（DE Senior 必考）
  - S3 Lifecycle: STANDARD → IA → Glacier → Deep Archive 转换日 + 各自 retrieval cost
  - Glue DPU 选型 + Spot
  - Snowflake 仓库自动暂停 + multi-cluster scaling + resource monitor
  - 算账题练习：每月 100TB 入仓 + 1000 个 dashboard + 10 个 ETL，预算估算
- [ ] **Terraform 进阶**
  - State backend (S3 + DynamoDB lock)
  - Module composition + remote module
  - `for_each` vs `count`（重要）
  - Provider versioning 锁定
  - `depends_on` 显式 / 隐式
  - Sensitive 变量 + Secrets Manager 集成
  - CI/CD with Terraform: plan in PR, apply on merge

### Week 10：Data Quality + Governance

- [ ] **DQ 框架**
  - dbt tests（基础）
  - Great Expectations（重型，仍主流）
  - Soda Core（轻量 YAML）
  - Monte Carlo / Anomalo（商业 observability）
  - **Data Contract**（趋势，2024-2026 年 EU 市场快速升温）
- [ ] **Lineage**
  - OpenLineage 标准（你 iodp 里 DynamoDB 那张 lineage 表是自造的，要学会 OpenLineage 怎么做）
  - Marquez / Datahub / OpenMetadata / Amundsen
- [ ] **Governance**
  - GDPR：right to be forgotten, data minimization → 在数据湖里的实现挑战
  - PII 处理：tokenization / hashing / column-level encryption
  - Access control: Lake Formation / Unity Catalog / Snowflake row access policy
- [ ] **QA→DE 转型加分点**：
  - 你 QA 6+ 年经验 = 天然懂 test pyramid / equivalence partitioning / boundary value
  - 把这些直接映射到数据：
    - unit test = dbt singular test
    - integration test = dbt schema test cross-model
    - contract test = data contract / schema registry compatibility
    - regression test = snapshot test on Gold tables
  - **面试金句**："I bring testing-first mindset to data pipelines. In my QA past I've seen too many production fires from untested edge cases — I treat data contracts and DQ rules as first-class, not afterthought."

### Week 11：System Design（DE 风味）

- [ ] **经典题型**（每题练 1 小时白板，[这本书](https://www.amazon.com/Data-Engineering-Design-Architecting-Pipelines/dp/B0BV87HF12) 或 Alex Xu 的 *System Design Interview Vol 2* 章节）
  1. Design **Uber surge pricing** 数据通路（streaming + window aggregation）
  2. Design **Netflix watch history** 仓库（CDC + slowly changing user prefs）
  3. Design **Reddit clickstream** lakehouse（你 iodp 几乎就是这个）
  4. Design **Stripe payments** 数据契约 + reconciliation
  5. Design **Spotify wrapped**（年度聚合 + 全球排行）
  6. Design **Airbnb fraud detection** 特征仓 (offline + online feature store)
  7. Design **TikTok recommendation** training data pipeline
- [ ] **答题套路**（5 步法）
  1. **Clarify**：业务量级（QPS / DAU / 数据量 / 延迟 SLA / 准确性 SLA）
  2. **High-level**：source → ingest → storage → compute → serve
  3. **Deep dive 2-3 模块**：选最有 trade-off 的（如 storage format / streaming engine）
  4. **Trade-off**：每个选型给出 alternative + 为什么没选
  5. **Operational concerns**：monitoring / alerting / DR / cost / governance / on-call
- [ ] **download_channel + iodp 能讲的**：
  - download_channel = 简化版 Reddit clickstream（外部 vendor S3 drop + restate）
  - iodp = 简化版 fraud detection（streaming + agent 决策）
  - 面试官问 "design a clickstream lakehouse" 你直接搬 iodp 答，**有真实代码、真实部署、真实成本数字** = 全场最强答案

### Week 12：Coding + Mock Interview

- [ ] **Python coding**（DE 不像 SDE 那么偏算法，但 EU 一些公司还是会出）
  - 基础数据结构（list/dict/set/tuple）+ 时间复杂度
  - LeetCode Easy/Medium 50 道（重点 array / hash / two-pointer / BFS）
  - **DE 专属常考题**：
    - "Given event log, find sessions"（gap > 30min 切分）
    - "Find top-K by category"（heap 或 SQL window）
    - "Implement rate limiter"
    - "Parse CSV/JSON with corrupt rows"（边界处理）
- [ ] **Mock interview**
  - [Pramp](https://www.pramp.com) 免费 peer mock，每周 2 次
  - 找一个 DE 朋友 / [interviewing.io](https://interviewing.io) 付费 mock 2-3 次
  - 录音回听自己的英语表达
- [ ] **STAR stories 定稿**（往 [STAR_STORIES.md](STAR_STORIES.md) 里写 8-10 个，覆盖：）
  - "Tell me about a time you debugged a hard data quality issue"
  - "Tell me about a time you optimized a slow pipeline"
  - "Tell me about a time you disagreed with a stakeholder"
  - "Tell me about a project you're most proud of"
  - "Tell me about a time you failed"
  - "Tell me about a time you mentored someone"（即使你是个体贡献者，也要有版本）
  - "Tell me about a time you had to learn something new fast"（QA→DE 转型本身就是答案）

---

## 2. 深度掌握你的 4 个项目（你说每行代码都会懂，那就给你一个 checklist）

### 2.1 download_channel — 必须能答的问题

- [ ] **架构** Why Glue not Lambda? → 单日数亿行，Lambda 15min/10GB 限制扛不住
- [ ] **窄→宽演进** Why narrow first then wide? → 通道集合不稳定时窄表加列零成本；稳定后宽表查询友好
- [ ] **Snowpipe vs COPY INTO 手动** Why pipe? → 事件驱动 + serverless 计费 + 无需自己 schedule
- [ ] **Dynamic Table vs MV vs Stream+Task** Why Dynamic? → 增量声明式 + DAG 自动管理 + 不依赖 stream offset
- [ ] **Restate 处理** How idempotent? → DELETE + INSERT + DynamoDB checkpoint + file MD5
- [ ] **DQ 卡点** What checks? → 四象限和 = total（contract）+ row count drift + null rate per column
- [ ] **Cost** 每天处理 X GB，成本明细？（你周 9 部署完会有真实数字）
- [ ] **Failure mode** 如果 dropzone 桶半夜挂了怎么处理？→ Glue 等待 + alert + manual replay
- [ ] **Schema evolution** 如果 Data.ai 加了新 channel 怎么办？→ Bronze 透传 + Silver pivot 加 case + Gold dynamic table 自动重算
- [ ] **Backfill** 一年前数据要重灌怎么做？→ Glue Job partition 参数化 + 限速 + 资源隔离

### 2.2 iodp — 必须能答的问题

- [ ] **Why MSK Serverless vs MSK Provisioned vs Kinesis vs SQS?**
- [ ] **Streaming exactly-once 怎么做的？** → idempotent producer + checkpoint to S3 + Iceberg ACID
- [ ] **Iceberg vs Delta vs Hudi 为什么选 Iceberg？**
- [ ] **DLQ replay 机制** 重放时怎么避免重复入 Bronze？
- [ ] **DQ + Lineage 表为什么用 DynamoDB 不用 RDS？** → on-demand cost + low latency + key-value access pattern
- [ ] **Glue Streaming Job 的 watermark 怎么设的？**
- [ ] **OpenSearch indexer 用来干嘛？** → Gold 层物化 + 业务方半结构化检索
- [ ] **Agent 层怎么 reason over data？**（LangGraph state machine）
- [ ] **如果用 Flink 重写 iodp 会怎么改？**

### 2.3 trtllm-ci — 必须能答的问题（这是真实工作，深度最高）

- [ ] CI pipeline 全貌（你已经掌握）
- [ ] GPU 资源调度怎么做的
- [ ] flaky test 处理策略
- [ ] 跨 multi-node 怎么调度
- [ ] **关键**：把这段经验**翻译成 DE 语言**：
  - "调度 1000+ daily jobs" → "orchestrating distributed compute workload"
  - "GPU 节点池" → "compute resource pool with bin-packing"
  - "perf rerun 机制" → "automatic retry with exponential backoff"
  - "disagg pipeline" → "multi-stage workflow with dependencies"

### 2.4 word2pdf — 简略覆盖即可

- [ ] 一句话能说清架构（前端 + 后端 + 部署）
- [ ] 不深挖，DE 面试不会问

---

## 3. 求职操作（与代码无关，但同样关键）

### 3.1 简历

- [ ] **欧洲格式**：A4，**1 页**（< 8 年经验）/ 最多 2 页。砍掉照片、出生日期、婚姻状况（GDPR 友好）
- [ ] **3 个版本**：
  - 通用版（投常规公司）
  - **Streaming heavy 版**（投 Adyen / Bol / Wise）
  - **Lakehouse heavy 版**（投 Booking / ASML / Picnic）
- [ ] 项目段落格式：`Tech: ... | Impact: <number>`（必须有数字，哪怕是 demo 数字）
- [ ] 找 1-2 个 DE 在职朋友 review 一遍，砍掉 buzzword

### 3.2 LinkedIn

- [ ] Title 改成 "Data Engineer | ex-NVIDIA / Data.ai | AWS + Snowflake + Spark"
- [ ] About 板块用上面 0.3 那段叙事
- [ ] Featured 区贴 GitHub repo 链接（iodp + download_channel）
- [ ] 设置 "Open to work" 内部可见
- [ ] 每周 1-2 条技术帖（写你这周学了啥，dbt incremental 的坑、Iceberg metadata 解析等），半年内涨到 1000 follower

### 3.3 投递渠道（带 sponsorship）

- [ ] **UK**：[Otta](https://otta.com) / [Welcome to the Jungle](https://welcometothejungle.com) 筛 "visa sponsor"；UK gov 官方 [Skilled Worker sponsor list](https://www.gov.uk/government/publications/register-of-licensed-sponsors-workers) 必查
- [ ] **NL**：[IamExpat jobs](https://www.iamexpat.nl/career) / [Relocate.me](https://relocate.me) / [Honeypot](https://honeypot.io)
- [ ] **跨地区**：LinkedIn + [Ladder](https://www.lifeatladder.com)
- [ ] **目标公司清单**（按 sponsorship 友好度排序）
  - **NL**：Adyen、Booking.com、ING、Bol.com、Picnic、ASML、Mollie、TomTom、Albert Heijn / AH、Just Eat Takeaway
  - **UK**：Wise、Revolut、Monzo、Cloudflare London、AWS London、Spotify London、Deliveroo、Bumble、ARM、ASOS、Trainline
  - 每周投 10-15 家（每家都改 cover letter，提对应技术栈关键词）

### 3.4 面试节奏（典型 EU mid-level DE 流程）

```
1. 招聘官 30min（语速、签证、薪资期望、为什么转 DE）
2. Technical screen 60min（SQL + Python + 简历项目深挖）
3. Take-home 4-8h（dbt 项目 / Spark 性能调优 / 系统设计文档）— 一定要在 deadline 内交，code review 比纯做题更被看重
4. Onsite / Virtual loop 4-5 轮：
   - System design 1h
   - Coding 1h
   - Behavioral 45min（hiring manager）
   - Domain deep dive 1h（你简历项目）
   - Bar raiser / culture fit 30min
5. Offer + 谈判（薪资上下浮动 10-15% 是 EU 的常态）
```

### 3.5 谈判

- [ ] 永远不先报数。问"What's the budget for this role?"
- [ ] 多个 offer 同步推进（用 hiring manager 做对方时间表压力）
- [ ] **签证条款写进合同**：legal cost 谁付、relocation package、family visa（你有家属的话）
- [ ] NL 30% ruling（前 5 年 30% 收入免税）：开口问 HR 是否符合资格、谁负责申请

---

## 4. 自检清单（每周日花 30 分钟自评）

每周自问：
- [ ] 这周新增了哪些**面试可讲**的具体细节？（不是"学了 dbt"而是"在 download_channel 上跑通了 incremental merge，记下了 X 行/分钟"）
- [ ] 这周面试题练习多少道？SQL / Python / System Design 各几道？
- [ ] 本周投了几家公司？回复率多少？
- [ ] 这周有没有跟一个真人聊过 DE 话题？（mock interview / 朋友 / LinkedIn 陌生人）
- [ ] 健康：睡眠够不够、有没有运动 3 次以上？（**面试季垮掉的人 80% 是身体先崩**）

---

## 5. 6 周加速版（如果时间紧）

如果你想 6 周搞定不是 12 周：

| 周 | 重点 | 砍掉的 |
|---|---|---|
| 1 | SQL + 数据建模 | 不做 Hard 题 |
| 2 | dbt（最重要的关键词补齐） | — |
| 3 | Spark + Glue 性能 + download_channel 部署跑数 | — |
| 4 | Airflow + 简历定稿 + LinkedIn 改 | 不补 Flink，简单提一下即可 |
| 5 | System design 5 题 + STAR stories | 不深挖 lakehouse 三家对比 |
| 6 | Mock interview 4-6 次 + 开始投递 | — |

---

## 6. 心态

- 你 6+ 年 QA 在 3 个全球大厂 + 1 段 NVIDIA infra 自建 + 2 个深度 portfolio。**这不是没经验、没项目的求职者**。
- Mid-level DE offer 不缺。**主要瓶颈是签证和讲故事，不是技术**。
- QA 出身不是减分项。在数据契约 / 测试驱动开发 / 数据可观测性 / SLA 这些当下最热的细分里，QA 出身**反而是优势**，前提是你会包装。
- 不要为了简历好看再造项目。**把现有 4 个吃透 + 补 dbt/Airflow 关键词 + 把故事讲圆 = 够了。**

---

**下一步**：把这份 TODOLIST 打印出来贴墙上，每周日划掉一行。3 个月后回头看，你会发现你已经过了。
