# Download Channel ETL — PLAN.md

> 本文件只设计 **ETL 数据通路**：Data.ai 数据科学团队定期把 Download Channel 数据落到上游 S3 桶，我们用 AWS Glue 批量清洗落到 Bronze S3，再用 Glue 做窄→宽 pivot 落到 Silver S3，再由 Snowpipe 导入 Snowflake，在 Snowflake 内做 Gold 预聚合。**先把流程定死，本文件不写任何代码。**
>
> 参考实现：`iodp/iodp-bigdata`（同事的项目；同样是 Bronze/Silver/Gold Medallion，但 iodp 用 Kafka MSK + Glue Streaming + S3 Iceberg + Athena，我们这里 Bronze/Silver 都在 S3 + Glue，Gold 换成 Snowflake Dynamic Table）。

---

## 1. 项目背景

- **数据来源**：Data.ai（前 App Annie）的 *Download Channel Report*。该报告把每个 App 的下载量按"通道"切分，从 2022 年 11 月那次大版本开始，通道被建模为 **Featured x Paid 两个维度的 2x2 矩阵**，四象限 MECE（无兜底 `other`）：
  - **维度 1（下载从何而来，一级汇总维度）**：**Featured**（商店主动推荐：编辑精选、榜单、推广位等店内推荐位） vs **Organic**（用户主动发现：搜索、分类浏览、直达等）
  - **维度 2（是否有付费归因，二级维度）**：Paid（归因到付费投放） vs Unpaid（自然到达）
  - 四象限叶子：`paid_featured` / `unpaid_featured` / `paid_organic` / `unpaid_organic`
  - 汇总等式（**下游对外主口径 = featured + organic**）：
    - `downloads_featured = downloads_paid_featured + downloads_unpaid_featured`
    - `downloads_organic  = downloads_paid_organic  + downloads_unpaid_organic`
    - `downloads_total    = downloads_featured      + downloads_organic`
  - > 注意"organic"一词在此处统一指"**非 featured 的用户主动发现流量**"（维度 1 的一侧），不再指"非 paid"。历史上的"Organic = 非 Paid"语义在本项目里用 `unpaid_*` 表达，避免歧义。
- **数据量级**：几百亿行级别。单日新增可达数亿行（~1M apps x 200 countries x 4 devices），单日原始文件几十 GB。**Lambda 扛不住此量级，故选 AWS Glue（Spark 分布式）作为 ETL 引擎。**
- **上游交付方式**：Data.ai 数据科学团队（甲方/合作方）定期把数据科学加工后的文件 **PUT 到一个上游 S3 桶**（命名建议：`dataai-dropzone-<env>-<acct>`）。文件命名按日期分区。
- **下游消费方**：BI / 业务方查 Gold 层 Snowflake 视图。
- **本项目范围**：**只管 ETL** —— 上游 S3 → Bronze S3 → Silver S3 → Snowflake Gold。**不含**：业务前端、Agent、报表平台、上游数据科学。

## 2. 数据来源 & 上游约束

| 项 | 取值 / 假设 |
|---|---|
| 上游桶 | `dataai-dropzone-<env>-<acct>`（外部团队 PUT，我们只读） |
| 文件格式 | 上游可能是 **gzip CSV** 或 **Parquet**（待 Data.ai 同事确认；Bronze 落 Parquet） |
| 分区 | 按日期：`s3://…/download_channel/dt=YYYY-MM-DD/<schema_version>/part-*.csv.gz` |
| 到达节奏 | 每天一批，但 **trailing 7 天会被 restate**（preview → finalized）。Data.ai API 文档里写"Weekly finalized Tue 8am PT"，我们的 Glue Job 必须做幂等覆盖写入 |
| Schema 版本 | **窄表**（dropzone 当前唯一上传格式）。早期设计预留过 `wide/` 上传分支，但实际从未启用，已下线。Silver 层做 narrow → wide pivot |
| 无 PII | 通道下载量是聚合数据，不含用户级 PII；但 `product_id`（App 唯一 ID）按合同保密，不出 AWS 账号 |

## 3. 架构总览

```
┌────────────────────┐
│  Data.ai 数据科学  │
│   (外部团队)       │
│  S3: dropzone/     │
│  download_channel/ │
└────────┬───────────┘
         │ PutObject (每天一批)
         ▼
┌──────────────────────────────────────────────────────────┐
│  AWS Glue Workflow（每日定时 / 手动触发）                 │
│                                                          │
│  ┌─────────────────────┐    ┌─────────────────────┐     │
│  │  Bronze Glue Job    │───▶│  Silver Glue Job    │     │
│  │  · schema 校验      │    │  · 窄→宽 pivot      │     │
│  │  · 类型规范化       │    │  · DQ 卡点           │     │
│  │  · 去重             │    │  · 统一宽表 schema   │     │
│  │  · 写 Bronze Parquet│    │  · 写 Silver Parquet │     │
│  └──────┬──────────────┘    └──────┬──────────────┘     │
│         │                          │                     │
│         ▼                          ▼                     │
│  ┌──────────────┐           ┌──────────────┐            │
│  │  DynamoDB    │           │  DLQ S3      │            │
│  │  checkpoint  │           │  (坏文件 +   │            │
│  │  + 运行锁    │           │   错误 json) │            │
│  └──────────────┘           └──────────────┘            │
└──────────────────────────────────────────────────────────┘
         │                          │
         ▼                          ▼
┌──────────────────┐         ┌──────────────────┐
│  Bronze S3       │         │  Silver S3       │
│  (原始 Parquet,  │         │  (统一宽表       │
│   保留窄/宽)     │         │   Parquet)       │
│  + Glue Catalog  │         │  + Glue Catalog  │
│  + Athena ad-hoc │         │                  │
└──────────────────┘         └────────┬─────────┘
                                      │ s3:ObjectCreated → SNS
                                      ▼
                             ┌──────────────────┐
                             │  Snowpipe        │
                             │  (1 条 Pipe,     │
                             │   AUTO_INGEST)   │
                             └────────┬─────────┘
                                      │ COPY INTO
                                      ▼
                             ┌──────────────────┐
                             │ Snowflake SILVER │
                             │  DC_WIDE (1 张表)│
                             └────────┬─────────┘
                                      │ Dynamic Table (增量)
                                      ▼
                             ┌──────────────────┐
                             │ Snowflake GOLD   │
                             │  DC_DAILY_BY_APP │
                             │  DC_DAILY_BY_CTY │
                             │  DC_TREND        │
                             └────────┬─────────┘
                                      │ GRANT SELECT
                                      ▼
                                 业务方 / BI
```

- **Bronze（S3 Parquet）**：Glue Bronze Job 清洗后写入，**保留原始 schema**（窄表路径 `narrow/`）。Glue Catalog 注册，Athena 可即席查（故障排查用）。
- **Silver（S3 Parquet）**：Glue Silver Job 做窄→宽 pivot + DQ 校验，**统一为宽表 schema** 输出。所有数据到这一层都是同一套列。
- **Snowflake Silver 表**：Snowpipe 从 Silver S3 自动 `COPY INTO`，只有 **1 张表 `DC_WIDE`**、**1 条 Pipe**。
- **Gold（Snowflake Dynamic Tables）**：基于 Silver 表的预聚合，Snowflake 内置增量 + 自动调度。

## 4. 数据流向（逐跳）

### 4.1 上游 → Glue Bronze Job → Bronze S3（**Bronze 化**）

- **触发器**：EventBridge 定时规则，每日 **UTC 10:00**（Data.ai 通常 PT 凌晨交付，留几小时 buffer）触发 Glue Workflow。也支持手动触发（Console / CLI / Makefile）。
- **Glue Bronze Job 职责**（把"乱"的 raw 变成"齐整"的 Bronze）：
  1. **查 DynamoDB checkpoint**：读取上次处理到哪个 `(dt, store)` 分区、文件 MD5，确定本次需要处理的增量文件 + restate 文件
  2. 读 dropzone：支持 gzip-csv 和 parquet 两种输入
  3. **Schema 校验**：对照 [§5/§6](#5-数据模型--窄表bronze-层保留原始格式) 的字段表逐列校验。失败文件 → DLQ
  4. **类型规范化**：日期 → DATE、数值 → DOUBLE、字符串 trim
  5. **去重**：以 (dt, product_id, country, device, channel) 为 key（窄表）或 (dt, product_id, country, device) 为 key（宽表）
  6. **写 Bronze**：Parquet snappy，路径见 [§8](#8-s3-目录布局)
  7. **更新 DynamoDB checkpoint**：记录 input_files, output_partition, in_count, out_count, dlq_count, file_md5, job_run_id
- **幂等性（Restate 处理）**：trailing 7 天会被 Data.ai restate。Bronze Job 检测到同一 `(dt, store)` 分区有新文件（MD5 变化）→ **覆盖写** Bronze（先 DELETE 旧 parquet 再写新文件）。DynamoDB checkpoint 记录每个分区的最后处理时间 + 文件 MD5，用于判断是否需要重处理。

### 4.2 Bronze S3 → Glue Silver Job → Silver S3（**Silver 化**）

- **触发**：由 Glue Workflow 串联，Bronze Job 成功后自动触发 Silver Job。
- **Silver Job 职责**（把 Bronze 的窄表 pivot 成统一宽表 schema）：
  1. 读 Bronze 窄表 → pivot 成宽表格式：
     ```
     GROUP BY (dt, product_id, app_store, country, device)
     SUM(IFF(channel IN ('paid_featured','unpaid_featured'), downloads, 0)) AS downloads_featured
     SUM(IFF(channel IN ('paid_organic', 'unpaid_organic'),  downloads, 0)) AS downloads_organic
     ...（四象限叶子列同理）
     downloads_total = downloads_featured + downloads_organic
     ```
  2. **DQ 卡点**（见 [§12](#12-dq-卡点)）：通过 → 写 Silver；不通过 → 写 DLQ + 告警，不写 Silver
  3. **写 Silver**：统一宽表 Parquet，路径 `s3://silver-bucket/download_channel/dt=YYYY-MM-DD/store=<store>/*.parquet`
  4. **Restate 覆盖**：与 Bronze 同理，先 DELETE 对应分区再写

### 4.3 Silver S3 → Snowpipe → Snowflake Silver（**Snowflake 化**）

- **机制**：**Snowpipe + S3 Event Notification (SNS)**。
  - Silver 桶配置 `s3:ObjectCreated:*` → SNS Topic
  - Snowpipe 订阅该 Topic，自动 `COPY INTO SILVER.DC_WIDE`
  - **只有 1 条 Pipe**（`PIPE_DC_WIDE`），因为 Silver 层已经统一为宽表 schema
- **Snowflake Silver 表**：§6 宽表列 + `_loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()` 供下游做 CDC
- **Restate 处理**：Snowpipe `COPY INTO` 是追加语义。对于 restate 的分区，Silver Glue Job 写新文件前已 DELETE 旧文件，Snowpipe 会加载新文件。Snowflake 侧需要一个定期 `MERGE` 或 `DELETE + INSERT` 机制来处理重复数据（按主键 `(dt, product_id, app_store, country, device)` 去重），可用 **Snowflake Task** 每日执行一次去重。

### 4.4 Snowflake Silver → Gold（**预聚合化**）

- **机制**：**Snowflake Dynamic Tables**，`TARGET_LAG = '15 minutes'`，`WAREHOUSE = COMPUTE_WH_DC`。
  - 自动按 Silver 表的 CDC 流增量重算
  - 不需要外部 Airflow / Glue Trigger
- **每张 Gold 表**：见 [§9](#9-snowflake-对象布局)。

## 5. 数据模型 — 窄表（Bronze 层保留原始格式）

> dropzone 唯一上传格式。每行一个 (date, app, country, device, channel) 的下载量。**EAV 风格**，扩展性好但聚合时要 PIVOT。Bronze 层保留原样；Silver Glue Job 负责 pivot 成宽表。

| 列 | 类型 | 必填 | 说明 |
|---|---|---|---|
| dt | DATE | Yes | 日期分区键（UTC） |
| product_id | NUMBER(38,0) | Yes | Data.ai 统一 App ID（DNA-unified） |
| app_store | VARCHAR(16) | Yes | `ios` \| `google-play` |
| country | CHAR(2) | Yes | ISO 3166-1 alpha-2 |
| device | VARCHAR(32) | Yes | `iphone` \| `ipad` \| `android-phone` \| `android-tablet` |
| channel | VARCHAR(32) | Yes | 四象限：`paid_featured` \| `paid_organic` \| `unpaid_featured` \| `unpaid_organic` |
| downloads | NUMBER(38,0) | Yes | 估计下载量 |
| share_pct | NUMBER(6,4) |   | 该 channel 占当日该 (app,country,device) 的占比 |
| is_estimate_final | BOOLEAN |   | 是否已 finalize（preview 期间为 FALSE） |
| ingest_ts | TIMESTAMP_NTZ | Yes | Glue Job 写入时间（Bronze 必填） |

**主键（逻辑）**：`(dt, product_id, app_store, country, device, channel)` —— Glue 去重就用这套。

**S3 Bronze 分区路径**：
```
s3://iodp-dc-bronze-<env>-<acct>/download_channel/narrow/
    dt=2026-04-25/
    store=ios/
    part-00000.snappy.parquet
```

## 6. 数据模型 — 宽表（Silver 层 + Snowflake 统一格式）

> Silver 层和 Snowflake 的统一 schema。每行一个 (date, app, country, device)，channel 被 PIVOT 成多列。由 Silver Glue Job 从 Bronze 窄表 pivot 而来。

| 列 | 类型 | 必填 | 说明 |
|---|---|---|---|
| dt | DATE | Yes | 日期分区键 |
| product_id | NUMBER(38,0) | Yes |  |
| app_store | VARCHAR(16) | Yes |  |
| country | CHAR(2) | Yes |  |
| device | VARCHAR(32) | Yes |  |
| downloads_total | NUMBER(38,0) | Yes | = downloads_featured + downloads_organic |
| downloads_featured | NUMBER(38,0) | Yes | = downloads_paid_featured + downloads_unpaid_featured（商店主动推荐流量） |
| downloads_organic | NUMBER(38,0) | Yes | = downloads_paid_organic + downloads_unpaid_organic（用户主动发现流量） |
| downloads_paid_featured | NUMBER(38,0) |   | 付费 x 编辑精选/榜单/推广位 |
| downloads_paid_organic | NUMBER(38,0) |   | 付费 x 搜索/浏览/直达等用户发起路径 |
| downloads_unpaid_featured | NUMBER(38,0) |   | 非付费 x 编辑精选/榜单/推广位 |
| downloads_unpaid_organic | NUMBER(38,0) |   | 非付费 x 搜索/浏览/直达等用户发起路径 |
| paid_share | NUMBER(6,4) |   | (downloads_paid_featured + downloads_paid_organic) / downloads_total |
| featured_share | NUMBER(6,4) |   | downloads_featured / downloads_total |
| is_estimate_final | BOOLEAN |   |  |
| ingest_ts | TIMESTAMP_NTZ | Yes |  |

**主键（逻辑）**：`(dt, product_id, app_store, country, device)`。

**S3 Silver 分区路径**（统一宽表）：
```
s3://iodp-dc-silver-<env>-<acct>/download_channel/
    dt=2026-04-25/
    store=google-play/
    part-00000.snappy.parquet
```

> ⚠️ **本节列名属"基于公开方法论 + 用户描述的合理还原"**。Data.ai helpcenter 详细页面是登录态 gated，公开搜索引擎只能看到 paid/organic/featured 三档的描述。落地前需要从登录态 helpcenter 拿一份原始 schema 校对一遍，把列名对齐到 Data.ai 官方字段名。**这是开工前必须做的一件事**。

## 7. 兼容策略

- **窄 → 宽 pivot 在 Silver 层完成**：Bronze 保留 dropzone 原样的窄表 Parquet，Silver Glue Job 做 pivot。下游业务方完全不感知 EAV 风格的窄表。
- **对外只通过 Snowflake `SILVER.DC_WIDE`**：Gold 层 Dynamic Table 全部基于这张表。
- **历史背景**：早期设计预留过"上游也可能直接给宽表"的分支（dropzone `wide/` 前缀 + Bronze `wide/` 旁路），但实际从未启用，已下线。如果未来 Data.ai 又回到双格式上传，Silver pivot 逻辑可以保留，再加一条旁路即可。

## 8. S3 目录布局

```
s3://dataai-dropzone-<env>-<acct>/                ← 上游 PUT，我们只读
    download_channel/
        narrow/
            dt=YYYY-MM-DD/store=<store>/*.csv.gz

s3://iodp-dc-bronze-<env>-<acct>/                 ← Glue Bronze Job 写
    download_channel/
        narrow/dt=YYYY-MM-DD/store=<store>/*.parquet   ← 窄表原样
    dead_letter/                                   ← 解析/DQ 失败的原文件 + 错误 json
        YYYY-MM-DD/<original-key>
        YYYY-MM-DD/<original-key>.error.json
    athena-results/                                ← Athena 查询临时输出

s3://iodp-dc-silver-<env>-<acct>/                 ← Glue Silver Job 写
    download_channel/
        dt=YYYY-MM-DD/store=<store>/*.parquet      ← 统一宽表

s3://iodp-dc-scripts-<env>-<acct>/                ← Glue 脚本
    glue/bronze_etl.py
    glue/silver_etl.py
    glue/dlq_replay.py
```

生命周期（参考 iodp）：
- Bronze：30 天 → STANDARD_IA，90 天 → GLACIER_IR，365 天删
- Silver：30 天 → STANDARD_IA，90 天 → GLACIER_IR，365 天删
- dead_letter/：30 天删
- Dropzone：不动（外部桶，由 Data.ai 团队维护）
- Scripts：无生命周期

## 9. Snowflake 对象布局

```
SNOWFLAKE_ACCOUNT
└── DATABASE  IODP_DC_<ENV>
    ├── SCHEMA  RAW_STAGE                     ← 仅放 EXTERNAL STAGE / FILE FORMAT / PIPE
    │   ├── STAGE        SILVER_S3_STAGE      (URL = s3://iodp-dc-silver-<env>-<acct>/, STORAGE_INTEGRATION = …)
    │   ├── FILE FORMAT  PARQUET_FF
    │   └── PIPE         PIPE_DC_WIDE         (AUTO_INGEST=TRUE, COPY INTO SILVER.DC_WIDE)
    │
    ├── SCHEMA  SILVER
    │   └── TABLE  DC_WIDE                    (§6 列表 + _loaded_at)
    │
    └── SCHEMA  GOLD
        ├── DYNAMIC TABLE  DC_DAILY_BY_APP            TARGET_LAG=15min
        ├── DYNAMIC TABLE  DC_DAILY_BY_COUNTRY        TARGET_LAG=15min
        ├── DYNAMIC TABLE  DC_PAID_VS_ORGANIC_TREND   TARGET_LAG=1hour
        └── (业务方 GRANT SELECT 只到这里)

WAREHOUSE  COMPUTE_WH_DC_<ENV>                ← XS / 自动暂停 60s / MAX_CLUSTER_COUNT=1
ROLE       IODP_DC_LOAD_<ENV>                 ← Snowpipe 用，仅 INSERT 权限
ROLE       IODP_DC_TRANSFORM_<ENV>            ← Dynamic Table 重算用
ROLE       IODP_DC_READER_<ENV>               ← 业务方 GRANT 给 BI / 下游
STORAGE INTEGRATION  IODP_DC_S3_INT_<ENV>     ← AWS IAM Role 信任 Snowflake AWS 账号
TASK       IODP_DC_DEDUP_<ENV>                ← 每日去重 task（处理 Snowpipe 追加 + restate 导致的重复）
```

## 10. DynamoDB Checkpoint 表设计

**表名**：`iodp-dc-checkpoint-<env>`

| 属性 | 类型 | 键 | 说明 |
|---|---|---|---|
| partition_key | String | PK | `<layer>#<dt>#<store>` 例：`bronze#2026-04-25#ios` |
| status | String |  | `running` \| `succeeded` \| `failed` |
| last_processed_at | String |  | ISO 8601 时间戳 |
| input_files | List\<String\> |  | 处理的源文件 S3 key 列表 |
| file_md5s | Map\<String,String\> |  | 文件名 → MD5，用于检测 restate |
| in_count | Number |  | 输入行数 |
| out_count | Number |  | 输出行数 |
| dlq_count | Number |  | DLQ 行数 |
| job_run_id | String |  | Glue Job Run ID |
| lock_expires_at | String |  | 运行锁过期时间（当前时间 + 2h） |

**并发保护机制**：
- Glue Job 启动时先对目标分区写 `status=running` + `lock_expires_at=now()+2h`，用 DynamoDB **条件写**（`attribute_not_exists(status) OR status <> 'running' OR lock_expires_at < :now`）
- 如果条件写失败（说明上一轮还在跑且锁未过期）→ 跳过该分区 + 发告警
- Job 完成时更新 `status=succeeded/failed`，释放锁
- 锁超时 2 小时：防止 Job 崩溃后永久死锁

## 11. DLQ（死信通道）+ 重放机制

### 11.1 DLQ 写入

失败类型分两级：
- **可重试**（网络抖动、S3 throttle、临时性错误）→ Glue Job 内置 retry 3 次，不进 DLQ
- **不可重试**（schema 不匹配、数据损坏、DQ 不通过）→ 写 DLQ：
  ```
  s3://iodp-dc-bronze-<env>-<acct>/dead_letter/
      2026-04-25/original-key.csv.gz          ← 原文件拷贝
      2026-04-25/original-key.error.json      ← {error_type, message, stack_trace, timestamp, job_run_id, source_file}
  ```

### 11.2 人工检查

- **每周一次自动汇总**：EventBridge 定时（每周一 UTC 09:00）→ Lambda（轻量，只做统计）→ 扫描 `dead_letter/` 前缀 → SNS 邮件通知 oncall，内容包含：
  - 过去 7 天 DLQ 文件数
  - 按错误类型分组统计
  - 文件列表（前 20 条）
- **即时告警**：DLQ 文件数 > 0 时，Glue Job 结束后立即发 SNS 通知（不等周报）

### 11.3 重放

- **重放 Glue Job**（`glue/dlq_replay.py`）：读 DLQ 文件，移回 dropzone 对应路径结构，然后触发正常的 Bronze → Silver Workflow
- 触发方式：`make dlq-replay DATE=2026-04-25`（重放指定日期）或 `make dlq-replay-all`（重放全部）
- 重放前人工确认：检查 `.error.json` 确认问题已修复（如上游修了 schema），再执行重放

## 12. DQ 卡点（数据质量校验）

在 Silver Glue Job **写 Silver 之前**执行，不通过则该分区不写 Silver，写 DLQ + 告警。

| # | 检查项 | 阈值 | 动作 |
|---|---|---|---|
| 1 | **行数对比**：Bronze 写入 count（DynamoDB 记录）vs Silver Job 读到的 count | 差异 > 1% | 阻断 + 告警 |
| 2 | **关键列空值率**：`product_id`、`dt`、`country`、`app_store` | NULL > 0.1% | 阻断 + 告警 |
| 3 | **日期范围校验**：文件分区声称 `dt=2026-04-25` 但数据里出现超出 ±7 天的日期 | 超出行 > 0 | 阻断 + 告警 |
| 4 | **数值范围**：`downloads_*` 列出现负数 | 负数行 > 0 | 告警（不阻断，标记异常） |
| 5 | **等式校验**：`downloads_total != downloads_featured + downloads_organic` | 差异行 > 0.1% | 告警（不阻断，标记异常） |

## 13. Trigger 设计（调度策略）

### 13.1 日常定时

| 项 | 配置 |
|---|---|
| 机制 | **EventBridge Scheduled Rule** → 触发 Glue Workflow |
| 频率 | 每日 1 次，**UTC 10:00**（≈ PT 03:00，Data.ai 通常 PT 凌晨交付，留几小时 buffer） |
| Workflow | `dc-etl-workflow-<env>`：Bronze Job → (成功) → Silver Job |

### 13.2 手动触发

| 方式 | 命令 |
|---|---|
| Makefile | `make run-etl ENV=dev` （触发完整 Workflow） |
| | `make run-bronze ENV=dev DT=2026-04-25` （只跑 Bronze，指定日期） |
| | `make run-silver ENV=dev DT=2026-04-25` （只跑 Silver，指定日期） |
| AWS Console | Glue Console → Workflows → Run |
| AWS CLI | `aws glue start-workflow-run --name dc-etl-workflow-<env>` |

### 13.3 Backfill（历史数据批量灌入）

- `make backfill START=2026-01-01 END=2026-04-25 ENV=dev`
- 内部实现：循环按日期调用 Glue Job，DynamoDB checkpoint 防重复
- Glue Job 参数 `--backfill-mode=true`：跳过增量检查，强制处理指定日期范围
- 建议 backfill 时调大 DPU（如 20 DPU），日常跑用 10 DPU

## 14. 监控告警

| 告警 | 条件 | 机制 | 通知 |
|---|---|---|---|
| **Glue Job 失败** | 任一 Job 状态 = FAILED | CloudWatch Alarm on Glue metric | SNS → oncall 邮件/Slack |
| **DLQ 新增** | `dead_letter/` 下有新文件 | Glue Job 结束时检查 dlq_count > 0 | SNS → oncall |
| **DLQ 周报** | 每周一汇总 | EventBridge → Lambda → SNS | oncall 邮件 |
| **Snowpipe 延迟** | 当日 (UTC) `COPY_HISTORY` 没有 `PIPE_DC_WIDE` 的 COPY 记录；alert 每天 UTC 13:00 跑一次 | Snowflake Alert | 邮件 |
| **DynamoDB 锁超时** | `status=running` 且 `lock_expires_at` < 当前时间 | CloudWatch Alarm（自定义 metric，由 Glue Job 上报） | SNS → oncall |
| **数据缺失** | 某天预期 dropzone 有文件但 Bronze 为空 | EventBridge → Lambda 检查 → SNS | oncall |
| **DQ 告警** | §12 任一检查不通过 | Glue Job 内 → SNS | oncall |

## 15. 成本护栏

| 项 | 配置 | 原因 |
|---|---|---|
| Glue MaxDPU | 日常 **10 DPU**，backfill **20 DPU** | 防误配开太多 DPU |
| Glue Timeout | **120 分钟** | 防 Job 无限挂起 |
| Snowflake Warehouse | `AUTO_SUSPEND = 60`，`MAX_CLUSTER_COUNT = 1`，Size = **XS** | 只服务 Dynamic Table 重算，不需要大规格 |
| Snowpipe | 按文件计费，Silver 层文件数已合并，无小文件问题 | - |
| S3 生命周期 | Bronze/Silver 30d → IA，90d → GIR，365d 删 | 控制存储成本 |
| DynamoDB | On-Demand 计费（日写入量极低，几十次/天） | 不需要预置容量 |

## 16. Terraform 模块切分

参考 iodp，把 Lambda 模块换成 Glue + DynamoDB：

```
download_channel/
└── terraform/
    ├── backend.tf
    ├── main.tf                              ← 调度所有 module
    ├── variables.tf                         ← aws_region, environment, snowflake_account, snowflake_user, …
    ├── outputs.tf
    ├── locals.tf                            ← mandatory_tags
    ├── environments/
    │   ├── dev.tfvars
    │   └── prod.tfvars
    │
    └── modules/
        ├── networking/                      ← 复用 iodp（VPC + private subnet）
        │
        ├── storage/                         ← S3：bronze + silver + scripts + dlq
        │     main.tf  outputs.tf  variables.tf
        │
        ├── dynamodb/                        ← checkpoint 表
        │     main.tf  → iodp-dc-checkpoint-<env> (On-Demand)
        │
        ├── glue_catalog/                    ← Bronze + Silver Glue DB + 表注册
        │
        ├── glue_etl/                        ← Bronze Job + Silver Job + Workflow + EventBridge 定时触发
        │     main.tf  → IAM Role + 2 Jobs + Workflow + Trigger
        │     依赖：dropzone bucket ARN + bronze bucket + silver bucket + DynamoDB table
        │
        ├── glue_dlq_replay/                 ← DLQ 重放 Job
        │
        ├── snowflake/                       ← provider "snowflake" + database/schema/role/wh/integration
        │     main.tf  → IODP_DC_<ENV> + RAW_STAGE / SILVER / GOLD schema + WH + Task(dedup)
        │
        ├── snowpipe/                        ← S3→Snowflake 的 PIPE 与 SNS / SQS 桥接
        │     main.tf  → SNS Topic on silver bucket + Storage Integration + 1 个 PIPE + IAM 信任
        │
        ├── gold_dynamic_tables/             ← 在 Snowflake 里建 3 张 Dynamic Table
        │
        └── observability/                   ← CloudWatch Alarm + SNS Topic + Snowflake Alert + DLQ 周报 Lambda
```

> **注**：Snowflake 资源用官方 `Snowflake-Labs/snowflake` Terraform Provider。Storage Integration 的 IAM 信任配置是双向的（Snowflake 账号 ARN <-> AWS Role），必须先 `terraform apply` 一次拿 Snowflake 那边生成的 IAM_USER_ARN，再回 AWS 这边写信任策略 —— 这也是 §19 一键部署需要分两阶段的核心原因。

## 17. ETL 代码 / SQL 文件布局

```
download_channel/
├── glue/
│   ├── bronze_etl.py                       ← Glue Bronze Job 主入口
│   ├── silver_etl.py                       ← Glue Silver Job 主入口（含 pivot + DQ）
│   ├── dlq_replay.py                       ← DLQ 重放 Job
│   ├── lib/
│   │   ├── schema_v1_narrow.py             ← Bronze 窄表 schema 定义
│   │   ├── schema_v2_wide.py               ← Silver 宽表 schema 定义
│   │   ├── dq_checks.py                    ← §12 DQ 卡点实现
│   │   ├── checkpoint.py                   ← DynamoDB checkpoint 读写
│   │   └── dlq.py                          ← 失败写 DLQ
│   └── requirements.txt                    ← pyspark 依赖（Glue 内置大部分）
│
├── lambda/
│   └── dlq_weekly_report/
│       └── handler.py                      ← 每周 DLQ 汇总邮件（轻量 Lambda）
│
├── snowflake_sql/
│   ├── 01_database_schemas.sql
│   │   (Storage Integration 由 Terraform 管，见 modules/snowflake/main.tf)
│   ├── 03_silver_table.sql                 ← DC_WIDE（1 张表）
│   ├── 04_pipe.sql                         ← PIPE_DC_WIDE（1 条 Pipe）
│   ├── 05_gold_dynamic_tables.sql
│   └── 06_dedup_task.sql                   ← 每日去重 Task
│
├── athena_ddl/                             ← Bronze + Silver 在 Glue Catalog 注册（用于 ad-hoc）
│   ├── bronze_dc_narrow.sql
│   └── silver_dc_wide.sql
│
├── scripts/
│   ├── apply_athena_ddl.sh
│   ├── apply_snowflake_sql.sh              ← snowsql 跑 snowflake_sql/*.sql
│   └── upload_glue_scripts.sh              ← 上传 glue/*.py 到 scripts bucket
│
├── tests/
│   ├── test_bronze_etl.py
│   ├── test_silver_etl.py
│   ├── test_dq_checks.py
│   └── fixtures/
│       ├── sample_narrow.csv.gz
│       └── sample_wide.csv.gz
│
├── PLAN.md                                 ← ⬅ 本文件
└── Makefile                                ← §18
```

## 18. Makefile 目标 & 一键部署

参考 [iodp/iodp-bigdata/Makefile](iodp/iodp-bigdata/Makefile)，模仿它的"三阶段 init"思路（infra → ddl → triggers）。

| target | 作用 |
|---|---|
| `make check-tools` | 检查 aws / terraform / docker / snowsql / python3 |
| `make check-aws` | 校验 AWS 凭据 |
| `make check-snowflake` | 校验 SNOWFLAKE_USER / SNOWFLAKE_PASSWORD / SNOWFLAKE_ACCOUNT |
| `make upload-glue-scripts` | 跑 `scripts/upload_glue_scripts.sh`，上传 Glue 脚本到 scripts bucket |
| `make init` | **一键全部署**（见 §19） |
| `make deploy-infra-phase1` | `terraform apply` 但只到 `module.snowflake`（Snowflake DB/Schema/Role/Storage Integration 先建出来，Pipe 还没建） |
| `make apply-snowflake-sql` | 用 snowsql 跑 `snowflake_sql/01_…06_*.sql`（建表 + Pipe + Dynamic Table + Dedup Task） |
| `make deploy-infra-phase2` | `terraform apply` 完整版（Snowpipe + Glue EventBridge 触发启用） |
| `make apply-athena-ddl` | 跑 `apply_athena_ddl.sh`，注册 Bronze + Silver 表到 Glue Catalog |
| `make run-etl ENV=dev` | 手动触发完整 Glue Workflow |
| `make run-bronze ENV=dev DT=2026-04-25` | 手动触发 Bronze Job（指定日期） |
| `make run-silver ENV=dev DT=2026-04-25` | 手动触发 Silver Job（指定日期） |
| `make backfill START=… END=… ENV=dev` | 历史数据批量灌入 |
| `make dlq-review` | 列出 DLQ 中的文件 |
| `make dlq-replay DATE=2026-04-25` | 重放指定日期的 DLQ 文件 |
| `make status` | 显示 terraform output + Glue Job 最近运行状态 + Snowpipe COPY_HISTORY |
| `make destroy` | 反向销毁（§20） |
| `make help` | 帮助 |

## 19. 一键部署执行序（`make init`）

类比 iodp 的 `deploy-infra → deploy-ddl → enable-triggers`。Snowflake Storage Integration 的 IAM 信任是双向耦合，所以分阶段：

```
[1/6] check-tools + check-aws + check-snowflake
[2/6] upload-glue-scripts               → glue/*.py 上传到 scripts bucket
[3/6] deploy-infra-phase1               → terraform apply -target='module.storage' \
                                                          -target='module.networking' \
                                                          -target='module.dynamodb' \
                                                          -target='module.glue_catalog' \
                                                          -target='module.snowflake'
        ▶ 拿到 Snowflake Storage Integration 的 STORAGE_AWS_IAM_USER_ARN（terraform output）
[4/6] apply-snowflake-sql               → snowsql 建 Silver 表 + Pipe + Gold Dynamic Tables + Dedup Task
        ▶ Dynamic Table 此时尚无数据，处于空状态
[5/6] deploy-infra-phase2               → terraform apply（完整）
        ▶ 这一步会:
           - 把 phase1 拿到的 IAM_USER_ARN 写进 Silver 桶的 Storage Integration 信任策略
           - 给 Silver 桶配 SNS 通知 + Snowpipe 订阅
           - 创建 Glue ETL Workflow + Jobs + EventBridge 定时触发
           - 创建 DLQ Replay Job
           - 创建 observability 资源（CloudWatch Alarm + SNS + DLQ 周报 Lambda）
           - 跑 apply-athena-ddl 注册 Bronze + Silver 表
[6/6] 验证：手动触发一次 `make run-etl ENV=dev`
        → 往 dropzone 桶 PUT 一个 fixture 文件
        → Glue Workflow 跑完后 Silver S3 有宽表 Parquet
        → 几分钟内 Snowflake GOLD.DC_DAILY_BY_APP 应可见数据
```

**关键约束**：
- 阶段 3 必须先于阶段 4（Snowflake DB / Schema 不存在就没法建表）
- 阶段 4 必须先于阶段 5（Pipe 引用的目标表必须先存在）
- 阶段 5 之前不能开 Glue 定时触发（否则 Snowpipe 还没配好，数据到了 Silver S3 也进不了 Snowflake）

## 20. 销毁顺序（`make destroy`）

反向：
```
[1/3] disable triggers         （Glue EventBridge Rule Disable + Snowpipe 暂停 + Snowflake Task 暂停）
                                ← 防销毁过程中还有数据在飞
[2/3] terraform destroy         （AWS 侧：Glue / SNS / S3 数据 / Glue Catalog / DynamoDB /
                                  Snowflake Pipe 资源 / Storage Integration 全销）
[3/3] snowsql DROP DATABASE     （Snowflake 侧：手动 DROP DATABASE IODP_DC_<ENV> CASCADE，
                                  Terraform Snowflake provider 默认不级联删表）
```

⚠️ Snowflake 数据是 **真实业务数据**，destroy 前必须 `make snapshot-snowflake` 备份到 Bronze 桶 archive/ 前缀。

## 21. 待确认 / 开放问题（**实施前必须答清楚**）

| # | 问题 | 谁来回答 | 阻塞哪一步 |
|---|---|---|---|
| 1 | Data.ai 给的文件具体是 csv.gz 还是 parquet？分隔符？header 行有几行？ | Data.ai 数据科学同事 | Glue Bronze parser 实现 |
| 2 | 窄表（Bronze）和宽表（Silver）的**确切**列名：特别是要确认 (a) 四象限 `paid_featured / paid_organic / unpaid_featured / unpaid_organic` 的官方英文列名是否就是这样拼写；(b) Data.ai 内部是否还保留 `downloads_organic` 的别名（老 SDK/老报表还在用） | 我 / 业务方账号（登录态 helpcenter） | §5 §6 表头精确化 |
| 3 | Snowflake 账号已经存在么？还是 Terraform 也要建 Account？ | 平台/IT | §16 snowflake module 边界 |
| 4 | dropzone 桶在我们账号还是 Data.ai 账号？如果是对方账号，需要 cross-account read role | Data.ai 同事 / 我 | §16 glue_etl IAM |
| 5 | 历史数据迁移：老数据要不要重新清洗一遍灌入新 Bronze？还是只接新数据？ | 业务方 | 是否需要 backfill |
| 6 | Gold 层除了我列的 3 张 Dynamic Table，业务方还要哪些维度切片？ | 业务方 | §9 GOLD schema |
| 7 | 上游 restate 窗口确切是几天？（API 文档说 weekly Tue 8am PT，但 Download Channel 不一定一致） | Data.ai 同事 | Glue 幂等覆盖窗口 |
| 8 | Glue Job 运行在哪个 VPC / Subnet？是否需要复用 iodp 的网络配置？ | 平台/我 | §16 networking module |
| 9 | Snowflake 侧 restate 去重策略：用 MERGE 还是 DELETE+COPY？是否需要 Stream + Task？ | 我 | §4.3 去重机制 |

## 22. 附录：与 iodp 对照速查

| 维度 | iodp/iodp-bigdata | download_channel（本项目） |
|---|---|---|
| 数据源 | Kafka MSK (Streaming) | S3 PUT (Batch, 每天) |
| 数据量级 | - | **几百亿行**，单日数亿行 |
| Bronze 触发 | Glue Streaming Job 常驻 | **Glue Batch Job + EventBridge 定时** |
| Bronze 存储 | S3 + Iceberg + Athena | S3 + Parquet + Glue Catalog |
| Silver 处理引擎 | Glue Spark Job (cron) | **Glue Spark Job（Workflow 串联）** |
| Silver 存储 | S3 + Iceberg + Athena | **S3 + Parquet + Glue Catalog** |
| Gold 存储 | S3 + Iceberg + Athena | **Snowflake Dynamic Table** |
| Gold 调度 | Glue Trigger (cron 5/15/0 2) | **Snowflake Dynamic Table TARGET_LAG** |
| 元数据 | DynamoDB (DQ + Lineage) | **DynamoDB (Checkpoint + Lineage + 运行锁)** |
| DLQ | S3 + 手动重放 | **S3 + Glue Replay Job + 每周自动汇总** |
| 监控 | CloudWatch + SNS + OpenSearch indexer | CloudWatch + SNS + Snowflake Alert（暂不上 OpenSearch） |
| Terraform 入口 | `init` = infra → ddl → triggers | `init` = phase1 → snowflake-sql → phase2 |

---

**下一步**：等 §21 的 9 个问题有答案，特别是 **#1 #2 #4** 拍板后，按 §16 / §17 的目录骨架开始写 terraform / glue / sql。**当前阶段不动键盘写代码。**
