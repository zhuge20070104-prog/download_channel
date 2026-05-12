# Operations Handbook

download_channel pipeline 的日常运维手册：端到端冒烟测试、按层手动触发、数据验证、问题诊断。**不覆盖部署**——首次部署相关问题见 [DEPLOY-ISSUES.md](DEPLOY-ISSUES.md)。

## 0. AWS Region 默认值

Makefile 里的 `AWS_REGION` 默认从 `terraform/environments/$(ENV).tfvars` 里读 `aws_region` 字段。所以日常命令**不用手敲 `AWS_REGION=...`**，只传 `ENV=dev` 就够。

显式覆盖：`make <target> ENV=dev AWS_REGION=us-west-2`。

---

## 1. 端到端冒烟测试

整条管道：**seed → Bronze → Silver → Snowpipe → SILVER.DC_WIDE**

一条命令搞定（[Makefile](Makefile) 的 `demo` target）：

```bash
make demo ENV=dev DT=2026-05-11
# DT 不传就默认今天 UTC：make demo ENV=dev
```

会做三件事：
1. 触发 `iodp-dc-dropzone-seeder-dev` 生成 1000 groups × 4 channels = 4000 行 narrow Parquet 到 dropzone
2. 轮询 seed JobRunState 直到 SUCCEEDED（失败立刻退）
3. 触发 `dc-etl-workflow-dev` workflow，bronze → silver 串行跑

跑完看：
```bash
make status ENV=dev
```

期望：bronze + silver 都 SUCCEEDED。

---

## 2. 按层手动触发

如果只想跑单层（调试或部分重跑）：

| 任务 | 命令 |
|------|------|
| 仅 seed 写 dropzone | `aws glue start-job-run --job-name iodp-dc-dropzone-seeder-dev --arguments '{"--TARGET_DT":"2026-05-11","--TARGET_STORE":"ios","--ROW_COUNT":"1000","--SCENARIO":"clean"}'` |
| 仅 bronze | `make run-bronze ENV=dev DT=2026-05-11` |
| 仅 silver | `make run-silver ENV=dev DT=2026-05-11` |
| 整个 workflow（bronze → silver 自动串） | `make run-etl ENV=dev` |
| 回填多天 | `make backfill ENV=dev START=2026-04-01 END=2026-04-25` |

**注意**：`run-bronze` 和 `run-silver` 是异步的，**不会互等**。手动顺序触发要么用 `run-etl`（workflow 内置 bronze→silver trigger），要么 silver 之前 polling bronze SUCCEEDED 之后再跑。否则 silver 看到空 bronze 桶就直接 0 行退出。

---

## 3. 验证数据流（每层都能查）

### 3.1 S3 各层数据

```bash
# Dropzone (narrow input)
aws s3 ls s3://dataai-dropzone-dev-165518479671/download_channel/narrow/ --recursive --human-readable

# Bronze (validated narrow + _loaded_at)
aws s3 ls s3://iodp-dc-bronze-dev-165518479671/ --recursive --human-readable

# Silver (pivoted wide)
aws s3 ls s3://iodp-dc-silver-dev-165518479671/ --recursive --human-readable

# DLQ（如果 bronze 或 silver 拒绝了文件）
aws s3 ls s3://iodp-dc-bronze-dev-165518479671/dead_letter/ --recursive
```

### 3.2 Athena 查（bronze/silver 都注册了表）

```sql
SELECT * FROM bronze_dc_narrow LIMIT 10;
SELECT * FROM silver_dc_wide LIMIT 10;
```

### 3.3 Snowflake 查

```sql
USE DATABASE IODP_DC_DEV;

-- Silver 主表（Snowpipe 自动从 Silver S3 拉过来）
SELECT COUNT(*) FROM SILVER.DC_WIDE;        -- demo 跑完应该 1000
SELECT * FROM SILVER.DC_WIDE LIMIT 5;

-- BI 直查视图（屏蔽 restate 窗口内重复行）
SELECT * FROM SILVER.DC_WIDE_LATEST LIMIT 10;

-- Gold 层 Dynamic Tables（自动从 SILVER 刷新）
SELECT * FROM GOLD.DC_DAILY_BY_APP LIMIT 10;
SELECT * FROM GOLD.DC_DAILY_BY_COUNTRY LIMIT 10;
SELECT * FROM GOLD.DC_PAID_VS_ORGANIC_TREND LIMIT 10;
```

---

## 4. 诊断 Glue Job 失败

### 4.1 看 job 状态 + 错误消息

```bash
# 最近一次 run id（注意：用 --max-results 而非 --max-items，避免输出 NextToken）
RUN_ID=$(aws glue get-job-runs --job-name iodp-dc-bronze-etl-dev --max-results 1 \
  --query 'JobRuns[0].Id' --output text)

# job 终态
aws glue get-job-run --job-name iodp-dc-bronze-etl-dev --run-id "$RUN_ID" \
  --query 'JobRun.{State:JobRunState,Error:ErrorMessage}' --output json
```

### 4.2 拉 Python 脚本输出，过滤 Glue/Spark INFO 噪音

```bash
aws logs get-log-events \
  --log-group-name /aws-glue/jobs/output \
  --log-stream-name "$RUN_ID" \
  --query 'events[*].message' --output text \
  | grep -vE "^\s*(INFO|WARN|26/05|SLF4J|AnalyzerLogHelper|Drools|Resource|SparkContext|MemoryStore|BlockManager|Netty)" \
  | grep -vE "drools|getResourceAsStream|FileUtils" \
  | head -60
```

### 4.3 Bronze/Silver 关键日志标记速查

| 标记 | 含义 | 处理 |
|------|------|------|
| `Bronze ETL starting / Silver ETL starting` | Spark 起来了 | — |
| `Found 0 partitions to process` | 扫不到输入文件 | 检查上游写完没、IAM 能不能 list、cutoff 是否过滤了 |
| `[PROCESS] dt/store — N files` | 正在处理某分区 | — |
| `[SKIP] ... — MD5 unchanged` | checkpoint 认为没新数据 | 用 `BACKFILL_MODE=true` 强制重处理 |
| `[SKIP] ... — Bronze not succeeded` (silver) | bronze checkpoint 不是 succeeded | 重跑 bronze |
| `[LOCKED] ...` | 有别的 job 占锁 | DynamoDB 看是否 stale lock，stale-lock Lambda 会清 |
| `[EMPTY] ... — 0 rows` | spark.read 出 0 行 | 看文件是否真的空 / schema 解析失败 |
| `[DLQ] ...` | schema mismatch，文件移到 dead_letter | 看 `_error.json` 看哪列出问题 |
| `[DQ-BLOCK] ...` (silver) | DQ 卡点拒绝 | 看 `detail` 看哪个 check 失败 |
| Python `Exception` / stack trace | 代码 bug 或运行时错 | 看 traceback 定位 |

### 4.4 strace 故障树

| 症状 | 根因 |
|------|------|
| bronze SUCCEEDED 但 0 输出，`Found 0` | seed 还没写完就触发了 workflow（race）；用 `make demo` 避免，或 polling seed SUCCEEDED 再触发 |
| bronze FAILED，`AccessDenied dynamodb:PutItem on us-east-1` | 跨区域。`--AWS_REGION` 没传给 Glue job，CheckpointManager fallback 到 us-east-1。已修：[bronze_etl.py](glue/bronze_etl.py) `REQUIRED_ARGS` 含 `AWS_REGION` + terraform `default_arguments` 加了它 |
| silver FAILED，`Cannot convert column into bool` | PySpark Column 被当 Python bool 用了（`if col:` 而非 `if col is not None:`）。[dq_checks.py](glue/lib/dq_checks.py) 已修 |
| silver FAILED，`DQ-BLOCK row_count: Expected ~4000, got 1000` | DQ 拿 bronze narrow 行数作 silver wide baseline——错位 4 倍。已修：[silver_etl.py](glue/silver_etl.py) 改用 distinct group key 数 |
| silver `Found 0` 但 bronze 桶有数据 | silver 跑得太早（bronze 还没写完）；用 `make run-etl` 走 workflow，让 trigger 自动串联 |

---

## 5. 诊断 Snowpipe 不 ingest

silver 写出 → S3 PUT → SNS → SQS → Snowpipe COPY → SILVER.DC_WIDE。每个环节都可能断。

### 5.1 看 Pipe 状态

```sql
SELECT SYSTEM$PIPE_STATUS('IODP_DC_DEV.RAW_STAGE.PIPE_DC_WIDE');
```

关键字段：

| 字段 | 期望 / 含义 |
|------|-------------|
| `executionState` | `RUNNING`。`PAUSED` / `STOPPED_FEATURE_DISABLED` 都是问题 |
| `pendingFileCount` | 队列里待 COPY 的文件数。>0 说明在干活，等几秒会变 0 |
| `lastIngestedTimestamp` | 最近成功 COPY 时间。null 说明从未 ingest 过——事件链断了 |
| `notificationChannelName` | Snowflake 自家 SQS ARN（在 `782091841703` account）。SNS 必须订阅这个 |
| `lastForwardedFilePath` / `lastForwardedMessageTime` | 最近收到的事件——null 说明 SNS→SQS 没通 |
| `lastErrorReceivedTime` | 有值说明 COPY 报过错 |

### 5.2 看 COPY 历史

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'IODP_DC_DEV.SILVER.DC_WIDE',
  START_TIME => DATEADD(hour, -2, CURRENT_TIMESTAMP())
));
```

- **完全空** → Snowpipe 根本没尝试 load。事件链断 / storage integration IAM 读不到 silver
- **有 row，STATUS='Loaded'** → 正常 ✓
- **有 row，STATUS='Load failed'** → 看 `FIRST_ERROR_MESSAGE`（schema/IAM/格式问题）

### 5.3 手动 REFRESH（绕过事件链，直接扫文件）

```sql
ALTER PIPE IODP_DC_DEV.RAW_STAGE.PIPE_DC_WIDE REFRESH;
```

扫 stage URL，把 Snowpipe load history 没记录过的文件 enqueue。**已经被 Snowpipe "seen but not loaded" 的文件 REFRESH 不会再 queue**——这种情况要先删 silver 文件再重写，让 Snowflake 当新文件处理。

### 5.4 事件链各段查询

```bash
# A. silver bucket 通知配置正确？（应该看到 SNS 订阅 + prefix=download_channel/）
aws s3api get-bucket-notification-configuration \
  --bucket iodp-dc-silver-dev-165518479671

# B. SNS topic 的订阅列表（必须包含到 Snowflake SQS 的订阅）
aws sns list-subscriptions-by-topic \
  --topic-arn arn:aws:sns:ap-southeast-1:165518479671:iodp-dc-silver-s3-notify-dev

# C. 自己的 Snowpipe DLQ（事件没被 Snowpipe 5 次 ack 会进这里）
make snowpipe-dlq-status ENV=dev
make snowpipe-dlq-peek ENV=dev      # 看一条具体消息
make snowpipe-dlq-redrive ENV=dev   # 重投回主队列
```

### 5.5 常见症状速查

| 症状 | 根因 |
|------|------|
| `executionState=RUNNING, lastIngestedTimestamp=null, pendingFileCount=0` | SNS 没订阅 Snowflake 的 SQS。terraform 已修：[modules/snowpipe/main.tf](terraform/modules/snowpipe/main.tf) 加了 `data "snowflake_pipes"` + 第二条 subscription。重新 `make deploy ENV=dev` 应用 |
| `lastForwardedMessageTime` 有值但 `lastIngestedTimestamp` 老 | Snowpipe 收到事件但 COPY 失败。查 `COPY_HISTORY.FIRST_ERROR_MESSAGE` |
| `lastErrorReceivedTime` 有值 | 同上，COPY 失败 |
| REFRESH 之后 `pendingFileCount` 仍是 0 | 文件已经在 load history 里被记录为"看过"。删文件重写，或换文件名 |

---

## 6. DLQ 处理

### 6.1 Bronze/Silver 数据 DLQ（schema mismatch、DQ 拒绝）

```bash
# 列 DLQ 内容
aws s3 ls s3://iodp-dc-bronze-dev-165518479671/dead_letter/ --recursive

# 看具体 error.json
aws s3 cp s3://iodp-dc-bronze-dev-165518479671/dead_letter/<path>/_error.json -

# 修了上游问题后 replay 失败那天的所有 DLQ
make dlq-replay ENV=dev DATE=2026-04-25   # DATE 是 failed_at=<DATE> 那个分区
```

`DATE` 是**失败发生当天**（UTC），不是数据的 business dt——一个失败日通常含多个 business dt 的失败。

### 6.2 Snowpipe Delivery DLQ（Snowpipe 5 次 ack 失败的事件）

```bash
make snowpipe-dlq-status ENV=dev               # 看消息数
make snowpipe-dlq-peek ENV=dev                 # 看一条消息内容
make snowpipe-dlq-redrive ENV=dev              # AWS-native StartMessageMoveTask 重投
make snowpipe-dlq-redrive-status ENV=dev       # 看 redrive 任务进度
make snowpipe-dlq-redrive-cancel HANDLE=...    # 取消 in-flight redrive
```

**注意**：Snowpipe DLQ 只抓 **delivery 层**失败（Snowpipe 收到事件但 5 次都没 ack）。**COPY 层**失败（schema/parse error）**不在这里**——在 Snowflake 的 `COPY_HISTORY`。

---

## 7. AWS CLI 输出陷阱

| 陷阱 | 现象 | 修法 |
|------|------|------|
| `--max-items 1 --output text` | 输出多一行 `None` (NextToken)，导致后续命令拼接出错 | 用 `--max-results 1` |
| `aws logs tail` | `Invalid choice` | aws CLI 版本太老；用 `aws logs filter-log-events` 或 `get-log-events` |
| `aws logs filter-log-events` 返回 `None` | events 字段不存在（log stream 刚建/正在写） | 改用 `describe-log-streams` 找精确 stream 名，再 `get-log-events` |
