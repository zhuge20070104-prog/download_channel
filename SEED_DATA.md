# Seed Data & End-to-End Verification

记录把合成数据灌入 `dropzone` 桶、触发 Bronze → Silver workflow、追到 Snowflake `SILVER.DC_WIDE` 和 Gold Dynamic Tables 的完整路径，作为 deploy 完成后**第一次端到端验证**的剧本。

---

## 0. 前置条件

`make init ENV=dev` 已经全部成功（5 个 phase 都过）。检查方法:

```bash
# 1. Glue 作业都存在
aws glue list-jobs --region ap-southeast-1 \
  --query "JobNames[?contains(@,'iodp-dc')]" --output table

# 应该看到: iodp-dc-bronze-etl-dev / iodp-dc-silver-etl-dev /
#            iodp-dc-dropzone-seeder-dev / iodp-dc-dlq-replay-dev

# 2. Glue Workflow 存在
aws glue get-workflow --name dc-etl-workflow-dev --region ap-southeast-1 \
  --query 'Workflow.Name'

# 3. Snowpipe 在 Snowflake 端 RUNNING
snowsql -q "SELECT SYSTEM\$PIPE_STATUS('IODP_DC_DEV.RAW_STAGE.PIPE_DC_WIDE');"
# 期望: "executionState":"RUNNING"
```

---

## 1. 触发 seed (写 synthetic data 到 dropzone)

```bash
JOB_RUN_ID=$(aws glue start-job-run \
  --job-name "iodp-dc-dropzone-seeder-dev" \
  --region ap-southeast-1 \
  --query JobRunId --output text)
echo "JobRunId=$JOB_RUN_ID"
```

默认参数 (来自 [terraform/modules/dropzone_seeder/main.tf:102-109](terraform/modules/dropzone_seeder/main.tf#L102-L109)):
- `--TARGET_DT=2026-01-01`
- `--TARGET_STORE=ios`
- `--ROW_COUNT=1000` → 实际 **4000 行**（1000 个 (product, country, device) 组 × 4 个 channel）
- `--SCENARIO=clean`

要改参数:
```bash
aws glue start-job-run \
  --job-name "iodp-dc-dropzone-seeder-dev" \
  --region ap-southeast-1 \
  --arguments '{"--TARGET_DT":"2026-05-10","--TARGET_STORE":"google-play","--ROW_COUNT":"500"}'
```

### 等 seed 完成 (Python Shell job, 通常 30-90 秒)

```bash
aws glue get-job-run \
  --job-name "iodp-dc-dropzone-seeder-dev" \
  --run-id "$JOB_RUN_ID" \
  --region ap-southeast-1 \
  --query 'JobRun.{State:JobRunState,Started:StartedOn,Ended:CompletedOn}' \
  --output table
```

`State` = `SUCCEEDED` 才能继续。`FAILED` 看 CloudWatch Logs:
```bash
aws logs tail "/aws-glue/python-jobs/output" --since 5m --region ap-southeast-1
```

### 验证 dropzone 真的写了文件

```bash
DROPZONE=$(grep dropzone_bucket_name terraform/environments/dev.tfvars | sed -E 's/.*"([^"]+)".*/\1/')

aws s3 ls "s3://$DROPZONE/download_channel/narrow/dt=2026-01-01/store=ios/" \
  --recursive --human-readable --region ap-southeast-1
```

应该看到 `seed-<uuid>.parquet`，几十 KB 大小。

---

## 2. 触发 Bronze → Silver workflow

`dropzone_seeder` **不会自动触发** Bronze ETL（设计上手动控制，避免 demo 时误触）。手动启 workflow:

```bash
WORKFLOW_RUN_ID=$(aws glue start-workflow-run \
  --name "dc-etl-workflow-dev" \
  --region ap-southeast-1 \
  --query RunId --output text)
echo "WorkflowRunId=$WORKFLOW_RUN_ID"

# 或者用 Makefile 封装
make run-etl ENV=dev
```

Workflow 链路 ([terraform/modules/glue_etl/main.tf:187-216](terraform/modules/glue_etl/main.tf#L187-L216)):
- `dc-bronze-start-dev` trigger (ON_DEMAND) → `bronze_etl` job
- `dc-silver-after-bronze-dev` trigger (CONDITIONAL: bronze SUCCESS) → `silver_etl` job

### 监控 workflow 进度

```bash
# 整个 workflow 状态
aws glue get-workflow-run \
  --name "dc-etl-workflow-dev" \
  --run-id "$WORKFLOW_RUN_ID" \
  --region ap-southeast-1 \
  --query 'Run.{Status:Status,Stats:Statistics}' \
  --output table

# 单个 job 进度 (Spark job 通常 2-5 分钟)
aws glue get-job-runs --job-name "iodp-dc-bronze-etl-dev" --max-items 1 \
  --region ap-southeast-1 \
  --query 'JobRuns[0].{State:JobRunState,Started:StartedOn,Duration:ExecutionTime}'

aws glue get-job-runs --job-name "iodp-dc-silver-etl-dev" --max-items 1 \
  --region ap-southeast-1 \
  --query 'JobRuns[0].{State:JobRunState,Started:StartedOn,Duration:ExecutionTime}'
```

### 验证 Bronze 写出 Iceberg 表

Bronze 写到 `s3://<bronze>/download_channel/dt=<DT>/`（Iceberg 表，含 `metadata/` 和 `data/`）:

```bash
BRONZE=$(cd terraform && TF_DATA_DIR=~/.terraform-data/download-channel \
  terraform output -raw bronze_bucket_name)

aws s3 ls "s3://$BRONZE/download_channel/dt=2026-01-01/" \
  --recursive --human-readable --region ap-southeast-1 | head -20
```

期望看到 `metadata/<v>.metadata.json` + `data/*.parquet`。

### 验证 Silver 写出 Parquet

```bash
SILVER=$(cd terraform && TF_DATA_DIR=~/.terraform-data/download-channel \
  terraform output -raw silver_bucket_name)

aws s3 ls "s3://$SILVER/download_channel/dt=2026-01-01/" \
  --recursive --human-readable --region ap-southeast-1
```

每个 silver Parquet 文件落地的同一刻会触发 S3 event → SNS → SQS → Snowpipe 读队列 → COPY 进 Snowflake。

---

## 3. 验证 Snowpipe 把 Silver 灌进 Snowflake

Snowpipe 的延迟通常 **30 秒 ~ 2 分钟**（不是 streaming，靠 SQS 轮询）。

```sql
USE DATABASE IODP_DC_DEV;
USE SCHEMA SILVER;

-- Pipe 工作状态
SELECT SYSTEM$PIPE_STATUS('RAW_STAGE.PIPE_DC_WIDE');
-- 看 "pendingFileCount" 是否在下降, "lastIngestedFilePath" 是否更新

-- COPY 历史 (Snowpipe 加载的明细 + 是否报错)
SELECT
  FILE_NAME,
  ROW_COUNT,
  ROW_PARSED,
  ERROR_COUNT,
  STATUS,
  LAST_LOAD_TIME
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME=>'SILVER.DC_WIDE',
  START_TIME=>DATEADD(HOURS, -1, CURRENT_TIMESTAMP())
))
ORDER BY LAST_LOAD_TIME DESC
LIMIT 10;
-- 期望: STATUS='Loaded', ERROR_COUNT=0, ROW_COUNT > 0

-- 直接看表里的数据
SELECT COUNT(*) AS row_count FROM SILVER.DC_WIDE WHERE dt = '2026-01-01';
-- 期望: 1000 (silver 已把 4 channel 行聚合成 1 行宽表)

SELECT * FROM SILVER.DC_WIDE WHERE dt = '2026-01-01' LIMIT 3;
```

### 如果 COPY_HISTORY 是空的

排查顺序:

1. **Silver 真的写了吗** — 上一步 S3 ls 看到文件没？
2. **SQS 队列有消息吗**:
   ```bash
   aws sqs get-queue-attributes \
     --queue-url "$(aws sqs get-queue-url \
       --queue-name iodp-dc-snowpipe-queue-dev \
       --region ap-southeast-1 --query QueueUrl --output text)" \
     --attribute-names ApproximateNumberOfMessages \
     --region ap-southeast-1
   ```
   - `0` → S3 → SNS → SQS 链路没工作（看 silver 桶的 notification config）
   - `>0` 且不下降 → Snowpipe 没在 poll（看 IAM trust policy 还在不在）
3. **Snowpipe DLQ 有积压吗**:
   ```bash
   make snowpipe-dlq-status ENV=dev
   ```
4. **Pipe 自己 paused 了吗**:
   ```sql
   SELECT SYSTEM$PIPE_STATUS('RAW_STAGE.PIPE_DC_WIDE');
   -- 看 "executionState" 是不是 "RUNNING", 不是的话:
   ALTER PIPE RAW_STAGE.PIPE_DC_WIDE RESUME;
   ```

---

## 4. 验证 Gold Dynamic Tables (聚合层)

Gold 是 Dynamic Tables，按 `TARGET_LAG` 自动 refresh，**不是 streaming**。3 张表的 lag ([snowflake_sql/05_gold_dynamic_tables.sql](snowflake_sql/05_gold_dynamic_tables.sql)):
- `DC_DAILY_BY_APP` — TARGET_LAG=1 hour
- `DC_DAILY_BY_COUNTRY` — TARGET_LAG=1 hour
- `DC_PAID_VS_ORGANIC_TREND` — TARGET_LAG=1 hour

第一次 refresh 在 SILVER 数据落地后**最多 1 小时内**发生。要看的话:

```sql
USE SCHEMA GOLD;

-- 强制立即 refresh (不等 lag)
ALTER DYNAMIC TABLE DC_DAILY_BY_APP REFRESH;
ALTER DYNAMIC TABLE DC_DAILY_BY_COUNTRY REFRESH;
ALTER DYNAMIC TABLE DC_PAID_VS_ORGANIC_TREND REFRESH;

-- 看 refresh 历史
SELECT
  NAME,
  STATE,
  REFRESH_START_TIME,
  REFRESH_END_TIME,
  REFRESH_ACTION,
  NUMERIC_VALUE AS rows_inserted
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME_PREFIX=>'IODP_DC_DEV.GOLD.'))
ORDER BY REFRESH_START_TIME DESC
LIMIT 10;
-- 期望: STATE='SUCCEEDED', rows_inserted > 0

-- 看聚合结果
SELECT * FROM DC_DAILY_BY_APP WHERE dt = '2026-01-01' LIMIT 5;
SELECT * FROM DC_DAILY_BY_COUNTRY WHERE dt = '2026-01-01' LIMIT 5;
SELECT * FROM DC_PAID_VS_ORGANIC_TREND ORDER BY dt DESC LIMIT 5;
```

---

## 5. BI 视图层 (跳过 dedup 窗口的最新视图)

`07_bi_view.sql` 创建了 `SILVER.DC_WIDE_LATEST`，给 BI 直查用，**屏蔽 Restate 窗口内的重复行**（dedup task 还没跑时,Snowpipe 可能因 silver 覆盖写造成同 dt 双倍数据）:

```sql
SELECT COUNT(*) FROM SILVER.DC_WIDE_LATEST WHERE dt = '2026-01-01';
-- 应该 = SILVER.DC_WIDE 的去重后行数 (1000)
```

---

## 6. Scenario: 测试 DLQ 路径 (schema_break)

Seed 带个 `--SCENARIO=schema_break` 参数会故意写**缺字段** (`is_estimate_final` 被丢掉) 的 Parquet,触发 Bronze schema-mismatch DLQ:

```bash
aws glue start-job-run \
  --job-name "iodp-dc-dropzone-seeder-dev" \
  --region ap-southeast-1 \
  --arguments '{"--TARGET_DT":"2026-05-11","--TARGET_STORE":"ios","--ROW_COUNT":"100","--SCENARIO":"schema_break"}'

# Bronze 会把这批 record 写到 DLQ 而不是 main 表
make run-etl ENV=dev

# 等 bronze 跑完看 DLQ
make dlq-review ENV=dev
# 期望: s3://<bronze>/dead_letter/failed_at=<TODAY>/<source-uri>/<part>.json
```

回放 DLQ:
```bash
make dlq-replay ENV=dev DATE=2026-05-11
```

注意 `DATE` 是 **failure 发生的那天 (UTC)**, 不是数据的业务 dt — 见 [Makefile:182-185](Makefile#L182-L185) 的备注。

---

## 7. 完整 cleanup (准备下次重跑)

如果想清空数据重新 seed (不动 infra):

```bash
# 1. 清 dropzone / bronze / silver / scripts 不删, 只清数据
aws s3 rm "s3://$DROPZONE/download_channel/" --recursive --region ap-southeast-1
aws s3 rm "s3://$BRONZE/download_channel/"   --recursive --region ap-southeast-1
aws s3 rm "s3://$BRONZE/dead_letter/"        --recursive --region ap-southeast-1
aws s3 rm "s3://$SILVER/download_channel/"   --recursive --region ap-southeast-1

# 2. 清 Snowflake
snowsql -q "
USE DATABASE IODP_DC_DEV;
TRUNCATE TABLE SILVER.DC_WIDE;
ALTER PIPE RAW_STAGE.PIPE_DC_WIDE REFRESH;  -- 让 Snowpipe 重新扫 stage
"

# 3. 清 DynamoDB checkpoint (让 Bronze 把 seed 文件当成 new data)
aws dynamodb scan --table-name iodp-dc-checkpoint-dev \
  --region ap-southeast-1 \
  --query 'Items[*].pk.S' --output text | \
  xargs -I{} aws dynamodb delete-item \
    --table-name iodp-dc-checkpoint-dev --region ap-southeast-1 \
    --key '{"pk":{"S":"{}"}}'
```

---

## 8. 常见踩坑

| 现象 | 可能原因 | 验证方法 |
|---|---|---|
| Bronze job 一直 `FAILED` 没明显原因 | Glue 服务在 region 短暂故障 / DPU 用满 | `aws glue get-job-run --query 'JobRun.ErrorMessage'` |
| Silver 写完但 Snowpipe 没 ingest | silver bucket 的 S3 → SNS notification 没配 / `IODP_DC_S3_INT_DEV` 的 trust policy 漂移 | 看本文 §3 排查表 |
| `COPY_HISTORY` 全是 `LOAD_FAILED` | silver Parquet schema 跟 `SILVER.DC_WIDE` 不匹配 (Glue Silver 改过 schema 但 SQL 没改) | `SELECT FIRST_ERROR_MESSAGE FROM COPY_HISTORY` |
| Gold DT `REFRESH_HISTORY` 一直 `EXECUTING` | warehouse `COMPUTE_WH_DC_DEV` 被另一个 query 卡住 / DT 之间互相引用产生 lag chain | 看 warehouse query history |
| 跑 `make run-etl` 立刻 fail | TF state 上 `dc-etl-workflow-dev` 还没创建 (phase 2 没跑完) | `aws glue get-workflow --name dc-etl-workflow-dev` |

---

## 9. 把这次 seed 用作面试素材的取舍点

- **为什么 seed 不自动触发 Bronze**:  避免 demo 时点错按钮就连锁刷数据，cron schedule 也分开 (`daily_etl` EventBridge 是单独的 trigger,seed 不挂在上面)
- **为什么 silver→Snowpipe 用 SNS+SQS 不用 S3→Lambda**: Snowpipe AUTO_INGEST 原生支持 SQS，省去自己写 Lambda；SNS 同时 fan-out 给可能多个 subscriber（未来加 Athena trigger 用）
- **为什么 Gold 用 Dynamic Tables 不用 Task**: TARGET_LAG=1h 比 cron task 更适合「数据到了就刷」的语义，Snowflake 自己优化增量 vs full refresh
- **为什么 DLQ 路径用 `failed_at=<UTC>` 而不是业务 dt**: 一次 failure 可能跨多个业务 dt 的文件，按 failure 发生日聚合便于 ops 排查；见 DLQ 设计历史 ([commits 5325d09 → 9305514](https://example))
