# NEXT — 阅读计划：snowflake_sql + lambda

`glue/` 模块已看完。下一步把 Snowpipe → Snowflake → Gold/BI 的链路 +
旁路监控 Lambda 串起来。原则：**和被监控对象一起看**，不要把 Lambda 全堆到最后。

---

## 阅读顺序

```
0.  现在        lambda/stale_lock_check
                趁 Glue checkpoint/锁上下文还热

1.  snowflake_sql/02_storage_integration.sql
                Snowflake ↔ S3 凭证打通（不通这个 Snowpipe 跑不起来）

2.  snowflake_sql/03_silver_table.sql
                Silver 目标表 DDL —— 与 Glue WIDE_V2_SCHEMA 对契约

3.  snowflake_sql/04_pipe.sql
                ⭐ Snowpipe 主体：file format / COPY INTO / auto-ingest 触发

4.  snowflake_sql/06_dedup_task.sql
                Snowpipe 是 at-least-once，去重在这里

5.  snowflake_sql/05_gold_dynamic_tables.sql → 07_bi_view.sql
                Gold 聚合 + BI 出口

6.  snowflake_sql/08_freshness_alert.sql
    + lambda/dropzone_freshness_check
                端到端 SLO 哨兵 —— 上游入口 + Snowflake 出口一起 review 阈值

7.  最后        lambda/dlq_weekly_report
                收尾扫一眼周报触达
```

---

## 各步骤要带着问的问题（review 视角）

### 0. lambda/stale_lock_check
- 认定"锁过期"的阈值是多少？比 Glue Job timeout 长还是短？
  - 短 → 误杀正在跑的 job
  - 长 → stale 锁卡死分区数小时
- 强删锁（UpdateItem 清空 lock_holder）还是发告警让人来看？
- 与 Bronze 的 `[LOCKED]` skip 告警（`glue/bronze_etl.py:138`）语义如何区分：
  - 一个是"当前被别人持有"
  - 一个是"历史持有者死了没释放"
  - 告警文案别混

### 1. snowflake_sql/02_storage_integration.sql
- IAM 信任关系的 `STORAGE_AWS_EXTERNAL_ID` 是写死还是动态生成？
  写死 → 跨环境（dev/staging/prod）复制时有覆盖风险

### 2. snowflake_sql/03_silver_table.sql
- 列名/类型 vs Glue 输出的 `WIDE_V2_SCHEMA`（`glue/lib/schema_v2_wide.py`）一致吗？
- 这是契约漂移最高发的点 —— 用刚 fix dlq prefix drift 的同种眼光扫一遍
- `is_estimate_final` / `paid_share` / `featured_share` 的 nullability 两边对得上吗？

### 3. snowflake_sql/04_pipe.sql
- 触发方式：S3 event notification 直接 → Snowflake auto-ingest queue，
  还是经过 SNS / SQS？决定了告警链路和重试语义
- File format 是 Parquet 还是 CSV？
- Schema evolution 怎么处理？Glue 那边宽表加了新列，Snowpipe 会静默吃掉还是报错？
- `ON_ERROR` 设的是 CONTINUE / SKIP_FILE / ABORT_STATEMENT？错文件会不会拖累整批

### 4. snowflake_sql/06_dedup_task.sql
- 去重 key 是不是和 Glue 宽表的 `WIDE_V2_PK` 一致
  （`["dt", "product_id", "app_store", "country", "device"]`）
- 用 MERGE 还是 ROW_NUMBER 窗口去重？性能 + 幂等性差异
- Task 触发频率 vs Snowpipe 写入频率，会不会出现"读到一半新数据进来"的 race

### 5. snowflake_sql/05_gold_dynamic_tables.sql → 07_bi_view.sql
- Dynamic Table 的 `TARGET_LAG` 设多少？跟下游 BI 的刷新频率匹配吗
- Gold 层怎么处理 `is_estimate_final = false` 的 preview 数据：过滤掉？保留并打标？
- BI view 给到的字段子集，权限模型（row access policy / masking policy）有没有

### 6. 端到端 SLO（08 + dropzone_freshness_check）
- 两个告警的时间窗口是否一致？
  例如 dropzone 9am UTC 报 "昨天没数据"、snowflake 08 11am UTC 报 "今天表没更新"
- 中间任何一段挂了（Glue Bronze / Silver / Snowpipe），哪个会先报？决定 oncall 根因定位起点
- 阈值是绝对时间（"每天 9am 前必须有数据"）还是相对延迟（"距上次更新 > N 小时"）

### 7. lambda/dlq_weekly_report
- 周频触发：Cron 还是 EventBridge schedule？
- 收件人配置在哪：terraform / lambda env vars / SNS topic subscription？
- 0 错误时是否发 "绿灯" 邮件
  - 不发 → 人会忘记 lambda 是不是还活着（silent failure 风险）
  - 发 → 噪音
  - 折中：每周固定发，但 0 错误时邮件 subject 带 ✅

---

## 串完后值得做的两件事

1. **画一张端到端架构图**（如果 PLAN.md 里没有）：
   Data.ai → S3 dropzone → Glue Bronze → S3 Bronze → Glue Silver → S3 Silver
            → Snowpipe → Snowflake Silver → Dedup Task → Gold Dynamic Tables → BI View
   把每一段的 SLA / 触发方式 / 监控 lambda 标注上去

2. **写一份 contract drift 检查清单**：
   - Glue narrow schema ↔ dropzone 上游格式
   - Glue wide schema ↔ Snowflake silver 表 DDL
   - Wide_V2_PK ↔ dedup task 的去重 key
   - DLQ writer 路径 ↔ dlq_replay reader 路径（已 fix）
   每一对契约都该有一个自动化测试或 lint 守住，否则下一次漂移又是 silent failure

---

## 备忘

- `dlq_replay` 已修复：`failed_at=` 前缀 + 原 source_key 直接回写 dropzone
- `write_dlq_dataframe` 前缀已统一到 `failed_at=`
- Makefile / terraform 参数已改名 `REPLAY_DATE` → `FAILED_AT_DATE`
- 部署需要 `make build-glue && make tf-apply`
