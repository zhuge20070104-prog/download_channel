# GLUE_DEBUGGING.md — Glue Job 调试 Runbook

> 配套文件：[explanation.md](explanation.md) (架构) · [OPERATION.md](OPERATION.md) (日常运维) · [TEST.md](TEST.md) (数据校验) · [DEPLOY-ISSUES.md](DEPLOY-ISSUES.md) (踩坑)
>
> 本文回答：Glue job 出问题时按什么顺序查、能看到什么、看不到什么。

---

## TL;DR

- **Glue 默认不暴露 Spark History Server**。需要主动开 `--enable-spark-ui` + `--spark-event-logs-path`，event log 写到 S3，然后自己起 history server（Docker）才能看到标准 Spark UI 页面。
- **日常 80% 的 debug 只用** Glue console + CloudWatch Logs。只有性能 / skew / OOM 问题才需要 Spark UI。
- 本项目目前**只开了** `--enable-metrics` + `--enable-continuous-cloudwatch-log` + `--enable-auto-scaling`（见 [terraform/modules/glue_etl/main.tf:130-141](terraform/modules/glue_etl/main.tf#L130-L141)），Spark UI / job insights / observability metrics 都没开。

---

## 1. 本项目的 Glue Job 清单

| Job 名 | 定义文件 | 触发方式 | 作用 |
|--------|---------|---------|------|
| `iodp-dc-bronze-etl-${env}` | [terraform/modules/glue_etl/main.tf:111](terraform/modules/glue_etl/main.tf#L111) | EventBridge 每日 UTC 10:00 | Dropzone → Bronze 窄表 |
| `iodp-dc-silver-etl-${env}` | [terraform/modules/glue_etl/main.tf:150](terraform/modules/glue_etl/main.tf#L150) | Workflow（Bronze 成功后）| Bronze 窄表 → Silver 宽表 PIVOT |
| `iodp-dc-dropzone-seeder-${env}` | [terraform/modules/dropzone_seeder/main.tf:88](terraform/modules/dropzone_seeder/main.tf#L88) | 手工 / Workflow | 写 seed 数据到 Dropzone |
| `iodp-dc-glue-dlq-replay-${env}` | [terraform/modules/glue_dlq_replay/main.tf:10](terraform/modules/glue_dlq_replay/main.tf#L10) | 手工 | 从 DLQ replay 失败的 partition |

---

## 2. 调试入口：5 个地方按顺序查

### 2.1 Glue Console — Job runs 页面

**最先看这个**。AWS Console → Glue → ETL jobs → 选 job → Recent job runs。

每次 run 展示：

| 字段 | 含义 | 调试价值 |
|------|------|---------|
| Run ID | `jr_xxxxxxxx` | 后续 CloudWatch logs / metrics 都靠这个串起来 |
| Status | SUCCEEDED / FAILED / TIMEOUT / STOPPED | 一眼看结果 |
| Start time / End time | 开始/结束时间 | 跟告警时间对应 |
| Duration | 总耗时 | 跟历史比，找性能退化 |
| Capacity (DPU-hours) | DPU × 小时 | 成本估算 |
| Worker type / count | G.1X × N | 资源配置 |
| Glue version | 4.0 | 版本回溯 |
| Error message | 简短报错（exception 第一行）| 看不全要去 CloudWatch |
| **Logs / Output / Error logs** 链接 | 跳转到 CloudWatch | 看完整 stack trace |
| Job parameters | 这次 run 用的 `--KEY=VALUE` | 排查"为什么这次跟昨天不一样" |
| Continuous logs | 跳转到 logs-v2 | 看 driver/executor 实时输出 |
| Job insights（如启用）| AWS 自动诊断 | OOM、shuffle 热点、DPU 建议 |

### 2.2 CloudWatch Logs — 3 个 log group

```
/aws-glue/jobs/output       ← stdout: print(), INFO 级别日志
/aws-glue/jobs/error        ← stderr: Python 异常 stack trace ← 大多数情况看这个
/aws-glue/jobs/logs-v2      ← Spark driver/executor 详细日志（continuous logging 开启时）
```

**log stream 名 = run ID（`jr_xxx`）**。

#### Insights 查询示例

CloudWatch Logs Insights，selects log group `/aws-glue/jobs/error`：

```
fields @timestamp, @message
| filter @message like /Traceback/
| sort @timestamp desc
| limit 50
```

或找最近 24 小时所有失败 run 的报错首行：

```
fields @timestamp, @message
| filter @message like /Traceback|ERROR|Caused by/
| sort @timestamp desc
| limit 100
```

### 2.3 CloudWatch Metrics — Glue namespace

namespace: `Glue`，dimension: `JobName` + `JobRunId` + `Type=gluejob`。

#### 性能调试最有用的几个

| Metric | 用途 | 怀疑 |
|--------|------|------|
| `glue.driver.aggregate.elapsedTime` | Job 总耗时 | 跟基线比，找退化 |
| `glue.driver.aggregate.numCompletedTasks` | 总 task 数 | 异常多 → 数据膨胀；异常少 → 数据丢失 |
| `glue.driver.aggregate.shuffleBytesWritten` | shuffle 量 | 异动 → join skew / 没 broadcast 小表 |
| `glue.ALL.s3.filesystem.read_bytes` | 累计 S3 读量 | 突增 → 没 partition pruning |
| `glue.ALL.s3.filesystem.write_bytes` | 累计 S3 写量 | 突增 → 重复写 / 没去重 |
| `glue.ALL.system.cpuSystemLoad` | CPU 利用率 | 持续低 → IO bound，加 DPU 没用 |
| `glue.driver.BlockManager.disk.diskSpaceUsed_MB` | 磁盘 spill 量 | 大 → 内存不够，要么加 DPU 要么改代码 |

#### 怎么看

CloudWatch → Metrics → All metrics → Glue → JobName, JobRunId, Type → 选 job → 选 metric → 看趋势。

或用 CloudWatch Dashboard 永久 pin 住（本项目 [explanation.md §l](explanation.md) 有 Dashboard widget 配置）。

### 2.4 Glue Workflow — 上下游可视化

如果 Bronze 失败导致 Silver 没跑，去 **AWS Console → Glue → Workflows** 看那个失败的 run 的图：

- 红色节点 = 失败的 job
- 灰色节点 = 因上游失败而 skip 的 job
- 点节点能看 trigger 条件 / 执行历史

调试"为什么 Silver 没跑"通常 90% 是 Bronze 挂了 → workflow 阻断。

### 2.5 DynamoDB Checkpoint 表

本项目 Bronze/Silver 写 [explanation.md §a](explanation.md) 的 checkpoint 表 `iodp-dc-checkpoint-${env}`。

```bash
aws dynamodb get-item \
  --table-name iodp-dc-checkpoint-dev \
  --key '{"pk": {"S": "bronze#2026-05-13#ios"}}'
```

返回里的 `status` / `lock_expires_at` / `last_status` 告诉你：

- `running` + `lock_expires_at` 未到 → 还在跑
- `running` + `lock_expires_at` 已过 → **挂了但没释放锁**，下次 run 会抢锁
- `failed` → 上次失败，需要看 last_error
- `succeeded` → 正常完成

---

## 3. Spark UI（History Server）

### 3.1 为什么默认看不到

Glue 不像 EMR 给你一个常驻的 web 端口。Glue 把 Spark **event log**（每个 task / stage / shuffle 的 JSON 事件流）写到 S3，**没有自带的 web UI**。要看必须：

1. 开启 `--enable-spark-ui = true` 让 Glue 把 event log 写出来
2. 配置 `--spark-event-logs-path = s3://bucket/path/` 指定写哪
3. **自己起 Spark History Server** 读这个 S3 路径

本项目目前**没开**这两个参数，所以现在没 event log，也没 UI 可看。

### 3.2 启用 event log（改 Terraform）

[terraform/modules/glue_etl/main.tf:130-141](terraform/modules/glue_etl/main.tf#L130-L141)，在 `default_arguments` 里加：

```hcl
default_arguments = {
  # 现有的
  "--enable-auto-scaling"               = "true"
  "--enable-metrics"                    = "true"
  "--enable-continuous-cloudwatch-log"  = "true"

  # 加这两个
  "--enable-spark-ui"                   = "true"
  "--spark-event-logs-path"             = "s3://${var.scripts_bucket_name}/spark-event-logs/"

  # 推荐顺便加这个（Glue 4.0+，0 配置自动诊断）
  "--enable-job-insights"               = "true"

  # ...
}
```

`silver_etl` job（line 169）同样加。

Apply 之后下次 run 会写 event log 到 `s3://.../spark-event-logs/spark-application-xxx`。每次 run 一个文件，几 MB 到几十 MB。

#### 注意：IAM 权限

Glue execution role 需要 `s3:PutObject` 权限到这个路径。检查 [terraform/modules/glue_etl/main.tf](terraform/modules/glue_etl/main.tf) 的 IAM policy 是否覆盖 scripts bucket 全 prefix（应该已经覆盖）。

### 3.3 起 Spark History Server（按需 Docker）

**不要常驻 SHS**（白白付 EC2 钱）。需要看时本地起一个：

```bash
# 准备 AWS credentials（用 SSO 或 long-lived key，需要能读 S3）
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...

# 起容器，端口 18080
docker run -itd \
  -e SPARK_HISTORY_OPTS="\
    -Dspark.history.fs.logDirectory=s3a://iodp-dc-scripts-dev-${ACCOUNT_ID}/spark-event-logs/ \
    -Dspark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} \
    -Dspark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
  -p 18080:18080 \
  --name spark-history \
  public.ecr.aws/glue/sparkui:latest

# 打开浏览器
start http://localhost:18080   # Windows
# open http://localhost:18080  # macOS
# xdg-open http://localhost:18080  # Linux

# 看完关掉
docker stop spark-history && docker rm spark-history
```

### 3.4 Spark UI 里看什么

打开后是标准 Spark History Server 界面，跟本地 Spark / EMR 一样：

| Tab | 看什么 | 调试什么 |
|-----|--------|---------|
| **Jobs** | 每个 action 触发的 job，DAG 图 | 看哪个 action 最贵 |
| **Stages** | 每个 stage 的 task 数、duration 中位数 / 最大值 / **总时间** | **找 skew**：max task time >> median → 单个 task 拖后腿 |
| **Storage** | persist / cache 的 RDD/DataFrame | 看缓存是否生效、内存占用 |
| **Environment** | Spark 配置、Java/Scala 版本、JAR 列表 | 排查"配置不对"类问题 |
| **Executors** | 每个 executor 的 task 数、内存、GC、failed task | **找 OOM / GC 抖动**：GC time / total time > 10% 就有问题 |
| **SQL / DataFrame** | 每个 SQL plan，物理计划 | 看 join 是 broadcast 还是 sort-merge、是否触发 partition pruning |

### 3.5 数据 skew 怎么定位

最典型场景，Spark UI 是唯一能看清楚的工具：

1. **Stages tab** → 找耗时最长的 stage
2. 进 stage detail → **Summary Metrics for Tasks** 看 75th / max
   - 如果 max 是 median 的 10× 以上 → 严重 skew
3. **Tasks** 表格按 duration 排序 → 找最慢那条
4. 看它的 **Shuffle Read Size / Records**：如果是其他 task 的 100×，说明这个 key 太多记录
5. 修复：salting / broadcast join / pre-aggregate

本项目可能遇到 skew 的场景：
- `pivot_narrow_to_wide` 按 `(dt, product_id, app_store, country, device)` group：某个 product 在某个国家下载量极大 → 这个 group 的 task 慢
- DLQ replay 时所有失败 partition 集中在某天

---

## 4. 调试场景对照表

| 现象 | 第一步 | 第二步 | 第三步 |
|------|--------|--------|--------|
| Job 直接 FAILED | Glue console 看 error message | CloudWatch `/aws-glue/jobs/error` 找完整 stack trace | 改代码 / 数据 |
| Job 跑得慢（跟昨天比）| Glue console 看 duration + DPU usage | CloudWatch Metrics 看 `s3.read_bytes` / `shuffleBytesWritten` 异动 | Spark UI Stages tab 看具体哪个 stage 拖后腿 |
| Job 超时（TIMEOUT 状态）| 看 [terraform/modules/glue_etl/main.tf](terraform/modules/glue_etl/main.tf) 的 `timeout` 配置 | CloudWatch logs 看 driver 心跳是否中断 | Spark UI Executors tab 看是否 OOM / GC 抖 |
| 数据对不上 | [TEST.md](TEST.md) §1-§7 SQL 校验 | 看 Bronze/Silver 实际 S3 文件（`aws s3 ls`）| Glue logs 找 print() 输出 |
| 偶发失败 | 重跑看是否一致复现 | DLQ ([explanation.md §f](explanation.md)) 看是不是数据驱动的 | Spark UI 看 task retry 历史 |
| Silver 没跑 | Glue Workflows 看 DAG | DynamoDB checkpoint 表看 Bronze status | 看 Bronze run 的 error |
| Snowpipe 数没进 Snowflake | [TEST.md §7.3 COPY_HISTORY](TEST.md#L640) | [TEST.md §7.4 SYSTEM\$PIPE_STATUS](TEST.md#L663) | Snowpipe SQS DLQ ([explanation.md §m](explanation.md)) |
| Job 拿不到锁，被 skip | DynamoDB checkpoint 表看 `status=running` 的项 | 看 `lock_expires_at` 是否合理 | 锁超时 Lambda ([explanation.md §h](explanation.md)) |
| 输出 partition 数据缺失 | `MSCK REPAIR TABLE` 看 Athena 能不能发现 | Glue Catalog API 看 partition 是否注册 | 看 Bronze ETL 的 `create_partition` 调用 |

---

## 5. 推荐改进：本项目可以增强的可观测性

按 ROI 从高到低：

### 5.1 加 `--enable-job-insights`（5 分钟，0 成本）

Glue 4.0+ 自带的智能诊断，下次 run page 自动显示：
- Shuffle hot spot 提示
- Driver / Executor OOM 提示
- Slow task / data skew 提示
- 建议的 DPU 数量

改 [terraform/modules/glue_etl/main.tf](terraform/modules/glue_etl/main.tf) 两个 job 的 `default_arguments`，加一行：

```hcl
"--enable-job-insights" = "true"
```

### 5.2 开 Spark UI event log（10 分钟，~$0.01/run S3 存储）

见 §3.2。开了之后 event log 永久躺在 S3，需要时再起 SHS 看。

### 5.3 CloudWatch alarm 监控关键 metric（30 分钟）

```hcl
resource "aws_cloudwatch_metric_alarm" "glue_job_too_slow" {
  alarm_name          = "iodp-dc-bronze-etl-too-slow-${var.environment}"
  namespace           = "Glue"
  metric_name         = "glue.driver.aggregate.elapsedTime"
  dimensions = {
    JobName = aws_glue_job.bronze_etl.name
    Type    = "gluejob"
  }
  threshold           = 1800000   # 30 min in ms
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  alarm_actions       = [var.sns_alert_topic_arn]
}
```

### 5.4 CloudWatch Logs metric filter 计数 ERROR

```hcl
resource "aws_cloudwatch_log_metric_filter" "glue_errors" {
  name           = "iodp-dc-glue-error-count-${var.environment}"
  log_group_name = "/aws-glue/jobs/error"
  pattern        = "Traceback"
  metric_transformation {
    name      = "GlueJobTracebackCount"
    namespace = "Custom/Glue"
    value     = "1"
  }
}
```

配套加 alarm（>0 触发），让 Python 异常实时告警，不用每次手工去翻 log。

---

## 6. 三个常见误区

### 6.1 "Glue 自带 Spark UI"

❌ 错。Glue 不暴露 web UI 端口。必须开 event log + 自己起 SHS。

### 6.2 "看 CloudWatch Metrics 就够了，不用 Spark UI"

❌ 错。CloudWatch Metrics 是**聚合**指标（whole job 维度），Spark UI 是**细粒度**视图（stage / task 维度）。性能调优、找 skew、看 SQL plan，**只能靠 Spark UI**。

### 6.3 "Job 失败了，去看 `/aws-glue/jobs/output`"

❌ 错。Python 异常 / Java exception 在 `/aws-glue/jobs/error` （stderr）。output 是 stdout（你 print 的东西）。两个 log group 都要看。

---

## 7. 速查 — Run ID 怎么用

拿到一个 `jr_xxxxxxxx` 后：

```bash
# 1. 看 Glue API 返回的完整信息
aws glue get-job-run \
  --job-name iodp-dc-bronze-etl-dev \
  --run-id jr_xxxxxxxx \
  --predecessors-included

# 2. CloudWatch logs（log stream 名 = run ID）
aws logs get-log-events \
  --log-group-name /aws-glue/jobs/error \
  --log-stream-name jr_xxxxxxxx \
  --limit 100

# 3. 拿到这次 run 的 metrics
aws cloudwatch get-metric-statistics \
  --namespace Glue \
  --metric-name glue.driver.aggregate.elapsedTime \
  --dimensions Name=JobName,Value=iodp-dc-bronze-etl-dev \
               Name=JobRunId,Value=jr_xxxxxxxx \
               Name=Type,Value=gluejob \
  --start-time 2026-05-15T00:00:00Z \
  --end-time 2026-05-16T00:00:00Z \
  --period 60 \
  --statistics Sum Maximum
```

---

## 8. 一句话总结

**Glue debug 80% 靠 Glue console + CloudWatch Logs**（看 error / output），**性能问题靠 Spark UI**（需要先开 event log + 自己起 SHS），**生产侧用 CloudWatch Alarm + Metric Filter 兜底告警**。三层叠加才是完整的可观测性。
