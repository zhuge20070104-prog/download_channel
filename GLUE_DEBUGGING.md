# GLUE_DEBUGGING.md — Glue Job 调试 Runbook

> 配套文件：[explanation.md](explanation.md) (架构) · [OPERATION.md](OPERATION.md) (日常运维) · [TEST.md](TEST.md) (数据校验) · [DEPLOY-ISSUES.md](DEPLOY-ISSUES.md) (踩坑)
>
> 本文回答：Glue job 出问题时按什么顺序查、能看到什么、看不到什么。

---

## TL;DR

- **Glue 默认不暴露 Spark History Server**。需要主动开 `--enable-spark-ui` + `--spark-event-logs-path`，event log 写到 S3，然后自己起 history server（Docker）才能看到标准 Spark UI 页面。
- **日常 80% 的 debug 只用** Glue console + CloudWatch Logs。只有性能 / skew / OOM 问题才需要 Spark UI。
- 本项目当前**已启用**：`--enable-auto-scaling` + `--enable-metrics` + `--enable-continuous-cloudwatch-log` + `--enable-spark-ui` + `--spark-event-logs-path` + `--enable-job-insights`（2026-05-16 落地，见 §9）。IAM 也补了 `cloudwatch:PutMetricData`，§2.3 的 fine-grained metric 现在才会真的有数据。

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

namespace: `Glue`，dimension: `JobName` + `JobRunId` + `Type`。**`Type` 的值随 metric 不同而不同**（不是统一的 `gluejob`）：

| metric 类型 | `Type` 维度值 | 谁发 |
|------------|--------------|------|
| Counter（累计计数）| `count` | Spark `GlueCloudWatchReporter`（走 `PutMetricData`） |
| Gauge（即时值）| `gauge` | Spark `GlueCloudWatchReporter`（走 `PutMetricData`） |
| Glue 服务 job-level（Duration / Failures）| `gluejob` | Glue 服务自己（不走 `PutMetricData`）|

不确定某个 metric 用哪个 `Type` 时，先 `aws cloudwatch list-metrics --namespace Glue --metric-name <name>` 一下就知道了。

#### 性能调试最有用的几个

| Metric | `Type` | 用途 | 怀疑 |
|--------|--------|------|------|
| `glue.driver.aggregate.elapsedTime` | `gluejob` | Job 总耗时 | 跟基线比，找退化 |
| `glue.driver.aggregate.numCompletedTasks` | `count` | 总 task 数 | 异常多 → 数据膨胀；异常少 → 数据丢失 |
| `glue.driver.aggregate.shuffleBytesWritten` | `count` | shuffle 量 | 异动 → join skew / 没 broadcast 小表 |
| `glue.ALL.s3.filesystem.read_bytes` | `count` | 累计 S3 读量 | 突增 → 没 partition pruning |
| `glue.ALL.s3.filesystem.write_bytes` | `count` | 累计 S3 写量 | 突增 → 重复写 / 没去重 |
| `glue.ALL.system.cpuSystemLoad` | `gauge` | CPU 利用率 | 持续低 → IO bound，加 DPU 没用 |
| `glue.driver.BlockManager.disk.diskSpaceUsed_MB` | `gauge` | 磁盘 spill 量 | 大 → 内存不够，要么加 DPU 要么改代码 |

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

~~本项目目前**没开**这两个参数，所以现在没 event log，也没 UI 可看。~~

**更新（2026-05-16）**：已启用。event log 落到：
- Bronze: `s3://<scripts-bucket>/spark-event-logs/bronze/`
- Silver: `s3://<scripts-bucket>/spark-event-logs/silver/`

具体本地起 SHS 的命令见 [§9.5](#95-本地起-spark-history-server)。

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

**不要常驻 SHS**（白白付 EC2 钱）。需要看时本地起一个，**长期 IAM key + jnshubham 镜像**的最小可用版本：

```bash
# 1. 准备 AWS credentials（长期 IAM key 示例；SSO/AWS_PROFILE 用户见 §9.5）
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# 2. 起容器（镜像自带 ENTRYPOINT 直接跑 HistoryServer，不要覆盖命令）
docker run -itd \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://iodp-dc-scripts-dev-${ACCOUNT_ID}/spark-event-logs/silver/ -Dspark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} -Dspark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
  -p 18080:18080 \
  --name spark-history \
  jnshubham/glue_sparkui:latest

# 3. 打开浏览器
explorer.exe "http://localhost:18080"  # WSL
# start http://localhost:18080         # Windows 原生
# open http://localhost:18080          # macOS
# xdg-open http://localhost:18080      # Linux

# 4. 看完关掉
docker stop spark-history && docker rm spark-history
```

> 完整命令（SSO 凭据、PowerShell、踩坑）见 [§9.5](#95-本地起-spark-history-server)。

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

### 5.1 加 `--enable-job-insights`（5 分钟，0 成本）— ✅ 已实施 (2026-05-16)

Glue 4.0+ 自带的智能诊断，下次 run page 自动显示：
- Shuffle hot spot 提示
- Driver / Executor OOM 提示
- Slow task / data skew 提示
- 建议的 DPU 数量

改 [terraform/modules/glue_etl/main.tf](terraform/modules/glue_etl/main.tf) 两个 job 的 `default_arguments`，加一行：

```hcl
"--enable-job-insights" = "true"
```

### 5.2 开 Spark UI event log（10 分钟，~$0.01/run S3 存储）— ✅ 已实施 (2026-05-16)

见 §3.2 / §9.5。开了之后 event log 永久躺在 S3，需要时再起 SHS 看。

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

---

## 9. 部署 → Seed → 看 Spark UI / CloudWatch（实操 runbook，2026-05-16 落地）

> 本节对应 §3 Spark UI + §5.1 / §5.2 推荐项的落地实操。一条龙覆盖：怎么 deploy、怎么 seed、怎么进 Spark UI 看 DAG / Skew、怎么在 CloudWatch 看 `PutMetricData` 上来的 fine-grained metric。

### 9.1 当前已启用的可观测性参数

[terraform/modules/glue_etl/main.tf](terraform/modules/glue_etl/main.tf) Bronze + Silver 两个 Job 的 `default_arguments` 现在包含：

| 参数 | 值 | 作用 |
|------|-----|------|
| `--enable-spark-ui` | `"true"` | 输出 Spark event log |
| `--spark-event-logs-path` | Bronze: `s3://<scripts>/spark-event-logs/bronze/`<br>Silver: `s3://<scripts>/spark-event-logs/silver/` | event log 落点，两个 job 分目录 |
| `--enable-job-insights` | `"true"` | Glue 自动诊断（OOM / skew / DPU 建议） |
| `--enable-metrics` | `"true"` | Spark `GlueCloudWatchReporter` 推 fine-grained metric |
| `--enable-continuous-cloudwatch-log` | `"true"` | driver/executor 实时日志写 `/aws-glue/jobs/logs-v2` |
| `--enable-auto-scaling` | `"true"` | DPU 自动伸缩 |

IAM 同步补的：`iodp-dc-glue-execution-${env}` role 现在带 `cloudwatch:PutMetricData` statement（之前漏了，metrics 全 403 丢弃）。`iodp-dc-dropzone-seeder-${env}` 同步补。

### 9.2 部署

只动了 IAM policy + `default_arguments`，全是 in-place update，Glue Job 不会重建，Snowflake / S3 状态完全不动：

```bash
make deploy ENV=dev
```

### 9.3 Seed + 触发 ETL

```bash
make demo ENV=dev DT=2026-05-16   # seed → wait → Bronze → Silver
make status ENV=dev                # 等 Bronze + Silver 都 SUCCEEDED
```

### 9.4 确认 event log 写出来了

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws s3 ls "s3://iodp-dc-scripts-dev-${ACCOUNT_ID}/spark-event-logs/bronze/" --recursive
aws s3 ls "s3://iodp-dc-scripts-dev-${ACCOUNT_ID}/spark-event-logs/silver/" --recursive
```

应该看到形如 `spark-application-1715846421000_xxxxx` 的文件，每次 run 一个，几 MB ~ 几十 MB。

### 9.5 本地起 Spark History Server

> **不要常驻 SHS**——白白付 EC2 钱。需要看时本地起一个 Docker，看完关掉。

下面 PowerShell 和 Bash 两个版本**不能混用**（变量赋值、续行、env 引用语法全不一样），按你的 shell 选一份。两份命令都以"看 Silver"为例，要看 Bronze 把路径里 `/silver/` 换成 `/bronze/`。

**镜像选型**：推荐 `jnshubham/glue_sparkui:latest`（社区镜像，自带 ENTRYPOINT 直接跑 HistoryServer，命令最简短，**2026-05-16 实测可用**）。备选 `amazon/aws-glue-libs:glue_libs_4.0.0_image_01`（AWS 官方，但需要在 `docker run` 末尾追加 `/home/glue_user/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer` 才能启动）。**不要用** `public.ecr.aws/glue/sparkui:latest`——这个仓库不存在（早期 AWS 博客的错误引用）。

#### 9.5.0 凭据准备：AWS_PROFILE 用户怎么生成 env vars

容器内的 Hadoop S3A driver 是 Java 进程，**不认识 `AWS_PROFILE`**，也读不到宿主机的 `~/.aws/`。所以必须把 profile 解析成 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`（SSO 还会带 `AWS_SESSION_TOKEN`），再传给 docker。

```bash
# SSO profile：先 login（如果之前没 login 或 token 过期）
aws sso login --profile $AWS_PROFILE

# 把 profile 的 credentials 解析成 export 语句并执行
eval "$(aws configure export-credentials --profile $AWS_PROFILE --format env)"

# 验证三个 env var 都有值
echo "key=${AWS_ACCESS_KEY_ID:0:8}...  token_len=${#AWS_SESSION_TOKEN}"
```

- 输出含 `AWS_SESSION_TOKEN` → 你是临时凭据（SSO/AssumeRole），下面用 **SSO 版** docker run
- 没有 `AWS_SESSION_TOKEN` → 长期 IAM key，下面用 **长期 key 版** docker run
- SSO token 1 小时左右过期，过期后新 run 列不出来 → 重新 `aws sso login` + `eval ...` + `docker rm -f spark-history` 再起

#### 9.5.1 Bash / WSL — 长期 IAM key（默认推荐，命令最简）

```bash
export AWS_ACCESS_KEY_ID="<AKIA...>"
export AWS_SECRET_ACCESS_KEY="<your_secret>"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

docker run -itd \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://iodp-dc-scripts-dev-${ACCOUNT_ID}/spark-event-logs/silver/ -Dspark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} -Dspark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY}" \
  -p 18080:18080 \
  --name spark-history \
  jnshubham/glue_sparkui:latest

# 验证
docker ps | grep spark-history
docker logs spark-history --tail 30   # 期望看到 "Bound HistoryServer to 0.0.0.0:18080"

# 打开
explorer.exe "http://localhost:18080"  # WSL 调 Windows 浏览器
# xdg-open http://localhost:18080      # Linux
# open     http://localhost:18080      # macOS

# 看完
docker stop spark-history && docker rm spark-history
```

#### 9.5.2 Bash / WSL — SSO / 临时凭据版

在长期 key 版基础上，加 `AWS_SESSION_TOKEN` env var、加 `s3a.session.token`、加 `s3a.aws.credentials.provider=TemporaryAWSCredentialsProvider`：

```bash
# 先按 §9.5.0 跑过 eval "$(aws configure export-credentials ...)"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

docker run -itd \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://iodp-dc-scripts-dev-${ACCOUNT_ID}/spark-event-logs/silver/ -Dspark.hadoop.fs.s3a.access.key=${AWS_ACCESS_KEY_ID} -Dspark.hadoop.fs.s3a.secret.key=${AWS_SECRET_ACCESS_KEY} -Dspark.hadoop.fs.s3a.session.token=${AWS_SESSION_TOKEN} -Dspark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider" \
  -p 18080:18080 \
  --name spark-history \
  jnshubham/glue_sparkui:latest
```

#### 9.5.3 PowerShell（Windows 原生，不走 WSL）

长期 key 版：

```powershell
$env:AWS_ACCESS_KEY_ID     = "<AKIA...>"
$env:AWS_SECRET_ACCESS_KEY = "<your_secret>"
$ACCOUNT_ID = (aws sts get-caller-identity --query Account --output text)

docker run -itd `
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://iodp-dc-scripts-dev-$ACCOUNT_ID/spark-event-logs/silver/ -Dspark.hadoop.fs.s3a.access.key=$env:AWS_ACCESS_KEY_ID -Dspark.hadoop.fs.s3a.secret.key=$env:AWS_SECRET_ACCESS_KEY" `
  -p 18080:18080 --name spark-history `
  jnshubham/glue_sparkui:latest

start http://localhost:18080

docker stop spark-history; docker rm spark-history
```

SSO 版在 `SPARK_HISTORY_OPTS` 里照 §9.5.2 加 `s3a.session.token` + `TemporaryAWSCredentialsProvider` 两段。

#### 9.5.4 三个踩坑（按出现顺序）

1. **不要在 `jnshubham/glue_sparkui` 镜像后面追加 spark-class 命令**。这个镜像 ENTRYPOINT 已经在跑 HistoryServer，多写的命令会被当成 HistoryServer 的**位置参数 = logDirectory**，覆盖掉 `SPARK_HISTORY_OPTS` 里的 `-Dspark.history.fs.logDirectory=s3a://...`，结果就是去本地文件系统找一个不存在的目录 → `FileNotFoundException`。
   - 区分：`amazon/aws-glue-libs:...` 没有这种 ENTRYPOINT，**必须**追加 spark-class 命令；`jnshubham/glue_sparkui:...` **不要**追加。

2. **`AWS_PROFILE` 在容器内不生效**。`AWS_PROFILE` 只是给 AWS CLI 用的，告诉 CLI 去读 `~/.aws/credentials`。容器内的 Java + Hadoop S3A driver 既看不到 `~/.aws/` 也不认识 `AWS_PROFILE` —— 必须按 §9.5.0 把 profile 解析成三个 env var 再传给 docker。表现：报 `CredentialInitializationException: Access key, secret key or session token is unset`。

3. **长期 key 和 SSO 的 `SPARK_HISTORY_OPTS` 不一样**。长期 key（`AKIA...`）只需 access key + secret 两段；SSO/临时凭据**必须**加 `s3a.session.token` 和 `s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider`。反过来：长期 key 用了 Temporary provider 会立即报 "session token unset"。

#### 9.5.5 WSL 用户特别注意：Docker Desktop 集成

如果 `docker ps` 直接报错（不是 docker 命令本身的问题），说明 Docker Desktop 没开 WSL 集成：**Docker Desktop → Settings → Resources → WSL Integration → 勾上你的 Ubuntu 发行版 → Apply & Restart**。

### 9.6 在 Spark UI 里看 DAG / Skew

| 想看 | Tab | 怎么判断 |
|------|-----|---------|
| **DAG 可视化** | **Jobs** → 点 Job ID | 看到 stages 依赖图 |
| **DAG 物理计划** | **SQL / DataFrame** | join 是 `BroadcastHashJoin` 还是 `SortMergeJoin`、是否 partition pruning |
| **Data Skew** | **Stages** → 点最慢 stage → **Summary Metrics for Tasks** | "Duration" 的 `Max` 是否 `Median` 的 10× |
| **Skew 元凶 key** | 同页 → **Tasks** 表按 Duration 倒序 | 最慢 task 的 `Shuffle Read Size` 远大于其他 |
| **OOM / GC** | **Executors** | `GC Time / Task Time` > 10% 有问题 |

本项目 Silver 最可能 skew 的点：`pivot_narrow_to_wide` 按 `(dt, product_id, app_store, country, device)` group——某 product × 某国家下载量极大时单 task 拖整体（见 §3.5）。

#### 9.6.1 Stages 按 Duration 倒序排（操作）

点 Stages 表的 `Duration` 列头两次 → URL 自动变成 `.../stages/?&completedStage.sort=Duration&completedStage.desc=true`，最慢 stage 排最上。Tasks 表内部 / Jobs tab 都是同样玩法，任何列点两次切升降序。

#### 9.6.2 dev seed 数据为什么看不到 skew（重要认知）

第一次跑通 SHS 打开 Silver 的 Stages，**会发现根本看不出问题**，特征：

- 最慢 stage 才 5s 左右，绝大多数 < 0.5s
- **每个 stage 都是 `1/1` task**（Spark 判断数据太小不值得切多 partition）
- Input / Output / Shuffle 全是 KB 级（最大几十 KB）

含义：dev seed 在"单 task 串行就秒杀"的规模上，**skew 这个概念不成立**——一个 task 没有"max vs median"可比，Summary Metrics 全是同一个数。Stage 0 那种 5s 长尾通常是首次读 parquet 的 S3 冷启动握手（建连接、读 footer、读 schema），跟数据量无关，跟 skew 更无关。

要让 Spark UI 真有东西可看，**必须人为放大数据**：

- **造法 A（练手）**：改 [terraform/modules/dropzone_seeder/](terraform/modules/dropzone_seeder/) 给某个 `product_id` 灌 1M 条 events，其他不动，跑 `make demo` 后 `pivot_narrow_to_wide` 这步就会出现一个 task >> 其他的明显倾斜
- **造法 B（不练）**：面试场景下重点是讲清楚"我会读 Spark UI、知道去哪看 Max/Median ratio、知道修法（salting / broadcast / pre-aggregate）"，prod 真发生 skew 时数据自然到位，不必现在硬造

所以"第一次打开 SHS 看 Silver 觉得没东西看"是**正常现象**，不是配置错了。这条认知值得记，避免后面反复怀疑 SHS / event log / Spark UI 是不是有问题。

### 9.7 CloudWatch 看 `PutMetricData` 上来的数据

#### 入口 A — 现成 Dashboard（最快）

```
AWS Console → CloudWatch → Dashboards → iodp-dc-etl-dev
```

定义在 [terraform/modules/observability/main.tf:404-439](terraform/modules/observability/main.tf#L404-L439)，已 pin Duration + Failures 两个 widget。

或命令行直接拿 URL：

```bash
echo "https://$(aws configure get region).console.aws.amazon.com/cloudwatch/home?region=$(aws configure get region)#dashboards:name=iodp-dc-etl-dev"
```

#### 入口 B — Metrics Explorer（看 fine-grained metric）

```
AWS Console → CloudWatch → Metrics → All metrics
  → 命名空间 "Glue"
  → 维度组合 "JobName, JobRunId, Type"
  → 选 job + 这次 run 的 ID
  → 勾 metric (例: glue.driver.aggregate.shuffleBytesWritten)
```

或命令行（拿最近 run 的 ID）：

```bash
RUN_ID=$(aws glue get-job-runs --job-name iodp-dc-silver-etl-dev --max-results 1 \
  --query 'JobRuns[0].Id' --output text)

aws cloudwatch get-metric-statistics \
  --namespace Glue \
  --metric-name glue.driver.aggregate.shuffleBytesWritten \
  --dimensions Name=JobName,Value=iodp-dc-silver-etl-dev \
               Name=JobRunId,Value="$RUN_ID" \
               Name=Type,Value=count \
  --start-time $(date -u -d "1 hour ago" +%Y-%m-%dT%H:%M:%SZ) \
  --end-time   $(date -u +%Y-%m-%dT%H:%M:%SZ) \
  --period 60 --statistics Sum Maximum
```

> `Type` 值随 metric 类型变（`count` / `gauge` / `gluejob`），详见 §2.3。Spark fine-grained metric 用 `count` 或 `gauge`，不是 `gluejob`。

#### 验证 `PutMetricData` 真有数据上来

```bash
aws cloudwatch list-metrics \
  --namespace Glue \
  --metric-name glue.driver.aggregate.shuffleBytesWritten \
  --dimensions Name=JobName,Value=iodp-dc-silver-etl-dev \
  --query 'Metrics[].Dimensions' --output table
```

权限补上前这个返回**空**；补完跑 demo，应该看到 RunId 出现在结果里。

### 9.8 重要认知差异：Job-level vs Fine-grained metric

| 类型 | 谁发的 | 走 `PutMetricData`？ | 没权限会怎样 |
|------|--------|---------------------|------------|
| **Job-level**（Duration / Failures / DPU-hours）| Glue 服务自己 | ❌ 不走 | Dashboard 仍正常 |
| **Fine-grained**（shuffle / S3 IO / spill / executor 内存等 §2.3 列的那些）| Spark `GlueCloudWatchReporter` | ✅ 走 | 全部 403 丢弃，§2.3 表里所有 metric 查不到 |

所以之前 IAM 漏了 `cloudwatch:PutMetricData` 时，Dashboard 看起来一切正常，但你按 §2.3 在 Metrics Explorer 里找 `shuffleBytesWritten` 等都是空——容易误判。修了之后才是完整可观测。

### 9.9 一条龙命令汇总

```bash
# ① Deploy
make deploy ENV=dev

# ② Seed 2026-05-16 + 跑全流程
make demo ENV=dev DT=2026-05-16
make status ENV=dev    # 等两个 SUCCEEDED

# ③ 看 Spark UI — 完整命令见 §9.5，本节只是路径示意
# 凭据准备：       §9.5.0 (SSO/AWS_PROFILE 必读)
# Bash 长期 key：  §9.5.1 (默认推荐)
# Bash SSO：       §9.5.2
# PowerShell：     §9.5.3
# 踩坑：           §9.5.4 (ENTRYPOINT / AWS_PROFILE / 凭据类型 三个坑)

# ③' 看 CloudWatch
# Dashboard:  CloudWatch → Dashboards → iodp-dc-etl-dev
# Metrics:    CloudWatch → Metrics → Glue → JobName/JobRunId/Type
```
