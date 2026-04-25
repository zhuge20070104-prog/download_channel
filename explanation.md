# Download Channel ETL — 五大运维机制详解

> 本文用大白话 + 生活类比 + 代码示意，解释 PLAN.md 里提到的五个关键运维机制。
> 每一节都回答三个问题：**这是什么？为什么要有？怎么实现的？**

---

## a) Glue Job 串联保护（并发锁）

### 这是什么

防止"昨天的活没干完，今天又来一批活"导致同一份数据被两个 Job 同时处理。

### 生活类比

想象一间只能容纳一个理发师的理发店。门口挂了一个"营业中/空闲"的牌子：
- 顾客来了，看到"空闲" → 翻成"营业中" → 进去理发 → 理完翻回"空闲"
- 第二个顾客来了，看到"营业中" → 不进去，在门口等（或者走了下次再来）
- 如果理发师突然晕倒（Job 崩溃），牌子永远挂着"营业中"，后面的人永远进不来 → 所以牌子上还写了一个时间"2 小时后自动翻回空闲"（锁超时）

DynamoDB 里的 `status` + `lock_expires_at` 就是那块牌子。

### 工作流程

```
每日 UTC 10:00，EventBridge 触发 Glue Workflow

Glue Bronze Job 启动
  │
  ├── 1. 读 DynamoDB：当前 partition (如 bronze#2026-04-25#ios) 的状态是什么？
  │     ├── status = "running" 且 lock_expires_at > 现在  → 说明上一轮还没跑完
  │     │     → 跳过这个分区，发 SNS 告警："分区 bronze#04-25#ios 被锁，跳过"
  │     │
  │     ├── status = "running" 但 lock_expires_at < 现在  → 上一轮崩溃了，锁过期了
  │     │     → 可以抢锁，继续处理
  │     │
  │     └── status = "succeeded" 或 "failed" 或 不存在  → 正常，可以处理
  │
  ├── 2. 用 DynamoDB 条件写抢锁（原子操作，不怕并发）
  │     ConditionExpression:
  │       attribute_not_exists(status)          # 第一次
  │       OR status <> 'running'               # 上一轮已完成
  │       OR lock_expires_at < :now            # 上一轮崩了，锁过期
  │     写入: status = "running", lock_expires_at = now + 2h
  │
  │     如果条件写失败 → 说明有人在这一瞬间抢先锁了 → 跳过
  │
  ├── 3. 正常处理数据...
  │
  └── 4. 处理完成 → 更新 DynamoDB：status = "succeeded", 清除 lock_expires_at
```

### 为什么锁过期时间是 2 小时

- 正常日常 Job 跑 10-20 分钟
- Backfill 场景可能跑 60-90 分钟
- 2 小时给足余量，超过了肯定是出了问题

### DynamoDB 中实际的记录长这样

```json
{
  "partition_key":    "bronze#2026-04-25#ios",
  "status":          "running",
  "lock_expires_at": "2026-04-25T12:00:00Z",
  "last_processed_at": "2026-04-25T10:00:15Z",
  "input_files":     ["narrow/dt=2026-04-25/store=ios/part-00000.csv.gz"],
  "file_md5s":       {"part-00000.csv.gz": "a1b2c3d4e5f6..."},
  "in_count":        850000000,
  "out_count":       849999500,
  "dlq_count":       500,
  "job_run_id":      "jr_abc123"
}
```

---

## b) Bronze → Silver 数据质量卡点（DQ Check）

### 这是什么

在 Silver Glue Job 把数据写入 Silver S3 **之前**，先过一道"体检"。体检不合格的数据不准进 Silver，转去 DLQ（死信区）。

### 生活类比

机场安检。你（数据）从值机柜台（Bronze）走到登机口（Silver），中间必须过安检（DQ Check）：
- 身份证过期 → product_id 是 NULL → 不让过，扣下来（DLQ）
- 行李超重 → downloads 出现负数 → 记一笔警告，但还是让过了（告警不阻断）
- 机票日期对不上 → 文件说是 4 月 25 日的数据，里面出现了 1 月 1 日的行 → 不让过

### 五项检查

```
Bronze S3 的数据
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│  DQ Check #1: 行数对比                                         │
│  Bronze 写入时记录了 out_count = 8.5 亿行（在 DynamoDB 里）    │
│  Silver Job 读到 8.49 亿行 → 差 0.12% → 超过 1% 阈值了吗？    │
│  没超过 → PASS                                                  │
│  超过 → BLOCK: 数据可能在传输中丢了，不敢往下灌                │
└─────────────────┬───────────────────────────────────────────────┘
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  DQ Check #2: 关键列空值率                                     │
│  SELECT count(*) WHERE product_id IS NULL → 85000 / 8.5 亿    │
│  = 0.001% → 阈值 0.1% → PASS                                  │
│                                                                 │
│  如果空值率 = 5% → BLOCK: 上游可能出了问题，大量 app 没有 ID  │
└─────────────────┬───────────────────────────────────────────────┘
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  DQ Check #3: 日期范围校验                                     │
│  当前处理 dt=2026-04-25 的分区                                 │
│  扫描数据中所有 dt 值 → 全是 2026-04-25？PASS                  │
│  出现了 2026-01-01 → BLOCK: 数据串了，这不是今天的数据         │
│  出现了 2026-04-20（在 ±7 天内）→ PASS（restate 场景正常）     │
└─────────────────┬───────────────────────────────────────────────┘
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  DQ Check #4: 数值范围                                         │
│  downloads_total, downloads_featured, ... 是否有负数？          │
│  有 → WARN（告警但不阻断，标记异常行）                          │
│  没有 → PASS                                                    │
└─────────────────┬───────────────────────────────────────────────┘
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│  DQ Check #5: 等式校验                                         │
│  downloads_total == downloads_featured + downloads_organic？    │
│  每一行都成立 → PASS                                            │
│  0.5% 的行不成立 → 超过 0.1% 阈值 → WARN（告警但不阻断）      │
│  这些行可能是上游计算精度问题，先放进来，但通知人工检查          │
└─────────────────┬───────────────────────────────────────────────┘
                  ▼
         全部 PASS → 写入 Silver S3
         任一 BLOCK → 不写 Silver，把失败数据写入 DLQ，发告警邮件
```

### 为什么是"阻断"和"告警"两级

- **阻断**（行数差、空值率、日期串）：这些错误说明数据本身就是错的，灌进 Silver 会污染所有下游。必须拦住。
- **告警**（负数、等式不平）：这些可能是上游精度问题，如果阻断会导致每天的数据都灌不进来。先放进来，通知人工排查。

---

## c) Restate 感知（Data.ai 更正历史数据）

### 这是什么

Data.ai 每天给的数据不是"终稿"。最近 7 天的数据都是"预估值"，下周二才会变成"终值"。这意味着同一个日期（比如 4 月 20 日）的数据，会被重新发送多次，每次可能数值不同。

我们的系统必须识别"这个日期的数据之前处理过了，但现在来了一版更新的" → 用新版本**覆盖**旧版本。

### 生活类比

期末考试改卷。老师改完一遍给你分数 85 分（preview），第二天复核改成 88 分（finalized）。你的成绩单上应该显示 88，不能显示 85+88=173。

DynamoDB 里记录的 MD5 就是"成绩单上写了你是 85 分"。当新的 88 分来了，MD5 变了 → 触发覆盖。

### 工作流程

```
4 月 25 日 UTC 10:00，Bronze Job 启动
  │
  ├── 查 dropzone 发现这些文件需要处理:
  │     dt=2026-04-25 (今天新的)
  │     dt=2026-04-24 (restate，昨天的预估更新了)
  │     dt=2026-04-23 (restate)
  │     dt=2026-04-22 (restate)
  │     dt=2026-04-21 (restate)
  │     dt=2026-04-20 (restate)
  │     dt=2026-04-19 (restate)
  │
  ├── 对每个 dt，检查 DynamoDB：
  │     dt=2026-04-25 → DynamoDB 无记录 → 新数据，直接处理
  │     dt=2026-04-24 → DynamoDB 有记录，file_md5 = "aaa..."
  │                     dropzone 里的文件 MD5 = "bbb..." → 不一样！→ 需要覆盖
  │     dt=2026-04-20 → DynamoDB 有记录，file_md5 = "ccc..."
  │                     dropzone 里的文件 MD5 = "ccc..." → 一样 → 跳过，省资源
  │
  ├── 对需要处理的日期:
  │     Bronze: 先 DELETE s3://bronze/.../v1/dt=2026-04-24/store=ios/*.parquet
  │             再写新的 parquet 文件
  │     更新 DynamoDB: file_md5 = "bbb...", status = "succeeded"
  │
  └── Silver Job 同理：覆盖 Silver S3 对应分区
```

### Snowflake 侧的去重

Snowpipe 是追加（append）语义。如果 Silver S3 的 dt=04-24 分区被覆盖重写，Snowpipe 会把新文件当作"新数据"再 COPY 一遍，导致 Snowflake 表里 dt=04-24 有两份（旧+新）。

解决方案：Snowflake 每日跑一个 Task，对 restate 窗口内的数据做去重：

```sql
-- 伪代码
MERGE INTO SILVER.DC_WIDE AS target
USING (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY dt, product_id, app_store, country, device
    ORDER BY _loaded_at DESC  -- 保留最新的
  ) AS rn
  FROM SILVER.DC_WIDE
  WHERE dt >= DATEADD('day', -7, CURRENT_DATE())
) AS dedup
ON target.dt = dedup.dt
   AND target.product_id = dedup.product_id
   AND ...
WHEN MATCHED AND dedup.rn > 1 THEN DELETE;
```

---

## d) 监控告警

### 这是什么

系统不会说话，出了问题你不看 Console 就不知道。监控告警 = 给系统装了一个"感觉不对劲就打电话给你"的保姆。

### 七类告警一览

```
┌──────────────────┐
│  1. Glue Job 失败│  Job 状态变成 FAILED
│  紧急程度: ★★★★★  │  → CloudWatch Alarm → SNS → 邮件/Slack
│  例子: Bronze Job  │  "今天的数据完全没处理"
│  OOM 崩溃了       │
└──────────────────┘

┌──────────────────┐
│  2. DLQ 新增文件  │  Glue Job 结束时发现 dlq_count > 0
│  紧急程度: ★★★★   │  → SNS → 邮件
│  例子: 500 行      │  "有 500 行数据解析失败，需要人工看一下"
│  schema 校验失败  │
└──────────────────┘

┌──────────────────┐
│  3. DLQ 周报      │  每周一 UTC 09:00 自动汇总
│  紧急程度: ★★      │  → EventBridge → Lambda → SNS → 邮件
│  例子: "过去 7 天  │  "方便你周一上班扫一眼"
│  共 3 个 DLQ 文件"│
└──────────────────┘

┌──────────────────┐
│  4. Snowpipe 延迟│  COPY_HISTORY 最近 2h 无新数据
│  紧急程度: ★★★    │  → Snowflake Alert → 邮件
│  例子: Silver S3  │  "数据到了 S3 但没进 Snowflake"
│  有新文件但       │
│  Snowpipe 没动    │
└──────────────────┘

┌──────────────────┐
│  5. 锁超时        │  DynamoDB status=running 超过 2 小时
│  紧急程度: ★★★    │  → CloudWatch Alarm → SNS
│  例子: Job 崩了   │  "可能 Job 崩了没释放锁，明天的 Job 也跑不了"
│  锁没释放         │
└──────────────────┘

┌──────────────────┐
│  6. 数据缺失      │  某天预期有文件但 Bronze 为空
│  紧急程度: ★★★    │  → EventBridge → Lambda → SNS
│  例子: Data.ai    │  "上游是不是没交数据？"
│  今天没有 PUT     │
└──────────────────┘

┌──────────────────┐
│  7. DQ 告警       │  §12 任一检查不通过
│  紧急程度: ★★★★   │  → Glue Job 内 → SNS
│  例子: product_id │  "数据质量有问题，Silver 没灌"
│  空值率 5%        │
└──────────────────┘
```

### 告警不等于你要半夜起来修

- 1, 2, 5, 7 = P1，工作时间尽快处理（数据管道断了）
- 4, 6 = P2，白天看一下（可能是上游延迟，不一定是我们的问题）
- 3 = P3，每周一扫一眼

---

## e) 成本护栏

### 这是什么

防止配置错误或代码 Bug 导致 AWS 和 Snowflake 的账单爆炸。

### 生活类比

信用卡的消费限额。不是说你不能花钱，而是防止你的卡被盗刷后一夜之间刷出 100 万。

### 逐项解释

**1. Glue MaxDPU = 10（日常）/ 20（backfill）**

```
1 DPU = 4 vCPU + 16GB RAM = $0.44/小时

日常 10 DPU:
  10 DPU × 20 分钟 = $0.44 × 10 × (20/60) ≈ $1.47/天
  每月 ≈ $44

如果不设上限，代码里写错了一个配置变成 100 DPU:
  100 DPU × 20 分钟 ≈ $14.7/天
  每月 ≈ $440  ← 10 倍！

Backfill 时临时调到 20 DPU（通过 Makefile 参数控制），跑完自动回落。
```

**2. Glue Timeout = 120 分钟**

```
正常 Job 跑 10-20 分钟。

如果不设 Timeout，以下 Bug 会让 Job 永远跑下去:
  - 死循环
  - 等待一个永远不会来的 S3 响应
  - Spark shuffle 在某个坏分区无限重试

120 分钟到了 → Glue 自动杀掉 → 状态变 TIMEOUT → 触发告警 #1
```

**3. Snowflake Warehouse AUTO_SUSPEND = 60s, MAX_CLUSTER_COUNT = 1**

```
Snowflake 按"仓库运行时间"计费。

AUTO_SUSPEND = 60: 仓库空闲 60 秒后自动休眠（不计费）。
  Dynamic Table 每 15 分钟刷新一次，每次可能只需要 10 秒。
  如果不设 AUTO_SUSPEND，仓库会一直开着:
    XS 仓库 24 小时 = 24 credits ≈ $72/天
  设了之后: 每次 10 秒 × 96 次/天 = ~960 秒 = ~$0.27/天  ← 260 倍差距

MAX_CLUSTER_COUNT = 1: 不允许自动扩多个集群。
  我们的场景不需要并发查询，1 个集群足够。
  如果自动扩到 10 个集群 → 费用 × 10。
```

**4. S3 生命周期**

```
数据一直存着不删 = 存储费一直涨。

Bronze + Silver 数据的生命周期:
  0-30 天:   STANDARD    ($0.023/GB/月)  ← 最近的数据，可能要回查
  30-90 天:  STANDARD_IA ($0.0125/GB/月) ← 不常用但偶尔要看
  90-365 天: GLACIER_IR  ($0.004/GB/月)  ← 几乎不看，合规保留
  365 天后:  删除

假设每天 50GB 新数据:
  不设生命周期: 50GB × 365 天 × $0.023 = $420/年
  设了生命周期: 大部分数据 90 天后进 GLACIER = ~$100/年  ← 省 75%
```

**5. DynamoDB On-Demand**

```
Checkpoint 表每天写入量极低（几十次 PUT/GET）。
On-Demand 计费: $1.25 / 百万写 = 每天 < $0.001。

如果误配成 Provisioned（100 WCU/RCU）:
  每月 ≈ $60  ← 贵 10000 倍

所以明确用 On-Demand，不要预置容量。
```

---

## 总结：这五个机制互相怎么配合

```
EventBridge 每日 UTC 10:00 触发
         │
         ▼
    ┌─ (a) 并发锁 ─── 检查 DynamoDB 锁 → 抢锁 → 或跳过 + 告警(d)
    │
    ▼
  Bronze Glue Job
    │  ├── (c) 检测 restate → MD5 比对 → 覆盖写 or 跳过
    │  ├── (e) 受 MaxDPU + Timeout 约束
    │  └── 失败 → DLQ + 告警(d)
    │
    ▼
  Silver Glue Job
    │  ├── (b) DQ 卡点 → 5 项检查 → 通过才写 Silver
    │  ├── (c) 覆盖 Silver 对应分区
    │  ├── (e) 受 MaxDPU + Timeout 约束
    │  └── DQ 不通过 → DLQ + 告警(d)
    │
    ▼
  完成 → 更新 DynamoDB (a) → 释放锁
    │
    ▼
  Snowpipe → Snowflake → Dynamic Table → BI
    │                        │
    └─ (d) 延迟告警          └─ (e) AUTO_SUSPEND + MAX_CLUSTER
```
