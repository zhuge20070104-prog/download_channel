# Download Channel ETL — 六大运维机制详解

> 本文用大白话 + 生活类比 + 代码示意，解释 PLAN.md 里提到的六个关键运维机制。
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

### ⚠️ 重要澄清：lock_expires_at 自己不会杀 Job

`lock_expires_at` **只是 DynamoDB 里的一个时间戳字段，本身没有任何"杀进程"的能力**。它纯粹是一张"过期的牌子"，给**下一个**要跑的 Job 看的——下一个 Job 读到 `lock_expires_at < 现在` 时，推理"上一轮肯定死了"，然后抢锁继续跑。

真正能强制结束跑飞 Job 的是另一个机制：**Glue Job Timeout = 120 分钟**（见 §e）。这是 AWS Glue 平台级配置，时间一到 AWS 直接 kill 进程，不管代码在干什么。

两套机制分工：

| 机制 | 作用对象 | 谁来强制执行 | 能不能杀进程 |
|------|---------|-------------|-------------|
| `lock_expires_at`（DynamoDB） | **下一个**要跑的 Job | Job 代码自己读 DynamoDB 判断 | ❌ 不能 |
| Glue Timeout = 120 分钟 | **当前**正在跑的 Job | AWS Glue 服务平台级强制 | ✅ 能 |

### Job 卡死的完整故事

```
10:00  Job 启动，写 DynamoDB: status=running, lock_expires_at=12:00
10:30  Job hang 死（比如 S3 调用永远不返回）
12:00  ← 关键时刻
       ① AWS Glue 看：跑了 120 分钟了 → 强制 TIMEOUT，杀进程
       ② Job 被杀，cleanup 代码来不及跑 → DynamoDB 里 status 还是 "running"
       ③ CloudWatch 看到 Job 状态 = TIMEOUT → 触发告警 #1
次日10:00  新 Job 启动
       ① 读 DynamoDB: status=running, lock_expires_at=昨天12:00
       ② 现在 > lock_expires_at → 锁过期，抢锁成功，正常处理
```

### 为什么这两个时间都是 ~2 小时？特意对齐的

Glue Timeout（120 分钟）≈ lock_expires_at（2 小时）。这样 AWS 杀 Job 的瞬间，DynamoDB 锁差不多同时过期，下一轮 Job 不用等额外时间就能接手。

反例：如果 `lock_expires_at` 设成 24 小时，AWS 在 2 小时杀了 Job 之后，锁还要再挂 22 小时才过期——这期间所有后续 Job 都会被锁挡住。

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

### Snowflake 侧的链路与去重（含 BI 视图方案）

#### 1. 重复是怎么产生的：Snowpipe 是追加语义

Snowpipe 监听 S3 事件，看到新文件就 COPY 进表。它**不知道 S3 上的旧文件被覆盖了**——对它来说，"旧文件被删 + 新文件出现" = "一个新文件"，所以新文件的内容被原样追加到 `SILVER.DC_WIDE`。

```
Day 1 (4/21):
  Glue 写 s3://silver/dt=2026-04-20/store=ios/part-00000.parquet (预估值)
  Snowpipe COPY → SILVER.DC_WIDE 新增 1 行

Day 6 (4/26):
  Glue 删旧 part-00000.parquet，写新版本 part-00000.parquet (修正值)
  Snowpipe 看到"新文件" → 又 COPY 一次 → SILVER.DC_WIDE 又新增 1 行

→ 同一 (dt, product_id, ...) 组合现在有 2 行：一旧一新
```

#### 2. _loaded_at：判断新旧的依据

[03_silver_table.sql:24](snowflake_sql/03_silver_table.sql#L24) 定义了一列：

```sql
_loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
```

每次 Snowpipe COPY 一行，`_loaded_at` 自动写入"被 COPY 进来的时刻"。同一 PK 下，`_loaded_at` 大的就是新版本。

#### 3. ROW_NUMBER 与 rn：用 SQL 标记重复行

`rn` 是 `ROW_NUMBER() AS rn` 的别名（row_number = 行号）。逻辑：
- `PARTITION BY ...` —— 按指定列**分组**（同组内序号独立编号）
- `ORDER BY ... DESC` —— 组内从大到小排，最大的得 1，次大的得 2，依此类推

**举例**。假设 `SILVER.DC_WIDE` 有以下行（4/26 上游 restate 了 dt=04-20）：

| 行 | dt | product_id | app_store | country | device | downloads_total | _loaded_at |
|---|---|---|---|---|---|---|---|
| ① | 2026-04-20 | 12345 | ios | US | iphone | **1000** | 2026-04-21 10:05 |
| ② | 2026-04-20 | 12345 | ios | US | iphone | **1050** | 2026-04-26 10:05 |
| ③ | 2026-04-20 | 12345 | ios | US | ipad   | 500  | 2026-04-21 10:05 |
| ④ | 2026-04-19 | 12345 | ios | US | iphone | 800  | 2026-04-20 10:05 |
| ⑤ | 2026-04-20 | 67890 | android | DE | pixel | 300  | 2026-04-21 10:05 |

执行：

```sql
ROW_NUMBER() OVER (
  PARTITION BY dt, product_id, app_store, country, device
  ORDER BY _loaded_at DESC
) AS rn
```

**Step 1: PARTITION BY 分组**（5 列值都一样的行划到一组）

| 组 | 成员 | 组内行数 |
|---|---|---|
| A | ① + ② （04-20, 12345, ios, US, iphone） | **2 行 ← 有重复** |
| B | ③ （04-20, 12345, ios, US, ipad） | 1 行 |
| C | ④ （04-19, 12345, ios, US, iphone） | 1 行 |
| D | ⑤ （04-20, 67890, android, DE, pixel） | 1 行 |

**Step 2: 组内按 _loaded_at DESC 编号**

| 行 | downloads_total | _loaded_at | rn | 含义 |
|---|---|---|---|---|
| ② | 1050 | 4/26 10:05 | **1** | 组 A 最新版本 → 保留 |
| ① | 1000 | 4/21 10:05 | **2** | 组 A 老版本（被 restate）→ 该删 |
| ③ | 500  | 4/21 10:05 | **1** | 独苗 → 保留 |
| ④ | 800  | 4/20 10:05 | **1** | 独苗 → 保留 |
| ⑤ | 300  | 4/21 10:05 | **1** | 独苗 → 保留 |

**关键直觉**：
- `rn = 1` —— 我是这组里最新的（独苗也算最新），保留
- `rn >= 2` —— 这组里有更新版本，我是老的，**等价于"被 restate 覆盖掉的旧行"**

只要表里出现 `rn >= 2`，就 100% 说明 Snowpipe 重复 COPY 了同一 PK。

#### 4. 物理去重：Dedup Task

[06_dedup_task.sql](snowflake_sql/06_dedup_task.sql) 的核心：

```sql
DELETE FROM SILVER.DC_WIDE
WHERE (...) IN (
  SELECT ...,
    ROW_NUMBER() OVER (
      PARTITION BY dt, product_id, app_store, country, device
      ORDER BY _loaded_at DESC
    ) AS rn
  FROM SILVER.DC_WIDE
  WHERE dt >= DATEADD('day', -10, CURRENT_DATE())  -- 只看近 10 天
)
WHERE rn > 1;  -- 删 rn>=2 的旧版本
```

每天 UTC 06:00 跑。结合上面的例子，行 ①（rn=2，老的 1000）被删除，留下行 ②（rn=1，新的 1050）。

> **为什么是 10 天而不是 7 天**：Data.ai restate 窗口是 ±7 天，留 3 天缓冲，防止 Glue Job 延迟一两天的边界情况。

#### 5. 20 小时窗口问题

Dedup Task 是日级调度，不是实时的。这导致一段时间内 Silver 含重复数据：

```
4/26 10:00  上游修正 dt=2026-04-20 数据
4/26 10:05  Glue Silver Job 完成，S3 覆盖；Snowpipe COPY 完成
            → SILVER.DC_WIDE 出现重复（双倍行）
4/26 10:20  Gold Dynamic Table 自动刷新（按 TARGET_LAG=15min）
            → 在重复数据上做 SUM → BI 仪表盘看到 2x 异常下载量 ⚠️
4/27 06:00  Dedup Task 跑，删 rn>=2 的旧行
4/27 06:15  Dynamic Table 自动重算 → 数据回到正常

错误窗口: 4/26 10:05 ~ 4/27 06:00 ≈ 20 小时
```

#### 6. 解决方案：BI 直查视图 DC_WIDE_LATEST

业务可接受 Gold 层 20 小时延迟（日报场景），但**临时分析师直查 Silver 不能看到错的数据**。方案：建一个查询时去重的视图 [07_bi_view.sql](snowflake_sql/07_bi_view.sql)。

```sql
CREATE OR REPLACE VIEW SILVER.DC_WIDE_LATEST AS
SELECT ... FROM SILVER.DC_WIDE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dt, product_id, app_store, country, device
  ORDER BY _loaded_at DESC
) = 1;
```

`QUALIFY` 是 Snowflake 的语法糖：在 SELECT 之后过滤窗口函数结果。`QUALIFY rn = 1` 等价于"只保留每组最新行"。

**性能**：99% 时间 Dedup Task 已清理过表，几乎所有组都只有 1 行，QUALIFY 几乎零开销（实测延时差 < 200ms）。仅在 20 小时窗口期间需过滤少量重复行。

**给 BI 团队的指引**：
> 查 Silver 数据请用 `IODP_DC_<ENV>.SILVER.DC_WIDE_LATEST`，不要直查 `DC_WIDE`。前者已自动屏蔽 Restate 重复行。Gold 层（`GOLD.DC_DAILY_BY_*`）在 20 小时窗口内会显示重复后数据，业务方知悉即可。

#### 7. 为什么 Gold Dynamic Table 不用这个视图

Snowflake Dynamic Table 的核心优势是**增量刷新**——只重算变化的那部分聚合，性能极高。但**一旦 SQL 里出现 ROW_NUMBER 等窗口函数，Snowflake 无法做增量优化，会退化成全表重算**。

```
现状（直读 SILVER.DC_WIDE）:
  Silver 改了 dt=04-20 → 只重算 dt=04-20 的 SUM → ~10 秒/次
  日成本 ≈ $0.27

如果改成读 DC_WIDE_LATEST 视图:
  ROW_NUMBER 必须扫全表才能确定 rn → 全表重算 → ~5 分钟/次
  每 15 分钟一次 × 96 次/天 → 日成本 ≈ $5.30  ← 贵 20 倍
```

所以 Gold 层接受 20 小时滞后，保住增量刷新性能；BI 直查走视图，消除窗口期数据错误。**两层不同 SLA、不同方案。**

#### 8. 决策对比：为什么选视图方案而不是其他

| 方案 | 改动量 | BI 错误窗口 | Gold 错误窗口 | 成本影响 |
|------|--------|------------|--------------|---------|
| 现状（仅 Dedup Task） | 0 | 20 小时 | 20 小时 | 基线 |
| **视图方案（已选）** | +1 视图文件 | **0** | 20 小时（业务接受） | 几乎为 0 |
| Stream 触发 Task | 改 Task 配置 | 几分钟 | 几分钟 | warehouse 启动次数↑ |
| Glue 直接 MERGE | Glue + 凭据耦合 | 0 | 0 | 中 |
| Dynamic Table 读视图 | 改聚合 SQL | 0 | 0 | **20 倍 warehouse 费用** |

视图方案以**最小代价**消除了 BI 直查侧的错误，对现有 ETL 零侵入。

#### 9. 完整链路总览

```
S3 Bronze 覆盖 → S3 Silver 覆盖 → Snowpipe 追加（产生重复）
       ↓
SILVER.DC_WIDE
   ├── BI 直查 → 走 DC_WIDE_LATEST 视图（实时去重，0 延迟）
   └── Gold Dynamic Table → 直读 DC_WIDE（含重复，20 小时延迟接受）
       ↓
06:00 Dedup Task DELETE rn>=2 → SILVER 物理去重
       ↓
06:15 Dynamic Table 自动增量刷新 → Gold 数据正确
```

#### 10. 实施清单

- [x] 新增 [snowflake_sql/07_bi_view.sql](snowflake_sql/07_bi_view.sql)：定义 `SILVER.DC_WIDE_LATEST` 视图，授权给 `IODP_DC_TRANSFORM_${ENV}` / `IODP_DC_READER_${ENV}`
- [x] [Makefile:72](Makefile#L72) 注释 "Run Snowflake SQL (01-06)" → "(01-07)"
- [x] `scripts/apply_snowflake_sql.sh` 自动按文件名排序执行 `[0-9]*.sql`，下次 `make apply-snowflake-sql` 时新视图自动部署
- [ ] 通知 BI 团队改用 `DC_WIDE_LATEST`（部署后操作）

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

### 什么是 backfill：历史数据回填

"backfill" 在数据工程里 = **历史数据回填**。日常 Job 只处理"昨天/今天"的增量数据，backfill 是**一次性把过去某段时间的数据全部跑一遍**。

#### 在这个项目里，backfill 有三种典型场景

| 场景 | 触发原因 | 数据量级 |
|---|---|---|
| ① **首次上线** | 新部署到 prod，要把 Data.ai 历史几个月/几年的数据一次性灌进 Bronze/Silver | 日常的几十~几百倍 |
| ② **修复历史错误** | 发现 3 个月前某个字段解析有 bug，需要重跑那段时间所有分区 | 几十天 × 每天数据量 |
| ③ **超大 restate** | Data.ai 偶发一次性更正大范围历史（不只是 ±7 天窗口） | 视范围而定 |

#### 为什么 backfill 要把 DPU 从 10 调到 20

日常 Job 处理 1 天数据（约几亿行），10 DPU 跑 10-20 分钟够用。Backfill 一次要处理几十/几百天的数据：
- **不调 DPU** → Job 跑几十小时 → 撞上 Glue Timeout = 120 分钟（见 §e.2）→ 被强杀
- **调到 20 DPU** → 并行度翻倍，能在 Timeout 内跑完

#### 怎么"临时"调（关键设计）

通过 Makefile 参数传给单次 Job 启动，**Job 定义本身的默认值仍然是 10**：

```bash
make run-bronze-backfill DPU=20 START_DATE=2025-01-01 END_DATE=2025-03-31
```

跑完后下一次日常调度自动用回 10 DPU，不会留下高成本配置。这就是"成本护栏"的本意：**默认收紧，临时放开，跑完自动回落**——避免一次性把 Job 改大后忘记改回来导致每天都按 backfill 规模烧钱。

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

### 前置概念：Snowflake "仓库" 是计算引擎，不是数据库

这是 Snowflake 计费模型最反直觉的地方。Snowflake 把"存数据"和"算数据"拆成两层：

| 层 | 是什么 | 计费方式 | 一直在吗 |
|---|---|---|---|
| **存储层**（Database/Schema/Table） | 表、视图、数据本体（在 S3 上） | 按 GB/月 | ✅ 一直在，永远能访问元数据 |
| **计算层**（Warehouse） | 一组虚拟机（vCPU + RAM） | **按运行秒数 × 仓库大小** 计费 | ❌ 默认不开，用时启动 |

`SILVER.DC_WIDE` 这张表的数据**永远在** Snowflake 后台的 S3 上躺着，谁都能看到它"存在"。但你想 **跑 SELECT/INSERT/聚合** 时，Snowflake 必须分配一组机器去做扫描和计算——这组机器就是 Warehouse。**机器在跑 = 烧钱**。

跟传统数据库（MySQL/PostgreSQL）的区别：
- 传统数据库：服务器你自己买/租，24 小时常驻，成本固定
- Snowflake：SaaS 按计算时长收费，**默认应当"用完休眠"**，查询来了再 1-3 秒拉起来

### "仓库开着" = 那组虚拟机在运行，不管有没有 SQL 在跑

```
9:00:00.0  你跑了一条 SELECT，0.5 秒返回结果
9:00:00.5  ← SQL 跑完了，但仓库还在运行（等下一条 query）
9:01:00.5  ← 仍在运行（已空跑 60 秒）
9:01:00.5  ← AUTO_SUSPEND=60s 触发 → 仓库休眠

→ 这一段你被收了 60 秒的钱，不是 0.5 秒
```

如果 `AUTO_SUSPEND` 不设或设得很大，仓库就**永远不睡**——即使一整天没人查询，仍然按"24 小时 × 仓库大小"计费。

### 配置含义

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

### "BI 临时查询不会等很久吗？"

不会。XS 仓库**冷启动 1-3 秒**，分析师点查询按钮几乎察觉不到延迟。这就是 Snowflake 的核心卖点——**休眠不等于宕机，是"按需启动"**。所以"用完就睡"是默认推荐姿势，不是性能妥协。

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

## f) DLQ（死信区）的结构与重消费

### 这是什么

DLQ = Dead Letter Queue。Bronze/Silver Job 任何一种"这一批数据没法正常入库"的情况，都把**失败的数据**和**一份事故说明** (`.error.json`) 同时写到 DLQ S3 前缀。两个用途：

1. **取证**：dropzone 的源文件可能受 TTL 影响过几个月就消失，DLQ 保下副本，事后能查
2. **重消费**：修完问题后，把 DLQ 里的数据按一定流程塞回管道重跑

### DLQ 的三类入口

| 来源 | 触发条件 | error_type | 写法 |
|---|---|---|---|
| Bronze schema 校验 | 列名/类型对不上 | `SCHEMA_MISMATCH` | `copy_to_dlq`（拷源文件）+ `write_dlq_error_json`（写说明） |
| Bronze/Silver 异常兜底 | try/except 接住的任何抛错 | `PROCESSING_ERROR` / `SILVER_PROCESSING_ERROR` | 仅 `write_dlq_error_json`（源文件还在原桶，不复制） |
| Silver DQ 阻断 | §b 的 DQ Check 任一阻断项 | `DQ_BLOCK` | `write_dlq_dataframe`（把整批失败 DataFrame 写成 parquet） |

### DLQ key 结构

```
s3://<dlq-bucket>/dead_letter/failed_at=<today>/<完整源路径>[.error.json]
```

举例：

```
源:    s3://dropzone/download_channel/narrow/dt=2025-12-15/store=ios/part-00000.parquet
副本:  s3://dlq/dead_letter/failed_at=2026-04-27/download_channel/narrow/dt=2025-12-15/store=ios/part-00000.parquet
说明:  s3://dlq/dead_letter/failed_at=2026-04-27/download_channel/narrow/dt=2025-12-15/store=ios/part-00000.parquet.error.json
```

### 为什么 key 要这样设计

最初的实现是 `dead_letter/<today>/<filename>`，把源 key 剥到只剩文件名，会出两个硬伤：

#### 问题 1: 分区信息全丢

`dead_letter/2026-04-27/part-00000.parquet` 只能告诉你"今天 fail 的"，没法回答：是哪天（`dt=?`）的数据？哪个 store？而修复后的 key 自身就携带这些信息，list S3 直接 `--prefix .../dt=2025-12-15/` 即可锁定一批。

#### 问题 2: 文件名碰撞 → 静默丢数据

Spark 写出来的 part 文件名都长一个样（`part-00000-xxx.snappy.parquet`）。两个不同分区同一天 fail：

```
源 A: .../dt=2025-12-15/store=ios/part-00000.parquet
源 B: .../dt=2025-12-15/store=google-play/part-00000.parquet
```

旧设计两条都拼成 `dead_letter/2026-04-27/part-00000.parquet`，**第二条 `copy_object` 直接把第一条覆盖**——第一份失败数据无声消失。修复后两条 key 各自带完整源路径，互不撞。

#### 问题 3: failed_at 和 dt= 必须解耦

`failed_at=` 是失败发生当天，`dt=` 是数据本身的业务日期。两者经常错开：

- 日常 `failed_at ≈ dt + 1 天`
- Backfill 重跑 2025-Q4 旧数据失败：`failed_at=2026-04-27`，`dt=2025-10-xx`
- §c 的 Restate 修 7 天前的数据失败：`failed_at` 是今天，`dt` 是一周前

把两个日期各自显式写在 key 里，比埋成单一字符串日期清晰得多。

### error.json 的内容

每份失败数据旁边都有一份说明。格式：

```json
{
  "error_type": "SCHEMA_MISMATCH",
  "error_message": "Missing columns: {'share_pct'}",
  "timestamp": "2026-04-27T10:23:11.482312+00:00",
  "job_run_id": "glue-jr-abc123",
  "source_file": "s3://dropzone/download_channel/narrow/dt=2025-12-15/store=ios/part-00000.parquet",
  "original_key": "download_channel/narrow/dt=2025-12-15/store=ios/part-00000.parquet",
  "extra": null
}
```

字段速查：

| 字段 | 用处 |
|---|---|
| `error_type` | 枚举式分类（`SCHEMA_MISMATCH` / `PROCESSING_ERROR` / `SILVER_PROCESSING_ERROR` / `DQ_BLOCK`），便于批量过滤"先解决某一类" |
| `error_message` | 人话说的具体原因（schema diff / 异常字符串等） |
| `timestamp` + `job_run_id` | 出问题时去 CloudWatch 拉这次 Job Run 的完整 stack |
| `source_file` / `original_key` | 溯源；DLQ key 已经携带这些信息，这里冗余记录便于程序化解析 |
| `extra` | 可选 dict，调用方可塞额外上下文（schema diff、命中的 DQ 规则名、行数等），目前未使用 |

### 重消费流程

DLQ 不是"自动回放队列"。要把数据塞回管道有几条路，选哪条取决于失败原因：

#### 路径 1: 上游本身就是坏数据 → 等上游 restate

最常见。Data.ai 给错的数据，让上游下次 PUT 修正版到 dropzone 同路径。新文件 MD5 不同 → §c 的 MD5 比对自动触发覆盖处理。**完全不需要碰 DLQ**，DLQ 只是事后查证的留底。

#### 路径 2: 我们的代码有 bug → 修代码 + BACKFILL_MODE 重跑

修完 bug 后，**dropzone 文件没动、MD5 没变**——日常路径会被 §c 的 MD5 比对跳过。要强制重跑，启动 Bronze Job 时带：

```
BACKFILL_MODE=true TARGET_DT=2025-12-15 TARGET_STORE=ios
```

[bronze_etl.py](glue/bronze_etl.py) 里 `BACKFILL_MODE=true` 就是用来**绕过 MD5 比对、强制重处理**指定分区的开关。

#### 路径 3: 数据被 dropzone TTL 删了 → 从 DLQ 拷回

如果原数据已被 dropzone 生命周期策略删掉（例如几个月前的），从 DLQ 拷回：strip 掉 `dead_letter/failed_at=YYYY-MM-DD/` 前缀，剩下的就是要恢复到 dropzone 的相对路径，然后走路径 2。

### 和 §c restate 的关系

路径 1 走的就是 §c 的 restate 路径（MD5 不同 → 覆盖处理）。路径 2、3 用 `BACKFILL_MODE` 显式绕过 MD5 检查。三条路最后都汇到同一套 Bronze→Silver 写入逻辑，DLQ 不引入额外 ETL 入口。

---

## g) 整体架构理解（端到端追踪 + 组件分工澄清）

> 这一节不是新机制，而是把 §a-§e 串起来回答几个高频疑问：整条链路是怎么触发的？DynamoDB 到底干了哪些活？Bronze 和 Silver 是什么关系？restate 数据怎么一行一行流过整个链路的？

### 1. 触发机制：链路上有 5 种不同的触发方式

很多人会以为整条链路都是"S3 文件来了 → 自动触发下游"。**其实不是**。S3 ObjectCreated 事件只用在 Silver→Snowpipe 这一段。

| 链路环节 | 触发机制 | 备注 |
|---|---|---|
| Data.ai → dropzone | 外部 PUT | 不归我们管 |
| **Bronze Job** | **EventBridge Cron（每日 UTC 10:00）** | 不是 S3 事件！批处理 Job 必须按定时扫整个分区，不能每个文件触发一次 |
| **Silver Job** | **Glue Workflow 串联（Bronze 成功后）** | 不是 S3 事件！由 Workflow 在 Bronze 成功结束时自动拉起 |
| **Snowpipe → SILVER.DC_WIDE** | **S3 ObjectCreated → SNS → Pipe** | ✅ 这里才是 S3 事件，Silver 桶有新 parquet 时实时触发 |
| **Gold Dynamic Table** | **Snowflake 内部 TARGET_LAG 机制** | Snowflake 自己监视 SILVER 表的变化，不看 S3 |
| **Dedup Task** | **Snowflake CRON（每天 06:00）** | 不是 S3 事件 |
| **DLQ 周报 Lambda** | **EventBridge Cron（周一 09:00）** | 不是 S3 事件 |
| **DLQ / 锁 / DQ 即时告警** | **Glue Job 内代码主动 SNS publish** | 代码里调 `send_alert()` |
| **CloudWatch Alarms** | **CloudWatch Metric 阈值** | 看 Glue Job 失败次数指标 |

**触发链路一图**：

```
                     [EventBridge Cron 每日 10:00]
                                │
                                ▼
                     ┌─────────────────────┐
                     │  Bronze Glue Job    │
                     └──────────┬──────────┘
                                │ (Glue Workflow 串联，不是 S3 事件)
                                ▼
                     ┌─────────────────────┐
                     │  Silver Glue Job    │← 写完产生 S3 ObjectCreated
                     └──────────┬──────────┘
                                │ ★ S3 ObjectCreated → SNS → Snowpipe
                                ▼
                     ┌─────────────────────┐
                     │  Snowpipe COPY INTO │
                     └──────────┬──────────┘
                                │ (Snowflake 内部 TARGET_LAG)
                                ▼
                     ┌─────────────────────┐
                     │ Gold Dynamic Tables │
                     └─────────────────────┘
```

整条链路里 **★ 只有 Silver→Snowpipe 这一段**真正用 S3 ObjectCreated。其他都是 cron / workflow 串联 / 代码主动调用 / Snowflake 内置机制。

### 2. DynamoDB 的三大用途（不只是比对 MD5）

DynamoDB checkpoint 表（`iodp-dc-checkpoint-<env>`）实际承担 **3 件事**，三个用途共用同一条记录：

| 用途 | 用到的字段 | 解决什么问题 | 在哪一节 |
|---|---|---|---|
| ① **并发锁** | `status`, `lock_expires_at` | 防止"昨天的活没干完，今天又来一批"导致同一分区被两个 Job 同时处理 | §a |
| ② **Restate 检测** | `file_md5s` | 比对源文件 MD5 → 决定是否覆盖写 | §c |
| ③ **审计 / 血缘** | `input_files`, `in_count`, `out_count`, `dlq_count`, `job_run_id`, `last_processed_at` | 出问题时回溯：什么时候、由哪个 Job Run、读了哪些文件、写出多少行、丢了多少进 DLQ | §d 监控告警的输入 |

只看 ②（MD5）就漏掉了"防并发"和"审计"两层。

### 3. Bronze 和 Silver 的关系：同骨架，独立状态，工作流串联

#### 相同的骨架代码模式

两个 Job 的执行流程是**同一套模板**：

```
1. 读 DynamoDB → MD5 比对 → 决定要不要处理
2. 抢并发锁（写 status=running, lock_expires_at）
3. 删旧 parquet → 写新 parquet（覆盖原位置）
4. 更新 DynamoDB（释放锁、记录行数）
5. 失败 → 写 DLQ + send_alert
```

DynamoDB 里两层是**完全独立的两条记录**：

```
bronze#2026-04-25#ios   ← Bronze Job 自己的状态
silver#2026-04-25#ios   ← Silver Job 自己的状态
                        ← 两者互不感知，各自维护自己的锁和 MD5
```

#### 不同的具体处理

| 步骤 | Bronze Job | Silver Job |
|---|---|---|
| 读 | dropzone csv.gz / parquet | Bronze v1 或 v2 parquet |
| 转换 | csv → parquet，schema 校验，类型规范化，按 PK 去重 | **窄表 v1 → 宽表 pivot**（v2 透传），算 paid_share / featured_share |
| 校验 | schema 列名 / 类型 | **5 项 DQ Check**（行数、空值率、日期范围、负数、等式） |
| 写入路径 | `bronze/v1/...` 或 `bronze/v2/...`（保留 schema 版本） | `silver/...`（统一宽表，不分 v1/v2） |
| Schema 输出 | 跟 Data.ai 给的一样 | **统一为宽表 schema**，下游全部基于这套列 |

#### 精确说法

- **代码独立** ✓：[glue/bronze_etl.py](glue/bronze_etl.py) 和 [glue/silver_etl.py](glue/silver_etl.py) 是两个独立文件
- **状态独立** ✓：DynamoDB 两条记录互不引用
- **运行时独立** ✓：两个 Glue Job，各自独立的 DPU、Timeout、IAM Role
- **执行不独立** ⚠️：通过 Glue Workflow 串联，**Bronze 成功才触发 Silver**；Silver Job **读取的是 Bronze 写出的 parquet**

更准确的描述：**两层各管一段，互不踩对方的状态，但 Silver 消费 Bronze 的输出**。

### 4. 端到端数据流追踪（Restate 场景）

> 这一节回答："restate 来了，dropzone / Bronze / Silver / Snowflake / Gold 各层的状态怎么变？"

也澄清一个常见误解：**dropzone 的 `dt=` 分区是"业务日期"，不是"上传日期"**。4/26 上游修正 4/24 数据时，是直接 PUT 覆盖 `dt=2026-04-24/` 这个老文件夹，**不会**把 4/24 的行塞进 `dt=2026-04-25/` 文件夹。restate 检测靠 DynamoDB 里 file_md5 比对——同路径文件 MD5 变了 → 知道是新版本。

**追踪一行**：`(product_id=12345, app_store=ios, country=US, device=iphone, dt=2026-04-24)`，下载量 4/25 上报 1000，4/26 修正为 1050。

#### Day 1（4/25 上午）— 首次处理 4/24 数据

| 时刻 | 操作 | 影响 |
|---|---|---|
| 4/25 10:00 | Data.ai PUT `dropzone/wide/dt=2026-04-24/store=ios/wide.csv.gz`（MD5=aaa111，downloads=1000） | dropzone 有 1 个文件 |
| 4/25 10:00 | EventBridge 触发 Bronze Job → 查 DynamoDB `bronze#2026-04-24#ios` 不存在 → 处理 | DynamoDB 写入 file_md5=aaa111, status=succeeded |
| 4/25 10:01 | Bronze 写 `bronze/v2/dt=2026-04-24/store=ios/part-00000.snappy.parquet`（downloads=1000） | Bronze S3 有 1 个文件 |
| 4/25 10:02 | Silver Job 由 Workflow 拉起 → DQ 通过 → 写 `silver/.../dt=2026-04-24/store=ios/part-00000.snappy.parquet` | Silver S3 有 1 个文件，触发 ObjectCreated |
| 4/25 10:05 | Snowpipe 自动 COPY INTO → 表里新增 1 行 `{downloads_total=1000, _loaded_at='2026-04-25 10:05'}` | SILVER.DC_WIDE 1 行 |
| 4/25 10:20 | Gold Dynamic Table 自动刷新 | GOLD.DC_DAILY_BY_APP 1 行(1000) |

**Day 1 结束状态**：每一层都是一致的 1000 ✓

#### Day 2（4/26 上午）— 4/24 数据被 restate

| 时刻 | 操作 | 影响 |
|---|---|---|
| 4/26 10:00 | Data.ai PUT 覆盖 `dropzone/wide/dt=2026-04-24/store=ios/wide.csv.gz`（**MD5=bbb222**，downloads=**1050**） | dropzone 同路径文件被覆盖 |
| 4/26 10:00 | Bronze Job 启动 → DynamoDB file_md5='aaa111' vs 当前 dropzone MD5='bbb222' → **检测到 restate** | 决定要重处理 |
| 4/26 10:01 | Bronze **DELETE** 旧 `bronze/v2/.../part-00000.snappy.parquet`（物理消失）→ 写新 parquet（1050） | Bronze S3 旧文件已删，新文件 1050 |
| 4/26 10:02 | Silver Job 同理 → DELETE 旧 Silver parquet → 写新 Silver parquet（1050） | Silver S3 旧文件已删，新文件 1050 |
| 4/26 10:05 | 🔥 Snowpipe 看到"新文件出现"（**它不订阅 ObjectRemoved**）→ COPY INTO → **新增 Row #2** `{downloads_total=1050, _loaded_at='2026-04-26 10:05'}` | SILVER.DC_WIDE **变成 2 行**（1000 + 1050）⚠️ |
| 4/26 10:20 | Gold Dynamic Table 重算 → SUM(1000+1050) | GOLD.DC_DAILY_BY_APP 显示 **2050（错！）** ⚠️ |
| 4/26 起 | BI 直查 SILVER.DC_WIDE_LATEST 视图 | QUALIFY rn=1 过滤 → 看到 1050 ✓ |
| 4/27 06:00 | Dedup Task 跑 → DELETE WHERE rn>1 → 删 Row #1 | SILVER.DC_WIDE 只剩 1 行(1050) |
| 4/27 06:15 | Dynamic Table 自动重算 | GOLD.DC_DAILY_BY_APP 1050 ✓ |

### 5. 核心洞察：S3 物理删除 ≠ Snowflake 数据删除

| 操作 | S3 状态 | Snowflake SILVER.DC_WIDE 状态 |
|---|---|---|
| 4/25 首次处理 | 1 个 parquet（1000） | 1 行（1000） |
| 4/26 Glue 删旧 | 0 个 parquet | **1 行（1000）依然在！** Snowflake 不感知 S3 删除 |
| 4/26 Glue 写新 | 1 个 parquet（1050） | 仍是 1 行（1000） |
| 4/26 Snowpipe COPY | 1 个 parquet（1050） | **2 行：1000 + 1050** ⚠️ |
| 4/27 06:00 Dedup Task | 1 个 parquet（1050） | 1 行（1050）✓ |

**核心**：Glue 在 S3 上的"覆盖"只覆盖 S3 文件，**Snowflake 表里旧行是 4/25 那次 COPY 留下的"快照"，跟 S3 文件无关，必须靠 Dedup Task 主动删**。

这就是为什么需要：
- §c 的 Dedup Task（每天物理去重）
- §c.6 的 DC_WIDE_LATEST 视图（窗口期 BI 兜底）
- §d.4 的 Snowpipe 延迟告警（COPY 失败时的兜底）

### 6. 一句话回答几个常见问题

| 问题 | 答案 |
|---|---|
| 整条链路都是 S3 事件触发的吗？ | 不是。只有 Silver→Snowpipe 是 S3 事件。Bronze/Silver 是定时调度，Gold 是 Snowflake 内部机制。 |
| DynamoDB 只是用来比对 MD5 吗？ | 不是。还兼任并发锁（§a）和审计血缘（§d）。 |
| Bronze 和 Silver 是不是一个 Job？ | 不是。两个独立 Glue Job，独立代码，独立 DynamoDB 状态，独立锁。靠 Glue Workflow 串联。 |
| Restate 来了，Snowflake 怎么"过去的数据变了"？ | S3 覆盖只删 S3 文件，Snowflake 旧行不会自动消失。两步：① Snowpipe 立即灌新行（产生重复）；② 次日 06:00 Dedup Task 删旧行。20 小时窗口期 BI 走 DC_WIDE_LATEST 视图兜底。 |
| Gold 表为什么不用 DC_WIDE_LATEST 视图？ | ROW_NUMBER 会破坏 Dynamic Table 的增量刷新，让 warehouse 成本变 20 倍。Gold 可接受 20 小时延迟，BI 直查走视图。 |
| Restate 文件放在 dropzone 哪里？ | 直接覆盖原 `dt=YYYY-MM-DD/` 文件夹（按业务日期分区），不会塞进当天的文件夹。MD5 变了就触发 Bronze 重处理。 |

---

## h) 锁超时 Lambda 的实现细节：DynamoDB Scan 与分页

> 这一节展开 [lambda/stale_lock_check/handler.py](lambda/stale_lock_check/handler.py) 里两处容易踩坑的实现细节：(1) 用 `Scan` + `FilterExpression` 找 stale 锁的成本结构；(2) `LastEvaluatedKey` 分页机制。

### 1. `Scan` + `FilterExpression` 的成本结构

代码片段：

```python
scan_kwargs = {
    "FilterExpression": "#s = :running AND lock_expires_at < :now",
    ...
}
while True:
    resp = table.scan(**scan_kwargs)
    stale.extend(resp.get("Items", []))
    if "LastEvaluatedKey" not in resp:
        break
    scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
```

三个关键事实让这种用法在表变大后会变贵、变慢：

1. **`Scan` 是真·全表扫描**：从第一条 item 顺序读到最后一条，每个 item 都从存储里加载到服务端
2. **`FilterExpression` 是"读完再过滤"**：服务端读出全部 item 之后才丢弃不匹配的；**不匹配的 item 也按 RCU 计费**
3. **计费基准是 `ScannedCount`（扫描量），不是 `Count`（返回量）**：哪怕过滤后 0 行返回，你也付了"全表的钱"

#### 当前规模实测推算

| 维度 | 数值 |
|---|---|
| partition_key 形态 | `bronze#2026-04-25#ios` / `silver#2026-04-25#ios` 等 |
| 每天新增行数 | ~4 行（bronze/silver × ios/android） |
| 一年 | ~1460 行 |
| 三年 | ~4400 行 |
| 单 item 大小 | ~500 字节 |
| 三年整表 | ~2 MB |
| 单次 Scan RCU | ~250（On-Demand） |
| 单次 Scan 费用 | ~$0.0000625 |
| 每 30 分钟扫一次的月费 | ~$3 |

**结论**：当前规模成本不疼。但分区数会**线性增长**（除非 TTL 清理），三年后单次扫描会从 1 次 API 调用变成多次分页。

#### 教科书优化：稀疏 GSI（Global Secondary Index）

DynamoDB GSI 的关键性质：**只索引"含有该字段"的 item**。如果代码在 Job 成功后**删除** `status` 字段（而不是写 `status='succeeded'`），GSI 里就只剩"当下在跑"的几行：

```
主表（4400 items）:
  bronze#2024-12-01#ios   (无 status 字段)                              ← 不进 GSI
  bronze#2024-12-01#and   (无 status 字段)                              ← 不进 GSI
  ...
  bronze#2026-04-26#ios   status=running, lock_expires_at=2026-04-26T12:00Z   ← 进 GSI
  silver#2026-04-26#ios   status=running, lock_expires_at=2026-04-26T12:00Z   ← 进 GSI

status-GSI（通常 0-4 行）:
  bronze#2026-04-26#ios   status=running   ...
  silver#2026-04-26#ios   status=running   ...
```

Lambda 改成：

```python
table.query(
    IndexName='status-index',
    KeyConditionExpression='#s = :running',
    FilterExpression='lock_expires_at < :now',
    ExpressionAttributeNames={'#s': 'status'},
    ExpressionAttributeValues={':running': 'running', ':now': now_iso},
)
```

无论主表多大，扫描量恒等于"当前 running 的分区数"（通常 0-4 个）。

**当前不必动**——成本和延迟都还在可接受范围；如果将来分区数过万再加 GSI。

### 2. `LastEvaluatedKey` 分页机制

#### 为什么需要分页

DynamoDB `Scan` / `Query` 有**硬限制**：单次响应最多返回 1 MB 数据（不可调）。这是平台限制，防止单次调用占用过多后端资源、阻塞太久、撑爆客户端内存。

如果表大于 1 MB，**单次 scan 拿不全**——必须分多次调用拼起来。

#### `LastEvaluatedKey` 是什么

把它想成"读书读到哪儿了的书签"：

- DynamoDB 扫到 1 MB 上限时停下，**记下当前扫到哪个 item**
- 把这个位置（其实就是该 item 的主键）打包成一个不透明 dict 放在响应里，字段名叫 `LastEvaluatedKey`
- 客户端下一次 scan 把它当作 `ExclusiveStartKey` 传回去，意思是"**从这个 item 之后**继续扫"（exclusive = 不含这个 item 自己）
- 扫到表尾时，响应里**不会有 `LastEvaluatedKey` 字段** → 客户端知道扫完了

#### 一个具体例子

假设 checkpoint 表有 10000 个 item、~500 字节/个、共 ~5 MB（虚构场景，便于看清分页）：

```
第 1 次 table.scan(FilterExpression=...):
  服务端扫 partition_key='bronze#2024-01-01#ios' .. 'bronze#2024-12-31#and'（~2000 个，~1 MB）
  Filter 应用: status='running' AND lock_expires_at < now → 这一段全是历史完成的，0 个匹配
  响应:
    Items:            []                                     ← 过滤后 0 个
    Count:            0                                       ← 返回数
    ScannedCount:     2000                                    ← 实际扫了 2000 个（计费基准！）
    LastEvaluatedKey: {"partition_key": "bronze#2024-12-31#and"}   ← 还没扫完的书签

第 2 次 table.scan(ExclusiveStartKey={'partition_key': 'bronze#2024-12-31#and'}, ...):
  从书签之后开始扫又一段 ~1 MB
  响应:
    Items:            [{"partition_key": "bronze#2025-06-15#ios", ...}]  ← 命中 1 个 stale
    LastEvaluatedKey: {"partition_key": "bronze#2025-06-30#and"}

... 重复第 3、第 4 次 ...

第 5 次 table.scan(ExclusiveStartKey=...):
  这次扫到表尾
  响应:
    Items:            []
    ScannedCount:     2000
    (没有 LastEvaluatedKey 字段！)

  代码里:
    if "LastEvaluatedKey" not in resp:
        break       # ← 这里跳出 while True

最终 stale 列表 = 5 次响应的 Items 全拼起来
```

#### 代码逐行映射

```python
stale = []
scan_kwargs = {"FilterExpression": ..., ...}

while True:
    resp = table.scan(**scan_kwargs)              # 单次最多读 1 MB
    stale.extend(resp.get("Items", []))           # 累加这一页的过滤结果
    if "LastEvaluatedKey" not in resp:            # 没有书签 = 已扫到表尾
        break
    scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]
                                                  # 把书签塞进下一次 scan 的参数
```

这是 DynamoDB Scan/Query 的**强制 idiom**，不分页就是 bug。

#### 两个新手陷阱

**陷阱 A：只拿第一页就完事**

```python
resp = table.scan(...)
stale = resp.get("Items", [])    # ← bug：只是第一页
```

- 测试环境表小（< 1 MB）：偶然能 work，看起来正常
- prod 表长大后：**只检查前 1 MB，后面的 stale 锁悄无声息漏掉**——告警逻辑还在跑、还偶尔报告，只是不全。这种"半静默 bug"很难被发现。

**陷阱 B：用 `Items` 判断有没有更多页**

```python
while True:
    resp = table.scan(**scan_kwargs)
    if not resp["Items"]:    # ← bug：这一页过滤后是空，不代表表扫完了
        break
    ...
```

`ScannedCount`（实际扫的）和 `Count`（过滤后的）是两个不同概念。一页可能 `ScannedCount=2000` 但 `Count=0`（这一段根本没 stale 锁），表后面还有数据要扫。**判断"扫完了"唯一可靠的信号就是 `LastEvaluatedKey` 缺失**。

当前代码用的就是 `if "LastEvaluatedKey" not in resp` 这个正确写法。

#### 三个 count 字段的区分

| 字段 | 含义 | 用途 |
|---|---|---|
| `ScannedCount` | 这次实际扫了多少 item | 计费基准 |
| `Count` | 过滤后剩多少 item | == `len(Items)` |
| 累加的 `len(stale)` | 所有页累计的过滤后命中 | 业务用的最终结果 |

---

## i) Snowflake Storage Integration（机制详解 + 为什么删掉了 SQL 文件）

> ⚠️ **历史变更（2026-04-30）**：原 `snowflake_sql/02_storage_integration.sql` **已删除**。
> 此对象**唯一**由 Terraform 管理 → [terraform/modules/snowflake/main.tf:201](terraform/modules/snowflake/main.tf#L201) 的 `snowflake_storage_integration.s3_int` 资源。
> 删除原因见本节末尾"为什么删除 02_storage_integration.sql"小节。
>
> 本节保留 Storage Integration 的机制讲解，知识用于理解 Terraform 那个资源在做什么。

### 这是什么

Storage Integration = Snowflake 里的一个**对象**，作用是让 Snowflake **安全地访问 S3**——不用在 SQL 里硬写 AWS access key / secret key。它是 Snowpipe 和 External Stage 能去 S3 拉 parquet 的"通行证"。

整条链路里它所处的位置：

```
[Glue Silver Job 写 S3 parquet]
        ↓
[S3 ObjectCreated → SNS]
        ↓
[Snowpipe]   ← 这一步要用 Terraform 创建的 Storage Integration
        ↓ AssumeRole 拿临时凭据
[GET S3 parquet → COPY INTO SILVER.DC_WIDE]
```

### 生活类比

你（Snowflake）要去隔壁公司（AWS）的仓库（S3 桶）取货。两种取货方式：

- **方式 A（不安全）**：你随身揣着一把万能钥匙，谁见到都能复制 → 等价于把 AWS access key 写在 SQL 里
- **方式 B（安全）**：隔壁公司给你办一张"访客身份证"+ 一个"暗号"。每次去取货时出示身份证 + 念暗号，对方核对没问题才开门 → 这就是 IAM Role + External ID 模式

Storage Integration 就是这张"访客身份证 + 暗号"在 Snowflake 这一侧的存根。

### Terraform 资源逐字段拆解（[terraform/modules/snowflake/main.tf:201](terraform/modules/snowflake/main.tf#L201)）

```hcl
resource "snowflake_storage_integration" "s3_int" {
  name                      = "IODP_DC_S3_INT_${local.env_upper}"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_allowed_locations = ["s3://${var.silver_bucket_name}/"]
  storage_aws_role_arn      = var.snowpipe_iam_role_arn != "" ? var.snowpipe_iam_role_arn : "arn:aws:iam::000000000000:role/placeholder"
  comment                   = "S3 integration for Silver bucket — ${var.environment}"
}
```

| 字段 | 含义 | 类比 |
|---|---|---|
| `IODP_DC_S3_INT_${ENV}` | Integration 的名字 | 访客身份证的"卡号" |
| `TYPE = EXTERNAL_STAGE` | 用途：给 External Stage / Snowpipe 用 | 身份证的用途："仓库取货" |
| `STORAGE_PROVIDER = 'S3'` | 对接 AWS S3（也可对接 Azure / GCS） | 仓库在哪家公司 |
| `ENABLED = TRUE` | 启用 | 身份证当前有效 |
| `STORAGE_AWS_ROLE_ARN` | AWS 那边为 Snowflake 准备的 IAM Role ARN | "对方公司给我办的角色编号" |
| `STORAGE_ALLOWED_LOCATIONS` | 这张通行证只允许访问这一个 S3 桶/前缀 | 身份证只能进 1 号仓库，不能进 2 号 |

### 为什么要 `STORAGE_ALLOWED_LOCATIONS`

**最小权限原则的双保险**。即使 AWS IAM Role 给的权限范围过宽（比如能访问 5 个桶），Snowflake 这一侧仍然只允许这个 Integration 走到 silver 桶。哪怕有人用这个 Integration 去 `CREATE STAGE URL='s3://别的桶/'`，Snowflake 会在 SQL 层直接拒绝。

### 创建之后的"对接动作"——Snowflake 自动生成两个值，要回贴到 AWS

创建 Storage Integration 时（不管是 `CREATE STORAGE INTEGRATION` 还是 `terraform apply`），Snowflake 会**自动生成**两个值：

| 自动生成的值 | 作用 |
|---|---|
| `STORAGE_AWS_IAM_USER_ARN` | Snowflake 那一侧的"内部 IAM User ARN"，每个 Snowflake 账号唯一 |
| `STORAGE_AWS_EXTERNAL_ID` | 随机生成的"暗号"，每次重建对象都会变 |

这两个值必须贴回 AWS 那个 IAM Role 的 Trust Policy 里，AWS 才认 Snowflake 来 AssumeRole：

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "<贴 STORAGE_AWS_IAM_USER_ARN>" },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": {
        "sts:ExternalId": "<贴 STORAGE_AWS_EXTERNAL_ID>"
      }
    }
  }]
}
```

**纯手工模式下**怎么拿这两个值：在 Snowflake 跑 `DESC INTEGRATION IODP_DC_S3_INT_<ENV>`，抄出来再去 AWS Console 改 Trust Policy。这就是被删掉的 02_storage_integration.sql 注释里说的流程。

**Terraform 模式下**完全不需要人参与：`snowflake_storage_integration` 资源的 `storage_aws_iam_user_arn` 和 `storage_aws_external_id` 是 **resource attribute（输出属性）**，[main.tf:156-157](terraform/main.tf#L156-L157) 直接把它们喂给 snowpipe module，[snowpipe/main.tf:23,27](terraform/modules/snowpipe/main.tf#L23) 的 IAM Role Trust Policy 直接消费。`terraform apply` 一次性闭环。

### Terraform 一次 apply 怎么打破"鸡生蛋蛋生鸡"

表面上的循环依赖：

- AWS IAM Role 的 Trust Policy 需要 Snowflake 的 IAM_USER_ARN + ExternalId
- Snowflake Storage Integration 的 `storage_aws_role_arn` 需要 AWS IAM Role 的 ARN

[main.tf:144-146](terraform/main.tf#L144-L146) 的注释明确写了破解办法：

```hcl
# Use predictable IAM role ARN to break circular dependency with snowpipe.
# The snowpipe module creates this role with this exact name.
snowpipe_iam_role_arn = "arn:aws:iam::${var.aws_account_id}:role/iodp-dc-snowpipe-s3-${var.environment}"
```

**核心点：IAM Role 的 ARN 是可预测的字符串**，格式 `arn:aws:iam::<账号>:role/<名字>`，账号 ID 你早就知道、名字是你自己定的命名规范。所以即使 IAM Role 还没建出来，你也能**先拼一个 ARN 字符串**塞给 Snowflake。Snowflake 创建 Integration 时**只把 ARN 当字符串存下来，不会去 AWS 验证 Role 是否真的存在**——验证发生在第一次实际 AssumeRole 时。

Terraform 的 DAG 顺序：

```
snowflake_storage_integration（用拼出来的 ARN 字符串）
    ↓ 输出真实的 storage_aws_iam_user_arn + storage_aws_external_id
aws_iam_role（用上面的 outputs 写 Trust Policy；名字正好就是上面拼的那个）
    ↓
两边对上 ✓ —— 一次 apply 完成
```

### Snowpipe 真正用到这把"通行证"是在什么时候

接 §g 的端到端追踪，Snowpipe 收到 S3 ObjectCreated 通知之后：

```
1. Snowpipe 找到 IODP_DC_S3_INT_PROD 这个 Integration
2. 用自己的身份（STORAGE_AWS_IAM_USER_ARN）向 AWS STS 请求:
     AssumeRole(
       RoleArn    = arn:aws:iam::123456789012:role/iodp-dc-snowpipe-s3-prod,
       ExternalId = ABC12345_SFCRole=2_xyzRandomString==
     )
3. AWS STS 校验:
     - 这个 IAM User 在 Trust Policy 的 Principal 里 ✓
     - ExternalId 匹配 Condition ✓
   → 颁发 ~15 分钟有效的临时 access key + secret + session token
4. Snowpipe 拿临时凭据 s3:GetObject 拉 parquet
5. 解析 parquet → COPY INTO SILVER.DC_WIDE
6. 临时凭据快过期了，下一次要拉文件时再 AssumeRole 一次
```

整个链路里 **Snowflake 账号和 AWS 账号之间没有任何长期密钥流动**，只有 15 分钟级别的临时凭据。轮换、撤销、审计都比 access key 模式干净得多。

### 为什么需要 External ID（"暗号"）

防 **confused deputy 攻击**。如果只校验 Principal.AWS 不校验 ExternalId，理论上：

1. 别的 Snowflake 账号 X（比如某竞争对手）知道了你的 IAM Role ARN（IAM Role ARN 不算秘密，可能在文档/截图里泄露）
2. X 在自己的 Snowflake 里 `CREATE STORAGE INTEGRATION` 指向你的 Role ARN
3. 如果你的 Trust Policy 没要求 ExternalId，X 用自己 Snowflake 的 IAM User 去 AssumeRole 也能成功
4. → X 用你的 IAM Role 权限读你的 S3 ⚠️

加了 ExternalId 之后：X 拿到的 ExternalId 是 X 自己 Snowflake 生成的（跟你的不一样），AssumeRole 在 STS 这一层就被拒。**ExternalId 是"只有 Snowflake 和 AWS 双方知道"的对暗号**。

### ⚠️ ExternalId 重建的副作用

不管用 SQL 的 `CREATE OR REPLACE` 还是 Terraform 的资源 destroy/recreate，**Snowflake 都会重新生成 ExternalId**。后果：

- 老的 ExternalId 立即失效；AWS 那边的 Trust Policy 如果没同步更新 → Snowpipe 立刻停摆，Silver 层数据停止灌进 Snowflake
- Terraform 模式下，`snowflake_storage_integration` 的 `force_new` 字段触发时（如重命名）会重建对象，连带 `aws_iam_role.assume_role_policy` 也跟着 in-place update → 一次 apply 内闭环，无人工干预
- 不想触发重建时：改 `storage_allowed_locations` 之类的字段在 Terraform 里是 in-place update（背后是 `ALTER STORAGE INTEGRATION`），不会重置 ExternalId，安全
- 手工 SQL 模式下：用 `ALTER STORAGE INTEGRATION ... SET ...`，**避免** `CREATE OR REPLACE`

### 为什么删除 `snowflake_sql/02_storage_integration.sql`

发现：

| 对象 | SQL 文件 | Terraform |
|---|---|---|
| `IODP_DC_S3_INT_<ENV>` | `CREATE OR REPLACE STORAGE INTEGRATION ...`（已删） | `snowflake_storage_integration.s3_int`（保留） |
| AWS IAM Role + Trust Policy | 注释里说"DESC INTEGRATION → 抄值 → 手动贴" | `aws_iam_role.snowpipe_s3_access` 直接消费 module outputs |

两者管同一个 Snowflake 对象，行为重叠。**留着 SQL 的具体风险**：

1. **ExternalId 被偷偷重置 → Snowpipe 停摆**：`apply_snowflake_sql.sh` 用 `[0-9]*.sql` 通配符按文件名顺序跑（[scripts/apply_snowflake_sql.sh:24](scripts/apply_snowflake_sql.sh#L24)）。如果有人做日常 schema 变更（比如改 03/04/05），跑 `make apply-snowflake-sql` → 02 也会被一起跑 → `CREATE OR REPLACE` → ExternalId 重置，Trust Policy 来不及更新 → 生产 Snowpipe 立即 403。
2. **配置漂移**：SQL 里 ALLOWED_LOCATIONS 写法是 `s3://iodp-dc-silver-${ENV_LOWER}-${AWS_ACCOUNT_ID}/`，Terraform 里是 `s3://${var.silver_bucket_name}/`。表面同义，但桶名变更时（如 rebrand）两边只改一边就漂移了。
3. **IaC 边界含糊**：同一对象两套 source of truth，code review 时不知道改哪边。

**结论**：Storage Integration 由 Terraform 唯一管。SQL 文件删除。

#### Terraform 已有这条注释（[modules/snowpipe/main.tf:8-9](terraform/modules/snowpipe/main.tf#L8-L9)）

```
# Note: The actual Snowflake Pipe/Stage/FileFormat objects are created via
# snowflake_sql/04_pipe.sql, not Terraform (provider limitations with AUTO_INGEST).
```

意思是 **04_pipe.sql 这种**才是真正"Terraform provider 能力不够、必须留 SQL"的对象（AUTO_INGEST Pipe 有些字段 Terraform Snowflake provider 当时不支持）。Storage Integration 不属于这一类——provider 完全支持，没理由留 SQL。

### 一句话总结

Storage Integration 是 **Snowflake → AWS 的安全握手凭据**。本项目由 [terraform/modules/snowflake/main.tf:201](terraform/modules/snowflake/main.tf#L201) 唯一管理，配合 [terraform/modules/snowpipe/main.tf:15](terraform/modules/snowpipe/main.tf#L15) 的 IAM Role 一次 apply 闭环。原 SQL 文件 02_storage_integration.sql 在 2026-04-30 删除，原因是与 Terraform 重复 + `CREATE OR REPLACE` 有偷偷重置 ExternalId 的隐患。

---

## j) 稀疏 GSI 改造：把"全表扫描"换成"精准 Query"

> §h 末尾提到 stale-lock Lambda 用 Scan + FilterExpression 找跑飞的锁——表大了会变贵变慢。本节讲我们怎么提前消掉这个潜在瓶颈：让"业务字段"和"GSI 索引字段"分家，再加一个稀疏 GSI 让 Lambda 直接 Query 命中。

### 这是什么

把 checkpoint 表里 **`status` 字段**从"长期保存"改成"只在跑动时存在"。Job 完成的时候**物理删除** `status`，把"上一轮结果"挪到一个新的 `last_status` 字段里。然后给 `status` 建一个 GSI——因为 DynamoDB 的 GSI 只索引"含有 GSI key 的 item"，没有 status 字段的 item 不进 GSI，索引天然变得"稀疏"，永远只装当下在跑的那几条。

### 生活类比

公司前台一本"今天还在开会的人"登记簿。

- **改造前**：登记簿就是公司全员名册。每个人下面都有"会议状态"一栏，写着"开会中 / 已散会 / 在工位"。前台要找跑飞的会议得从头翻到尾，逐个看"开会中"的人结束时间是不是过了。员工越多翻得越慢。

- **改造后**：登记簿拆成两本：
  - **全员名册**（主表）：状态栏只在你"正在开会"的时候才填字；开完会前台**用涂改液把状态栏刷白**，连带"会议结束时间"一起刷掉。
  - **开会中索引**（稀疏 GSI）：自动只收录"状态栏有字的人"。
  
  前台找跑飞的会议直接翻索引——名册再厚也不影响，索引永远只有当下在开会的几个人。

### 设计要点：两个字段分别管两件事

```
┌─ 运行期字段（Job 跑的时候才存在，release 时 REMOVE）─┐
│  status            = "running"                       │
│  lock_expires_at   = ISO 时间戳 (lock TTL)           │
└──────────────────────────────────────────────────────┘

┌─ 完成态字段（持久保留，跨次运行不丢）─────────────────┐
│  last_status       = "succeeded" / "failed"          │
│  last_processed_at, file_md5s, in_count, out_count,  │
│  dlq_count, input_files, job_run_id                  │
└──────────────────────────────────────────────────────┘
```

为什么必须拆成两个字段、不能在一个 `status` 上做文章？

- 单字段方案：`status` 同时表达"在跑"和"上次跑完是什么结果"——两件事一旦合在一起，GSI 里就永远塞着所有完成态 item（"succeeded" / "failed" 也是 status 值），"稀疏"就是空话。
- 双字段方案：`status` 只表达"正在跑"；上次的结果由 `last_status` 表达。两件事各管一摊，GSI 才能真正稀疏。

### 改动 1：release_lock 用 UpdateItem REMOVE

```python
# 旧：put_item 整体写一遍，status="succeeded" 留在 item 里
# 新：update_item，REMOVE 掉运行期字段，SET last_status
update_expr = (
    "SET last_status = :ls, last_processed_at = :ts, ... "
    "REMOVE #s, lock_expires_at"
)
table.update_item(Key={"partition_key": pk}, UpdateExpression=update_expr, ...)
```

**举例**：bronze 处理 4/25 ios 完成时，DynamoDB item 字段对比：

| 字段 | 处理前（running 状态） | 旧设计完成后 | 新设计完成后 |
|---|---|---|---|
| `partition_key` | `bronze#2026-04-25#ios` | 同左 | 同左 |
| `status` | `"running"` | `"succeeded"` | **(无此字段)** |
| `lock_expires_at` | `2026-04-25T12:00Z` | `1970-01-01T00:00Z`（哑值） | **(无此字段)** |
| `last_status` | (无) | (无此字段) | `"succeeded"` |
| `file_md5s` | (无) | `{...}` | `{...}` |

新设计里 `status` 字段被物理删除——这条 item 立刻从 `status-index` GSI 里消失。

### 改动 2：acquire_lock 的 ConditionExpression 简化成两条

```python
ConditionExpression="attribute_not_exists(#s) OR lock_expires_at < :now"
```

为什么是这两条？穷举所有可能的初始状态：

| 当前 item 状态 | `attribute_not_exists(#s)` | `lock_expires_at < :now` | 能否抢锁 |
|---|---|---|---|
| 全新分区（item 不存在） | ✅ true | (无属性) | ✅ |
| 上一轮成功完成（status 已 REMOVE） | ✅ true | (无属性) | ✅ |
| 上一轮失败（status 已 REMOVE） | ✅ true | (无属性) | ✅ |
| 当前正在跑，锁未过期 | ❌ false (status="running") | ❌ false (未来时间) | ❌ |
| 上一轮崩溃，锁已过期 | ❌ false (status="running") | ✅ true | ✅ |

> ⚠️ 为什么不能用旧的 `#s <> :running`？DynamoDB 对**缺失属性**的比较返回 false（不是 true）。完成态 item 的 status 已被 REMOVE，`#s <> :running` 既不命中也不报错，整条 OR 走不通。所以**必须**用 `attribute_not_exists(#s)` 替代。

### 改动 3：needs_reprocess 改读 last_status

```python
if item.get("last_status") != "succeeded":
    return True  # 上一轮没成功 → 重处理
```

**三种实际情况**：

| 上一轮发生了什么 | item 里 last_status 是什么 | 行为 |
|---|---|---|
| 成功完成 | `"succeeded"` | 接下来比 file_md5s |
| release 时写了 failed | `"failed"` | 直接重处理 |
| 崩溃（acquire 后 release 前挂掉） | (字段不存在) | 直接重处理 |

第三种情况要展开：acquire_lock 用的是 `put_item`，**整个 item 都被替换成 4 个字段**（status, lock_expires_at, last_processed_at, job_run_id），原来的 `last_status` 和 `file_md5s` 全没了。这是**故意保留的**老行为——崩溃后 file_md5s 缺失，下一轮 needs_reprocess 即使不看 last_status 也会因为 MD5 对不上而重处理，正好兜底。

### 改动 4：silver 检查 bronze 的 last_status

```python
bronze_ckpt = checkpoint.get_checkpoint("bronze", dt, store)
if not bronze_ckpt or bronze_ckpt.get("last_status") != "succeeded":
    return  # 跳过
```

**为什么不需要显式检查 `status == "running"`**？因为 bronze 在跑时 put_item 已经把 `last_status` 清掉了。silver 读到的 `last_status` 是 None → `None != "succeeded"` → 跳过。一行检查同时覆盖三种情况：bronze 没记录、bronze 在跑、bronze 上次失败。

**举例**：

| 场景 | bronze item 内容 | silver 看到的 last_status | silver 行为 |
|---|---|---|---|
| bronze 还在跑 4/25 ios | `status="running"`, last_status 不存在 | `None` | 跳过 ✓ |
| bronze 4/25 ios 已成功 | `last_status="succeeded"`, file_md5s={...} | `"succeeded"` | 进入处理 ✓ |
| bronze 4/25 ios 上次失败 | `last_status="failed"` | `"failed"` | 跳过 ✓ |
| bronze 从未处理过 4/25 ios | (整条 item 不存在) | (`bronze_ckpt is None` 分支) | 跳过 ✓ |

### 改动 5：Lambda 从 Scan 改成 Query GSI

```python
# 旧：Scan 全表 + FilterExpression
table.scan(
    FilterExpression="#s = :running AND lock_expires_at < :now",
    ...
)

# 新：直接 Query 稀疏 GSI
table.query(
    IndexName="status-index",
    KeyConditionExpression=Key("status").eq("running") & Key("lock_expires_at").lt(now_iso),
)
```

**规模对比**（假设 3 年累计 ~4400 行，当下在跑 0~4 条）：

| 维度 | Scan + Filter（旧） | Query GSI（新） |
|---|---|---|
| 扫描的 item 数 | 4400（全表） | 0~4（GSI 里就这么多） |
| 计费基准 ScannedCount | 4400 | 0~4 |
| RCU 消耗 | 跟主表行数线性增长 | 跟当下在跑的 Job 数线性增长（≈ 恒定） |
| 延迟 | 表大了要分页 | 单次 Query 命中 |

> ⚠️ **主要动机不是省钱**——当前规模 Scan 一次也就 ~$0.0000625，每月 $3。真正动机是**让告警延迟和 RCU 消耗跟主表大小解耦**，将来表再大也不会冒头。

### 改动 6：Terraform 加 GSI 定义 + IAM 收紧

```hcl
# modules/dynamodb/main.tf
attribute { name = "status";          type = "S" }
attribute { name = "lock_expires_at"; type = "S" }
global_secondary_index {
  name            = "status-index"
  hash_key        = "status"
  range_key       = "lock_expires_at"
  projection_type = "INCLUDE"
  non_key_attributes = ["partition_key", "last_processed_at", "job_run_id"]
}
```

```hcl
# modules/observability/main.tf — Lambda IAM
Action   = ["dynamodb:Query"]                       # 之前: ["dynamodb:Scan"]
Resource = [var.checkpoint_status_index_arn]        # 之前: 主表 ARN
```

两处收紧：

1. **Action**：`Scan` → `Query`，权限粒度变小。
2. **Resource**：主表 ARN → GSI ARN。Lambda 只能查 GSI，**碰不到主表数据**——符合最小权限原则。万一 Lambda 代码里被注入了 `table.scan()`，IAM 直接拒绝。

`projection_type = "INCLUDE"` 是个 GSI 优化项：告诉 DynamoDB GSI 里除了 PK/SK 还要冗余存哪几列。Lambda 告警邮件需要 `partition_key, last_processed_at, job_run_id`，所以这 3 列也存进 GSI 一份；如果用 `KEYS_ONLY`，Lambda Query 完拿不到这些字段，还得回主表 GetItem，徒增 RCU。

### 端到端追踪：一个分区的完整生命周期

跟踪 `(layer=bronze, dt=2026-04-25, store=ios)` 这条 item 在新设计下的状态变化：

```
T0  10:00:00  EventBridge 触发 Bronze Job
              │
              ▼
T1  10:00:05  acquire_lock: put_item
              ┌─────────────────────────────────────────┐
              │ partition_key:    bronze#2026-04-25#ios │
              │ status:           "running"   ← 在 GSI │
              │ lock_expires_at:  2026-04-25T12:00:00Z │
              │ last_processed_at: 2026-04-25T10:00:05Z │
              │ job_run_id:       jr_abc123              │
              └─────────────────────────────────────────┘
              ※ stale-lock Lambda 此刻 Query GSI 能看到这条
                但 lock_expires_at < now 不成立（12:00 > 现在）→ 不告警

T2  10:30:00  Lambda 30 分钟周期触发，Query GSI:
              KeyCondition: status="running" AND lock_expires_at < "2026-04-25T10:30:00Z"
              → 12:00 不小于 10:30 → 命中 0 条 → 无告警

T3  10:15:00  release_lock("succeeded"): update_item
              ┌─────────────────────────────────────────┐
              │ partition_key:     bronze#2026-04-25#ios │
              │ (status REMOVED)  ← 从 GSI 消失         │
              │ (lock_expires_at REMOVED)               │
              │ last_status:       "succeeded"           │
              │ last_processed_at: 2026-04-25T10:15:00Z │
              │ in_count:          850000000             │
              │ out_count:         849999500             │
              │ file_md5s:         {...}                 │
              │ job_run_id:        jr_abc123             │
              └─────────────────────────────────────────┘
              ※ GSI 现在 0 条，Lambda 怎么 Query 都查不到这条

T4  次日10:00 第二天 Bronze Job
              ① needs_reprocess: 读到 last_status="succeeded"
                 → 进入 file_md5s 比对
              ② 假设 dropzone MD5 没变 → 跳过该分区，不调 acquire_lock
              ③ item 状态保持 T3 的样子，不变
```

### 崩溃恢复路径（最关键的健壮性场景）

```
T0  10:00:00  Bronze acquire_lock → put_item 替换整个 item
              status="running", lock_expires_at=12:00
              ★ file_md5s / last_status 字段被 put_item 清掉了

T1  10:30:00  Glue Job OOM 崩溃，cleanup 没跑
              DynamoDB item 还停留在 status="running"

T2  12:00:00  AWS Glue Timeout（120 分钟）兜底强杀进程
              进程死了，DynamoDB 里 status 仍是 "running"

T3  12:30:00  stale-lock Lambda 周期触发
              Query GSI: status="running" AND lock_expires_at < "12:30"
              ★ 命中这条（12:00 < 12:30）→ 发告警邮件给 oncall

T4  次日10:00 第二天 Bronze Job 启动该分区
              ① needs_reprocess:
                 - last_status 字段不存在（T0 被 put_item 清掉）
                 - last_status != "succeeded" → 返回 True，要重处理
              ② acquire_lock:
                 - attribute_not_exists(#s) → false（status="running" 还在）
                 - lock_expires_at < now → true（昨天 12:00 早过了）
                 - 抢锁成功 ✓
              ③ 处理完 release_lock → status REMOVE，从 GSI 消失，告警状态自然恢复
```

### 成本与代价

| 项目 | 大小 |
|---|---|
| 主表多了 `last_status` 字段 | ~10 字节/item，4400 行 ≈ 44KB |
| GSI 存储（KEYS + INCLUDE 3 列） | 同时刻在跑 ≤ 4 条，≈ 2KB |
| GSI 写入开销 | acquire/release 各触发 1 次 GSI 同步，PAY_PER_REQUEST 计费可忽略 |
| Lambda RCU | 从 ~250/次 (Scan) 降到 1/次 (Query GSI) |

写入开销解释一下：每次 acquire_lock / release_lock 都会让 DynamoDB 在主表写完后**异步同步一次 GSI**（不阻塞主表写返回）。同步本身要花 1 WCU——主表 1 WCU + GSI 1 WCU = 2 WCU per write。On-Demand 模式下每天 ~30 次 Job × 2 = 60 WCU/天，每月 0.0X 美分级别，可忽略。

### 一句话回答

| 问题 | 答案 |
|---|---|
| 为什么不直接给主表加 GSI 就完了？ | 单字段 status 同时表达"在跑"和"历史结果"时，GSI 里永远塞着所有完成态 item，根本没法稀疏。必须把两件事拆成两个字段。 |
| 老的 `status="succeeded"` 设计有什么本质区别？ | 老设计：完成态 item 在 GSI 里，Lambda 必须再加 FilterExpression 才能过滤；GSI 大小跟主表一起涨。新设计：完成态 item 不进 GSI，KeyCondition 直接精确匹配，永远只扫"现在跑着的几条"。 |
| 为什么 acquire_lock 还用 put_item？ | 故意保留"整个 item 替换"的语义——崩溃后 file_md5s 自动消失，next run 一定重处理；不需要额外的恢复逻辑。 |
| silver 检查 bronze 状态为什么不需要显式查 status="running"？ | 因为 bronze 在跑时 put_item 已经把 `last_status` 清掉，silver 看到 last_status 缺失 → 一行 `last_status != "succeeded"` 就把"在跑"和"上次失败"和"从没跑过"全覆盖了。 |
| GSI 的 PROJECTION_TYPE 为什么用 INCLUDE 而不是 KEYS_ONLY？ | Lambda 告警邮件要打印 partition_key / last_processed_at / job_run_id；KEYS_ONLY 只投影 PK+SK，告警 Lambda 拿不到这些字段就要再回主表 GetItem，徒增 RCU。INCLUDE 多冗余 3 列，告警 Lambda 一次 Query 够用。 |

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

---

## Snowflake ↔ Snowpipe 模块的数据流与循环依赖

### 1. `storage_aws_iam_user_arn` 是 Snowflake 自动生成的

在 [terraform/modules/snowflake/main.tf:201-211](terraform/modules/snowflake/main.tf#L201-L211) 创建 `snowflake_storage_integration.s3_int` 时，Snowflake 会在自己的 AWS 账户里**自动绑定一个 IAM user**（每个 integration 一个），并返回这个 user 的 ARN 和一个 external_id。

所以这两个属性是 Snowflake 端**计算出来的（computed）输出**，不是用户输入的：

- `snowflake_storage_integration.s3_int.storage_aws_iam_user_arn` → Snowflake 端的 IAM user
- `snowflake_storage_integration.s3_int.storage_aws_external_id` → Snowflake 生成的 external ID

部署后可以在 Snowflake 中验证：

```sql
DESC INTEGRATION IODP_DC_S3_INT_<ENV>;
-- 会看到 STORAGE_AWS_IAM_USER_ARN 和 STORAGE_AWS_EXTERNAL_ID
```

### 2. 完整数据流

```
1. snowflake_storage_integration.s3_int 创建
        ↓
   Snowflake 在自己 AWS 账户里自动生成 IAM user + external_id
        ↓
2. Terraform provider 把这两个值读回来，存到 state 里：
   - storage_aws_iam_user_arn
   - storage_aws_external_id
        ↓
3. snowflake module 通过 outputs.tf 把它们暴露出去
        ↓
4. 根 main.tf 把它们作为输入传给 snowpipe module：
   snowflake_iam_user_arn = module.snowflake.storage_aws_iam_user_arn
   snowflake_external_id  = module.snowflake.storage_aws_external_id
        ↓
5. snowpipe module 用它们去构造 AWS IAM role 的 trust policy
   （让 Snowflake 那个 IAM user 能 AssumeRole 进来读 Silver bucket）
```

参见：
- [terraform/modules/snowflake/outputs.tf:39-47](terraform/modules/snowflake/outputs.tf#L39-L47) — 暴露两个 computed 属性
- [terraform/main.tf:155-156](terraform/main.tf#L155-L156) — 传给 snowpipe module

### 3. Terraform 自动处理的两件事

1. **依赖顺序**：因为 `module.snowpipe` 引用了 `module.snowflake.xxx`，Terraform 自动推断**先创建 snowflake，再创建 snowpipe**，无需写 `depends_on`。

2. **值的传递**：output 不是"导出文件"，而是 Terraform 内存里的 attribute。整个 `terraform apply` 一次性串联所有 module，snowpipe module 拿到的就是 Snowflake 真实返回的、最新的值。

### 4. 循环依赖与解法

`snowflake` 和 `snowpipe` 这两个 module 之间存在天然的双向依赖：

| 方向 | 需要什么 |
|------|---------|
| snowflake → 需要 AWS role ARN | 写在 `storage_aws_role_arn` 上 |
| snowpipe → 需要 Snowflake IAM user ARN | 写在 AWS role 的 trust policy 上 |

如果两个方向都写成 `module.xxx.yyy` 引用，Terraform 会报**循环依赖错误**，apply 失败。

**解法：打破其中一个方向，用"约定好的可预测字符串"代替真正的 output 引用。**

在 [terraform/main.tf:145](terraform/main.tf#L145)，snowflake module 收到的 `snowpipe_iam_role_arn` 是手写拼出来的 ARN 字符串，而不是 `module.snowpipe.role_arn`：

```hcl
snowpipe_iam_role_arn = "arn:aws:iam::${var.aws_account_id}:role/iodp-dc-snowpipe-s3-${var.environment}"
```

为什么选这个方向打破环：

- **snowflake → snowpipe** 用真 output：因为 IAM user ARN 和 external_id 是 Snowflake **随机生成**的，无法预测，必须等 integration 创建出来才能拿到。
- **snowpipe → snowflake** 用预测字符串：因为 AWS IAM role 的名字是我们**自己定的**，可以提前约定。snowpipe module 创建 role 时必须使用与该字符串完全一致的名字，否则 Snowflake 那边的 trust 关系就对不上。

