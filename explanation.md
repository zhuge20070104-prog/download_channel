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
