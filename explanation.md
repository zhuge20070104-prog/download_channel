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
  │     Bronze: 先 DELETE s3://bronze/.../narrow/dt=2026-04-24/store=ios/*.parquet
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
| 读 | dropzone csv.gz / parquet | Bronze 窄表 parquet |
| 转换 | csv → parquet，schema 校验，类型规范化，按 PK 去重 | **窄表 → 宽表 pivot**，算 paid_share / featured_share |
| 校验 | schema 列名 / 类型 | **5 项 DQ Check**（行数、空值率、日期范围、负数、等式） |
| 写入路径 | `bronze/narrow/...` | `silver/...`（统一宽表） |
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
| 4/25 10:00 | Data.ai PUT `dropzone/narrow/dt=2026-04-24/store=ios/narrow.csv.gz`（MD5=aaa111，downloads=1000） | dropzone 有 1 个文件 |
| 4/25 10:00 | EventBridge 触发 Bronze Job → 查 DynamoDB `bronze#2026-04-24#ios` 不存在 → 处理 | DynamoDB 写入 file_md5=aaa111, status=succeeded |
| 4/25 10:01 | Bronze 写 `bronze/narrow/dt=2026-04-24/store=ios/part-00000.snappy.parquet`（downloads=1000） | Bronze S3 有 1 个文件 |
| 4/25 10:02 | Silver Job 由 Workflow 拉起 → DQ 通过 → 写 `silver/.../dt=2026-04-24/store=ios/part-00000.snappy.parquet` | Silver S3 有 1 个文件，触发 ObjectCreated |
| 4/25 10:05 | Snowpipe 自动 COPY INTO → 表里新增 1 行 `{downloads_total=1000, _loaded_at='2026-04-25 10:05'}` | SILVER.DC_WIDE 1 行 |
| 4/25 10:20 | Gold Dynamic Table 自动刷新 | GOLD.DC_DAILY_BY_APP 1 行(1000) |

**Day 1 结束状态**：每一层都是一致的 1000 ✓

#### Day 2（4/26 上午）— 4/24 数据被 restate

| 时刻 | 操作 | 影响 |
|---|---|---|
| 4/26 10:00 | Data.ai PUT 覆盖 `dropzone/narrow/dt=2026-04-24/store=ios/narrow.csv.gz`（**MD5=bbb222**，downloads=**1050**） | dropzone 同路径文件被覆盖 |
| 4/26 10:00 | Bronze Job 启动 → DynamoDB file_md5='aaa111' vs 当前 dropzone MD5='bbb222' → **检测到 restate** | 决定要重处理 |
| 4/26 10:01 | Bronze **DELETE** 旧 `bronze/narrow/.../part-00000.snappy.parquet`（物理消失）→ 写新 parquet（1050） | Bronze S3 旧文件已删，新文件 1050 |
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

### GSI 四个字段逐个看（hash_key / range_key / projection_type / non_key_attributes）

GSI（Global Secondary Index）可以理解成 **「DynamoDB 给同一张表额外开的一份小表，按不同的 key 重新组织数据，方便快速查询」**。主表 PK 是 `partition_key`（如 `bronze#2026-04-25#ios`），适合"按某个分区查它的 checkpoint"；但 stale-lock Lambda 想问的是另一个问题：**"现在有哪些 Job 还在 running 状态、且锁过期了？"**——拿主表查就得 Scan 全表，越来越贵。所以建这个 GSI，让它能用 Query 直接定位。

四个字段的角色一览：

| 字段 | 作用 | 在这里填什么 |
|---|---|---|
| `hash_key` | GSI 的**分区键**（必填）—— 决定数据物理上分到哪个 partition | `status` |
| `range_key` | GSI 的**排序键**（可选）—— 同一个分区内按它排序，可以做范围查询 | `lock_expires_at` |
| `projection_type` | **投影类型** —— 决定 GSI 上能直接读到哪些字段 | `INCLUDE`（除 key 外，再额外带几个） |
| `non_key_attributes` | 当 `projection_type=INCLUDE` 时，列出**还要顺便带哪些字段** | `partition_key`、`last_processed_at`、`job_run_id` |

#### 1. `hash_key = "status"` —— 分区键

GSI 把数据按 `status` 字段的值**重新分桶**。

这个项目里 `status` 只有一个值 `"running"`（Job 完成时这个字段会被 `REMOVE`，所以 GSI 是**稀疏的**——只有"正在跑的 Job"的那几行才会出现在索引里）。

**举例**：主表里有 100 万行历史 checkpoint，但此刻只有 3 个 Job 在跑。GSI 里就只有 3 行。Lambda `Query("status = running")` 直接拿到这 3 行，不会扫到那 100 万行。

> ⚠️ DynamoDB 的"稀疏索引"性质：**只有写入了 hash_key 字段的 item 才会进 GSI**。`UpdateItem ... REMOVE status` 之后，那一行就从 GSI 里消失了。这是这个设计的核心。

#### 2. `range_key = "lock_expires_at"` —— 排序键

同一个分区（`status="running"`）内的 item 按 `lock_expires_at` 字段排序。

**举例**：当前 3 个 running 的 Job，Lambda 在 13:00 跑去找过期锁：

| partition_key | status | lock_expires_at |
|---|---|---|
| `bronze#2026-05-01#ios` | running | `2026-05-04T10:00:00Z` ← 已过期 |
| `bronze#2026-05-02#android` | running | `2026-05-04T11:30:00Z` ← 已过期 |
| `silver#2026-05-03#ios` | running | `2026-05-04T14:00:00Z` ← 还没过期 |

```python
table.query(
    IndexName="status-index",
    KeyConditionExpression=Key("status").eq("running") & Key("lock_expires_at").lt("2026-05-04T13:00:00Z"),
)
```

DynamoDB 直接定位到 `status="running"` 这个分区，沿着 `lock_expires_at` 升序往下扫，**只读到前两行就停**（第三行 14:00 > 13:00 不满足）。这就是 range_key 带来的"范围查询能省钱"。

如果没有 range_key、只有 hash_key，就得把 3 行全读回来再在客户端过滤。

#### 3. `projection_type = "INCLUDE"` —— 投影类型

GSI 物理上是另一张「小表」，存什么字段是可以选的。三种选择：

| 类型 | 存什么 | 优劣 |
|---|---|---|
| `KEYS_ONLY` | 只存主表 PK + GSI 的 hash/range key | 最便宜，但读到结果还得回主表查详情 |
| `INCLUDE` | KEYS_ONLY + 你指定的几个额外字段 | **折中**：常用字段直接从 GSI 拿，不常用的回主表 |
| `ALL` | 主表所有字段都拷一份 | 读最方便，但写放大 + 存储成本最高 |

这里选 `INCLUDE` —— 表示"我知道 Lambda 处理 stale lock 时只用得上几个特定字段，没必要把所有字段都拷过来"。

**为什么不用 `ALL`？** 主表里还有 `file_md5s`（文件指纹列表，可能很大）、`last_status`、`TTL` 等字段，Lambda 处理 stale lock 时根本用不上。`INCLUDE` 让 GSI 物理体积更小、写放大更小、更便宜。

**为什么不用 `KEYS_ONLY`？** Lambda 拿到锁后还需要 `partition_key`（用来定位主表行）、`last_processed_at`（日志里写"上次处理到哪"）、`job_run_id`（日志里写"卡死的是哪个 Glue run"）。如果用 KEYS_ONLY 还得多一次主表读取，徒增 RCU。

#### 4. `non_key_attributes = [...]` —— 投影哪些非 key 字段

只有在 `projection_type=INCLUDE` 时才需要填。这里指定了 3 个：

| 字段 | 用途 |
|---|---|
| `partition_key` | 主表的 hash key —— Lambda 拿到锁后要 `UpdateItem({partition_key: ...})` 去主表把 status REMOVE 掉，**没这个就找不到主表那一行** |
| `last_processed_at` | 上次处理到哪（用于打日志、debug） |
| `job_run_id` | 卡死的 Glue Job Run ID（用于打日志，便于在 AWS Console 找对应的 Glue 失败记录） |

> 注意：`status` 和 `lock_expires_at` **不需要列在这里**，因为它俩是 GSI 自己的 hash_key + range_key，DynamoDB 自动会带上。

#### 一图看清主表 vs GSI 的关系

```
主表（按 partition_key 分布，1M 行）
┌───────────────────────────────┬─────────┬────────────────┬──────────────┐
│ partition_key                 │ status  │ lock_expires_at │ ...其他字段  │
├───────────────────────────────┼─────────┼────────────────┼──────────────┤
│ bronze#2026-05-01#ios         │ running │ 10:00          │ file_md5s,...│
│ bronze#2026-05-02#android     │ running │ 11:30          │ file_md5s,...│
│ silver#2026-05-03#ios         │ running │ 14:00          │ file_md5s,...│
│ bronze#2026-04-30#ios         │ (无)    │ (无)           │ last_status= │
│ ... 100 万行历史 (无 status)  │         │                │ "succeeded"  │
└───────────────────────────────┴─────────┴────────────────┴──────────────┘

GSI status-index (按 status 分布，只 3 行 — 稀疏!)
┌─────────┬────────────────┬────────────────────────┬─────────────────┬────────────┐
│ status  │ lock_expires_at│ partition_key          │ last_processed  │ job_run_id │
│ (HASH)  │ (RANGE)        │ (INCLUDE)              │ _at (INCLUDE)   │ (INCLUDE)  │
├─────────┼────────────────┼────────────────────────┼─────────────────┼────────────┤
│ running │ 10:00          │ bronze#2026-05-01#ios  │ 09:55           │ jr_abc     │
│ running │ 11:30          │ bronze#2026-05-02#and..│ 11:25           │ jr_def     │
│ running │ 14:00          │ silver#2026-05-03#ios  │ 13:50           │ jr_ghi     │
└─────────┴────────────────┴────────────────────────┴─────────────────┴────────────┘
                                                  ↑ Lambda 想要的字段都在这里，不用回主表
```

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

---

## Snowflake 基础架构 (`01_database_schemas.sql` 解读)

参见 [snowflake_sql/01_database_schemas.sql](snowflake_sql/01_database_schemas.sql)。该文件只搭骨架（DB / Schema / Warehouse / Role + 顶层 USAGE），**不包含数据访问权限**。

### 1. WAREHOUSE 是什么 — 计算与存储分离

Snowflake 的核心架构：**存储与计算分离**。

| 层 | 对象 | 作用 |
|---|---|---|
| 存储层 | DATABASE / SCHEMA / TABLE | 存数据本身（表定义 + 实际数据） |
| 计算层 | **WAREHOUSE** | 一个虚拟计算集群（相当于一组 EC2），负责执行 SQL |
| 账户层 | ROLE / USER | 权限控制 |

**类比**：
- DB / Schema / Table = 图书馆的书架和书（数据躺在那里）
- WAREHOUSE = 来图书馆干活的那批工人（CPU + 内存）
- 没工人就没人能查书；不同工作负载可以派不同规模的工人队

**关联关系**：
- Warehouse **不属于任何 DB**，是独立对象，跨 DB 通用
- 任何 query 都必须指定一个 warehouse 才能跑
- 同一个 warehouse 可以查任何 DB 的任何 schema（只要有权限）

**例子**：

```sql
USE WAREHOUSE COMPUTE_WH_DC_DEV;  -- 选工人
USE DATABASE IODP_DC_DEV;         -- 选图书馆
USE SCHEMA SILVER;                -- 选哪一排书架
SELECT * FROM events;             -- 工人按 query 去找数据
```

本项目配置 `XSMALL` + `AUTO_SUSPEND = 60`：1 个节点，空闲 60 秒自动挂起（挂起期间**不计费**），有 query 来 1-2 秒自动启动。开发/小流量场景的标配。

### 2. RAW_STAGE schema 是什么 — Snowpipe 元数据收纳盒

**核心结论**：RAW_STAGE 里**没有表，一张都没有**。它是 Snowpipe 入库链路的"元数据收纳盒"，装的是 Snowpipe 工作时需要的 3 类配置（参见 [snowflake_sql/04_pipe.sql](snowflake_sql/04_pipe.sql)）：

| 对象类型 | 对象名 | 不是表，是啥？ | Snowpipe 用它干嘛 |
|---|---|---|---|
| FILE FORMAT | `PARQUET_FF` | 一组解析参数（"Parquet + Snappy"） | 告诉 Snowpipe 怎么解析 S3 文件 |
| STAGE | `SILVER_S3_STAGE` | 指向 S3 的命名连接 | 告诉 Snowpipe 去哪个 S3 路径找文件 |
| PIPE | `PIPE_DC_WIDE` | 一条自动加载规则（COPY INTO SQL） | 收到 SQS 通知后跑这段 SQL |

直观示意：

```
IODP_DC_DEV (Database)
│
├── RAW_STAGE (Schema)   ← 没有表！只是 3 个"配置/规则"对象
│   ├── PARQUET_FF        (FILE FORMAT)  → 解析参数
│   ├── SILVER_S3_STAGE   (STAGE)        → 指向 S3 路径的快捷方式
│   └── PIPE_DC_WIDE      (PIPE)         → 自动加载规则 (一段 SQL)
│
├── SILVER (Schema)      ← 这里才有表，有真数据
│   ├── DC_WIDE           (TABLE)        → Parquet 加载后的行数据
│   └── DC_WIDE_LATEST    (VIEW)
│
└── GOLD (Schema)        ← Dynamic Tables 聚合层
    └── ...
```

**记账本不在 RAW_STAGE**：要区分两类元数据——

| 元数据类型 | 存在哪里 | 例子 |
|---|---|---|
| **规则/定义元数据**（静态） | ✅ **RAW_STAGE schema** | Pipe 的 SQL 文本、Stage 的 S3 URL、FileFormat 的解析参数 |
| **运行时/历史元数据**（动态） | ❌ **不在 RAW_STAGE**，在账户级系统视图 | 哪个文件加载了、加载几行、是否失败、Pipe 当前状态 |

运行时部分这样查：

```sql
SYSTEM$PIPE_STATUS('PIPE_DC_WIDE')              -- pipe 当前状态
INFORMATION_SCHEMA.COPY_HISTORY(...)            -- 文件加载历史(14天)
SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY     -- pipe 用量计费(365天)
```

**一句话总结**：RAW_STAGE 装的是"Snowpipe 怎么干活"的规则；"Snowpipe 干了什么"的运行记录在 Snowflake 账户级元数据里，不在 RAW_STAGE。

### 3. Snowpipe 触发机理 (5 步全自动)

```
Step 1: CREATE PIPE ... AUTO_INGEST = TRUE
        Snowflake 返回 SQS ARN: arn:aws:sqs:us-east-1:xxx:sf-snowpipe-...
   │
Step 2: S3 bucket 配置 Event Notification → 上面那个 SQS (Terraform 自动化)
   │
Step 3: Glue 写入新 Parquet 到 s3://iodp-dc-silver-dev-xxx/download_channel/...
   │
Step 4: S3 自动发 SQS 消息 → Snowflake 后台轮询 SQS 收到消息
        → 匹配 Stage URL → 排队执行 PIPE_DC_WIDE 里的 COPY INTO
        → 用 serverless 计算（不用你的 warehouse, 单独计费）
   │
Step 5: 数据落到 SILVER.DC_WIDE 表
        + 内部记账本里加一条: file_name | status | rows_loaded | last_load_time
```

延迟通常 **30 秒 ~ 2 分钟**（S3 事件传播 + Snowpipe 排队）。

**去重机制**：Snowpipe 在记账本里**保留 14 天的已加载文件名**。同样文件名重新上传会被**跳过**。这就是为什么 Glue 写 Parquet 用 `part-00000-<uuid>.parquet` 这种带 UUID 的文件名——保证唯一，重新跑 Glue 不会被误判为重复。

### 4. Role 体系：职责分离 + 为什么挂到 SYSADMIN 下

#### 三个 role 是最小权限原则

| Role | 给谁用 | 能做什么 |
|---|---|---|
| `IODP_DC_LOAD` | Snowpipe 服务账号 | 只能 INSERT 进 SILVER |
| `IODP_DC_TRANSFORM` | Dynamic Table refresh | 能读 SILVER + 写 GOLD |
| `IODP_DC_READER` | BI 工具 / Tableau / 下游消费者 | 只能 SELECT |

好处：BI 工具 credential 泄漏，攻击者也只能读不能改写；ETL 出 bug 不会污染下游。

#### Role hierarchy 容易误解的点

`GRANT ROLE IODP_DC_READER_DEV TO ROLE SYSADMIN` **不是**"让 READER 拥有 SYSADMIN 权限"，而是反过来——**"让 SYSADMIN 能管理 READER 这个 role"**。

Snowflake 的 role 是层级的：`grant role A to role B` → **B 包含 A 的能力**。

#### 为什么必须挂到 SYSADMIN 下

Snowflake 官方推荐的最佳实践：

1. 不挂到 SYSADMIN 下，role 就成"孤儿"——只有 ACCOUNTADMIN 能管。而 ACCOUNTADMIN 是账户级最高权限（能开账单、删账户），日常运维不应该用它。
2. 所有自定义 role 都挂到 SYSADMIN 下，DBA 用 SYSADMIN 就能统一管理所有业务 role。

#### READER 用户实际权限会变大吗？不会

普通 reader 用户登录后只会 `USE ROLE IODP_DC_READER_DEV`，他能用的权限就只是这个 role 上 grant 的那些 SELECT 权限。READER role 被 grant 给 SYSADMIN，受影响的只是 **SYSADMIN 这个角色** 多了一项能切换到 READER 的能力，**和 reader 用户本身的权限无关**。

### 5. READER 实际读权限的 4 层授权链 (USAGE ≠ SELECT)

`01_database_schemas.sql` 只配置了前 3 层（USAGE），真正的 SELECT 权限在创建对象的 SQL 文件里：

| 层级 | 文件 | Grant | 作用 |
|---|---|---|---|
| 1. 计算 | [01_database_schemas.sql:39](snowflake_sql/01_database_schemas.sql#L39) | `USAGE ON WAREHOUSE` | 允许用 warehouse 跑 query |
| 2. 容器 | [01_database_schemas.sql:44](snowflake_sql/01_database_schemas.sql#L44) | `USAGE ON DATABASE` | 允许"看见"DB |
| 3. 命名空间 | [01_database_schemas.sql:50, 52](snowflake_sql/01_database_schemas.sql#L50-L52) | `USAGE ON SCHEMA SILVER/GOLD` | 允许"看见"schema 里有什么 |
| 4. **真正的读权限** | 见下面 3 个文件 | `SELECT ON ...` | **真正能读数据** |

第 4 层 SELECT 权限分散在：

```sql
-- snowflake_sql/03_silver_table.sql:32
GRANT SELECT ON TABLE SILVER.DC_WIDE TO ROLE IODP_DC_READER_${ENV};

-- snowflake_sql/05_gold_dynamic_tables.sql:85
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA GOLD TO ROLE IODP_DC_READER_${ENV};

-- snowflake_sql/07_bi_view.sql:50
GRANT SELECT ON VIEW SILVER.DC_WIDE_LATEST TO ROLE IODP_DC_READER_${ENV};
```

**关键概念：USAGE ≠ SELECT（容易踩坑）**

- `USAGE ON SCHEMA` = 让你能 `SHOW TABLES`、能 reference 这个 schema 下的对象（"知道这个 schema 存在，能看到目录"）
- `SELECT ON TABLE` = 真正能跑 `SELECT * FROM table`（"能打开书读内容"）

类比：USAGE 是"图书馆借阅证 + 知道某层楼在哪"，SELECT 才是"能借走那本书读"。

reader 用户实际跑 query 时，4 层权限都会被检查，任何一层缺了就报 `Insufficient privileges`：

```sql
USE WAREHOUSE COMPUTE_WH_DC_DEV;       -- 检查第 1 层 USAGE ON WAREHOUSE   ✓
USE DATABASE IODP_DC_DEV;              -- 检查第 2 层 USAGE ON DATABASE    ✓
USE SCHEMA SILVER;                     -- 检查第 3 层 USAGE ON SCHEMA      ✓
SELECT * FROM DC_WIDE LIMIT 10;        -- 检查第 4 层 SELECT ON TABLE      ✓
```

这种"对象和它的 grant 写在一起"的组织方式是 Snowflake 项目的常见 pattern——方便 review，不容易遗漏。

---

## Silver 宽表设计 (`03_silver_table.sql` 解读)

参见 [snowflake_sql/03_silver_table.sql](snowflake_sql/03_silver_table.sql)。这张 `DC_WIDE` 表是整个 ETL 的核心——Snowpipe 的写入终点 + Dynamic Table 和 BI 的读取起点。

### 1. `CLUSTER BY (dt)` 是什么 — 不是索引，是分区聚簇提示

**核心纠正**：Snowflake 的 `CLUSTER BY` **不是传统数据库的"建索引"**——它没有 B-tree、没有索引文件。

#### Snowflake 的存储模型：micro-partition

Snowflake 表数据自动切成**很多小块**，每块叫一个 **micro-partition**（50-500MB，列式压缩，不可改）：

```
DC_WIDE 表（实际物理存储）：
┌────────────────────────────────┐
│ Partition #1                   │
│   dt 范围: 2026-04-25 ~ 04-27  │
│   product_id 范围: 100 ~ 9999  │  ← 每个 partition 自动维护 min/max 元数据
│   ~200MB                       │
├────────────────────────────────┤
│ Partition #2                   │
│   dt 范围: 2026-04-28 ~ 05-01  │
└────────────────────────────────┘
```

#### 查询时的"分区裁剪"（Pruning）

`SELECT * FROM DC_WIDE WHERE dt = '2026-05-01'` 时，Snowflake 看每个 partition 的 `dt` min/max 元数据，**直接跳过所有不包含目标日期的 partition**。扫得越少 → 查询越快 + 用 warehouse 的钱越少。

#### `CLUSTER BY (dt)` 干啥

它是给 Snowflake 的**一个提示**："请尽量把 `dt` 相近的行塞进同一个 micro-partition 里"。

| 场景 | 没 CLUSTER BY | `CLUSTER BY (dt)` |
|---|---|---|
| 数据按 dt 顺序写入 | 自然就聚簇好，pruning 有效 | 一样有效 |
| 数据乱序写入 / 历史回填 | partition 内 dt 范围乱，pruning 失效 | Snowflake 后台服务自动重排，保持聚簇 |

本项目选 `dt` 做聚簇键的原因：
- BI 查询 99% 带日期过滤
- 数据天然按 dt 增长，聚簇维护成本低
- 不选 `product_id` 是因为查询模式不会"查某个 product 全部历史"

#### 与 OLTP 索引的关键区别

| 概念 | MySQL/Postgres 的 INDEX | Snowflake 的 CLUSTER BY |
|---|---|---|
| 是不是独立的物理结构 | ✅ 是（B-tree 文件） | ❌ 不是，只是数据**重排**的提示 |
| 强制唯一吗 | UNIQUE INDEX 可以 | ❌ 永远不强制 |
| 加速点查（按 ID 找一行） | ✅ 强项 | ❌ 不擅长，Snowflake 是 OLAP 不是点查系统 |
| 加速范围扫描 | 一般 | ✅ 强项（pruning） |

### 2. `TIMESTAMP_NTZ` vs `TIMESTAMP_LTZ` — 什么时候用哪个

Snowflake 有 3 种 timestamp：

| 类型 | 全名 | 怎么存 | 显示时 |
|---|---|---|---|
| `TIMESTAMP_NTZ` | **No Time Zone** | 存"墙上时钟"字面值，**不带时区信息** | 永远显示存进去那个值 |
| `TIMESTAMP_LTZ` | **Local Time Zone** | 内部存 UTC | 按**查询者 session 的时区**自动转换 |
| `TIMESTAMP_TZ` | with Time Zone | 存 UTC + 原始时区 | 显示原始时区下的时间 |

#### 直观例子

假设 Glue 在北京时间 16:00 写入数据（= UTC 08:00）：

```sql
INSERT INTO DC_WIDE VALUES (
  ...,
  ingest_ts  = '2026-05-01 08:00:00',   -- 业务约定写 UTC
  _loaded_at = CURRENT_TIMESTAMP()      -- Snowflake 自动填 UTC 08:00:00
);
```

**北京同事 vs 加州同事查同一行的对比**：

| 字段 | 北京同事看到 | 加州同事看到 |
|---|---|---|
| `ingest_ts` (NTZ) | `2026-05-01 08:00:00` | `2026-05-01 08:00:00` ← **一模一样** |
| `_loaded_at` (LTZ) | `2026-05-01 16:00:00` | `2026-05-01 01:00:00` ← **自动按本地时区转** |

#### 这张表为什么两种都用？

| 字段 | 类型 | 语义 |
|---|---|---|
| `ingest_ts` | `TIMESTAMP_NTZ` | **业务时间戳**——上游 Glue 已统一写成 UTC，下游 BI 计算"5 月 1 号下载量"不能因查询者时区不同而结果飘。NTZ 保证全公司看到同一个值，是 **single source of truth**。 |
| `_loaded_at` | `TIMESTAMP_LTZ` | **运维时间戳**——这一行什么时候落到 Snowflake 的。oncall 排查问题时希望看本地时间（北京 oncall 看北京时间，加州 oncall 看 PST），LTZ 自动转。 |

**记忆口诀**：
- 业务/分析数据 → **NTZ**（统一 UTC，谁查都一样）
- 运维/审计/日志 → **LTZ**（按本地时区显示，方便排查）

⚠️ **常见踩坑**：上游传过来的时间不是 UTC 而是混了本地时间，NTZ 会出 bug。**Bronze→Silver 边界统一转 UTC** 是基本规范。

### 3. 为什么宽表没有主键 / 外键 — OLAP 范式

**核心结论**：Snowflake **支持声明** PK / FK，但**不强制**——它们纯粹是文档和给 query optimizer 的暗示，**不会阻止重复，也不会检查外键完整性**。

#### 验证

```sql
CREATE TABLE DC_WIDE (
  ...,
  PRIMARY KEY (dt, product_id, app_store, country, device)
);
INSERT INTO DC_WIDE VALUES ('2026-05-01', 100, 'apple', 'US', 'iphone', ...);
INSERT INTO DC_WIDE VALUES ('2026-05-01', 100, 'apple', 'US', 'iphone', ...);  -- 重复
SELECT COUNT(*) FROM DC_WIDE;  -- 返回 2, 不会报错
```

**Snowflake 完全允许写两条相同的 PK**——和 MySQL 行为完全不一样。所以本项目**干脆不声明**，避免误导。

#### 为什么 OLAP 数据仓库都不强制约束？

| 原因 | 解释 |
|---|---|
| **性能** | 强制唯一意味着每次 INSERT 都要查全表，单批写百万行会卡死。OLAP 优先吞吐量。 |
| **append-only 设计** | Snowpipe 永远是追加，可能因 SQS 重投递、文件 replay 出现重复。写入时阻止重复 → Snowpipe 直接挂掉，反而更糟。 |
| **批处理范式** | 上游 Glue 已做 schema 校验和数据质量检查。仓库层信任上游 = 分层职责清晰。 |
| **关联很少要 FK** | OLAP join 用 dt + 业务键就够了，FK 完整性靠上游保证。 |

#### OLAP 怎么处理"主键唯一"需求？不靠 DB 强制，靠**模式**

**方法 1：定时去重 task**（本项目用这个）

参见 [snowflake_sql/06_dedup_task.sql](snowflake_sql/06_dedup_task.sql)——按逻辑主键 `(dt, product_id, app_store, country, device)` 定期去重。

**方法 2：查询时去重（用 `QUALIFY`）**

```sql
SELECT *
FROM DC_WIDE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dt, product_id, app_store, country, device
  ORDER BY ingest_ts DESC          -- 同 key 多条时，留最新的
) = 1;
```

参见 [snowflake_sql/07_bi_view.sql](snowflake_sql/07_bi_view.sql) 的 `DC_WIDE_LATEST` 视图——BI 直接查这个视图，自动只看最新版本。

**方法 3：MERGE 而不是 INSERT**（本项目没用，因为 Snowpipe 只支持 COPY = INSERT，不支持 MERGE）

#### 为什么叫"宽表" — Dimensional Modeling

| 范式 | OLTP（MySQL）| OLAP（Snowflake DC_WIDE）|
|---|---|---|
| 表设计 | 拆成小表 + FK：`products` / `countries` / `devices` / `downloads` 互相 join | **故意把所有维度摊开成一个宽表** |
| 查询性能 | join 多 → 慢 | 不 join → 飞快 |
| 约束 | FK 保证完整性 | 没 FK，因为没 join 需求 |
| 重复字符串 | normalize 节省空间 | 列式压缩后几乎不占空间 |

#### 一句话总结

> **OLTP 用约束保证正确性，OLAP 用 pipeline 保证正确性。** Snowflake 不强制 PK/FK 是特性不是 bug——它把"数据质量"的责任推给 Glue（上游写入时验证）和 dedup task（仓库层周期清理），换来超高的写入吞吐和查询性能。

---

## 04_pipe.sql 两个细节问题

### 1) `IODP_DC_S3_INT_${ENV}` 在哪里创建？

**在 Terraform 里创建**，不在 SQL 文件里。

具体位置：[terraform/modules/snowflake/main.tf:201](terraform/modules/snowflake/main.tf#L201)

```hcl
resource "snowflake_storage_integration" "s3_int" {
  name    = "IODP_DC_S3_INT_${local.env_upper}"
  type    = "EXTERNAL_STAGE"
  enabled = true
  ...
}
```

**为什么必须在 Terraform 里创建？** 因为 Storage Integration 创建后会生成一个 Snowflake 端的 IAM User ARN 和 External ID，需要拿到这两个值再去配置 AWS IAM Role 的 trust policy（让 Snowflake 能 assume 这个 role 去读 S3）。这是个 Snowflake ↔ AWS 的双向握手，Terraform 可以一次性 orchestrate 两边；纯 SQL 做不到。

执行顺序：
1. Terraform 创建 `snowflake_storage_integration.s3_int` + AWS IAM Role + trust policy
2. SQL [04_pipe.sql:14](snowflake_sql/04_pipe.sql#L14) 引用已经存在的 `IODP_DC_S3_INT_${ENV}` 来创建 stage

`01_database_schemas.sql` 里没有 storage integration，那个文件只管 database/schema/warehouse/role 这种纯 Snowflake 内部对象。

### 2) `GRANT OPERATE` / `GRANT MONITOR` on PIPE 是干嘛？

这两个权限是 Snowflake 对 PIPE 对象的标准管理权限：

- **`OPERATE`** — 允许角色对 pipe 执行运维操作：`ALTER PIPE … SET PIPE_EXECUTION_PAUSED = TRUE/FALSE`（暂停/恢复 ingest）、`ALTER PIPE … REFRESH`（手动重新扫描 stage 把漏掉的文件补进来）。如果 S3 SNS event 丢了或者 pipe 卡住了，需要这个权限去恢复。
- **`MONITOR`** — 允许角色查 pipe 状态：`SYSTEM$PIPE_STATUS('PIPE_DC_WIDE')`、`COPY_HISTORY`、`PIPE_USAGE_HISTORY` 这些。用来看有没有积压、有没有报错、最近一次 ingest 是什么时候。

注意一个细节：这里两个权限都 grant 给了 `IODP_DC_LOAD_${ENV}`（load 角色自己）。这意味着 load 角色既能跑 pipe 又能管 pipe。如果想做更严格的最小权限拆分，通常 `MONITOR` 给运维/SRE 角色，`OPERATE` 给 on-call 角色，`IODP_DC_LOAD_*` 只保留把数据写进 SILVER.DC_WIDE 所需的最少权限。当前这种写法是单角色简化模式，对小团队/单环境是合理的。

---

## Snowflake Senior 五问 — 一行配置背后的 cost / failure / tradeoff

> 看到一行配置（`AUTO_INGEST=TRUE` / `GRANT OPERATE` / `CLUSTER BY` / `TARGET_LAG` / `::BOOLEAN`），别只问"这是啥"，要问三连：
> 1. **为啥选这个，不选别的？**（tradeoff）
> 2. **这玩意烧多少钱？**（cost model）
> 3. **它怎么坏？怎么发现？怎么补？**（failure mode）

下面把这三问套在五个真实配置上。

### Q1. 为什么用 Snowpipe AUTO_INGEST，不用 Snowpipe Streaming / scheduled COPY？

#### 三种 ingest 方式对比

| 方式 | 怎么工作 | 延迟 | 计费方式 | 适合场景 |
|---|---|---|---|---|
| **Snowpipe AUTO_INGEST**（本项目用的）| S3 写文件 → SNS 通知 → Snowpipe 看到 → 自动 COPY | ~1 分钟 | 按文件数 + 数据量 | 文件已存在的批量 ETL |
| **Snowpipe Streaming** | Java/Python SDK 直接 push **行**进来（不走文件）| ~几秒 | 按行数 | IoT、点击流、CDC |
| **Scheduled COPY** | TASK 每 N 分钟跑一次 `COPY INTO` | = N 分钟 | 按 warehouse 起来的时间 | 不在乎延迟 + 批量大 |

#### 为啥选 AUTO_INGEST

看 [04_pipe.sql:19](snowflake_sql/04_pipe.sql#L19) — `AUTO_INGEST = TRUE`。原因有三：

1. **上游 Glue 已经写 Parquet 到 S3 了**。文件天生存在 → Snowpipe Streaming 用不上（那个是不写文件直接 push 行）。
2. **一天一次的批 ETL，几分钟延迟够用**。不需要 sub-second。
3. **比 scheduled COPY 省 warehouse 钱**。

#### 成本对比（粗估，XS warehouse $4/credit）

假设一天 100 个 Parquet 文件：

```
Snowpipe AUTO_INGEST:
  ~0.06 credits / 1000 files (Snowpipe serverless 收费)
  = 0.006 credits/天 ≈ $0.025/天 ≈ 月 $0.75

Scheduled COPY (每 15 min 跑一次, XS warehouse 起来 60s):
  60s × 96次/天 = 1.6 hours = 1.6 credits/天 = $6.4/天 ≈ 月 $192

差 ~250x
```

**关键直觉**：Snowpipe 是 "serverless"——文件来了就 COPY，没文件就不烧钱。Scheduled COPY 你要起一个 warehouse，哪怕这 15 分钟没文件也得起来 60 秒（minimum charge），白烧。

#### 反过来什么时候 Snowpipe 反而贵

文件**多且小**（一天 100 万个 5KB 小文件）：Snowpipe 的 "per file overhead" 累计 → 这时候反而该攒一攒批量 COPY。本项目一天几十到几百个 Parquet 文件，每个几 MB → 完全在 Snowpipe 的甜区。

---

### Q2. SNS event 丢了一条，怎么发现？怎么补？

#### 先理解为啥会丢

链路：`S3 PutObject → S3 event → SNS → Snowpipe SQS → Snowpipe service → COPY`

会丢的地方：
- SNS → SQS 偶发丢（AWS 只承诺 at-least-once，极小概率会 0 次）
- Pipe 暂停期间写的文件
- SNS 配置之前就上传的历史文件
- IAM/SQS policy 配错，event 来了进不了 queue（这种是大批量丢，容易发现）

#### 怎么发现

**最差的方式**：BI 同学问"今天数据怎么少了"。已经过几天了。

**主动发现 — 对账**：Glue 每次写完文件，自己记一笔 manifest（"今天写了 87 个文件"），跑 reconciliation：

```sql
-- 查 Snowflake 实际加载了多少
SELECT 
  DATE_TRUNC('day', LAST_LOAD_TIME) AS load_day,
  COUNT(*) AS files_loaded,
  SUM(ROW_COUNT) AS rows_loaded
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'IODP_DC_DEV.SILVER.DC_WIDE',
  START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
GROUP BY 1
ORDER BY 1;
```

跟 Glue 写的 manifest 对比，对不上就是丢了。

**实时检查 pipe 健康**：

```sql
SELECT SYSTEM$PIPE_STATUS('IODP_DC_DEV.RAW_STAGE.PIPE_DC_WIDE');
```

返回 JSON 类似：
```json
{
  "executionState": "RUNNING",
  "pendingFileCount": 0,
  "lastReceivedMessageTimestamp": "2026-04-30T14:22:01Z",
  "lastForwardedMessageTimestamp": "2026-04-30T14:22:03Z"
}
```

`pendingFileCount` 一直涨 = pipe 卡了。`lastReceivedMessageTimestamp` 长时间不更新但 S3 一直在写文件 → SNS 通路断了。

#### 怎么补

```sql
-- 场景 1: 丢的文件在最近 7 天内 → REFRESH 命令重新扫整个 stage
ALTER PIPE PIPE_DC_WIDE REFRESH;

-- 场景 2: 知道大概时间段 → 缩小范围
ALTER PIPE PIPE_DC_WIDE REFRESH MODIFIED_AFTER = '2026-04-25T00:00:00Z';

-- 场景 3: 超过 7 天 (REFRESH 限制) → 手工 COPY 单文件
COPY INTO IODP_DC_DEV.SILVER.DC_WIDE
FROM @SILVER_S3_STAGE/2026/04/15/file_xyz.parquet
FILE_FORMAT = (FORMAT_NAME = 'PARQUET_FF');
```

⚠️ **REFRESH 的坑**：它只补**没加载过的**文件——已经加载过的不会重复加。所以幂等的，可以放心跑。但有个 7 天窗口，超过 7 天的文件 REFRESH 不管。

**这就是为啥 [04_pipe.sql:52](snowflake_sql/04_pipe.sql#L52) 给 LOAD 角色 GRANT OPERATE**——出事了得有人能跑 REFRESH。

---

### Q3. DC_WIDE 的 clustering key

#### 项目里实际是什么

[03_silver_table.sql:26](snowflake_sql/03_silver_table.sql#L26):

```sql
CLUSTER BY (dt)
```

✅ 这个选择是对的。下面解释为啥。

#### 什么是 clustering（类比）

Snowflake 把表切成 16MB 的 **micro-partition**（小块）。每个块自动记录每列的 **min / max**。

类比：图书馆按书名首字母排架。
- 你查 "S 开头的书" → 馆员直接走到 S 区，跳过 A-R, T-Z（**partition pruning**）
- 不排架（随机堆）→ 整个图书馆翻一遍

`CLUSTER BY (dt)` = 按 `dt` 排架。

#### 为啥选 dt 不选别的

看 Gold 层的查询模式：

[05_gold_dynamic_tables.sql:77](snowflake_sql/05_gold_dynamic_tables.sql#L77):
```sql
WHERE dt >= DATEADD('day', -30, CURRENT_DATE())
```

[05_gold_dynamic_tables.sql:29](snowflake_sql/05_gold_dynamic_tables.sql#L29):
```sql
GROUP BY dt, product_id, app_store
```

所有查询都**按 dt 过滤或聚合**。CLUSTER BY (dt) 让"查最近 30 天" 只扫 ~30/365 = 8% 的 partition。

#### 数字感

假设 DC_WIDE 一年 365M 行，分成 1000 个 micro-partition：

| 场景 | 扫描 partition 数 | XS warehouse 时间 | credit |
|---|---|---|---|
| 不 cluster, 查 30 天 | 1000（全表）| ~30s | 0.008 |
| CLUSTER BY (dt), 查 30 天 | ~80 | ~3s | 0.0008 |

**单次查询省 10x。** 而 Gold 的 dynamic table 一天 refresh 几十次 → 一年累计差几百刀。

#### 为啥不 CLUSTER BY (dt, app_store) 多列？

- 多列 cluster key 的 prune 效果**递减**——第一列已经把数据切到 30 天了，第二列再切只能在那 30 天里再细分
- `app_store` 只有 2 个值（`ios` / `android`）→ 第二列基本没 prune 空间
- 多列 cluster 的**维护成本**（auto-clustering 重排数据）反而上升

**规则**：cluster key 选**最常出现在 WHERE / JOIN 的列**，且**基数适中**（不能太少像 boolean，也不能太多像 UUID）。`dt` 完美。

#### 隐藏成本：auto-clustering 自己烧钱

Snowflake 后台有个进程持续 reorganize 数据保持 cluster 状态，烧 credit。可以查：

```sql
SELECT * FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
  DATE_RANGE_START => DATEADD('day', -7, CURRENT_DATE()),
  TABLE_NAME => 'IODP_DC_DEV.SILVER.DC_WIDE'
));
```

写多读少的表 cluster 不划算，写少读多（本项目）划算。

---

### Q4. TARGET_LAG = 15 min vs 1 hour，credit 差多少

#### 项目实际配置

[05_gold_dynamic_tables.sql](snowflake_sql/05_gold_dynamic_tables.sql)（**当前状态**，原本两张 daily 表是 15 min，详见 Q6 的优化记录）：
- `DC_DAILY_BY_APP` → `TARGET_LAG = '30 minutes'`
- `DC_DAILY_BY_COUNTRY` → `TARGET_LAG = '30 minutes'`
- `DC_PAID_VS_ORGANIC_TREND` → `TARGET_LAG = '1 hour'`

#### TARGET_LAG 是啥

= "**我承诺这张表落后上游 DC_WIDE 不超过 X**"。

注意：你不是设 cron"每 15 分钟跑一次"。你设的是**SLA 上限**，Snowflake 自己看 DC_WIDE 多频繁更新、refresh 跑多久，反推该多频繁触发——可能 14 分钟一次，也可能 5 分钟一次。

#### 成本怎么来

每次 refresh = warehouse 起来跑 incremental query 一段时间。

**假设**（粗估，实际跑过才知道）：
- DC_WIDE 一天新增 ~1M 行
- 一次 incremental refresh = warehouse start (30s) + query (5s) ≈ 35s
- 用 XS warehouse, $4/credit/hour

| TARGET_LAG | 每天 refresh 次数 | 每天 warehouse 时间 | 每天 credit | 月成本 |
|---|---|---|---|---|
| 1 minute | 1440 | 14 hours | 14 | $1680 |
| 15 minutes | 96 | 0.93 hours | 0.93 | $112 |
| 1 hour | 24 | 0.23 hours | 0.23 | $28 |
| 24 hours | 1 | 0.01 hours | 0.01 | $1.2 |

**1 min vs 15 min 差 15 倍。15 min vs 1 hour 差 4 倍。1 min vs 24 hour 差 1400 倍。**

#### 项目这个配置合不合理？

`DC_DAILY_BY_APP` / `DC_DAILY_BY_COUNTRY` = **15 min**：BI dashboard 看 daily 数据，15 分钟刷新够"实时" → 合理。

`DC_PAID_VS_ORGANIC_TREND` = **1 hour**：30 天 trend 是分析用，不需要分钟级新鲜度 → 合理。

#### Senior 会接着问的

1. **谁定的这个数？** 通常 DE 跟 BI 团队聊："你们多频繁刷 dashboard？"→ 反推 lag。**这是产品决定不是技术决定**。如果 BI 一天就开两次 dashboard，15 min lag 是浪费。
2. **DOWNSTREAM lag**？Snowflake 支持 `TARGET_LAG = DOWNSTREAM`——意思是"下游被查到才刷"。如果 dashboard 早晚 9-6 才看，半夜不需要 refresh，可以省 50%+。本项目没用，是潜在优化点。
3. **incremental vs full refresh**？Dynamic Table 默认 incremental（只算变化的部分），但有些 SQL pattern（比如 `COUNT(DISTINCT)`）Snowflake 没法 incremental，会偷偷退化成 full refresh，巨贵。看 [05_gold_dynamic_tables.sql](snowflake_sql/05_gold_dynamic_tables.sql) 原本的 `COUNT(DISTINCT country)` —— **这条曾经是 full refresh！** 跑 `SHOW DYNAMIC TABLES;` 看 `refresh_mode` 列能验证。如果是 full，每 15 min 全表重算 → 月成本可能比上面表格高 10x。**✅ 这一条已在 Q6 修复**（删 COUNT DISTINCT + LAG 调到 30 min）。

---

### Q5. `is_estimate_final::BOOLEAN` 如果 Parquet 里是 String 怎么办？

#### Snowflake cast Parquet → BOOLEAN 的规则

[04_pipe.sql:46](snowflake_sql/04_pipe.sql#L46): `$1:is_estimate_final::BOOLEAN`

| Parquet 实际类型 | 值 | Snowflake `::BOOLEAN` 结果 |
|---|---|---|
| BOOLEAN | true | TRUE ✅ |
| STRING | `"true"` / `"TRUE"` / `"t"` / `"yes"` / `"1"` | TRUE ✅ |
| STRING | `"false"` / `"FALSE"` / `"f"` / `"no"` / `"0"` | FALSE ✅ |
| STRING | `"maybe"` / `""` / `"N/A"` / `"YES "`(带空格) | **❌ 报错** |
| INT | 0 | FALSE ✅ |
| INT | 1 | TRUE ✅ |
| INT | 其他（2, -1, 100）| **❌ 报错** |
| NULL | — | NULL（如果列 NOT NULL → 报错）|

#### 报错之后会发生啥（关键的坑）

COPY 有个参数 `ON_ERROR`：

```sql
ON_ERROR = 'ABORT_STATEMENT'  -- 整个 COPY 中止，没行加载（COPY 默认）
ON_ERROR = 'CONTINUE'          -- 跳过坏行，加载好行
ON_ERROR = 'SKIP_FILE'         -- 整个文件跳过
ON_ERROR = 'SKIP_FILE_<n>'     -- 坏行超 n 跳过整文件
```

**🚨 Snowpipe 的默认是 `SKIP_FILE`**（跟普通 COPY 不一样！普通 COPY 默认是 ABORT_STATEMENT）。

#### 本项目的 04_pipe.sql 没设 ON_ERROR

→ 用 Snowpipe 默认 = **SKIP_FILE** → **一个文件里有一行 cast 失败，整个文件被跳过，数据全丢，没有报错**。

也就是说：如果某天 Glue 写出来一个文件里某行 `is_estimate_final = "unknown"`，**整个文件 100 万行都进不来**，而且 pipe 不会显眼地报错——就静悄悄地丢了。

#### 怎么发现这种静默丢失

```sql
-- 看哪些文件 SKIP 掉了
SELECT FILE_NAME, STATUS, FIRST_ERROR_MESSAGE, ROW_COUNT
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
  TABLE_NAME => 'IODP_DC_DEV.SILVER.DC_WIDE',
  START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
))
WHERE STATUS != 'Loaded';
```

或者用专门的 validate 函数：

```sql
SELECT * FROM TABLE(VALIDATE_PIPE_LOAD(
  PIPE_NAME => 'IODP_DC_DEV.RAW_STAGE.PIPE_DC_WIDE',
  START_TIME => DATEADD('day', -7, CURRENT_TIMESTAMP())
));
```

#### 怎么防

三层防线，**最好的是第一层**：

1. **上游严格**：Glue 写 Parquet 时强制 schema，`is_estimate_final` 必须是真 BOOLEAN，不接受 string。一旦上游对了，Snowflake 这边永远不会 cast 失败。
2. **Pipe 配 ON_ERROR + 监控**：把 SKIP_FILE 改成 CONTINUE（保住好行）+ 配 SNS 告警监控 COPY_HISTORY 里的 STATUS != 'Loaded'。
3. **每日对账 job**：reconciliation（Q2 那个）能事后捕捉到。

本项目目前是 **0 层防线**——既没显式 ON_ERROR、也没监控。这是个潜在 bug，建议提一下。

---

### Q6. Q4 那个 COUNT(DISTINCT) 退化成 full refresh 的隐患，怎么修的

#### 起因

Q4 末尾埋的伏笔：[05_gold_dynamic_tables.sql](snowflake_sql/05_gold_dynamic_tables.sql) 里几个 `COUNT(DISTINCT)` 列让两张 daily Dynamic Table 退化成 full refresh —— 每 15 分钟全表重算一次。同时 `DC_PAID_VS_ORGANIC_TREND` 也是 full refresh，但根因不同：滑动窗口 `WHERE dt >= DATEADD('day', -30, CURRENT_DATE())` 边界天天动，Snowflake 没法增量化。

#### 成本分析的几个反直觉点

讨论时差点踩的坑："那就把不需要的字段删掉省钱"——**列式存储里这基本没用**。Snowflake 只读 SELECT 引用到的列，没引用的字段本来就没扫。多算一个 `SUM` 共享同一次 IO，几乎免费。

真正的成本驱动因素：

| 因素 | 影响 |
|---|---|
| 扫描行数 | ⭐⭐⭐ 最大（full vs incremental 差几十倍） |
| `COUNT(DISTINCT)` 基数 | ⭐⭐⭐ 大，且**阻止增量化** |
| GROUP BY 组合数 | ⭐⭐ 中（影响 shuffle） |
| SELECT 字段数 | ⭐ 几乎没影响 |
| 刷新频率 | ⭐⭐⭐ 直接乘数 |

→ **真正的杠杆是"让查询从 full → incremental"**，删 `COUNT(DISTINCT)` 是手段，不是目的。

#### 删字段前的下游引用检查（关键步骤）

memory 里有一条规则："Fix both sides of producer/consumer pairs"。删 Gold 列前先 grep 全仓：

```bash
grep -r "country_count\|device_count\|app_count" snowflake_sql/
# 只命中 05_gold_dynamic_tables.sql 自己
```

`07_bi_view.sql` 是 Silver 层 `DC_WIDE_LATEST` 视图，不读 Gold。下游安全 → 可以删。

如果跳过这步直接删，BI 视图或下游 dashboard 报 "column not found" 是分分钟的事。

#### 实际改动

| 表 | 改动 | 效果 |
|---|---|---|
| `DC_DAILY_BY_APP` | 15min→30min；删 `country_count`、`device_count` | full → incremental + 频率减半 |
| `DC_DAILY_BY_COUNTRY` | 15min→30min；删 `app_count`、`device_count` | full → incremental + 频率减半 |
| `DC_PAID_VS_ORGANIC_TREND` | **不动** | 仍 full（滑动窗口决定，删字段救不了） |

`row_count`（`COUNT(*)`）保留——它不是 DISTINCT，不阻止增量，且对监控数据完整性有用。

#### 为什么 TREND 没改成 30min（即使原话是"都改 30 分钟"）

它本来就是 1hr，改 30min 只会**翻倍开销**，且救不了它的 full-refresh 体质（根因在 `WHERE` 滑动窗口不在 LAG）。30 天 trend 图本来就不需要分钟级新鲜度，1hr 反而更对路。这种时候不能机械执行字面要求，要把账算明白告诉用户。

> 潜在的进一步优化：把 TREND 的 `WHERE dt >= DATEADD('day', -30, ...)` 去掉，让 Dynamic Table 聚合所有日期走 incremental，再让 BI 视图层做 30 天裁剪。本次没做，留作下一轮优化。

#### 部署后怎么验证 incremental 真的生效

```sql
SELECT name, refresh_action, refresh_trigger, target_lag_sec, cost_in_credits
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME => 'IODP_DC_DEV.GOLD.DC_DAILY_BY_APP'
))
ORDER BY refresh_start_time DESC LIMIT 10;
```

`refresh_action` 应该从 `FULL` → `INCREMENTAL`。或者直接：

```sql
SHOW DYNAMIC TABLES IN SCHEMA IODP_DC_DEV.GOLD;
-- 看 refresh_mode 列
```

#### 预期收益（粗估）

两张 daily 表叠加效应：
- 频率 15min → 30min ≈ 省 50%
- Full → Incremental（只扫当日新增 ~1M 行 vs 全表 N×1M 行）≈ 再省 70-90%
- 综合 ≈ 节省 ~85% 以上

实际数字部署后两周，用 Q4 的 credit 对照表 + `DYNAMIC_TABLE_REFRESH_HISTORY.cost_in_credits` 验证。

#### 这个故事的几个教训

1. **写文档时埋的"潜在问题"要回头修**——Q4 当时已经预测到了 full-refresh 风险，但没动手，是文档和代码脱节的典型。
2. **删字段前必须做下游引用检查**，否则 BI 炸。
3. **直觉常常错**——"删字段省 IO"在列存里基本无效，"调小 TARGET_LAG 让数据更新鲜"对 full refresh 表来说是直接乘数的烧钱。
4. **机械执行 ≠ 帮上忙**——用户说"都改 30min"时，TREND 改 30min 实际上是在帮倒忙，把账算明白比照单全收更有价值。

---

## 07_bi_view.sql 两个细节问题

### 问题 1：`实测延时差距 < 200ms` 到底是谁对比谁？

注释里这句话写得太省，容易让人误以为是"加了 QUALIFY 反而只慢 200ms 是因为 Snowflake 优化牛"。**真实原因是 Dedup Task 把活已经干完了，QUALIFY 在大部分时间里是空跑。**

**对比对象**：BI 跑同一句 SQL（例如 `SELECT count(*), sum(downloads_total) FROM ... WHERE dt='2026-05-01'`），两种走法：

| 查询路径 | 等价 SQL |
|---|---|
| **A（不去重）** | `SELECT ... FROM SILVER.DC_WIDE` |
| **B（用本视图）** | `SELECT ... FROM SILVER.DC_WIDE_LATEST`（即 A + `QUALIFY ROW_NUMBER() OVER (...) = 1`） |

**举例**：
- 直查底表 `DC_WIDE`：~1.0s
- 改查 `DC_WIDE_LATEST`：~1.1s（多花 < 200ms 跑窗口函数）

**为什么差距这么小**：

Dedup Task 凌晨 06:00 跑完后，每个 PK 组合（`dt + product_id + app_store + country + device`）在 `DC_WIDE` 里就只剩 1 行了。这时候：

```sql
ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _loaded_at DESC)
```

每个分区里只有 1 行 → 给所有行都打 `rn=1` → `QUALIFY rn=1` 等于不过滤任何行，纯属"陪跑"。Snowflake 只是多做了一次"每个分区排个序"的开销，而每个分区只有 1 行，排序近乎零成本。

**只有在 Snowpipe 新写入到次日 06:00 这 ~20 小时窗口内**，少数 PK 才会有 2 行，QUALIFY 才真的在过滤东西。但即便如此，开销也只是窗口函数本身，不会成数量级放大。

---

### 问题 2：为什么 `ROW_NUMBER` 会破坏 Dynamic Table 的增量刷新？

这是个 Snowflake 高频考点，搞清楚原理就懂了为什么 Gold 层不能图方便走这个视图。

#### 核心原理

Dynamic Table 的"增量刷新"靠的是 **只处理新增/变更的行**，不重扫整张源表。要做到这点，必须满足一个前提：

> **源表来一行新数据，目标表的结果只能影响这一行，不能牵连别的行。**

而 `ROW_NUMBER() OVER (PARTITION BY pk ORDER BY _loaded_at DESC)` 恰好相反——**新来一行可能改变同分区内其他行的排名**。

#### 举例说明

假设 PK = `(2026-05-01, prod_A, ios, US, iphone)`，源表 `DC_WIDE` 此刻只有一行：

| _loaded_at | rn | 是否进 LATEST |
|---|---|---|
| 09:00 | 1 | ✅ |

Dynamic Table 增量刷新时，把这行 INSERT 到目标表。

**Snowpipe 又 COPY 了一次，多了一行 `_loaded_at = 11:00`**：

| _loaded_at | rn | 是否进 LATEST |
|---|---|---|
| **11:00** | **1** | ✅ |
| 09:00 | **2** | ❌（之前是 1，现在变 2 了！） |

问题来了：
- 新行 11:00 → 需要 INSERT 到目标表
- **旧行 09:00 → 需要 DELETE**（它原本 rn=1 已经在目标表里，现在掉到 rn=2 不该出现）

Snowflake 拿到"插入了一行 11:00"这个增量信号时，**没法只看这一行就算出"还得删掉 09:00 那行"**——它必须重新扫整个分区才知道 09:00 的排名变了。

#### Snowflake 的应对

发现查询里有这种"牵连式"窗口函数后，Snowflake 会判定查询"不可增量刷新"，于是：

| 应对 | 后果 |
|---|---|
| 降级为 FULL REFRESH | 每次刷新整张表重算，N 天 × 1M 行全扫一遍，算力成本暴涨（参考前面 §Snowflake Senior 五问 第 4 节，TREND 表 full-refresh 一周烧了约 N 倍 credit）|
| 部分场景直接报错/警告 | `CREATE DYNAMIC TABLE` 阶段就拒绝 |

实际上和 §2024 那段"trend 表为什么变 full refresh"是同一类问题的两个面：那边是"Aggregation 变更引发降级"，这边是"窗口函数引发降级"。

#### 一句话总结

> **窗口函数的结果依赖整个分区，新增一行会"追溯影响"已有行的输出，破坏了"增量 = 只看 delta"的前提，所以 Dynamic Table 没法增量刷新。**

#### 这里的取舍

Gold 层是日报场景（业务能接受 ~20 小时数据稍有重复），所以 Gold 宁愿读 **未去重的 DC_WIDE** 享受增量刷新红利，也不接成本暴涨的 FULL REFRESH。

而 BI 直查视图 `DC_WIDE_LATEST` 是 **on-demand 查询**——BI 跑一次就执行一次，不需要"持续物化"，所以用窗口函数完全没问题。

#### 衍生：什么样的查询能被增量刷新？

可以记几个判断口诀：

| 操作 | 增量友好？ | 原因 |
|---|---|---|
| `WHERE`、`SELECT` 列、`CASE` | ✅ | 行级运算，每行独立 |
| `JOIN` | ✅（多数）| Snowflake 能追踪 join key 变化 |
| `GROUP BY ... SUM/COUNT/MIN/MAX` | ✅ | 增量聚合可累加（但 `MEDIAN/PERCENTILE` 不行）|
| `ROW_NUMBER / RANK / LAG / LEAD` | ❌ | 排名依赖整个分区 |
| `DISTINCT` | ⚠️ 部分场景 ok | 看 Snowflake 版本和数据特征 |
| `QUALIFY 含窗口函数` | ❌ | 同上 |

实操：建 Dynamic Table 后用以下 SQL 检查到底走的是增量还是全量：

```sql
SELECT name, refresh_mode, refresh_mode_reason
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
WHERE schema_name = 'GOLD';
-- refresh_mode_reason 会写明为什么不能 incremental
```

---

## DLQ 周报 Lambda：在 `failed_at=<DATE>/` 分区下还能读到 `.error.json` 吗？

### 背景

DLQ 改成按失败日期分区后，`lib/dlq.py` 写入的路径布局是：

```
dead_letter/failed_at=<DATE>/<original_source_key>             ← Bronze 原文件副本 (copy_to_dlq)
dead_letter/failed_at=<DATE>/<original_key>.error.json         ← 错误元数据
dead_letter/failed_at=<DATE>/dq_failure_dt=<业务dt>/*.parquet  ← Silver DQ 失败数据
```

而 `lambda/dlq_weekly_report/handler.py` 仍然只用 `prefix = "dead_letter/"` 去 list 文件。问题：还能扫到 `.error.json` 吗？

### 结论：能读到 ✅

S3 的 `Prefix` 是**字符串前缀匹配**，不是目录概念。`paginator.paginate(Bucket=bucket, Prefix="dead_letter/")` 会**递归列出**所有以 `dead_letter/` 开头的 key，无视层级深度，分区路径里的 `=` 对 S3 没有特殊含义。

### 举例

假设过去 7 天写入了这些 key：

```
dead_letter/failed_at=2026-05-01/raw/2026/05/01/file1.csv
dead_letter/failed_at=2026-05-01/raw/2026/05/01/file1.csv.error.json
dead_letter/failed_at=2026-05-02/dq_failure_dt=2026-04-30/part-0001.parquet
```

paginator 一次 list 调用就把这 3 个 key 全部返回。handler 逐条处理：

| Key | `endswith(".error.json")` | 进入 if 块 |
|---|---|---|
| `...file1.csv` | False | 否（仅计入 `total_dlq_files`） |
| `...file1.csv.error.json` | **True** | **是** → `get_object` 读取 JSON，统计 `error_type` |
| `...part-0001.parquet` | False | 否（仅计入 `total_dlq_files`） |

### 是否合理

逻辑正确，但有一处需要注意计数口径：

- `total_dlq_files` 把三类文件（原文件副本、error.json、DQ parquet）**全部计数**。一次失败实际产生 1 个 error.json + 1 个原文件副本，所以 `total_dlq_files` ≈ 2× 真正的失败数；
- `len(error_files)` 才是失败次数的准确口径；
- 报告邮件里同时打印这两个数字（"Total DLQ files" 和 "Error .json files"），阅读时以后者为准即可，无需改代码。

---

## 一次 review 触发的连锁修复：默认值 → 参数下线 → 注册 gap → v1/v2 清理

> 本节记录一次主动 code review 引出的连锁修复。起点是发现一个 Lambda 默认值跟业务现状不符；顺着这条线，又暴露了一个 Athena 分区注册 gap，最后把整个项目里残留的 v1/v2 命名一并清理掉。
>
> 每一步都问了一个问题："这个东西在业务上还成立吗？"。三个回答都是"不成立了"，于是连着改了三轮。

### 1. dropzone_freshness_check 的默认 schema version 是反的

#### 1.1 发现

[lambda/dropzone_freshness_check/handler.py](lambda/dropzone_freshness_check/handler.py) 的环境变量 `EXPECTED_VERSIONS` 默认值是 `"wide"`，docstring 上写着"v1 已停用就不查"。这条注释跟代码同时是反的：

- [glue/bronze_etl.py](glue/bronze_etl.py) 里 `DROPZONE_PREFIX = "download_channel/narrow/"` —— 上游实际只上传 narrow
- "wide" 在 dropzone 里**从未出现过**：项目早期设计预留过 dropzone 直接给宽表的旁路，但实际业务从未启用，全部数据走 narrow → Silver pivot 路径
- 如果 freshness Lambda 用 `wide` 默认值跑，每天都会告警"upstream missing"——这是个明显走样的告警

这种 bug 是 AI 容易漏掉的那种：从代码本身看不出来（`narrow` 和 `wide` 两个目录都"合法"，默认值是 `wide`，注释也"自洽"），需要业务侧的实际状态才能反过来发现 default 是错的。

#### 1.2 第一轮修：改默认值

最直觉的修法：把默认值从 `"wide"` 改成 `"narrow"`，注释也反过来。这一步 5 秒能改完。

但停下来问一句：**业务上 wide 还有可能回来吗？**

答案是不会。dropzone wide 旁路是早期设计的"万一上游也能直接给宽表"的兜底，4 年来从未启用。即使未来 Data.ai 又给宽表，也可以加一条新分支处理，不需要靠这个 env var 来切。

#### 1.3 第二轮修：把参数本身删掉

既然 `EXPECTED_VERSIONS` 唯一合理的值就是 `"narrow"`，把它当配置项就是死参数：

- 配置面增加了一个永远不会被用到的旋钮
- 后人读代码会以为"它能配多个值"，去想这个参数的语义
- terraform 里默认值跟 Python 默认值要保持一致，是个隐性的双写

最终改法：

```python
# lambda/dropzone_freshness_check/handler.py
SCHEMA_VERSION = "narrow"  # 模块常量，不再做 env var

def handler(event, context):
    # ...
    for store in stores:
        partition_prefix = f"{prefix_root}{SCHEMA_VERSION}/dt={dt_str}/store={store}/"
        ...
```

同时删掉了：
- `terraform/modules/observability/variables.tf` 里的 `expected_dropzone_versions` 变量定义
- `terraform/modules/observability/main.tf` 里给 Lambda 注入 `EXPECTED_VERSIONS` env var 的那一行
- `README.md` 里 "Tunable schedules" 节列出此变量的那一行

#### 1.4 取舍说明

这是个反 YAGNI 的决定。常见的工程默认是"保留灵活性"——env var 留着、terraform 默认值改一下就行。但保留灵活性的代价是**让代码假装它支持一种永远不会发生的场景**，看代码的人多花脑子去想"为什么要可配置"。

判断标准：**如果一个参数所有合法取值都收敛到一个，它就不该是参数。**

### 2. 顺手发现 Athena 看不到新分区

#### 2.1 触发

聊到上一步的 freshness Lambda 时，用户问起 Bronze 那个 Athena DDL 里的 `MSCK REPAIR TABLE` 是干嘛的。这个问题顺出了一个更大的问题：

> 全仓 grep `MSCK` / `create_partition` / `ADD PARTITION` / `add_partition` 的结果是：**只有 DDL 文件里那 3 条 `MSCK REPAIR`，再没有任何分区登记代码**。

这意味着：

- Glue Bronze/Silver Job 写 `dt=2026-05-03/store=ios/` 的新 Parquet → S3 上有数据 ✓
- 但 Glue Data Catalog 里的 partition 表**不知道**这条新分区存在
- Athena 查询 `SELECT * WHERE dt='2026-05-03'` → 直接返回 **0 行**（不是报错，是空结果）
- 唯一登记入口是手动跑 `make apply-athena-ddl`（里面会跑一次 MSCK）

#### 2.2 这个 gap 严不严重？要看 Athena 怎么用

项目主链路是 **dropzone → Glue Bronze → Glue Silver → Snowpipe → Snowflake**。Snowpipe 不靠 Glue Catalog（它读 S3 文件），所以这个 gap **不阻塞主链路**。Athena 是侧路 ad-hoc 通道，影响面取决于实际怎么用：

| 用法 | 受影响吗 |
|---|---|
| Spark 读 S3：`spark.read.parquet("s3://.../narrow/")` | ✗ 不受影响。Spark 自己 list S3、自己识别 Hive 风格分区，**绕过 Glue Catalog** |
| Athena Console 写 SQL：`SELECT ... WHERE dt='today'` | ✓ 受影响。Athena 严格信 Catalog，没登记的分区当不存在，**返回 0 行不报错** |

第二种"返回 0 行不报错"是最坑的——你以为今天没数据，其实是 Catalog 没登记。

#### 2.3 三种修法的取舍

| 方案 | 复杂度 | 代价 |
|---|---|---|
| **(a) Partition Projection** | 改 1 个 DDL | Athena 按规则即时算分区路径，不依赖 Catalog 元数据。`SHOW PARTITIONS` 不准（但这个项目不用），所以零运维。**最省事**。 |
| **(b) `glue.create_partition()` 写入时登记** | 改 2 个 ETL 文件 + 1 行 IAM | 与现有 Hive 表 layout 完全兼容，登记后 `SHOW PARTITIONS` 能用。需要处理 `AlreadyExistsException`。 |
| **(c) 定时跑 `MSCK REPAIR`** | Lambda + EventBridge | MSCK 随分区数线性变慢，3 个月后 list 几百个目录每天跑一次都嫌贵。**最差**。 |
| **(d) Apache Iceberg** | Glue Job 切 writer + Glue Catalog 切表类型 + Snowflake 改读 Iceberg 或继续读 Parquet（白写元数据） | Iceberg 真正的卖点是 ACID 写、schema 演进、time travel、行级 update/delete——这些需求**一个都没有**。**杀鸡用牛刀**。 |

最后选了 (b)——不是因为它最简单（其实 (a) 更省事），而是它跟现有架构契合度最高：保留 Hive 表 layout、保留可观测的 partition 列表、跟未来万一引入 dbt 兼容更好。

#### 2.4 实现要点

```python
# glue/bronze_etl.py
def register_bronze_partition(dt: str, store: str):
    # Best-effort：Athena 是侧路，注册失败不阻塞 ETL。
    # AlreadyExistsException 是预期情况（restate 重写、MSCK 已登记过），静默吞。
    db_name = f"iodp_dc_bronze_{ENVIRONMENT}"
    table_name = "dc_narrow"
    location = f"s3://{BRONZE_BUCKET}/download_channel/narrow/dt={dt}/store={store}/"
    try:
        glue_client.create_partition(
            DatabaseName=db_name,
            TableName=table_name,
            PartitionInput={
                "Values": [dt, store],
                "StorageDescriptor": {
                    "Location": location,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    },
                },
            },
        )
        print(f"[GLUE] Registered Athena partition {dt}/{store}")
    except glue_client.exceptions.AlreadyExistsException:
        pass
    except Exception as e:
        print(f"[WARN] Failed to register Athena partition {dt}/{store}: {e}")
```

调用点放在 Spark 写完 Parquet **之后**、`checkpoint.release_lock` **之前**。Silver ETL 镜像了完全相同的模式（DB/表名/路径不同）。

几个故意做的设计选择：

1. **失败不阻塞**：Athena 是侧路，数据已经在 S3、Snowpipe 主链路不靠 Glue Catalog，所以注册失败只 print warn，job 继续。
2. **AlreadyExistsException 静默吞**：restate 路径会重写已经存在的分区（`dt=2026-04-25/store=ios` 后续 ETL 又跑一遍），catalog 里那条记录早就有了，再调 `create_partition` 会 409。这是预期情况，不是错误。
3. **DLQ 路径不注册分区**：DQ 阻断走 DLQ 那条分支不调 `register_*_partition`。DLQ 数据本来就不该被 Athena 查到。

#### 2.5 IAM 的细节坑

[terraform/modules/glue_etl/main.tf](terraform/modules/glue_etl/main.tf) 原本已经有 `glue:BatchCreatePartition`，但**没有** `glue:CreatePartition`。这俩在 AWS IAM 里**是分开的两个 action**：

```diff
  Action = [
    "glue:GetDatabase",
    "glue:GetTable",
    "glue:CreateTable",
    "glue:UpdateTable",
    "glue:GetPartitions",
+   "glue:CreatePartition",
    "glue:BatchCreatePartition",
  ]
```

这是个容易漏的细节——AWS IAM 把 batch 和 single 视为两个独立的 action，看 IAM doc 时如果只看到 `BatchCreatePartition` 容易以为已经覆盖。

### 3. 顺手把 v1/v2 命名一并清理掉

#### 3.1 触发

修完 (2) 之后回头看，发现项目里 v1/v2 命名残留得很严重：

- Athena 表名 `dc_narrow_v1`（v1 后缀已无意义，因为永远没有 v2）
- S3 路径 `download_channel/v1/`（同上）
- 整个 [athena_ddl/bronze_dc_wide_v2.sql](athena_ddl/bronze_dc_wide_v2.sql) 文件描述一张永远空的表
- PLAN.md 第 3 节、第 5 节、第 6 节、第 7 节"兼容策略：窄 → 宽迁移"（这个章节描述的迁移**从未发生过**）大量 v1/v2 占位
- README.md 架构图里 "narrow/ wide/" 双分支
- explanation.md restate 时序示例里 `dropzone/wide/...wide.csv.gz`

#### 3.2 这种 stale 命名为什么必须改

不只是审美问题：

1. **每个 v1 后缀都在暗示"v2 存在"**——新人读代码会去找 v2，找不到再去搜 git history，浪费时间
2. **README 架构图直接误导外部 reviewer**——portfolio 项目里这种图是面试官第一眼看到的
3. **PLAN.md §7 整章是描述虚构 migration 的**——"v1 老数据保留在 `v1/` 路径里"、"新数据走 v2"——读了之后会以为业务上确实有过迁移

#### 3.3 改动范围

| 维度 | 旧 | 新 |
|---|---|---|
| Athena Bronze 表 | `dc_narrow_v1` | `dc_narrow` |
| Bronze S3 路径 | `download_channel/v1/...` | `download_channel/narrow/...` |
| Bronze wide 旁路 | `dc_wide_v2` 表 + `download_channel/v2/...` | 全部删除 |
| Athena DDL 文件 | `bronze_dc_narrow_v1.sql`, `bronze_dc_wide_v2.sql` | `bronze_dc_narrow.sql`（删除 wide） |

修了 11 个文件：DDL（重命名 + 删除 1 个）、bronze_etl.py / silver_etl.py、dlq_replay.py、dropzone_freshness_check handler、PLAN.md、README.md、explanation.md（含本节所在文档之前的 restate 时序示例）、两个 schema lib 的 docstring。

#### 3.4 故意没动的东西（避免过度修复）

| 没动的项目 | 理由 |
|---|---|
| Python 模块文件名 `glue/lib/schema_v1_narrow.py` 和 `schema_v2_wide.py` | 这是内部 Python 模块名，不是路径；改名要连带改 import 语句 + 导出常量名（`NARROW_V1_SCHEMA` / `WIDE_V2_SCHEMA`），扩散面大，收益边际 |
| 导出常量名 `NARROW_V1_PK` / `NARROW_V1_SCHEMA` / `WIDE_V2_SCHEMA` | 同上 |
| `terraform/modules/glue_catalog/` 里的资源名 `bronze_dc` / `silver_dc` | 这是 Glue **database** 名，本身没有 v1/v2 问题 |
| `TODOLIST.md` 里 "narrow→wide schema migration" 叙述 | 这是简历/portfolio 的 talking point，描述历史项目的概念演进，不是 stale 路径 |

判断标准是**改动距离用户/数据有多近**：S3 路径和 Athena 表名是对外接口（Athena Console 用户、未来 dbt 代码、文档读者会看到），必须准。Python 内部常量名只在 ETL 代码里用，对外不暴露，先不动也不会误导任何人。

#### 3.5 部署前必须知道的两个 caveat

这次改动**不是兼容性改动**——已部署环境需要手动收尾：

1. **Glue Catalog 旧表对象不会自动删**——已部署的 Catalog 里还有 `dc_narrow_v1` 表对象。新 DDL 跑下去会创建 `dc_narrow` 表，但旧表不会消失：

   ```sql
   DROP TABLE iodp_dc_bronze_<env>.dc_narrow_v1;
   ```

2. **S3 旧目录里的历史 Parquet 不会自动迁移**——`download_channel/v1/` 下的历史数据还在原地，新 ETL 会写到 `download_channel/narrow/`。两条选择：
   - 用 `aws s3 mv --recursive s3://<bronze>/download_channel/v1/ s3://<bronze>/download_channel/narrow/` 把历史搬过来，然后跑一次 MSCK 或新 DDL 让 Catalog 重新登记
   - 从 dropzone 跑一次 backfill（`make backfill BACKFILL_MODE=true`），让 ETL 重新写到新路径

如果是 portfolio 环境（无生产数据）就直接重新部署完事。

### 4. 这一连串修复的元规律

这三个问题都是同一种 pattern：**代码本身能跑、看起来"自洽"，但业务现实早已变了，代码没跟上**。AI（包括我自己）容易漏掉的部分，恰恰就是这种"业务真值"。

判断要不要修的两条朴素标准：

1. **这个东西在业务上还成立吗？** 如果不成立，stale 命名/默认值/参数都得改。
2. **修了之后有没有第二个东西要一起改？** 修代码不改 IAM、修 Python 默认值不改 terraform 默认值、修表名不删旧表对象——这些"半修复"在仓库里反复出现过。所以下面四件事尽量一次做完：
   - 改 writer，同时改 reader
   - 改代码，同时改 terraform
   - 改 prod 行为，同时更新文档（PLAN.md / README.md / explanation.md）
   - 删了某个东西后，grep 全仓确认没有残余引用

第二条特别值得 internalize：**半修复在这个仓库里反复出现过**（DLQ 失败覆盖那次的修复就是个例子，详见早期 commit 5325d09 → 9305514 链）。一次性把对的两边都改了，比修一边等下次再被同样的 gap 咬一次要划算。

---

## 同一天发现的另一个：Snowpipe freshness alert 把日批当流式

> 接着上面那一连串清理之后，又顺手 review 了一下 Snowflake 侧的 alert SQL。结果发现 `08_freshness_alert.sql` 里的 Snowpipe freshness alert 是**把日批当流式**写的，部署后第一天就会被它的每日误报刷屏。这一节记录这个 bug 怎么暴露的、怎么修的、以及为什么最后选了 daily UTC 13:00 而不是更早。

### 1. 暴露问题的注释

[snowflake_sql/08_freshness_alert.sql](snowflake_sql/08_freshness_alert.sql) 头部原本写：

```sql
-- 注意:
--   ACCOUNT_USAGE.COPY_HISTORY 有 ~45 分钟延迟，故检测窗口必须 >= 2h 才有意义。
--   工作时间窗口（PT 凌晨上游交付 + UTC 10:00 ETL）→ UTC 11:00 后必有数据，故定时 hourly。
```

实现：

```sql
SCHEDULE = '60 MINUTE'
IF (EXISTS (
  SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
  WHERE PIPE_NAME = '...PIPE_DC_WIDE'
    AND LAST_LOAD_TIME >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  HAVING COUNT(*) = 0
))
```

逐字翻译：每小时跑一次，如果**最近 2h 没有 COPY** 就告警。

### 2. 这是把流式语义套到日批上

如果是流式 pipeline（比如 iodp 那种 Kafka 24×7 持续 ingest），"最近 2h 没 COPY" 是合理的健康指标：流式管线本来就该 24 小时都有 COPY 活动。

但 Download Channel 是**日批**：每天 UTC 10:00 ETL 触发 → UTC ~11:00 Snowpipe COPY 完成 → **接下来 22 小时一片寂静**，直到第二天 UTC 11:00 新批次到达。

把这个时序套进 alert 条件，会得到下面这张表：

| UTC 时刻 | 距离 last COPY | "最近 2h 有 COPY?" | alert 结果 |
|---|---|---|---|
| 11:00 | ~0 min | ✓ | 静默 |
| 12:00 | 1 h | ✓ | 静默 |
| 13:00 | 2 h（边界） | 大概 ✓ | 静默（运气） |
| **14:00** | **3 h** | **✗** | **🔥 alert 触发** |
| 15:00 | 4 h | ✗ | 🔥 alert 触发 |
| ... | ... | ... | ... |
| 09:00 次日 | 22 h | ✗ | 🔥 alert 触发 |
| 11:00 次日 | 0 min（新批次） | ✓ | 静默 |

也就是 **每天约 21 小时都在告警**，从 UTC 14:00 一直响到第二天 UTC 11:00。部署上线第一天就会被淹没，然后大概率被人 SUSPEND 掉、变成"装饰性 alert"——配置在那里但没人信。

### 3. 修法选择

按日批语义大概有三种思路：

| 方案 | 表达式 | 评价 |
|---|---|---|
| (a) 改 schedule 只在窗口跑 | `SCHEDULE = 'USING CRON 0 11,12,13 * * * UTC'`，条件不变 | 治标——错过窗口的失败仍然不告警 |
| (b) 条件改成"今天还没 COPY" | `WHERE LAST_LOAD_TIME::DATE = CURRENT_DATE` + `daily UTC 13:00` schedule | 业务语义最清晰：今天该到的没到 → 告警 |
| (c) 条件改成"距离最近 COPY 超过 26h" | `HAVING DATEDIFF('hour', MAX(LAST_LOAD_TIME), now) >= 26` | 不依赖整点 schedule，但 26h 这个魔法数字不直白 |

最后选了 (b)。理由：alert 条件直接表达业务现实（"今天预期有数据，没有 → 异常"），不需要看 schedule 才能理解 alert 在守护什么不变量。

### 4. 时区那个坑：为什么用 SYSDATE() 不用 CURRENT_DATE

简化版（用户最先建议的写法）：

```sql
WHERE LAST_LOAD_TIME::DATE = CURRENT_DATE
```

实际写进去的版本：

```sql
WHERE CONVERT_TIMEZONE('UTC', LAST_LOAD_TIME)::DATE = SYSDATE()::DATE
```

为什么不直接 `CURRENT_DATE`？因为 **Snowflake 账号默认 `TIMEZONE = America/Los_Angeles`**。`CURRENT_DATE` 在 PT session 里返回的是 PT 日期，不是 UTC 日期。

举个会出问题的例子：

- alert 在 UTC 13:00 跑（= PT 06:00 今天）
- 上游某天提前到，Snowpipe COPY 发生在 UTC 04:00（= PT 21:00 **昨天**）
- `LAST_LOAD_TIME::DATE`（按 PT 解释）= 昨天 PT
- `CURRENT_DATE`（按 PT 解释）= 今天 PT
- 比较结果 false → **误报**（其实今天的数据已经到了）

`SYSDATE()` 在 Snowflake 里始终返回 **UTC** 的 TIMESTAMP_NTZ，与 session TZ 无关；`CONVERT_TIMEZONE('UTC', LAST_LOAD_TIME)` 把 LTZ 值显式拉到 UTC。两边都锁定在 UTC，无视账号/session 怎么配都跑对。

这种"靠默认值跑对"的代码，在跨账号迁移、CI 跑测试、不同 region 部署时是 landmines。**显式好过隐式**，多打一行 `CONVERT_TIMEZONE` 是值得的。

### 5. 为什么是 daily UTC 13:00 而不是 12:00

13:00 不是随便选的整点，但**也不是必须**——12:00 在大多数日子也能跑对。这一节解释 1 小时 buffer 是给谁留的。

#### 链路上一共有 4 段延迟

| 段 | 来源 | 正常 | 偶尔最坏 |
|---|---|---|---|
| ① | Glue Bronze ETL（DPU 启动 + 处理） | 10~15 min | 30~60 min（restate 多分区时） |
| ② | Glue Silver ETL（pivot + DQ + 写） | 10~15 min | 30~60 min |
| ③ | S3 ObjectCreated → SNS → Snowpipe → COPY 完成 | <1 min | 几分钟 |
| ④ | LAST_LOAD_TIME 物化进 ACCOUNT_USAGE.COPY_HISTORY | 5~15 min | **最长 45 min**（Snowflake 文档明确写死的上限） |

ETL 触发是 UTC 10:00。视图里能查到的时刻 = `10:00 + ① + ② + ③ + ④`。

#### 三个具体场景

**场景 A：普通日（数据少，没 restate）**

| 时刻 (UTC) | 事件 |
|---|---|
| 10:00 | EventBridge 触发 |
| 10:10 | Glue Bronze 完成（① = 10 min） |
| 10:20 | Glue Silver 完成（② = 10 min） |
| 10:21 | Snowpipe COPY 完成（③ = 1 min） |
| 10:36 | ACCOUNT_USAGE 物化（④ = 15 min） |
| **12:00 alert** | ✅ 不告警 |
| **13:00 alert** | ✅ 不告警 |

→ 普通日 12:00 完全够。

**场景 B：restate 日（7 天 × 2 store = 14 个分区要重写）**

| 时刻 (UTC) | 事件 |
|---|---|
| 10:00 | EventBridge 触发 |
| 10:45 | Glue Bronze 完成（① = 45 min，14 个分区重写慢） |
| 11:30 | Glue Silver 完成（② = 45 min） |
| 11:32 | Snowpipe COPY 完成 |
| **12:15** | ACCOUNT_USAGE 物化（④ = 43 min，接近 45 min 上限） |
| **12:00 alert** | ❌ **误报**（视图还没物化进来；Snowpipe 其实成功了） |
| **13:00 alert** | ✅ 不告警 |

→ restate 日 12:00 会误报。

**场景 C：极端慢日（罕见但不是不可能）**

| 时刻 (UTC) | 事件 |
|---|---|
| 10:00 | EventBridge 触发 |
| 11:00 | Glue Bronze 完成（① = 60 min，超大 restate） |
| 12:00 | Glue Silver 完成（② = 60 min） |
| 12:05 | Snowpipe COPY 完成 |
| **12:50** | ACCOUNT_USAGE 物化 |
| **12:00 alert** | ❌ **误报**（Silver 还在跑） |
| **13:00 alert** | ✅ 不告警 |

→ 极端慢日 12:00 误报；13:00 刚好够。

#### 为什么不是 12:30

12:30 也能 cover 场景 B。13:00 多出来的 30 分钟有两个边际好处：

1. **整点 cron 表达式干净**——`'USING CRON 0 13 * * * UTC'` 比 `'USING CRON 30 12 * * * UTC'` 一眼好认。
2. **多出 30 分钟代价几乎 0**——这个 alert 不是 SLA-critical，晚 30 分钟知道"今天没数据"完全可以接受。但少 30 分钟会让场景 C 误报。

**12:00 = 普通日刚好够，restate 日 / 慢日会误报。**
**13:00 = 把 4 段延迟的"偶尔最坏"全 cover 进去，0 误报。**

### 6. 这次修复的元教训

这个 bug 跟前面三个修复属于同一类，但增加了一个新维度：**抽象套错了语义层**。

前三个 bug 是"业务变了代码没跟上"——很容易识别。这一个是更隐蔽的：**代码看起来合理（流式管线常见的 freshness 检查模式），但套到日批 pipeline 上语义就反了**。

判断这种 bug 的窍门：**问"这个检查在 24 小时里有几个小时是 true"**。

- 流式 pipeline 上 "最近 2h 没 COPY"：true 出现的时刻 = 真出故障的时刻（罕见）→ 高信号
- 日批 pipeline 上 "最近 2h 没 COPY"：true 出现的时刻 = 每天大部分时间（21/24 小时）→ 几乎全是噪音

一个 alert 如果 "正常情况下大部分时间都是 true"，它在设计上就有问题——不管它的 schedule 多频繁、邮件文案多漂亮。**Alert 的本质是异常信号，正常态必须 false。**

---

## `apply_athena_ddl.sh` 的 `ACCOUNT_ID` 是什么

[scripts/apply_athena_ddl.sh:8](scripts/apply_athena_ddl.sh#L8) 把 `ACCOUNT_ID` 当第二个位置参数收进来：

```bash
ACCOUNT_ID="${2:?Usage: $0 <ENVIRONMENT> <AWS_ACCOUNT_ID> [AWS_REGION]}"
```

这就是 **AWS 12 位账号数字**（比如 `123456789012`），不是 IAM user/role 名、也不是任何 alias。

### 在脚本里的两个用途

1. **拼 Athena 查询输出桶**（[apply_athena_ddl.sh:13](scripts/apply_athena_ddl.sh#L13)）：

   ```bash
   OUTPUT_LOC="s3://iodp-dc-bronze-${ENVIRONMENT}-${ACCOUNT_ID}/athena-results/"
   ```

   Athena 跑每条 query 都需要一个 S3 路径写结果（即使是 DDL 也会写一个空结果文件）。这里复用了 Bronze 桶下的 `athena-results/` 子目录。

2. **替换 DDL 文件里的 `${ACCOUNT_ID}` 占位符**（[apply_athena_ddl.sh:22-24](scripts/apply_athena_ddl.sh#L22-L24)）：

   ```bash
   rendered=$(sed \
       -e "s/\${ENVIRONMENT}/${ENVIRONMENT}/g" \
       -e "s/\${ACCOUNT_ID}/${ACCOUNT_ID}/g" \
       "${ddl_file}")
   ```

   比如 [athena_ddl/bronze_dc_narrow.sql:17](athena_ddl/bronze_dc_narrow.sql#L17) 里写的是：

   ```sql
   LOCATION 's3://iodp-dc-bronze-${ENVIRONMENT}-${ACCOUNT_ID}/download_channel/narrow/'
   ```

   `sed` 把两个占位符替换成实际值后再丢给 Athena CLI。

### 为什么桶名要带 account ID

S3 bucket 名字是**全球唯一**的命名空间——不只是你的账号唯一，是**全球所有 AWS 用户共享一个命名空间**。如果只叫 `iodp-dc-bronze-prod`，第一个建桶的人占住名字之后，全世界其他人都建不了同名桶。

iodp 项目的命名约定是 `<service>-<env>-<account-id>`，靠 12 位 account ID 保证唯一性、同时让运维一眼能看出桶属于哪个账号（cross-account 排查时特别有用）。

### 怎么查当前 account ID

```bash
aws sts get-caller-identity --query Account --output text
```

`sts:GetCallerIdentity` 是个零权限 API（任何 IAM 主体都能调），不需要专门的 policy。

### 调用脚本

```bash
# 形式
./scripts/apply_athena_ddl.sh <ENVIRONMENT> <AWS_ACCOUNT_ID> [AWS_REGION]

# 例子
./scripts/apply_athena_ddl.sh dev 123456789012 us-east-1
```

第三个参数 region 可省，默认 `us-east-1`（[apply_athena_ddl.sh:9](scripts/apply_athena_ddl.sh#L9)）。

