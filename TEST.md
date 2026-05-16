# TEST.md — Data Quality 校验 & 业务场景 SQL 手册

> 配套文件: [explanation.md](explanation.md) (架构) · [OPERATION.md](OPERATION.md) (运维) · [DEPLOY-ISSUES.md](DEPLOY-ISSUES.md) (踩坑)
>
> 本文件覆盖三个层面:
> 1. **Snowflake 内部不变量** — `SILVER.DC_WIDE` 列与列的等式 / share 公式
> 2. **Athena 跨层一致性** — Bronze 窄表 → Silver 宽表 PIVOT 的逐行对账
> 3. **业务场景查询** — Top-N、趋势、维度矩阵、preview vs finalized

---

## 0. 命名与约定

| 层    | 物理位置                                                                  | 表 / 数据库                                  | 粒度                                                                |
| ----- | ------------------------------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------- |
| Bronze | `s3://iodp-dc-bronze-${env}-${acct}/download_channel/narrow/dt=.../store=.../` | Athena: `iodp_dc_bronze_${env}.dc_narrow`    | **窄表**: 每行 = 一个 `(dt, product, store, country, device, channel)` |
| Silver | `s3://iodp-dc-silver-${env}-${acct}/download_channel/dt=.../store=.../`        | Athena: `iodp_dc_silver_${env}.dc_wide` <br>Snowflake: `IODP_DC_${ENV}.SILVER.DC_WIDE` | **宽表**: 每行 = 一个 `(dt, product, store, country, device)`，4 个 channel pivot 成列 |
| Gold  | Snowflake Dynamic Tables                                                  | `IODP_DC_${ENV}.GOLD.DC_*`                   | 聚合结果（app/country/store 维度）                                  |

下面所有 SQL 以 `dev` 环境为例（`ENV=dev`、`env_lower=dev`）。其他环境替换大写 `DEV` / 小写 `dev` 即可。

### 关键不变量（pivot 后必须成立）

```text
# 来源切分（一级维度）
downloads_total    = downloads_featured + downloads_organic

# 付费切分（一级维度，正交于 featured/organic）
downloads_total    = (downloads_paid_featured  + downloads_paid_organic)
                   + (downloads_unpaid_featured + downloads_unpaid_organic)

# 四象限叶子（二级维度）
downloads_featured = downloads_paid_featured  + downloads_unpaid_featured
downloads_organic  = downloads_paid_organic   + downloads_unpaid_organic

# Share 列
paid_share         = (downloads_paid_featured + downloads_paid_organic) / downloads_total
featured_share     = downloads_featured / downloads_total
                   = (downloads_paid_featured + downloads_unpaid_featured) / downloads_total

# Bronze → Silver 行数关系
silver.row_count   = bronze.distinct(dt, product_id, app_store, country, device).count
                   ≈ bronze.row_count / 4    (当所有 group 都有 4 个 channel 时)
```

> ⚠️ Decimal(6,4) 精度：`paid_share` / `featured_share` 是 `NUMBER(6,4)`（最多 4 位小数）。
> 重算时与列存值的差异在 `1e-4` 量级是正常的（截断），超过 `1e-3` 才需要怀疑。

---

## 1. Snowflake — 等式不变量校验

> 这一节回答用户问题: **download_total = download_paid + download_organic 的逻辑验证**
> （注：原始拼写偏简，实际有两种切分方式，下面 1.2 和 1.3 都覆盖）

### 1.1 总量切分 ①: total = featured + organic

```sql
USE DATABASE IODP_DC_DEV;
USE SCHEMA SILVER;

-- 应该返回 0 行（全部满足等式）
SELECT
  dt, product_id, app_store, country, device,
  downloads_total,
  downloads_featured,
  downloads_organic,
  (downloads_featured + downloads_organic)               AS sum_split,
  downloads_total - (downloads_featured + downloads_organic) AS delta
FROM DC_WIDE
WHERE downloads_total <> (downloads_featured + downloads_organic)
ORDER BY ABS(downloads_total - (downloads_featured + downloads_organic)) DESC
LIMIT 100;
```

### 1.2 总量切分 ②: total = paid_total + unpaid_total

> 用户问的"download_paid + download_organic"，正确含义是 **paid 总量 + unpaid 总量**
> （都是付费维度的两端），见 [schema_v2_wide.py:38-44](glue/lib/schema_v2_wide.py#L38-L44)。

```sql
SELECT
  dt, product_id, app_store, country, device,
  downloads_total,
  (downloads_paid_featured   + downloads_paid_organic)    AS paid_total,
  (downloads_unpaid_featured + downloads_unpaid_organic)  AS unpaid_total,
  downloads_total
    - (downloads_paid_featured   + downloads_paid_organic)
    - (downloads_unpaid_featured + downloads_unpaid_organic) AS delta
FROM DC_WIDE
WHERE downloads_total <>
        (downloads_paid_featured   + downloads_paid_organic)
      + (downloads_unpaid_featured + downloads_unpaid_organic)
ORDER BY ABS(delta) DESC
LIMIT 100;
```

### 1.3 一级维度 → 四象限叶子的二级切分

```sql
-- featured = paid_featured + unpaid_featured
SELECT COUNT(*) AS featured_eq_violations
FROM DC_WIDE
WHERE downloads_featured <>
        (downloads_paid_featured + downloads_unpaid_featured);

-- organic = paid_organic + unpaid_organic
SELECT COUNT(*) AS organic_eq_violations
FROM DC_WIDE
WHERE downloads_organic <>
        (downloads_paid_organic + downloads_unpaid_organic);
```

### 1.4 按 dt 看违规率（dashboard 风格）

```sql
SELECT
  dt,
  COUNT(*)                                                AS total_rows,
  SUM(CASE WHEN downloads_total <>
           downloads_featured + downloads_organic
           THEN 1 ELSE 0 END)                             AS eq1_violations,
  SUM(CASE WHEN downloads_featured <>
           downloads_paid_featured + downloads_unpaid_featured
           THEN 1 ELSE 0 END)                             AS eq2_violations,
  SUM(CASE WHEN downloads_organic <>
           downloads_paid_organic + downloads_unpaid_organic
           THEN 1 ELSE 0 END)                             AS eq3_violations,
  ROUND(100.0 * SUM(CASE WHEN downloads_total <>
           downloads_featured + downloads_organic
           THEN 1 ELSE 0 END) / COUNT(*), 4)              AS eq1_violation_pct
FROM DC_WIDE
WHERE dt >= DATEADD('day', -14, CURRENT_DATE())
GROUP BY dt
ORDER BY dt DESC;
```

---

## 2. Snowflake — Share 列校验

### 2.1 paid_share 公式重算（容忍 1e-4 截断误差）

```sql
SELECT
  dt, product_id, app_store, country, device,
  downloads_total,
  downloads_paid_featured, downloads_paid_organic,
  paid_share                                                       AS paid_share_stored,
  ROUND(
    (downloads_paid_featured + downloads_paid_organic)
    / NULLIF(downloads_total, 0),
    4
  )                                                                AS paid_share_recomputed,
  ABS(paid_share - ROUND(
    (downloads_paid_featured + downloads_paid_organic)
    / NULLIF(downloads_total, 0),
    4
  ))                                                               AS delta
FROM DC_WIDE
WHERE downloads_total > 0
  AND ABS(paid_share - ROUND(
        (downloads_paid_featured + downloads_paid_organic)
        / NULLIF(downloads_total, 0),
        4
      )) > 0.0001                                                  -- 容忍 1e-4
ORDER BY delta DESC
LIMIT 100;
```

### 2.2 featured_share 公式重算

```sql
SELECT
  dt, product_id, app_store, country, device,
  downloads_total, downloads_featured,
  featured_share                                                   AS featured_share_stored,
  ROUND(downloads_featured / NULLIF(downloads_total, 0), 4)        AS featured_share_recomputed
FROM DC_WIDE
WHERE downloads_total > 0
  AND ABS(
        featured_share
        - ROUND(downloads_featured / NULLIF(downloads_total, 0), 4)
      ) > 0.0001
LIMIT 100;
```

### 2.3 share 取值范围 [0, 1]

```sql
SELECT
  COUNT_IF(paid_share < 0 OR paid_share > 1)         AS paid_share_oob,
  COUNT_IF(featured_share < 0 OR featured_share > 1) AS featured_share_oob,
  COUNT_IF(paid_share IS NULL)                       AS paid_share_null,
  COUNT_IF(featured_share IS NULL)                   AS featured_share_null,
  COUNT_IF(downloads_total = 0)                      AS zero_total_rows
FROM DC_WIDE;
```

> **预期**: `paid_share_null` ≈ `featured_share_null` ≈ `zero_total_rows`
> (Spark `cast("decimal(6,4)")` 在分母 0 时输出 `NULL`，这是合理的)

---

## 3. Snowflake — 维度健康度

### 3.1 主键唯一性

```sql
-- 应该返回 0 行；否则 Snowpipe 重复 COPY 或者 Silver Spark overwrite 失败
SELECT
  dt, product_id, app_store, country, device,
  COUNT(*) AS dup_count
FROM DC_WIDE
GROUP BY 1, 2, 3, 4, 5
HAVING COUNT(*) > 1
ORDER BY dup_count DESC
LIMIT 20;
```

### 3.2 NULL 率（关键列必须 0%）

```sql
SELECT
  COUNT(*)                                AS total_rows,
  100.0 * COUNT_IF(product_id IS NULL) / COUNT(*) AS pct_null_product,
  100.0 * COUNT_IF(country    IS NULL) / COUNT(*) AS pct_null_country,
  100.0 * COUNT_IF(app_store  IS NULL) / COUNT(*) AS pct_null_store,
  100.0 * COUNT_IF(device     IS NULL) / COUNT(*) AS pct_null_device,
  100.0 * COUNT_IF(dt         IS NULL) / COUNT(*) AS pct_null_dt
FROM DC_WIDE;
```

### 3.3 dt 分布 + 数据新鲜度

```sql
SELECT
  dt,
  COUNT(*)                  AS row_count,
  MIN(ingest_ts)            AS first_silver_write,
  MAX(ingest_ts)            AS last_silver_write,
  MAX(_loaded_at)           AS last_snowpipe_load,
  SUM(downloads_total)      AS total_downloads,
  COUNT_IF(is_estimate_final = TRUE)  AS finalized_rows,
  COUNT_IF(is_estimate_final = FALSE) AS preview_rows
FROM DC_WIDE
GROUP BY dt
ORDER BY dt DESC;
```

### 3.4 负值（DQ §4 仅告警，不阻断；这里全量复查）

```sql
SELECT
  COUNT_IF(downloads_total           < 0) AS neg_total,
  COUNT_IF(downloads_featured        < 0) AS neg_featured,
  COUNT_IF(downloads_organic         < 0) AS neg_organic,
  COUNT_IF(downloads_paid_featured   < 0) AS neg_pf,
  COUNT_IF(downloads_paid_organic    < 0) AS neg_po,
  COUNT_IF(downloads_unpaid_featured < 0) AS neg_uf,
  COUNT_IF(downloads_unpaid_organic  < 0) AS neg_uo
FROM DC_WIDE;
```

---

## 4. Athena Bronze（`dc_narrow` 窄表）

> ⚠️ **`dt` 在 Athena 端是 `STRING`，不是 `DATE`**（见 [athena_ddl/bronze_dc_narrow.sql:15](athena_ddl/bronze_dc_narrow.sql#L15) `PARTITIONED BY (dt STRING, store STRING)`）。
> Athena/Trino 不会自动把 `varchar` 跟 `date` 互相 cast，所以：
> - ❌ `WHERE dt = DATE '2026-05-13'`               → `TYPE_MISMATCH: varchar <= date`
> - ❌ `WHERE dt >= date_add('day', -14, current_date)` → 同上
> - ✅ `WHERE dt = '2026-05-13'`                    → 字符串比较，且**保留 partition pruning**
> - ✅ `WHERE dt >= date_format(date_add('day', -14, current_date), '%Y-%m-%d')`
>
> 注意不要用 `CAST(dt AS DATE) = DATE '...'`——左侧加函数会让 Athena 看不出过滤的是分区列，partition pruning 失效，全表扫。
>
> dt 字面量是 `'YYYY-MM-DD'`，字典序 == 时间序，字符串比较与日期比较等价。

### 4.0 分区登记（首次使用某 dt 时）

```sql
-- Bronze ETL 会自动调用 glue.create_partition；如果 Athena 看不见，手动同步：
MSCK REPAIR TABLE iodp_dc_bronze_dev.dc_narrow;

SHOW PARTITIONS iodp_dc_bronze_dev.dc_narrow;
```

### 4.1 行数 / 分区覆盖

```sql
SELECT
  dt, store,
  COUNT(*)                            AS row_count,
  COUNT(DISTINCT product_id)          AS distinct_products,
  COUNT(DISTINCT country)             AS distinct_countries,
  MIN(ingest_ts)                      AS first_ingest,
  MAX(ingest_ts)                      AS last_ingest
FROM iodp_dc_bronze_dev.dc_narrow
WHERE dt >= date_format(date_add('day', -14, current_date), '%Y-%m-%d')
GROUP BY dt, store
ORDER BY dt DESC, store;
```

### 4.2 channel 合法性（必须是 4 个枚举值之一）

```sql
-- 应该只返回 paid_featured / paid_organic / unpaid_featured / unpaid_organic
SELECT channel, COUNT(*) AS rows
FROM iodp_dc_bronze_dev.dc_narrow
WHERE dt = '2026-05-13'
GROUP BY channel
ORDER BY rows DESC;
```

### 4.3 窄表完整性：每个 group 必须有 4 个 channel

```sql
-- 如果一个 (dt, product, store, country, device) 不是恰好 4 行,
-- 说明上游 dropzone 有数据缺失，Silver pivot 出来的 share 会失真。
SELECT
  channel_count,
  COUNT(*) AS group_count
FROM (
  SELECT
    dt, product_id, app_store, country, device,
    COUNT(DISTINCT channel) AS channel_count
  FROM iodp_dc_bronze_dev.dc_narrow
  WHERE dt = '2026-05-13'
  GROUP BY 1, 2, 3, 4, 5
)
GROUP BY channel_count
ORDER BY channel_count DESC;
-- 预期: channel_count=4 占 100%
```

### 4.4 share_pct ∈ [0, 1] 且每 group 4 行之和 ≈ 1

```sql
-- 4 行 share_pct 加起来必须 ≈ 1.0 (Decimal(6,4) 精度容忍 1e-3)
SELECT
  dt, product_id, app_store, country, device,
  SUM(share_pct) AS share_sum,
  COUNT(*)       AS channel_count
FROM iodp_dc_bronze_dev.dc_narrow
WHERE dt = '2026-05-13'
GROUP BY 1, 2, 3, 4, 5
HAVING ABS(SUM(share_pct) - 1.0) > 0.001
ORDER BY ABS(SUM(share_pct) - 1.0) DESC
LIMIT 100;
```

### 4.5 NULL 率 / 负值

```sql
SELECT
  COUNT(*)                                                  AS total,
  100.0 * SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS pct_null_product,
  100.0 * SUM(CASE WHEN channel    IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS pct_null_channel,
  100.0 * SUM(CASE WHEN downloads  IS NULL THEN 1 ELSE 0 END) / COUNT(*) AS pct_null_downloads,
  SUM(CASE WHEN downloads < 0 THEN 1 ELSE 0 END)             AS neg_downloads,
  SUM(CASE WHEN share_pct < 0 OR share_pct > 1 THEN 1 ELSE 0 END) AS oob_share
FROM iodp_dc_bronze_dev.dc_narrow
WHERE dt = '2026-05-13';
```

### 4.6 重复主键（窄表 PK = dt+product+store+country+device+channel）

```sql
-- 应返回 0 行；Bronze 在 Spark 内做过 row_number 去重，不该再有
SELECT
  dt, product_id, app_store, country, device, channel,
  COUNT(*) AS dup_count
FROM iodp_dc_bronze_dev.dc_narrow
WHERE dt = '2026-05-13'
GROUP BY 1, 2, 3, 4, 5, 6
HAVING COUNT(*) > 1;
```

---

## 5. Athena Silver（`dc_wide` 宽表）

> ⚠️ 同 §4 注意：Athena Silver 的 `dt` 也是 `STRING`（[athena_ddl/silver_dc_wide.sql:21](athena_ddl/silver_dc_wide.sql#L21)）。字面量都用 `'2026-05-13'`，不要写 `DATE '2026-05-13'`。

### 5.1 行数 / 主键唯一

```sql
-- 行数 + 是否有重复主键
WITH base AS (
  SELECT
    dt, product_id, app_store, country, device,
    COUNT(*) AS cnt
  FROM iodp_dc_silver_dev.dc_wide
  WHERE dt = '2026-05-13'
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  SUM(cnt)                       AS total_rows,
  COUNT(*)                       AS distinct_groups,
  SUM(CASE WHEN cnt > 1 THEN 1 ELSE 0 END) AS duplicate_groups
FROM base;
-- 预期: total_rows = distinct_groups, duplicate_groups = 0
```

### 5.2 等式校验（与 §1 相同；放在 S3 侧便于独立验证 Snowpipe）

```sql
SELECT
  COUNT(*)                                                          AS total_rows,
  SUM(CASE WHEN downloads_total <>
           downloads_featured + downloads_organic
           THEN 1 ELSE 0 END)                                       AS eq1_violations,
  SUM(CASE WHEN downloads_total <>
           downloads_paid_featured + downloads_paid_organic
         + downloads_unpaid_featured + downloads_unpaid_organic
           THEN 1 ELSE 0 END)                                       AS eq2_violations,
  SUM(CASE WHEN downloads_featured <>
           downloads_paid_featured + downloads_unpaid_featured
           THEN 1 ELSE 0 END)                                       AS eq3_violations,
  SUM(CASE WHEN downloads_organic <>
           downloads_paid_organic + downloads_unpaid_organic
           THEN 1 ELSE 0 END)                                       AS eq4_violations
FROM iodp_dc_silver_dev.dc_wide
WHERE dt = '2026-05-13';
```

### 5.3 Share 重算

```sql
SELECT
  dt, product_id, app_store, country, device,
  paid_share,
  ROUND(
    CAST(downloads_paid_featured + downloads_paid_organic AS DOUBLE)
    / NULLIF(downloads_total, 0), 4
  ) AS paid_share_recalc,
  featured_share,
  ROUND(
    CAST(downloads_featured AS DOUBLE)
    / NULLIF(downloads_total, 0), 4
  ) AS featured_share_recalc
FROM iodp_dc_silver_dev.dc_wide
WHERE dt = '2026-05-13'
  AND downloads_total > 0
  AND (
    ABS(paid_share - ROUND(
      CAST(downloads_paid_featured + downloads_paid_organic AS DOUBLE)
      / NULLIF(downloads_total, 0), 4)) > 0.0001
    OR ABS(featured_share - ROUND(
      CAST(downloads_featured AS DOUBLE)
      / NULLIF(downloads_total, 0), 4)) > 0.0001
  )
LIMIT 100;
```

---

## 6. 跨层 Bronze → Silver PIVOT 一致性（**核心校验**）

> 这是把 [silver_etl.py:153](glue/silver_etl.py#L153) 的 `pivot_narrow_to_wide` 函数用 SQL 重写一遍，
> 然后逐字段与 Silver 列做差。任何不一致都意味着 ETL bug（或 Bronze/Silver 跨 dt 不同步）。

### 6.1 行数关系：silver.row_count = bronze.distinct_groups

```sql
WITH bronze_groups AS (
  SELECT COUNT(*) AS bronze_distinct_groups
  FROM (
    SELECT DISTINCT dt, product_id, app_store, country, device
    FROM iodp_dc_bronze_dev.dc_narrow
    WHERE dt = '2026-05-13'
  )
),
silver_count AS (
  SELECT COUNT(*) AS silver_rows
  FROM iodp_dc_silver_dev.dc_wide
  WHERE dt = '2026-05-13'
)
SELECT
  b.bronze_distinct_groups,
  s.silver_rows,
  b.bronze_distinct_groups - s.silver_rows AS delta
FROM bronze_groups b CROSS JOIN silver_count s;
-- 预期: delta = 0
```

### 6.2 全字段重算 + 行级对账（最核心的一条）

```sql
WITH bronze_pivot AS (
  SELECT
    dt, product_id, app_store, country, device,
    SUM(downloads)                                                      AS r_total,
    SUM(CASE WHEN channel IN ('paid_featured','unpaid_featured')
             THEN downloads ELSE 0 END)                                 AS r_featured,
    SUM(CASE WHEN channel IN ('paid_organic','unpaid_organic')
             THEN downloads ELSE 0 END)                                 AS r_organic,
    SUM(CASE WHEN channel = 'paid_featured'   THEN downloads ELSE 0 END) AS r_pf,
    SUM(CASE WHEN channel = 'paid_organic'    THEN downloads ELSE 0 END) AS r_po,
    SUM(CASE WHEN channel = 'unpaid_featured' THEN downloads ELSE 0 END) AS r_uf,
    SUM(CASE WHEN channel = 'unpaid_organic'  THEN downloads ELSE 0 END) AS r_uo,
    MIN(COALESCE(is_estimate_final, FALSE))                              AS r_final
  FROM iodp_dc_bronze_dev.dc_narrow
  WHERE dt = '2026-05-13'
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  s.dt, s.product_id, s.app_store, s.country, s.device,
  -- 总量
  s.downloads_total            AS s_total,
  b.r_total                    AS b_total,
  s.downloads_total - b.r_total AS d_total,
  -- featured / organic
  s.downloads_featured - b.r_featured  AS d_featured,
  s.downloads_organic  - b.r_organic   AS d_organic,
  -- 四象限叶子
  s.downloads_paid_featured   - b.r_pf  AS d_pf,
  s.downloads_paid_organic    - b.r_po  AS d_po,
  s.downloads_unpaid_featured - b.r_uf  AS d_uf,
  s.downloads_unpaid_organic  - b.r_uo  AS d_uo,
  -- is_estimate_final 一致性
  s.is_estimate_final          AS s_final,
  b.r_final                    AS b_final
FROM iodp_dc_silver_dev.dc_wide s
JOIN bronze_pivot b
  ON s.dt = b.dt
 AND s.product_id = b.product_id
 AND s.app_store  = b.app_store
 AND s.country    = b.country
 AND s.device     = b.device
WHERE s.dt = '2026-05-13'
  AND (
       s.downloads_total            <> b.r_total
    OR s.downloads_featured         <> b.r_featured
    OR s.downloads_organic          <> b.r_organic
    OR s.downloads_paid_featured    <> b.r_pf
    OR s.downloads_paid_organic     <> b.r_po
    OR s.downloads_unpaid_featured  <> b.r_uf
    OR s.downloads_unpaid_organic   <> b.r_uo
    OR s.is_estimate_final          <> b.r_final
  )
LIMIT 100;
-- 预期: 返回 0 行
```

### 6.3 ANTI JOIN — 找出 bronze 有 / silver 没有 的 group

```sql
-- bronze 里存在但 silver 里没有 (silver 丢数)
SELECT b.*
FROM (
  SELECT DISTINCT dt, product_id, app_store, country, device
  FROM iodp_dc_bronze_dev.dc_narrow
  WHERE dt = '2026-05-13'
) b
LEFT JOIN iodp_dc_silver_dev.dc_wide s
  ON s.dt = b.dt AND s.product_id = b.product_id
 AND s.app_store = b.app_store AND s.country = b.country AND s.device = b.device
WHERE s.product_id IS NULL
LIMIT 50;

-- 反向: silver 里多出来的 group (silver 幻影行 / Bronze 已被删除但 Silver 没刷新)
SELECT s.dt, s.product_id, s.app_store, s.country, s.device
FROM iodp_dc_silver_dev.dc_wide s
LEFT JOIN (
  SELECT DISTINCT dt, product_id, app_store, country, device
  FROM iodp_dc_bronze_dev.dc_narrow
  WHERE dt = '2026-05-13'
) b
  ON s.dt = b.dt AND s.product_id = b.product_id
 AND s.app_store = b.app_store AND s.country = b.country AND s.device = b.device
WHERE b.product_id IS NULL
  AND s.dt = '2026-05-13'
LIMIT 50;
```

### 6.4 share 列与窄表 share_pct 求和一致

```sql
-- silver.paid_share ≈ SUM(bronze.share_pct WHERE channel like 'paid_%')
-- 容忍 Decimal(6,4) 双重截断 → 2e-4
WITH bronze_share AS (
  SELECT
    dt, product_id, app_store, country, device,
    SUM(CASE WHEN channel IN ('paid_featured','paid_organic')
             THEN share_pct ELSE 0 END) AS b_paid_share,
    SUM(CASE WHEN channel IN ('paid_featured','unpaid_featured')
             THEN share_pct ELSE 0 END) AS b_featured_share
  FROM iodp_dc_bronze_dev.dc_narrow
  WHERE dt = '2026-05-13'
  GROUP BY 1, 2, 3, 4, 5
)
SELECT
  s.dt, s.product_id, s.app_store, s.country, s.device,
  s.paid_share, b.b_paid_share,
  s.featured_share, b.b_featured_share,
  ABS(s.paid_share     - b.b_paid_share)     AS d_paid,
  ABS(s.featured_share - b.b_featured_share) AS d_featured
FROM iodp_dc_silver_dev.dc_wide s
JOIN bronze_share b
  ON s.dt = b.dt AND s.product_id = b.product_id
 AND s.app_store = b.app_store AND s.country = b.country AND s.device = b.device
WHERE s.dt = '2026-05-13'
  AND (
       ABS(s.paid_share     - b.b_paid_share)     > 0.0002
    OR ABS(s.featured_share - b.b_featured_share) > 0.0002
  )
LIMIT 50;
```

---

## 7. 跨层 Silver S3 ↔ Snowflake `SILVER.DC_WIDE`

> 用来定位 Snowpipe 是否漏 COPY、是否 lag。

### 7.1 行数对账

```sql
-- Athena 侧
SELECT COUNT(*) FROM iodp_dc_silver_dev.dc_wide WHERE dt = '2026-05-13';

-- Snowflake 侧
SELECT COUNT(*) FROM IODP_DC_DEV.SILVER.DC_WIDE WHERE dt = '2026-05-13';
```

### 7.2 按 dt 看 SUM(downloads_total) 对账

```sql
-- Athena
SELECT dt, COUNT(*) AS rows, SUM(downloads_total) AS total
FROM iodp_dc_silver_dev.dc_wide
GROUP BY dt ORDER BY dt DESC;

-- Snowflake
SELECT dt, COUNT(*) AS rows, SUM(downloads_total) AS total
FROM IODP_DC_DEV.SILVER.DC_WIDE
GROUP BY dt ORDER BY dt DESC;
```

### 7.3 Snowpipe COPY_HISTORY 验证（最近 12h 都 COPY 过哪些文件）

```sql
SELECT
  file_name,
  pipe_name,
  status,
  row_count,
  row_parsed,
  first_error_message,
  last_load_time
FROM TABLE(
  IODP_DC_DEV.INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME  => 'IODP_DC_DEV.SILVER.DC_WIDE',
    START_TIME  => DATEADD('hour', -12, CURRENT_TIMESTAMP())
  )
)
ORDER BY last_load_time DESC
LIMIT 50;
```

### 7.4 Snowpipe 待处理量

```sql
SELECT
  SYSTEM$PIPE_STATUS('IODP_DC_DEV.RAW_STAGE.PIPE_DC_WIDE') AS pipe_status;
-- pendingFileCount > 0 表示有 file 排队，正常应秒级清零。
```

---

## 8. Gold Dynamic Tables 验证

### 8.1 DC_DAILY_BY_APP 与 SILVER 直接 GROUP BY 对账

```sql
WITH src_agg AS (
  SELECT
    dt, product_id, app_store,
    SUM(downloads_total)     AS s_total,
    SUM(downloads_featured)  AS s_featured,
    SUM(downloads_organic)   AS s_organic,
    COUNT(*)                 AS s_rows
  FROM IODP_DC_DEV.SILVER.DC_WIDE
  WHERE dt = '2026-05-13'
  GROUP BY 1, 2, 3
)
SELECT
  g.dt, g.product_id, g.app_store,
  g.downloads_total AS g_total, s.s_total,
  g.row_count       AS g_rows,  s.s_rows
FROM IODP_DC_DEV.GOLD.DC_DAILY_BY_APP g
JOIN src_agg s USING (dt, product_id, app_store)
WHERE g.downloads_total <> s.s_total
   OR g.row_count       <> s.s_rows
LIMIT 50;
-- 预期: 0 行（除非 Dynamic Table 还没刷新，看 8.4 状态）
```

### 8.2 DC_DAILY_BY_COUNTRY

```sql
SELECT
  g.dt, g.country, g.app_store,
  g.downloads_total,
  SUM(s.downloads_total) AS recalc
FROM IODP_DC_DEV.GOLD.DC_DAILY_BY_COUNTRY g
JOIN IODP_DC_DEV.SILVER.DC_WIDE s USING (dt, country, app_store)
WHERE g.dt = '2026-05-13'
GROUP BY 1, 2, 3, 4
HAVING g.downloads_total <> SUM(s.downloads_total)
LIMIT 50;
```

### 8.3 DC_PAID_VS_ORGANIC_TREND — paid_ratio / featured_ratio 检查

```sql
SELECT
  dt, app_store,
  downloads_total,
  downloads_paid_featured + downloads_paid_organic AS recalc_paid,
  paid_ratio,
  ROUND(
    (downloads_paid_featured + downloads_paid_organic) / NULLIF(downloads_total, 0),
    4
  ) AS recalc_paid_ratio,
  featured_ratio,
  ROUND(downloads_featured / NULLIF(downloads_total, 0), 4) AS recalc_featured_ratio
FROM IODP_DC_DEV.GOLD.DC_PAID_VS_ORGANIC_TREND
WHERE dt >= DATEADD('day', -30, CURRENT_DATE())
ORDER BY dt DESC, app_store
LIMIT 60;
```

### 8.4 Dynamic Table 刷新状态

```sql
SHOW DYNAMIC TABLES IN SCHEMA IODP_DC_DEV.GOLD;

SELECT
  name,
  state,
  refresh_end_time,
  refresh_end_time IS NULL OR
    TIMESTAMPDIFF('minute', refresh_end_time, CURRENT_TIMESTAMP()) > 60 AS is_stale
FROM TABLE(IODP_DC_DEV.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY(
  NAME_PREFIX => 'IODP_DC_DEV.GOLD'
));


-- 强制刷新（如果数据看着还没进 Gold，又等不及 TARGET_LAG）:
ALTER DYNAMIC TABLE IODP_DC_DEV.GOLD.DC_DAILY_BY_APP        REFRESH;
ALTER DYNAMIC TABLE IODP_DC_DEV.GOLD.DC_DAILY_BY_COUNTRY    REFRESH;
ALTER DYNAMIC TABLE IODP_DC_DEV.GOLD.DC_PAID_VS_ORGANIC_TREND REFRESH;
```

---

## 9. 业务场景查询

### 9.1 Top-N apps（按 store 看当日下载王）

```sql
SELECT
  app_store,
  product_id,
  SUM(downloads_total)     AS total_dl,
  SUM(downloads_paid_featured + downloads_paid_organic) AS paid_dl,
  ROUND(SUM(downloads_paid_featured + downloads_paid_organic) * 100.0
        / NULLIF(SUM(downloads_total), 0), 2) AS paid_pct
FROM IODP_DC_DEV.SILVER.DC_WIDE
WHERE dt = '2026-05-13'
GROUP BY app_store, product_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY app_store ORDER BY total_dl DESC) <= 10
ORDER BY app_store, total_dl DESC;
```

### 9.2 国家 × 平台 矩阵

```sql
SELECT
  country,
  SUM(CASE WHEN app_store = 'ios'         THEN downloads_total END) AS ios_total,
  SUM(CASE WHEN app_store = 'google-play' THEN downloads_total END) AS gp_total
FROM IODP_DC_DEV.SILVER.DC_WIDE
WHERE dt = '2026-05-13'
GROUP BY country
ORDER BY ios_total DESC NULLS LAST
LIMIT 20;
```

### 9.3 paid_ratio 周趋势（按 store）

```sql
SELECT
  DATE_TRUNC('week', dt) AS week_start,
  app_store,
  SUM(downloads_total)                                                   AS total,
  ROUND(
    SUM(downloads_paid_featured + downloads_paid_organic) * 100.0
    / NULLIF(SUM(downloads_total), 0), 2
  ) AS paid_pct
FROM IODP_DC_DEV.SILVER.DC_WIDE
WHERE dt >= DATEADD('day', -56, CURRENT_DATE())
GROUP BY 1, 2
ORDER BY 1 DESC, 2;
```

### 9.4 周对周（WoW）：本周 7 天累计 vs 前 7 天累计

> 真正的 "周对周" 应该比 **7 天累计 vs 7 天累计**，而不是单天 vs 单天（后者只是 "同一星期几相隔 7 天"，受 weekday seasonality 影响小但仍是 1 天数据，不是 1 周）。
> 设 `:dt = '2026-05-13'`，本周窗口 = `[dt-6, dt]`，前一周窗口 = `[dt-13, dt-7]`。

```sql
WITH weekly AS (
  SELECT
    product_id, app_store,
    SUM(CASE WHEN dt BETWEEN DATEADD('day', -6,  DATE '2026-05-13')
                         AND DATE '2026-05-13'
             THEN downloads_total END) AS this_week,
    SUM(CASE WHEN dt BETWEEN DATEADD('day', -13, DATE '2026-05-13')
                         AND DATEADD('day', -7,  DATE '2026-05-13')
             THEN downloads_total END) AS prior_week
  FROM IODP_DC_DEV.SILVER.DC_WIDE
  WHERE dt BETWEEN DATEADD('day', -13, DATE '2026-05-13')
              AND DATE '2026-05-13'
  GROUP BY product_id, app_store
)
SELECT
  product_id, app_store,
  this_week,
  prior_week,
  ROUND(100.0 * (this_week - prior_week) / NULLIF(prior_week, 0), 2) AS wow_pct
FROM weekly
WHERE this_week IS NOT NULL
ORDER BY wow_pct DESC NULLS LAST
LIMIT 20;
```

> 想要"同一 weekday 单天对比"（DoD-7）或"7 天滚动均值"的写法，见 [explanation.md §v](explanation.md)。

### 9.5 preview vs finalized 数据分布（提醒下游谨慎用）

```sql
SELECT
  dt,
  COUNT_IF(is_estimate_final = TRUE)  AS finalized_rows,
  COUNT_IF(is_estimate_final = FALSE) AS preview_rows,
  ROUND(100.0 * COUNT_IF(is_estimate_final = TRUE) / COUNT(*), 1) AS pct_finalized
FROM IODP_DC_DEV.SILVER.DC_WIDE
GROUP BY dt
ORDER BY dt DESC;
-- dt < today - 7d 理论上应该都 finalized；如果新数据有 preview，正常
```

---

## 10. Debug Recipes（出问题时按这个查）

| 症状                                              | 第一步查                                    | 第二步                                                                |
| ------------------------------------------------- | ------------------------------------------- | --------------------------------------------------------------------- |
| Snowflake 行数 < Athena Silver 行数                | §7.3 `COPY_HISTORY`                         | §7.4 `SYSTEM$PIPE_STATUS`；必要时 `ALTER PIPE ... REFRESH`            |
| Snowflake `paid_share` ≠ 重算值                    | §2.1 行级 delta                             | §6.4 与 bronze.share_pct 比；超过 2e-4 才看 ETL 代码                  |
| Silver 等式不成立 (total ≠ featured + organic)     | §5.2                                        | §6.2 行级对账定位是 bronze 数据问题还是 silver pivot 问题             |
| Bronze 某 group 不到 4 个 channel                   | §4.3                                        | 看 dropzone 同 prefix 文件 → DLQ 是否吃掉了某些 channel               |
| Gold 数与 Silver 对不上                            | §8.4 `is_stale`                             | `ALTER DYNAMIC TABLE ... REFRESH` 并重跑 §8.1                          |
| `downloads_total = 0` 但 share 不是 NULL           | §2.3 `zero_total_rows` vs `paid_share_null` | Spark `cast("decimal")` 分母 0 → NULL；如果非 NULL 说明上游异常       |
| Bronze share_pct 4 行加起来 ≠ 1                    | §4.4                                        | Decimal(6,4) 截断 ≤ 1e-3 正常；> 1e-3 说明上游 share_pct 算错         |

---

## 附录 A — 一键 Smoke Test (Snowflake)

```sql
-- 把 :target_dt 换成要校验的日期
SET target_dt = '2026-05-13';

WITH violations AS (
  SELECT 'eq_total_split1'    AS check_name, COUNT(*) AS n FROM IODP_DC_DEV.SILVER.DC_WIDE
    WHERE dt = $target_dt AND downloads_total <> downloads_featured + downloads_organic
  UNION ALL SELECT 'eq_total_split2', COUNT(*) FROM IODP_DC_DEV.SILVER.DC_WIDE
    WHERE dt = $target_dt AND downloads_total <>
          downloads_paid_featured + downloads_paid_organic
        + downloads_unpaid_featured + downloads_unpaid_organic
  UNION ALL SELECT 'eq_featured', COUNT(*) FROM IODP_DC_DEV.SILVER.DC_WIDE
    WHERE dt = $target_dt AND downloads_featured <>
          downloads_paid_featured + downloads_unpaid_featured
  UNION ALL SELECT 'eq_organic', COUNT(*) FROM IODP_DC_DEV.SILVER.DC_WIDE
    WHERE dt = $target_dt AND downloads_organic <>
          downloads_paid_organic + downloads_unpaid_organic
  UNION ALL SELECT 'paid_share_oob', COUNT(*) FROM IODP_DC_DEV.SILVER.DC_WIDE
    WHERE dt = $target_dt AND (paid_share < 0 OR paid_share > 1)
  UNION ALL SELECT 'featured_share_oob', COUNT(*) FROM IODP_DC_DEV.SILVER.DC_WIDE
    WHERE dt = $target_dt AND (featured_share < 0 OR featured_share > 1)
  UNION ALL SELECT 'duplicate_pk', COUNT(*) FROM (
    SELECT dt, product_id, app_store, country, device, COUNT(*) AS c
    FROM IODP_DC_DEV.SILVER.DC_WIDE WHERE dt = $target_dt
    GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1)
  UNION ALL SELECT 'null_product', COUNT(*) FROM IODP_DC_DEV.SILVER.DC_WIDE
    WHERE dt = $target_dt AND product_id IS NULL
)
SELECT * FROM violations ORDER BY n DESC, check_name;
-- 全部 n = 0 即通过
```

## 附录 B — 一键 Smoke Test (Athena, Bronze→Silver)

```sql
WITH bronze_recalc AS (
  SELECT
    dt, product_id, app_store, country, device,
    SUM(downloads) AS r_total,
    SUM(CASE WHEN channel IN ('paid_featured','unpaid_featured') THEN downloads ELSE 0 END) AS r_featured,
    SUM(CASE WHEN channel = 'paid_featured'   THEN downloads ELSE 0 END) AS r_pf,
    SUM(CASE WHEN channel = 'paid_organic'    THEN downloads ELSE 0 END) AS r_po,
    SUM(CASE WHEN channel = 'unpaid_featured' THEN downloads ELSE 0 END) AS r_uf,
    SUM(CASE WHEN channel = 'unpaid_organic'  THEN downloads ELSE 0 END) AS r_uo
  FROM iodp_dc_bronze_dev.dc_narrow
  WHERE dt = '2026-05-13'
  GROUP BY 1, 2, 3, 4, 5
),
mismatch AS (
  SELECT COUNT(*) AS pivot_mismatch
  FROM iodp_dc_silver_dev.dc_wide s
  JOIN bronze_recalc b USING (dt, product_id, app_store, country, device)
  WHERE s.dt = '2026-05-13'
    AND (s.downloads_total          <> b.r_total
      OR s.downloads_featured       <> b.r_featured
      OR s.downloads_paid_featured  <> b.r_pf
      OR s.downloads_paid_organic   <> b.r_po
      OR s.downloads_unpaid_featured<> b.r_uf
      OR s.downloads_unpaid_organic <> b.r_uo)
)
SELECT * FROM mismatch;
-- pivot_mismatch = 0 即通过
```
