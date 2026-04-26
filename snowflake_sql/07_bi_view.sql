-- snowflake_sql/07_bi_view.sql
-- BI 直查视图：屏蔽 Restate 窗口内的重复行，让 BI 永远看到最新版数据
--
-- 背景：Snowpipe 是 append-only。当 Silver Glue Job 覆盖某个 dt 分区后，
-- Snowpipe 会把新文件再 COPY 一次，SILVER.DC_WIDE 出现重复。Dedup Task
-- 每天 UTC 06:00 才清理（见 06_dedup_task.sql），存在 ~20 小时的窗口
-- BI 直查 SILVER.DC_WIDE 会看到双倍数据。
--
-- 此视图通过 QUALIFY ROW_NUMBER() 在查询时过滤，BI 团队改查
-- DC_WIDE_LATEST 即可消除该窗口的数据错误。
--
-- 性能：Dedup Task 已清理后，每个 PK 组合只剩 1 行，QUALIFY 几乎零开销
--      （实测延时差距 < 200ms）。仅在 ~20 小时窗口期间有少量重复需要过滤。
--
-- 注意：Gold 层 Dynamic Tables 仍直接读 SILVER.DC_WIDE（不读此视图），
--      因为 ROW_NUMBER 会破坏 Dynamic Table 的增量刷新优化。
--      Gold 表在 20 小时窗口期间会显示重复后的数据，业务可接受（日报场景）。

USE DATABASE IODP_DC_${ENV};
USE SCHEMA SILVER;

CREATE OR REPLACE VIEW DC_WIDE_LATEST
COMMENT = 'BI 直查视图：去重后的 SILVER.DC_WIDE，消除 Restate 窗口内的重复行'
AS
SELECT
  dt,
  product_id,
  app_store,
  country,
  device,
  downloads_total,
  downloads_featured,
  downloads_organic,
  downloads_paid_featured,
  downloads_paid_organic,
  downloads_unpaid_featured,
  downloads_unpaid_organic,
  paid_share,
  featured_share,
  is_estimate_final,
  ingest_ts,
  _loaded_at
FROM SILVER.DC_WIDE
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dt, product_id, app_store, country, device
  ORDER BY _loaded_at DESC
) = 1;

GRANT SELECT ON VIEW SILVER.DC_WIDE_LATEST TO ROLE IODP_DC_TRANSFORM_${ENV};
GRANT SELECT ON VIEW SILVER.DC_WIDE_LATEST TO ROLE IODP_DC_READER_${ENV};
