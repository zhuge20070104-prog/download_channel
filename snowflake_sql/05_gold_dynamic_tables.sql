-- snowflake_sql/05_gold_dynamic_tables.sql
-- Gold layer Dynamic Tables for BI aggregations

USE DATABASE IODP_DC_${ENV};
USE SCHEMA GOLD;

-- ════════════════════════════════════════════════════════════════
--  1. Daily by App — app-level daily aggregation
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE DC_DAILY_BY_APP
  TARGET_LAG = '30 minutes'
  WAREHOUSE  = COMPUTE_WH_DC_${ENV}
  COMMENT    = 'Daily downloads aggregated by app and store'
AS
SELECT
  dt,
  product_id,
  app_store,
  SUM(downloads_total)           AS downloads_total,
  SUM(downloads_featured)        AS downloads_featured,
  SUM(downloads_organic)         AS downloads_organic,
  AVG(paid_share)                AS avg_paid_share,
  AVG(featured_share)            AS avg_featured_share,
  COUNT(*)                       AS row_count
FROM IODP_DC_${ENV}.SILVER.DC_WIDE
GROUP BY dt, product_id, app_store;

-- ════════════════════════════════════════════════════════════════
--  2. Daily by Country — country-level daily aggregation
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE DC_DAILY_BY_COUNTRY
  TARGET_LAG = '30 minutes'
  WAREHOUSE  = COMPUTE_WH_DC_${ENV}
  COMMENT    = 'Daily downloads aggregated by country and store'
AS
SELECT
  dt,
  country,
  app_store,
  SUM(downloads_total)           AS downloads_total,
  SUM(downloads_featured)        AS downloads_featured,
  SUM(downloads_organic)         AS downloads_organic,
  COUNT(*)                       AS row_count
FROM IODP_DC_${ENV}.SILVER.DC_WIDE
GROUP BY dt, country, app_store;

-- ════════════════════════════════════════════════════════════════
--  3. Paid vs Organic Trend — trailing 30 days
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE DYNAMIC TABLE DC_PAID_VS_ORGANIC_TREND
  TARGET_LAG = '1 hour'
  WAREHOUSE  = COMPUTE_WH_DC_${ENV}
  COMMENT    = 'Paid vs organic downloads trend - trailing 30 days'
AS
SELECT
  dt,
  app_store,
  SUM(downloads_total)              AS downloads_total,
  SUM(downloads_featured)           AS downloads_featured,
  SUM(downloads_organic)            AS downloads_organic,
  SUM(downloads_paid_featured)      AS downloads_paid_featured,
  SUM(downloads_paid_organic)       AS downloads_paid_organic,
  SUM(downloads_unpaid_featured)    AS downloads_unpaid_featured,
  SUM(downloads_unpaid_organic)     AS downloads_unpaid_organic,
  DIV0(SUM(downloads_paid_featured) + SUM(downloads_paid_organic),
       NULLIF(SUM(downloads_total), 0))  AS paid_ratio,
  DIV0(SUM(downloads_featured),
       NULLIF(SUM(downloads_total), 0))  AS featured_ratio
FROM IODP_DC_${ENV}.SILVER.DC_WIDE
WHERE dt >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY dt, app_store;

-- ════════════════════════════════════════════════════════════════
--  Grants
-- ════════════════════════════════════════════════════════════════

GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA GOLD TO ROLE IODP_DC_TRANSFORM_${ENV};
GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA GOLD TO ROLE IODP_DC_READER_${ENV};
