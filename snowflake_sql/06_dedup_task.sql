-- snowflake_sql/06_dedup_task.sql
-- Snowflake Task: 每日去重，处理 Snowpipe 追加 + Restate 导致的重复行
--
-- Snowpipe 是 append-only 语义。当 Silver Glue Job 覆盖写某个 dt 分区后，
-- Snowpipe 会把新文件当作新数据再 COPY 一次，导致同一 (dt, product_id, ...) 出现多行。
-- 此 Task 每日凌晨 UTC 06:00 运行，对 restate 窗口内（最近 10 天）的数据做去重，
-- 保留每个主键组合中 _loaded_at 最新的那一行。

USE DATABASE IODP_DC_${ENV};
USE SCHEMA SILVER;

-- 1. 创建去重 Task
-- snowsql splits on `;` by default, so a multi-statement BEGIN..END body
-- gets cut at the first inner `;` and Snowflake sees EOF mid-CREATE TASK.
-- Snowflake allows a single SQL statement as TASK body (no scripting block
-- needed), so we keep the DELETE bare. If this ever grows to >1 statement,
-- wrap the body in `$$ BEGIN ... END $$` dollar-quoting instead of BEGIN/END.
CREATE OR REPLACE TASK IODP_DC_DEDUP_${ENV}
  WAREHOUSE = COMPUTE_WH_DC_${ENV}
  SCHEDULE  = 'USING CRON 0 6 * * * UTC'     -- 每日 UTC 06:00
  COMMENT   = 'Daily dedup for restate window in SILVER.DC_WIDE'
AS
DELETE FROM SILVER.DC_WIDE
WHERE (_loaded_at, dt, product_id, app_store, country, device) IN (
  SELECT _loaded_at, dt, product_id, app_store, country, device
  FROM (
    SELECT
      _loaded_at,
      dt,
      product_id,
      app_store,
      country,
      device,
      ROW_NUMBER() OVER (
        PARTITION BY dt, product_id, app_store, country, device
        ORDER BY _loaded_at DESC
      ) AS rn
    FROM SILVER.DC_WIDE
    WHERE dt >= DATEADD('day', -10, CURRENT_DATE())
  )
  WHERE rn > 1
);

-- 2. 启用 Task
ALTER TASK IODP_DC_DEDUP_${ENV} RESUME;

-- 3. 验证 Task 状态
-- SHOW TASKS LIKE 'IODP_DC_DEDUP_%';
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) WHERE NAME = 'IODP_DC_DEDUP_${ENV}' ORDER BY SCHEDULED_TIME DESC LIMIT 5;
