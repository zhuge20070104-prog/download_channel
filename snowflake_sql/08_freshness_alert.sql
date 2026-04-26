-- snowflake_sql/08_freshness_alert.sql
-- Snowpipe 延迟告警（PLAN.md §14 告警 #4）
--
-- 检测原理:
--   Snowpipe 把 Silver S3 的 Parquet COPY 进 SILVER.DC_WIDE。
--   ACCOUNT_USAGE.COPY_HISTORY 记录每次 COPY 的时间。
--   如果最近 N 小时一次都没 COPY 过 → Pipe 卡了/没收到 SNS 事件/IAM 失效，必须告警。
--
-- 注意:
--   ACCOUNT_USAGE.COPY_HISTORY 有 ~45 分钟延迟，故检测窗口必须 >= 2h 才有意义。
--   工作时间窗口（PT 凌晨上游交付 + UTC 10:00 ETL）→ UTC 11:00 后必有数据，故定时 hourly。

USE DATABASE IODP_DC_${ENV};

-- ════════════════════════════════════════════════════════════════
--  Email Notification Integration (account-level, idempotent)
--
--  ALERT_EMAIL 在 apply_snowflake_sql.sh 阶段由 ${ALERT_EMAIL} 占位符注入。
--  邮箱地址必须先在 Snowflake 用户档案里 verify 过，否则 SYSTEM$SEND_EMAIL 会失败。
-- ════════════════════════════════════════════════════════════════

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS IODP_DC_EMAIL_NOTIF_${ENV}
  TYPE               = EMAIL
  ENABLED            = TRUE
  ALLOWED_RECIPIENTS = ('${ALERT_EMAIL}')
  COMMENT            = 'Email integration for Download Channel ETL alerts';

-- ════════════════════════════════════════════════════════════════
--  Alert: Snowpipe 静默 (无新 COPY 超过 2 小时)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT IODP_DC_SNOWPIPE_FRESHNESS_${ENV}
  WAREHOUSE = COMPUTE_WH_DC_${ENV}
  SCHEDULE  = '60 MINUTE'
  COMMENT   = 'Alerts when Snowpipe has not loaded any Silver file in the last 2h'
IF (EXISTS (
  SELECT 1
  FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
  WHERE PIPE_NAME = 'IODP_DC_${ENV}.RAW_STAGE.PIPE_DC_WIDE'
    AND LAST_LOAD_TIME >= DATEADD('hour', -2, CURRENT_TIMESTAMP())
  HAVING COUNT(*) = 0
))
THEN
  CALL SYSTEM$SEND_EMAIL(
    'IODP_DC_EMAIL_NOTIF_${ENV}',
    '${ALERT_EMAIL}',
    '[DC-ETL] Snowpipe freshness alert (${ENV})',
    'Snowpipe IODP_DC_${ENV}.RAW_STAGE.PIPE_DC_WIDE has not loaded any file in the past 2 hours. ' ||
    'Investigate: (1) S3 SNS notification on silver bucket; (2) Storage Integration trust policy; ' ||
    '(3) Pipe status via SYSTEM$PIPE_STATUS(''IODP_DC_${ENV}.RAW_STAGE.PIPE_DC_WIDE'').'
  );

ALTER ALERT IODP_DC_SNOWPIPE_FRESHNESS_${ENV} RESUME;

-- ════════════════════════════════════════════════════════════════
--  Alert: Dynamic Table 卡住 (Gold 层超过 1h 没刷新)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT IODP_DC_DYNAMIC_TABLE_LAG_${ENV}
  WAREHOUSE = COMPUTE_WH_DC_${ENV}
  SCHEDULE  = '60 MINUTE'
  COMMENT   = 'Alerts when any GOLD Dynamic Table refresh lag exceeds its target (×3 buffer)'
IF (EXISTS (
  SELECT 1
  FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY
  WHERE QUALIFIED_NAME LIKE 'IODP_DC_${ENV}.GOLD.%'
    AND STATE = 'FAILED'
    AND DATA_TIMESTAMP >= DATEADD('hour', -1, CURRENT_TIMESTAMP())
))
THEN
  CALL SYSTEM$SEND_EMAIL(
    'IODP_DC_EMAIL_NOTIF_${ENV}',
    '${ALERT_EMAIL}',
    '[DC-ETL] Dynamic Table refresh failed (${ENV})',
    'One or more Dynamic Tables in IODP_DC_${ENV}.GOLD failed to refresh in the past hour. ' ||
    'Check: SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.DYNAMIC_TABLE_REFRESH_HISTORY ' ||
    'WHERE QUALIFIED_NAME LIKE ''IODP_DC_${ENV}.GOLD.%'' ORDER BY DATA_TIMESTAMP DESC;'
  );

ALTER ALERT IODP_DC_DYNAMIC_TABLE_LAG_${ENV} RESUME;

-- ════════════════════════════════════════════════════════════════
--  Verification
-- ════════════════════════════════════════════════════════════════
-- SHOW ALERTS LIKE 'IODP_DC_%' IN SCHEMA PUBLIC;
-- SELECT * FROM TABLE(INFORMATION_SCHEMA.ALERT_HISTORY()) ORDER BY SCHEDULED_TIME DESC LIMIT 10;
