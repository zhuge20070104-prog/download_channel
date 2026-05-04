-- snowflake_sql/08_freshness_alert.sql
-- Snowpipe 延迟告警（PLAN.md §14 告警 #4）
--
-- 检测原理:
--   Snowpipe 把 Silver S3 的 Parquet COPY 进 SILVER.DC_WIDE。
--   ACCOUNT_USAGE.COPY_HISTORY 记录每次 COPY 的时间。
--   每天 UTC 13:00 检查"今天 (UTC) 是否已经 COPY 过"——没有则
--   Pipe 卡了/没收到 SNS 事件/IAM 失效/上游 Glue Workflow 没产出文件，必须告警。
--
-- 注意:
--   ACCOUNT_USAGE.COPY_HISTORY 有 ~45 分钟延迟，所以 alert 必须在每日批次落地后留足 buffer。
--   日批时序: PT 凌晨上游交付 + UTC 10:00 ETL → UTC ~11:00 Snowpipe COPY 完成。
--   语义: 每天 UTC 13:00 跑一次（已留 ~2h buffer 等 ACCOUNT_USAGE 物化），
--         检查"今天 (UTC) 是否已经有过 COPY"，没有则告警。
--   不用 hourly + "最近 2h 没 COPY" 模式，因为日批语境下 2h 窗口外没 COPY 是正常的，
--   会每天误报 ~21 小时。

USE DATABASE IODP_DC_${ENV};

-- ════════════════════════════════════════════════════════════════
--  Email Notification Integration (account-level)
--
--  ALERT_EMAIL 在 apply_snowflake_sql.sh 阶段由 ${ALERT_EMAIL} 占位符注入。
--  邮箱地址必须先在 Snowflake 用户档案里 verify 过，否则 SYSTEM$SEND_EMAIL 会失败。
--  apply_snowflake_sql.sh 的 preflight 会拦下未注册的邮箱（详见 README）。
--
--  使用 CREATE OR REPLACE（而非 IF NOT EXISTS）以便修改 alarm_email 后重新部署
--  时 ALLOWED_RECIPIENTS 真的更新；未变更时配合 --force 才会执行。
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE NOTIFICATION INTEGRATION IODP_DC_EMAIL_NOTIF_${ENV}
  TYPE               = EMAIL
  ENABLED            = TRUE
  ALLOWED_RECIPIENTS = ('${ALERT_EMAIL}')
  COMMENT            = 'Email integration for Download Channel ETL alerts';

-- ════════════════════════════════════════════════════════════════
--  Alert: Snowpipe 静默 (今天 UTC 还没 COPY 过)
-- ════════════════════════════════════════════════════════════════

CREATE OR REPLACE ALERT IODP_DC_SNOWPIPE_FRESHNESS_${ENV}
  WAREHOUSE = COMPUTE_WH_DC_${ENV}
  SCHEDULE  = 'USING CRON 0 13 * * * UTC'
  COMMENT   = 'Daily check at UTC 13:00; alerts if today (UTC) has no Snowpipe COPY into PIPE_DC_WIDE yet'
IF (EXISTS (
  -- LAST_LOAD_TIME 是 TIMESTAMP_LTZ；显式 CONVERT_TIMEZONE 到 UTC 后取 DATE，
  -- 避免依赖 session TIMEZONE 默认值（Snowflake 默认 America/Los_Angeles）。
  -- SYSDATE() 始终返回 UTC NTZ，::DATE 即"今天 UTC 的日期"。
  SELECT 1
  FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
  WHERE PIPE_NAME = 'IODP_DC_${ENV}.RAW_STAGE.PIPE_DC_WIDE'
    AND CONVERT_TIMEZONE('UTC', LAST_LOAD_TIME)::DATE = SYSDATE()::DATE
  HAVING COUNT(*) = 0
))
THEN
  CALL SYSTEM$SEND_EMAIL(
    'IODP_DC_EMAIL_NOTIF_${ENV}',
    '${ALERT_EMAIL}',
    '[DC-ETL] Snowpipe freshness alert (${ENV})',
    'Snowpipe IODP_DC_${ENV}.RAW_STAGE.PIPE_DC_WIDE has not loaded any file today (UTC). ' ||
    'Expected daily load completes by UTC ~11:00; this alert ran at UTC 13:00 and found no COPY events for today. ' ||
    'Investigate: (1) S3 SNS notification on silver bucket; (2) Storage Integration trust policy; ' ||
    '(3) Pipe status via SYSTEM$PIPE_STATUS(''IODP_DC_${ENV}.RAW_STAGE.PIPE_DC_WIDE''); ' ||
    '(4) Glue Bronze/Silver Workflow run history (upstream may have not produced files).'
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
