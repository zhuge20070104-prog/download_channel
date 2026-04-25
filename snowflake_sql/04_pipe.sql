-- snowflake_sql/04_pipe.sql
-- File Format, External Stage, and Snowpipe

USE DATABASE IODP_DC_${ENV};
USE SCHEMA RAW_STAGE;

-- 1. File Format
CREATE OR REPLACE FILE FORMAT PARQUET_FF
  TYPE = PARQUET
  COMPRESSION = SNAPPY;

-- 2. External Stage
CREATE OR REPLACE STAGE SILVER_S3_STAGE
  STORAGE_INTEGRATION = IODP_DC_S3_INT_${ENV}
  URL = 's3://iodp-dc-silver-${ENV_LOWER}-${AWS_ACCOUNT_ID}/download_channel/'
  FILE_FORMAT = PARQUET_FF;

-- 3. Pipe (AUTO_INGEST)
CREATE OR REPLACE PIPE PIPE_DC_WIDE
  AUTO_INGEST = TRUE
  COMMENT = 'Auto-ingest Silver S3 Parquet into SILVER.DC_WIDE'
AS
COPY INTO IODP_DC_${ENV}.SILVER.DC_WIDE (
  dt, product_id, app_store, country, device,
  downloads_total, downloads_featured, downloads_organic,
  downloads_paid_featured, downloads_paid_organic,
  downloads_unpaid_featured, downloads_unpaid_organic,
  paid_share, featured_share, is_estimate_final, ingest_ts
)
FROM (
  SELECT
    $1:dt::DATE                        AS dt,
    $1:product_id::NUMBER(38,0)        AS product_id,
    $1:app_store::VARCHAR(16)          AS app_store,
    $1:country::CHAR(2)                AS country,
    $1:device::VARCHAR(32)             AS device,
    $1:downloads_total::NUMBER(38,0)   AS downloads_total,
    $1:downloads_featured::NUMBER(38,0) AS downloads_featured,
    $1:downloads_organic::NUMBER(38,0)  AS downloads_organic,
    $1:downloads_paid_featured::NUMBER(38,0)   AS downloads_paid_featured,
    $1:downloads_paid_organic::NUMBER(38,0)    AS downloads_paid_organic,
    $1:downloads_unpaid_featured::NUMBER(38,0) AS downloads_unpaid_featured,
    $1:downloads_unpaid_organic::NUMBER(38,0)  AS downloads_unpaid_organic,
    $1:paid_share::NUMBER(6,4)         AS paid_share,
    $1:featured_share::NUMBER(6,4)     AS featured_share,
    $1:is_estimate_final::BOOLEAN      AS is_estimate_final,
    $1:ingest_ts::TIMESTAMP_NTZ        AS ingest_ts
  FROM @SILVER_S3_STAGE
);

-- 4. Grants
GRANT OPERATE ON PIPE PIPE_DC_WIDE TO ROLE IODP_DC_LOAD_${ENV};
GRANT MONITOR ON PIPE PIPE_DC_WIDE TO ROLE IODP_DC_LOAD_${ENV};

-- 5. Verify
-- SHOW PIPES;
-- SELECT SYSTEM$PIPE_STATUS('PIPE_DC_WIDE');
