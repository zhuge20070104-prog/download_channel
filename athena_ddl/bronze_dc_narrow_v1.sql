-- athena_ddl/bronze_dc_narrow_v1.sql
-- Bronze narrow table (v1) for Athena ad-hoc queries

CREATE EXTERNAL TABLE IF NOT EXISTS iodp_dc_bronze_${ENVIRONMENT}.dc_narrow_v1 (
    product_id        BIGINT,
    app_store         STRING,
    country           STRING,
    device            STRING,
    channel           STRING,
    downloads         BIGINT,
    share_pct         DECIMAL(6,4),
    is_estimate_final BOOLEAN,
    ingest_ts         TIMESTAMP
)
PARTITIONED BY (dt STRING, store STRING)
STORED AS PARQUET
LOCATION 's3://iodp-dc-bronze-${ENVIRONMENT}-${ACCOUNT_ID}/download_channel/v1/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE iodp_dc_bronze_${ENVIRONMENT}.dc_narrow_v1;
