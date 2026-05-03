-- athena_ddl/bronze_dc_narrow.sql
-- Bronze narrow table for Athena ad-hoc queries

CREATE EXTERNAL TABLE IF NOT EXISTS iodp_dc_bronze_${ENVIRONMENT}.dc_narrow (
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
LOCATION 's3://iodp-dc-bronze-${ENVIRONMENT}-${ACCOUNT_ID}/download_channel/narrow/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE iodp_dc_bronze_${ENVIRONMENT}.dc_narrow;
