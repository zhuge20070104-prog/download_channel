-- athena_ddl/silver_dc_wide.sql
-- Silver unified wide table for Athena ad-hoc queries

CREATE EXTERNAL TABLE IF NOT EXISTS iodp_dc_silver_${ENVIRONMENT}.dc_wide (
    product_id                BIGINT,
    app_store                 STRING,
    country                   STRING,
    device                    STRING,
    downloads_total           BIGINT,
    downloads_featured        BIGINT,
    downloads_organic         BIGINT,
    downloads_paid_featured   BIGINT,
    downloads_paid_organic    BIGINT,
    downloads_unpaid_featured BIGINT,
    downloads_unpaid_organic  BIGINT,
    paid_share                DECIMAL(6,4),
    featured_share            DECIMAL(6,4),
    is_estimate_final         BOOLEAN,
    ingest_ts                 TIMESTAMP
)
PARTITIONED BY (dt STRING, store STRING)
STORED AS PARQUET
LOCATION 's3://iodp-dc-silver-${ENVIRONMENT}-${ACCOUNT_ID}/download_channel/'
TBLPROPERTIES ('parquet.compression'='SNAPPY');

MSCK REPAIR TABLE iodp_dc_silver_${ENVIRONMENT}.dc_wide;
