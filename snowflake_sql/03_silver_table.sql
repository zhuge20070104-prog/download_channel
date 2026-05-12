-- snowflake_sql/03_silver_table.sql
-- Silver unified wide table

USE DATABASE IODP_DC_${ENV};
USE SCHEMA SILVER;

CREATE TABLE IF NOT EXISTS DC_WIDE (
  dt                        DATE          NOT NULL,
  product_id                NUMBER(38,0)  NOT NULL,
  app_store                 VARCHAR(16)   NOT NULL,
  country                   CHAR(2)       NOT NULL,
  device                    VARCHAR(32)   NOT NULL,
  downloads_total           NUMBER(38,0)  NOT NULL,
  downloads_featured        NUMBER(38,0)  NOT NULL,
  downloads_organic         NUMBER(38,0)  NOT NULL,
  downloads_paid_featured   NUMBER(38,0),
  downloads_paid_organic    NUMBER(38,0),
  downloads_unpaid_featured NUMBER(38,0),
  downloads_unpaid_organic  NUMBER(38,0),
  paid_share                NUMBER(6,4),
  featured_share            NUMBER(6,4),
  is_estimate_final         BOOLEAN,
  ingest_ts                 TIMESTAMP_NTZ NOT NULL,
  _loaded_at                TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (dt)
COMMENT = 'Download Channel unified wide table - loaded by Snowpipe';

-- Grants
GRANT INSERT ON TABLE SILVER.DC_WIDE TO ROLE IODP_DC_LOAD_${ENV};
GRANT SELECT ON TABLE SILVER.DC_WIDE TO ROLE IODP_DC_TRANSFORM_${ENV};
GRANT SELECT ON TABLE SILVER.DC_WIDE TO ROLE IODP_DC_READER_${ENV};
