-- snowflake_sql/02_storage_integration.sql
-- S3 Storage Integration for Snowpipe
--
-- Note: After creating, run DESC INTEGRATION to get:
--   STORAGE_AWS_IAM_USER_ARN  → update AWS IAM trust policy
--   STORAGE_AWS_EXTERNAL_ID   → update AWS IAM trust condition

USE DATABASE IODP_DC_${ENV};

CREATE OR REPLACE STORAGE INTEGRATION IODP_DC_S3_INT_${ENV}
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::${AWS_ACCOUNT_ID}:role/iodp-dc-snowpipe-s3-${ENV_LOWER}'
  STORAGE_ALLOWED_LOCATIONS = ('s3://iodp-dc-silver-${ENV_LOWER}-${AWS_ACCOUNT_ID}/');

-- Verify: run these after creation to get IAM ARN and External ID
-- DESC INTEGRATION IODP_DC_S3_INT_${ENV};
