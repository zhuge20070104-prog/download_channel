# modules/snowflake/main.tf

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
}

locals {
  env_upper = upper(var.environment)
}

# ════════════════════════════════════════════════════════════════
#  Database + Schemas
# ════════════════════════════════════════════════════════════════

resource "snowflake_database" "dc" {
  name    = "IODP_DC_${local.env_upper}"
  comment = "Download Channel data — ${var.environment}"
}

resource "snowflake_schema" "raw_stage" {
  database = snowflake_database.dc.name
  name     = "RAW_STAGE"
  comment  = "External stage, file format, and pipe objects"
}

resource "snowflake_schema" "silver" {
  database = snowflake_database.dc.name
  name     = "SILVER"
  comment  = "Unified wide table loaded by Snowpipe"
}

resource "snowflake_schema" "gold" {
  database = snowflake_database.dc.name
  name     = "GOLD"
  comment  = "Aggregated Dynamic Tables for BI"
}

# ════════════════════════════════════════════════════════════════
#  Warehouse
# ════════════════════════════════════════════════════════════════

resource "snowflake_warehouse" "dc" {
  name                = "COMPUTE_WH_DC_${local.env_upper}"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  max_cluster_count   = 1
  min_cluster_count   = 1
  comment             = "Download Channel compute warehouse — ${var.environment}"
}

# ════════════════════════════════════════════════════════════════
#  Roles
# ════════════════════════════════════════════════════════════════

resource "snowflake_account_role" "load" {
  name    = "IODP_DC_LOAD_${local.env_upper}"
  comment = "Snowpipe load role — INSERT only"
}

resource "snowflake_account_role" "transform" {
  name    = "IODP_DC_TRANSFORM_${local.env_upper}"
  comment = "Dynamic Table refresh role"
}

resource "snowflake_account_role" "reader" {
  name    = "IODP_DC_READER_${local.env_upper}"
  comment = "BI / downstream read-only role"
}

# Grant roles to SYSADMIN
resource "snowflake_grant_account_role" "load_to_sysadmin" {
  role_name        = snowflake_account_role.load.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "transform_to_sysadmin" {
  role_name        = snowflake_account_role.transform.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "reader_to_sysadmin" {
  role_name        = snowflake_account_role.reader.name
  parent_role_name = "SYSADMIN"
}

# ════════════════════════════════════════════════════════════════
#  Grants — Warehouse
# ════════════════════════════════════════════════════════════════

resource "snowflake_grant_privileges_to_account_role" "wh_load" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.load.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.dc.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "wh_transform" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.transform.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.dc.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "wh_reader" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.reader.name
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.dc.name
  }
}

# ════════════════════════════════════════════════════════════════
#  Grants — Database
# ════════════════════════════════════════════════════════════════

resource "snowflake_grant_privileges_to_account_role" "db_load" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.load.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.dc.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "db_transform" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.transform.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.dc.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "db_reader" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.reader.name
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.dc.name
  }
}

# ════════════════════════════════════════════════════════════════
#  Grants — Schemas
# ════════════════════════════════════════════════════════════════

resource "snowflake_grant_privileges_to_account_role" "schema_raw_stage_load" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.load.name
  on_schema {
    schema_name = "\"${snowflake_database.dc.name}\".\"${snowflake_schema.raw_stage.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_silver_load" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.load.name
  on_schema {
    schema_name = "\"${snowflake_database.dc.name}\".\"${snowflake_schema.silver.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_silver_transform" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.transform.name
  on_schema {
    schema_name = "\"${snowflake_database.dc.name}\".\"${snowflake_schema.silver.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_gold_transform" {
  privileges        = ["USAGE", "CREATE DYNAMIC TABLE"]
  account_role_name = snowflake_account_role.transform.name
  on_schema {
    schema_name = "\"${snowflake_database.dc.name}\".\"${snowflake_schema.gold.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_gold_reader" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.reader.name
  on_schema {
    schema_name = "\"${snowflake_database.dc.name}\".\"${snowflake_schema.gold.name}\""
  }
}

resource "snowflake_grant_privileges_to_account_role" "schema_silver_reader" {
  privileges        = ["USAGE"]
  account_role_name = snowflake_account_role.reader.name
  on_schema {
    schema_name = "\"${snowflake_database.dc.name}\".\"${snowflake_schema.silver.name}\""
  }
}

# ════════════════════════════════════════════════════════════════
#  Storage Integration
# ════════════════════════════════════════════════════════════════

resource "snowflake_storage_integration" "s3_int" {
  name    = "IODP_DC_S3_INT_${local.env_upper}"
  enabled = true

  storage_provider          = "S3"
  storage_allowed_locations = ["s3://${var.silver_bucket_name}/"]
  storage_aws_role_arn      = var.snowpipe_iam_role_arn

  comment = "S3 integration for Silver bucket — ${var.environment}"
}

