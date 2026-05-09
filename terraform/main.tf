# terraform/main.tf
# Root module — wires all sub-modules together
#
# Module dependency graph:
#   networking ─────────────────────────────┐
#   storage ────────────┐                   │
#   dynamodb ───────────┤                   │
#   glue_catalog ───────┤                   │
#                       ├─→ glue_etl ───────┤─→ observability
#                       │                   │
#   snowflake ──────────┼─→ snowpipe        │
#                       │                   │
#                       └─→ glue_dlq_replay │
#   gold_dynamic_tables ←── snowflake outputs

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.mandatory_tags
  }
}

provider "snowflake" {
  account   = var.snowflake_account
  user      = var.snowflake_user
  password  = var.snowflake_password
  role      = var.snowflake_role
  warehouse = var.snowflake_warehouse
}

# ════════════════════════════════════════════════════════════════
#  Core Infrastructure
# ════════════════════════════════════════════════════════════════

module "networking" {
  source = "./modules/networking"

  environment        = var.environment
  vpc_cidr           = var.vpc_cidr
  availability_zones = var.availability_zones
  tags               = local.mandatory_tags
}

module "storage" {
  source = "./modules/storage"

  environment         = var.environment
  aws_account_id      = var.aws_account_id
  dead_letter_prefix  = "dead_letter/"
  ia_transition_days      = 30
  glacier_transition_days = 90
  expiration_days         = 365
  tags                    = local.mandatory_tags
}

module "dynamodb" {
  source = "./modules/dynamodb"

  environment = var.environment
  tags        = local.mandatory_tags
}

module "glue_catalog" {
  source = "./modules/glue_catalog"

  environment = var.environment
  tags        = local.mandatory_tags
}

# ════════════════════════════════════════════════════════════════
#  Observability (creates SNS topic used by glue_etl)
# ════════════════════════════════════════════════════════════════

module "observability" {
  source = "./modules/observability"

  environment = var.environment
  aws_region  = var.aws_region
  # Use predictable job names to break circular dependency with glue_etl
  glue_job_names = [
    "iodp-dc-bronze-etl-${var.environment}",
    "iodp-dc-silver-etl-${var.environment}",
  ]
  bronze_bucket_name          = module.storage.bronze_bucket_name
  bronze_bucket_arn           = module.storage.bronze_bucket_arn
  checkpoint_table_name       = module.dynamodb.checkpoint_table_name
  checkpoint_table_arn        = module.dynamodb.checkpoint_table_arn
  checkpoint_status_index_arn = module.dynamodb.checkpoint_status_index_arn
  dropzone_bucket_name        = var.dropzone_bucket_name
  dropzone_bucket_arn         = "arn:aws:s3:::${var.dropzone_bucket_name}"
  alarm_email                 = var.alarm_email
  triggers_enabled            = var.triggers_enabled
  tags                        = local.mandatory_tags
}

# ════════════════════════════════════════════════════════════════
#  Glue ETL (depends on storage, dynamodb, observability)
# ════════════════════════════════════════════════════════════════

module "glue_etl" {
  source = "./modules/glue_etl"

  environment          = var.environment
  dropzone_bucket_name = var.dropzone_bucket_name
  bronze_bucket_name   = module.storage.bronze_bucket_name
  bronze_bucket_arn    = module.storage.bronze_bucket_arn
  silver_bucket_name   = module.storage.silver_bucket_name
  silver_bucket_arn    = module.storage.silver_bucket_arn
  scripts_bucket_name  = module.storage.scripts_bucket_name
  scripts_bucket_arn   = module.storage.scripts_bucket_arn
  checkpoint_table_name = module.dynamodb.checkpoint_table_name
  checkpoint_table_arn  = module.dynamodb.checkpoint_table_arn
  sns_alert_topic_arn   = module.observability.sns_alert_topic_arn
  triggers_enabled      = var.triggers_enabled
  glue_dpu              = var.glue_dpu_standard
  glue_timeout_minutes  = var.glue_timeout_minutes
  tags                  = local.mandatory_tags
}

module "glue_dlq_replay" {
  source = "./modules/glue_dlq_replay"

  environment              = var.environment
  glue_execution_role_arn  = module.glue_etl.glue_execution_role_arn
  scripts_bucket_name      = module.storage.scripts_bucket_name
  bronze_bucket_name       = module.storage.bronze_bucket_name
  dropzone_bucket_name     = var.dropzone_bucket_name
  tags                     = local.mandatory_tags
}

# ════════════════════════════════════════════════════════════════
#  Dropzone Seeder (demo / test data generator — non-prod use)
# ════════════════════════════════════════════════════════════════

module "dropzone_seeder" {
  source = "./modules/dropzone_seeder"

  environment          = var.environment
  dropzone_bucket_name = var.dropzone_bucket_name
  scripts_bucket_name  = module.storage.scripts_bucket_name
  scripts_bucket_arn   = module.storage.scripts_bucket_arn
  tags                 = local.mandatory_tags
}

# ════════════════════════════════════════════════════════════════
#  Snowflake
# ════════════════════════════════════════════════════════════════

module "snowflake" {
  source = "./modules/snowflake"

  environment        = var.environment
  silver_bucket_name = module.storage.silver_bucket_name
  # Use predictable IAM role ARN to break circular dependency with snowpipe.
  # The snowpipe module creates this role with this exact name.
  snowpipe_iam_role_arn = "arn:aws:iam::${var.aws_account_id}:role/iodp-dc-snowpipe-s3-${var.environment}"
}

module "snowpipe" {
  source = "./modules/snowpipe"

  environment             = var.environment
  silver_bucket_name      = module.storage.silver_bucket_name
  silver_bucket_arn       = module.storage.silver_bucket_arn
  silver_sns_topic_arn    = module.storage.silver_sns_topic_arn
  snowflake_iam_user_arn  = module.snowflake.storage_aws_iam_user_arn
  snowflake_external_id   = module.snowflake.storage_aws_external_id
  tags                    = local.mandatory_tags
}

module "gold_dynamic_tables" {
  source = "./modules/gold_dynamic_tables"

  environment        = var.environment
  database_name      = module.snowflake.database_name
  gold_schema        = module.snowflake.gold_schema
  warehouse_name     = module.snowflake.warehouse_name
  transform_role_name = module.snowflake.transform_role_name
  reader_role_name    = module.snowflake.reader_role_name
}
