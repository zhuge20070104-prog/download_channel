# terraform/environments/dev.tfvars

environment    = "dev"
aws_region     = "us-east-1"
aws_account_id = "123456789012"  # TODO: replace with actual account ID

cost_center = "engineering-data-platform"
team_owner  = "data-engineering@company.com"

# Networking
vpc_cidr           = "10.2.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b"]

# External
dropzone_bucket_name = "dataai-dropzone-dev-123456789012"  # TODO: replace

# Observability
alarm_email = "data-engineering-dev@company.com"  # TODO: replace

# Snowflake
snowflake_account   = "xy12345.us-east-1"  # TODO: replace
snowflake_user      = "TERRAFORM_SVC_DEV"  # TODO: replace
snowflake_role      = "ACCOUNTADMIN"
snowflake_warehouse = "COMPUTE_WH"

# Glue
glue_dpu_standard    = 10
glue_timeout_minutes = 120

# Deploy orchestration
triggers_enabled = true
