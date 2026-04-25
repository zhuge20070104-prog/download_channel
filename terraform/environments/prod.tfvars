# terraform/environments/prod.tfvars

environment    = "prod"
aws_region     = "us-east-1"
aws_account_id = "987654321098"  # TODO: replace with actual account ID

cost_center = "engineering-data-platform"
team_owner  = "data-engineering@company.com"

# Networking
vpc_cidr           = "10.3.0.0/16"
availability_zones = ["us-east-1a", "us-east-1b", "us-east-1c"]

# External
dropzone_bucket_name = "dataai-dropzone-prod-987654321098"  # TODO: replace

# Observability
alarm_email = "data-engineering-oncall@company.com"  # TODO: replace

# Snowflake
snowflake_account   = "xy12345.us-east-1"  # TODO: replace
snowflake_user      = "TERRAFORM_SVC_PROD"  # TODO: replace
snowflake_role      = "ACCOUNTADMIN"
snowflake_warehouse = "COMPUTE_WH"

# Glue
glue_dpu_standard    = 20
glue_timeout_minutes = 180

# Deploy orchestration
triggers_enabled = true
