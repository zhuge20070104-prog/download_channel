# terraform/environments/dev.tfvars

environment    = "dev"
aws_region     = "ap-southeast-1"
aws_account_id = "165518479671"

cost_center = "engineering-data-platform"
team_owner  = "fredric2010@outlook.com"

# Networking
vpc_cidr           = "10.2.0.0/16"
availability_zones = ["ap-southeast-1a", "ap-southeast-1b"]

# External
dropzone_bucket_name = "dataai-dropzone-dev-165518479671"  # TODO: replace

# Observability
alarm_email = "fredric2010@outlook.com"  

# Snowflake
# TODO: 这个account的话，需要到snowflake 网站上去申请。
snowflake_account   = "xy12345.us-east-1"  # TODO: replace
snowflake_user      = "TERRAFORM_SVC_DEV"  # TODO: replace
snowflake_role      = "ACCOUNTADMIN"
snowflake_warehouse = "COMPUTE_WH"

# Glue
glue_dpu_standard    = 10
glue_timeout_minutes = 120

# Deploy orchestration
triggers_enabled = true
