# terraform/locals.tf
# FinOps mandatory tags — all resources must carry these

locals {
  mandatory_tags = {
    Environment = var.environment
    CostCenter  = var.cost_center
    Project     = "IODP-DownloadChannel"
    ManagedBy   = "Terraform"
    Owner       = var.team_owner
    DataClass   = "internal"
    CreatedDate = formatdate("YYYY-MM-DD", timestamp())
  }

  env_upper = upper(var.environment)
  env_lower = lower(var.environment)
}
