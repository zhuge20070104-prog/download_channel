# terraform/environments/backend-dev.hcl
# Per-env backend values for `terraform init -backend-config=...`.
# Used together with backend.tf (partial config).
#
# Note: the state bucket itself can live in any region (independent of the
# resources you're deploying). Pick whatever region your state bucket is in.

bucket         = "iodp-terraform-state-dev"
region         = "ap-southeast-1"
dynamodb_table = "iodp-terraform-locks-dev"