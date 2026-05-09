# terraform/backend.tf
# S3 remote state + DynamoDB lock.
#
# Backend blocks do NOT support variable interpolation (Terraform limitation).
# To avoid hardcoding region/bucket here, we use *partial configuration*:
# only the static fields stay in this file, and per-env values are passed
# at `terraform init` via -backend-config files.
#
# Usage:
#   terraform init -reconfigure -backend-config=environments/backend-dev.hcl
#   terraform init -reconfigure -backend-config=environments/backend-prod.hcl
#
# See terraform/environments/backend-{dev,prod}.hcl for the per-env values.

terraform {
  backend "s3" {
    key     = "download-channel/terraform.tfstate"
    encrypt = true
  }
}
