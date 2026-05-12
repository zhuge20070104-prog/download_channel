# terraform/variables.tf

variable "aws_region" {
  description = "AWS region for all resources. No default — every env must set this in tfvars to avoid silently picking us-east-1."
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID (used in S3 bucket naming)"
  type        = string
}

variable "snowflake_pipe_sqs_arn" {
  description = <<-EOT
    Snowflake-managed SQS ARN for Snowpipe AUTO_INGEST (PIPE_DC_WIDE).
    Snowflake auto-allocates this on PIPE creation, in *Snowflake's* AWS
    account (e.g. arn:aws:sqs:ap-southeast-1:782091841703:sf-snowpipe-...).
    Auto-populated by Makefile's deploy-infra-phase2 via
    scripts/get_pipe_sqs_arn.sh which queries SYSTEM$PIPE_STATUS at deploy
    time. Default empty handles first-deploy bootstrap (before 04_pipe.sql
    has run): bucket notification queue block is skipped, terraform
    proceeds, next deploy after pipe creation picks up the ARN.
  EOT
  type        = string
  default     = ""
}


variable "environment" {
  description = "Deployment environment"
  type        = string
  validation {
    condition     = contains(["prod", "staging", "dev"], var.environment)
    error_message = "environment must be prod, staging, or dev."
  }
}

variable "cost_center" {
  description = "FinOps cost center tag"
  type        = string
  default     = "engineering-data-platform"
}

variable "team_owner" {
  description = "Team owner email for tagging"
  type        = string
  default     = "data-engineering@company.com"
}

# ─── Networking ───

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.2.0.0/16"
}

variable "availability_zones" {
  description = "AZs for subnet deployment. No default — must match aws_region; every env must set this in tfvars."
  type        = list(string)
}

# ─── External ───

variable "dropzone_bucket_name" {
  description = "Upstream dropzone S3 bucket name (read-only)"
  type        = string
}

# ─── Observability ───

variable "alarm_email" {
  description = "Email address for CloudWatch alarm notifications"
  type        = string
}

# ─── Deployment orchestration ───

variable "triggers_enabled" {
  description = "Whether scheduled triggers (EventBridge, Lambda cron) are active. Set false during Phase 1 deploy."
  type        = bool
  default     = true
}

# ─── Snowflake ───

variable "snowflake_organization_name" {
  description = "Snowflake organization name (left half of <ORG>-<ACCOUNT>, e.g. QNPCBZM)"
  type        = string
}

variable "snowflake_account_name" {
  description = "Snowflake account name (right half of <ORG>-<ACCOUNT>, e.g. GL59064)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake user for Terraform provisioning"
  type        = string
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role for provisioning"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_warehouse" {
  description = "Snowflake admin warehouse for provisioning"
  type        = string
  default     = "COMPUTE_WH"
}

# ─── Glue ───

variable "glue_dpu_standard" {
  description = "Number of DPUs for standard Glue jobs"
  type        = number
  default     = 10
}

variable "glue_timeout_minutes" {
  description = "Glue job timeout in minutes"
  type        = number
  default     = 120
}
