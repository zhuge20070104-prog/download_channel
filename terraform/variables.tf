# terraform/variables.tf

variable "aws_region" {
  description = "AWS region for all resources. No default — every env must set this in tfvars to avoid silently picking us-east-1."
  type        = string
}

variable "aws_account_id" {
  description = "AWS account ID (used in S3 bucket naming)"
  type        = string
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

variable "snowflake_account" {
  description = "Snowflake account identifier (e.g. xy12345.us-east-1)"
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
