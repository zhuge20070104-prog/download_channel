# modules/glue_etl/variables.tf

variable "environment" {
  type = string
}

variable "aws_region" {
  type        = string
  description = "AWS region — passed to Glue jobs as --AWS_REGION so CheckpointManager hits the right DynamoDB endpoint instead of the hardcoded us-east-1 fallback."
}

variable "dropzone_bucket_name" {
  type = string
}

variable "bronze_bucket_name" {
  type = string
}

variable "bronze_bucket_arn" {
  type = string
}

variable "silver_bucket_name" {
  type = string
}

variable "silver_bucket_arn" {
  type = string
}

variable "scripts_bucket_name" {
  type = string
}

variable "scripts_bucket_arn" {
  type = string
}

variable "checkpoint_table_name" {
  type = string
}

variable "checkpoint_table_arn" {
  type = string
}

variable "sns_alert_topic_arn" {
  type = string
}

variable "triggers_enabled" {
  type    = bool
  default = true
}

variable "glue_dpu" {
  type    = number
  default = 10
}

variable "glue_timeout_minutes" {
  type    = number
  default = 120
}

variable "tags" {
  type    = map(string)
  default = {}
}
