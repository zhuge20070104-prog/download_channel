# modules/snowpipe/variables.tf

variable "environment" {
  type = string
}

variable "silver_bucket_name" {
  type = string
}

variable "silver_bucket_arn" {
  type = string
}

variable "silver_sns_topic_arn" {
  type = string
}

variable "snowflake_iam_user_arn" {
  description = "Snowflake Storage Integration IAM user ARN for trust policy"
  type        = string
}

variable "snowflake_external_id" {
  description = "Snowflake Storage Integration external ID for trust policy"
  type        = string
}

variable "sns_alert_topic_arn" {
  description = "SNS topic ARN to receive CloudWatch alarms for DLQ / queue-stale events"
  type        = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
