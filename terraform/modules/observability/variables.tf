# modules/observability/variables.tf

variable "environment" {
  type = string
}

variable "aws_region" {
  description = "AWS region used by the dashboard widgets to query metrics"
  type        = string
}

variable "glue_job_names" {
  description = "List of Glue job names to monitor"
  type        = list(string)
}

variable "bronze_bucket_name" {
  type = string
}

variable "bronze_bucket_arn" {
  type = string
}

variable "alarm_email" {
  type = string
}

variable "triggers_enabled" {
  type    = bool
  default = true
}

variable "checkpoint_table_name" {
  type = string
}

variable "checkpoint_table_arn" {
  type = string
}

variable "checkpoint_status_index_arn" {
  description = "ARN of the sparse status-index GSI (Lambda Query target)"
  type        = string
}

variable "dropzone_bucket_name" {
  type = string
}

variable "dropzone_bucket_arn" {
  type = string
}

variable "stale_lock_check_schedule" {
  description = "EventBridge schedule for stale lock check Lambda (default: every 30 minutes)"
  type        = string
  default     = "rate(30 minutes)"
}

variable "dropzone_freshness_schedule" {
  description = "EventBridge schedule for dropzone freshness Lambda (default: daily UTC 11:00, 1h after ETL)"
  type        = string
  default     = "cron(0 11 * * ? *)"
}

variable "expected_dropzone_stores" {
  description = "App stores expected daily under dropzone"
  type        = list(string)
  default     = ["ios", "google-play"]
}

variable "tags" {
  type    = map(string)
  default = {}
}
