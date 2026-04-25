# modules/observability/variables.tf

variable "environment" {
  type = string
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

variable "tags" {
  type    = map(string)
  default = {}
}
