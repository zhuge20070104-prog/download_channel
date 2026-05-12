# modules/glue_dlq_replay/variables.tf

variable "environment" {
  type = string
}

variable "aws_region" {
  type        = string
  description = "AWS region — passed to Glue job as --AWS_REGION for boto3 clients."
}

variable "glue_execution_role_arn" {
  type = string
}

variable "scripts_bucket_name" {
  type = string
}

variable "bronze_bucket_name" {
  type = string
}

variable "dropzone_bucket_name" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
