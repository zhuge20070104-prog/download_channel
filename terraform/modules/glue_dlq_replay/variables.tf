# modules/glue_dlq_replay/variables.tf

variable "environment" {
  type = string
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
