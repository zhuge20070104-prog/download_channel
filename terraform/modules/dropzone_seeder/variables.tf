# modules/dropzone_seeder/variables.tf

variable "environment" {
  type = string
}

variable "dropzone_bucket_name" {
  type = string
}

variable "scripts_bucket_name" {
  type = string
}

variable "scripts_bucket_arn" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
