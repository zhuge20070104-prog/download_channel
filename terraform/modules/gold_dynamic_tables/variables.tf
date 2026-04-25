# modules/gold_dynamic_tables/variables.tf

variable "environment" {
  type = string
}

variable "database_name" {
  type = string
}

variable "gold_schema" {
  type = string
}

variable "warehouse_name" {
  type = string
}

variable "transform_role_name" {
  type = string
}

variable "reader_role_name" {
  type = string
}
