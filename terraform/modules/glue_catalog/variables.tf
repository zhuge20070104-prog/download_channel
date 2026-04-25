# modules/glue_catalog/variables.tf

variable "environment" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
