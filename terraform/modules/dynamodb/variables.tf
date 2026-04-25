# modules/dynamodb/variables.tf

variable "environment" {
  type = string
}

variable "tags" {
  type    = map(string)
  default = {}
}
