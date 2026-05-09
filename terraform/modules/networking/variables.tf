# modules/networking/variables.tf

variable "environment" {
  type = string
}

variable "vpc_cidr" {
  type    = string
  default = "10.2.0.0/16"
}

variable "availability_zones" {
  type = list(string)
}

variable "tags" {
  type    = map(string)
  default = {}
}
