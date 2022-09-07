variable region {
  default = "ap-northeast-2"
}
variable "name" {}
variable "ubuntu_ami" {}
variable "procon_instance_type" {
  default = "t3.medium"
}
variable "private_key" {}
variable "kafka_instance_type" {
  default = "t3.medium"
}
variable "work_cidr" {}
variable "key_pair_name" {}
variable "kafka_url" {}
variable "timezone" {}
variable "tags" {
    type = map(string)
}
