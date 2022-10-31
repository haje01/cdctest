variable "name" {}
variable "nodename" {}
variable "ubuntu_ami" {}
variable "instance_type" {
  default = "t3.medium"
}
variable "private_key" {}
variable "work_cidr" {}
variable "key_pair_name" {}
variable "confluent_url" {}
variable "filebeat_url" {}

variable "tags" {
    type = map(string)
}
