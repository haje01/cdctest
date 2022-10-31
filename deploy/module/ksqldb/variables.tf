variable "name" {}
variable "nodename" {}
variable "ubuntu_ami" {}
variable "instance_type" {
  default = "t3.medium"
}
variable "private_key" {}
variable "work_cidr" {}
variable "confluent_url" {}
variable "key_pair_name" {}
variable "kafka_private_ip" {}
variable "timezone" {}

variable "tags" {
    type = map(string)
}
