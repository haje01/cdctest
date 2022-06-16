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
variable "db_user" {}
variable "db_port" {}
variable "kafka_url" {}
variable "kafka_jdbc_connector" {}
variable "mysql_jdbc_driver" {}
variable "tags" {
    type = map(string)
}
