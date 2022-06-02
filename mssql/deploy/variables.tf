variable region {
  default = "ap-northeast-2"
}
variable "name" {}
variable "ubuntu_ami" {}
variable "sqlserver_ami" {}
variable "sqlserver_instance_type" {
  default = "m5.xlarge"
}
variable "insel_instance_type" {
  default = "t3.medium"
}
variable "kafka_instance_type" {
  default = "t3.medium"
}
variable "work_cidr" {}
variable "key_pair_name" {}
variable "private_key_path" {}
variable "sqlserver_user" {}
variable "sqlserver_passwd" {}
variable "db_user" {}
variable "db_passwd" {}
variable "db_port" {}
variable "kafka_jdbc_connect" {}
variable "sqlserver_jdbc_driver" {}
variable "tags" {
    type = map(string)
}
