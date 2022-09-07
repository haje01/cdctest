variable region {
  default = "ap-northeast-2"
}
variable "name" {}
variable "ubuntu_ami" {}
variable "mysql_instance_type" {
  default = "m5.large"
}
variable "insel_instance_type" {
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
variable "mysql_dbzm_connector" {}
variable "mssql_dbzm_connector" {}
variable "timezone" {}
variable "tags" {
    type = map(string)
}
