variable "name" {}
variable "ubuntu_ami" {}
variable "kafka_instance_type" {
  default = "t3.medium"
}
variable "work_cidr" {}
variable "key_pair_name" {}
variable "private_key" {}
variable "consumer_sg_id" {}

variable "kafka_jdbc_connector" {}
variable "kafka_s3_sink" {}
variable "mysql_jdbc_driver" {}
variable "mysql_dbzm_connector" {}
variable "mssql_dbzm_connector" {}
variable "timezone" {}

variable "tags" {
    type = map(string)
}
