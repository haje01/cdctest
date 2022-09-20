variable region {
  default = "ap-northeast-2"
}
variable "name" {}
variable "ubuntu_ami" {}
variable "ksqldb_instance_type" {}
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

variable "kafka_s3_sink" {}
variable "timezone" {}
variable "tags" {
    type = map(string)
}
