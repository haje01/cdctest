variable "name" {}
variable "ubuntu_ami" {}
variable "kafka_instance_type" {
  default = "t3.medium"
}
variable "work_cidr" {}
variable "key_pair_name" {}
variable "private_key" {}
variable "producer_sg_id" {}
variable "consumer_sg_id" {}
variable "ksqldb_sg_id" {}

variable "confluent_url" {}
variable "kafka_s3_sink" {}
variable "timezone" {}

variable "tags" {
    type = map(string)
}
