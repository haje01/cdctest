module "kafka" {
  source = "../module/kafka"
  name = var.name
  ubuntu_ami = var.ubuntu_ami
  private_key = var.private_key
  kafka_instance_type = var.kafka_instance_type
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  producer_sg_id = module.producer.sg_id
  consumer_sg_id = module.consumer.sg_id
  ksqldb_sg_id = module.ksqldb.sg_id
  kafka_s3_sink = var.kafka_s3_sink
  timezone = var.timezone
  tags = var.tags
}

module "producer" {
  source = "../module/kfktest"
  name = var.name
  nodename = "producer"
  ubuntu_ami = var.ubuntu_ami
  instance_type = var.procon_instance_type
  private_key = var.private_key
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  filebeat_url = var.filebeat_url
  tags = var.tags
}

module "consumer" {
  source = "../module/kfktest"
  name = var.name
  nodename = "consumer"
  ubuntu_ami = var.ubuntu_ami
  instance_type = var.procon_instance_type
  private_key = var.private_key
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  filebeat_url = ""
  tags = var.tags
}


module "ksqldb" {
  source = "../module/ksqldb"
  name = var.name
  nodename = "ksqldb"
  ubuntu_ami = var.ubuntu_ami
  instance_type = var.ksqldb_instance_type
  private_key = var.private_key
  work_cidr = var.work_cidr
  key_pair_name = var.key_pair_name
  depends_on = [module.kafka.id]
  kafka_private_ip = module.kafka.private_ip
  timezone = var.timezone
  tags = var.tags
}