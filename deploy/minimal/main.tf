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
  kafka_url = var.kafka_url
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
  kafka_url = var.kafka_url
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
  kafka_url = var.kafka_url
  tags = var.tags
}
