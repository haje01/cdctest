output "kafka_public_ip" {
  value = module.kafka.public_ip
}

output "kafka_private_ip" {
  value = module.kafka.private_ip
}

output "kafka_instance_id" {
  value = module.kafka.instance_id
}

output "producer_public_ip" {
  value = module.producer.public_ip
}

output "consumer_public_ip" {
  value = module.consumer.public_ip
}
