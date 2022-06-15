output "mysql_public_ip" {
  value = aws_instance.mysql.public_ip
}

output "mysql_private_ip" {
  value = aws_instance.mysql.private_ip
}

output "inserter_public_ip" {
  value = module.inserter.public_ip
}

output "inserter_private_ip" {
  value = module.inserter.private_ip
}

output "selector_public_ip" {
  value = module.selector.public_ip
}

output "selector_private_ip" {
  value = module.selector.private_ip
}

output "kafka_public_ip" {
  value = module.kafka.public_ip
}

output "kafka_private_ip" {
  value = module.kafka.private_ip
}

output "consumer_public_ip" {
  value = module.consumer.public_ip
}

output "db_user" {
  value = var.db_user
}

output "db_passwd" {
  sensitive = true
  value = random_string.db_passwd
}
