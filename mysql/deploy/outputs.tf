output "mysql_public_ip" {
  value = aws_instance.mysql.public_ip
}

output "mysql_private_ip" {
  value = aws_instance.mysql.private_ip
}

output "inserter_public_ip" {
  value = aws_instance.inserter.public_ip
}

output "inserter_private_ip" {
  value = aws_instance.inserter.private_ip
}

output "selector_public_ip" {
  value = aws_instance.selector.public_ip
}

output "selector_private_ip" {
  value = aws_instance.selector.private_ip
}

output "kafka_public_ip" {
  value = aws_instance.kafka.public_ip
}

output "kafka_private_ip" {
  value = aws_instance.kafka.private_ip
}

output "consumer_public_ip" {
  value = aws_instance.consumer.public_ip
}

output "db_user" {
  value = var.db_user
}

output "db_passwd" {
  sensitive = true
  value = var.db_passwd
}

output "private_key_path" {
  value = var.private_key_path
}