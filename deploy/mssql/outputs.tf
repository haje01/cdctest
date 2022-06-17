output "mssql_public_ip" {
  value = aws_instance.mssql.public_ip
}

output "mssql_private_ip" {
  value = aws_instance.mssql.private_ip
}

output "mssql_user" {
  value = var.mssql_user
}

output "mssql_passwd" {
  sensitive = true
  value = random_string.mssql_passwd
}

output "mssql_admin_passwd" {
  value = "${rsadecrypt(aws_instance.mssql.password_data,file(var.private_key))}"
  sensitive = true
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

output "kafka_instance_id" {
  value = module.kafka.instance_id
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
