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
  value = "${rsadecrypt(aws_instance.mssql.password_data,file(env.KFKTEST_SSH_PKEY))}"
  sensitive = true
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
  value = random_string.db_passwd
}
