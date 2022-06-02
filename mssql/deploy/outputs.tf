output "sqlserver_public_ip" {
  value = aws_instance.sqlserver.public_ip
}

output "sqlserver_private_ip" {
  value = aws_instance.sqlserver.private_ip
}

output "sqlserver_user" {
  value = var.sqlserver_user
}

output "sqlserver_passwd" {
  sensitive = true
  value = var.sqlserver_passwd
}

output "sqlserver_admin_passwd" {
  value = "${rsadecrypt(aws_instance.sqlserver.password_data,file(var.private_key_path))}"
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