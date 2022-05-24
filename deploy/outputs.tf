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

output "debezium_public_ip" {
  value = aws_instance.debezium.public_ip
  # value = aws_eip.debezium.public_ip
}

output "debezium_private_ip" {
  value = aws_instance.debezium.private_ip
}

output "db_user" {
  value = var.db_user
}

output "db_passwd" {
  sensitive = true
  value = var.db_passwd
}