output "sg_id" {
  value = aws_security_group.ksqldb.id
}

output "public_ip" {
  value = aws_instance.ksqldb.public_ip
}

output "private_ip" {
  value = aws_instance.ksqldb.private_ip
}
