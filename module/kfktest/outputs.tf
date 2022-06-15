output "sg_id" {
  value = aws_security_group.kfktest.id
}

output "public_ip" {
  value = aws_instance.kfktest.public_ip
}

output "private_ip" {
  value = aws_instance.kfktest.private_ip
}
