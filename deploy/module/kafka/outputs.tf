output "sg_id" {
  value = aws_security_group.kafka.id
}

output "public_ip" {
  value = aws_instance.kafka.public_ip
}

output "private_ip" {
  value = aws_instance.kafka.private_ip
}

output "instance_id" {
  value = aws_instance.kafka.id
}