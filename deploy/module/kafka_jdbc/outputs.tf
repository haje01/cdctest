output "kafka_public_ip" {
  value = aws_instance.kafka.public_ip
}

output "kafka_private_ip" {
  value = aws_instance.kafka.private_ip
}

output "sg_id" {
  value = aws_security_group.kafka.id
}

output "public_ip" {
  value = aws_instance.kafka.public_ip
}

output "private_ip" {
  value = aws_instance.kafka.private_ip
}