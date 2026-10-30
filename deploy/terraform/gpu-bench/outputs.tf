output "public_ip" {
  value = aws_instance.gpu.public_ip
}

output "ssh" {
  value = "ssh ubuntu@${aws_instance.gpu.public_ip}"
}

output "router_url" {
  value = "http://${aws_instance.gpu.public_ip}:30080"
}
