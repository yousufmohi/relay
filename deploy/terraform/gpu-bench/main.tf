# GPU benchmark environment: ONE spot GPU VM running single-node k3s with the
# NVIDIA device plugin. Created for a benchmark session, destroyed after:
#
#   make bench-gpu      # terraform apply + run E1–E3 (+E8) on vLLM + copy results back
#   make bench-down     # terraform destroy
#
# Default: AWS g6.12xlarge (4× NVIDIA L4 24 GB, 48 vCPU) as a spot instance,
# so four vLLM workers each get a whole GPU (Qwen2.5-7B-Instruct in bf16 ≈
# 15 GB weights + ~5 GB KV cache per GPU).

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws  = { source = "hashicorp/aws", version = "~> 5.80" }
    http = { source = "hashicorp/http", version = "~> 3.4" }
  }
}

provider "aws" {
  region = var.region
}

data "http" "my_ip" {
  url = "https://checkip.amazonaws.com"
}

locals {
  my_cidr = "${chomp(data.http.my_ip.response_body)}/32"
}

# Ubuntu 22.04 with NVIDIA driver preinstalled (AWS Deep Learning Base AMI).
data "aws_ami" "dlami" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["Deep Learning Base OSS Nvidia Driver GPU AMI (Ubuntu 22.04)*"]
  }
  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

data "aws_vpc" "default" {
  default = true
}

resource "aws_key_pair" "bench" {
  key_name_prefix = "relay-bench-"
  public_key      = file(var.ssh_public_key_path)
}

resource "aws_security_group" "bench" {
  name_prefix = "relay-bench-"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "ssh from the operator"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
  }
  ingress {
    description = "router NodePort from the operator"
    from_port   = 30080
    to_port     = 30080
    protocol    = "tcp"
    cidr_blocks = [local.my_cidr]
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "gpu" {
  ami                    = data.aws_ami.dlami.id
  instance_type          = var.instance_type
  key_name               = aws_key_pair.bench.key_name
  vpc_security_group_ids = [aws_security_group.bench.id]

  instance_market_options {
    market_type = "spot"
    spot_options {
      max_price                      = var.spot_max_price
      spot_instance_type             = "one-time"
      instance_interruption_behavior = "terminate"
    }
  }

  root_block_device {
    volume_size = 200 # model weights + images
    volume_type = "gp3"
  }

  user_data = file("${path.module}/cloud-init.sh")

  tags = {
    Name    = "relay-gpu-bench"
    project = "relay"
  }
}
