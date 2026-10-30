variable "region" {
  description = "AWS region with g6 spot capacity"
  type        = string
  default     = "us-east-1"
}

variable "instance_type" {
  description = "GPU instance type (g6.12xlarge = 4x L4; g5.12xlarge = 4x A10G is an alternative)"
  type        = string
  default     = "g6.12xlarge"
}

variable "spot_max_price" {
  description = "Max spot price in USD/hour (on-demand g6.12xlarge is ~4.60)"
  type        = string
  default     = "3.00"
}

variable "ssh_public_key_path" {
  description = "Public key installed on the VM"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}
