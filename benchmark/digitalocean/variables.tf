variable "digitalocean_token" {
  type      = string
  sensitive = true
}

variable "benchmark_auth_token" {
  description = "Bearer token for benchmark SQL and authenticated heap-profile capture."
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = can(regex("^[A-Za-z0-9._~-]*$", var.benchmark_auth_token))
    error_message = "benchmark_auth_token may contain only letters, digits, dot, underscore, tilde, or hyphen."
  }
}

variable "spaces_access_key" {
  type      = string
  sensitive = true
}

variable "spaces_secret_key" {
  type      = string
  sensitive = true
}

variable "region" {
  type    = string
  default = "nyc3"
}

variable "spaces_region" {
  type    = string
  default = "nyc3"
}

variable "spaces_bucket_name" {
  type = string
}

variable "spaces_force_destroy" {
  type    = bool
  default = false
}

variable "registry_name" {
  type = string
}

variable "registry_tier" {
  type    = string
  default = "basic"
}

variable "camu_image" {
  type = string
}

variable "ssh_key_name" {
  type = string
}

variable "ssh_cidr" {
  type    = list(string)
  default = ["0.0.0.0/0"]
}

variable "benchmark_cidr" {
  type    = list(string)
  default = ["0.0.0.0/0"]
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "droplet_size" {
  description = "DigitalOcean droplet plan for each benchmark node."
  type        = string
  default     = "s-4vcpu-8gb"
}

variable "benchmark_droplet_size" {
  description = "DigitalOcean droplet plan for benchmark client nodes."
  type        = string
  default     = "s-2vcpu-4gb"
}

variable "benchmark_image" {
  description = "Docker image for the benchmark service."
  type        = string
  default     = ""
}

variable "benchmark_rate" {
  description = "Records per second per topic for the benchmark service."
  type        = number
  default     = 10000
}

variable "benchmark_topics" {
  description = "Comma-separated topic names for the benchmark service to test."
  type        = string
  default     = ""
}

variable "benchmark_storage_modes" {
  description = "Comma-separated storage modes matching benchmark_topics."
  type        = string
  default     = ""
}

variable "benchmark_partitions" {
  description = "Number of partitions for benchmark topics."
  type        = number
  default     = 12
}

variable "benchmark_message_bytes" {
  description = "Record size in bytes for benchmark service."
  type        = number
  default     = 1024
}
