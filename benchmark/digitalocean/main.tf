locals {
  node_count = 5
  node_names = [for index in range(local.node_count) : format("camu-benchmark-%02d", index + 1)]
}

data "digitalocean_ssh_key" "benchmark" {
  name = var.ssh_key_name
}

resource "digitalocean_vpc" "benchmark" {
  name     = "camu-benchmark-vpc-${var.region}"
  region   = var.region
  ip_range = var.vpc_cidr
}

resource "digitalocean_spaces_bucket" "benchmark" {
  name          = var.spaces_bucket_name
  region        = var.spaces_region
  acl           = "private"
  force_destroy = var.spaces_force_destroy
}

resource "digitalocean_container_registry" "benchmark" {
  name                   = var.registry_name
  region                 = var.region
  subscription_tier_slug = var.registry_tier
}

resource "digitalocean_firewall" "benchmark" {
  name        = "camu-benchmark-firewall"
  droplet_ids = [for node in digitalocean_droplet.camu : node.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.ssh_cidr
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "8080"
    source_addresses = var.benchmark_cidr
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "8080"
    source_addresses = [var.vpc_cidr]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "8081"
    source_addresses = [var.vpc_cidr]
  }

  inbound_rule {
    protocol         = "tcp"
    port_range       = "9092"
    source_addresses = var.benchmark_cidr
  }

  outbound_rule {
    protocol              = "tcp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "udp"
    port_range            = "1-65535"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }

  outbound_rule {
    protocol              = "icmp"
    destination_addresses = ["0.0.0.0/0", "::/0"]
  }
}

resource "digitalocean_droplet" "camu" {
  for_each = toset(local.node_names)

  name       = each.key
  region     = var.region
  size       = var.droplet_size
  image      = "ubuntu-24-04-x64"
  vpc_uuid   = digitalocean_vpc.benchmark.id
  ssh_keys   = [data.digitalocean_ssh_key.benchmark.id]
  monitoring = true
  ipv6       = true

  user_data = templatefile("${path.module}/cloud-init.yaml.tftpl", {
    node_name          = each.key
    camu_image         = var.camu_image
    digitalocean_token = var.digitalocean_token
    registry_endpoint  = digitalocean_container_registry.benchmark.endpoint
    bucket_name        = digitalocean_spaces_bucket.benchmark.name
    vpc_cidr           = var.vpc_cidr
    spaces_region      = var.spaces_region
    spaces_access_key  = var.spaces_access_key
    spaces_secret_key  = var.spaces_secret_key
  })

  tags = ["camu", "benchmark"]
}
