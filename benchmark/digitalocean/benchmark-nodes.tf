locals {
  benchmark_node_count = 2
  benchmark_node_names = [for i in range(local.benchmark_node_count) : format("camu-benchmark-client-%02d", i + 1)]
}

resource "digitalocean_droplet" "benchmark_client" {
  for_each = toset(local.benchmark_node_names)

  name       = each.key
  region     = var.region
  size       = var.benchmark_droplet_size
  image      = "ubuntu-24-04-x64"
  vpc_uuid   = digitalocean_vpc.benchmark.id
  ssh_keys   = [data.digitalocean_ssh_key.benchmark.id]
  monitoring = true

  user_data = templatefile("${path.module}/cloud-init-benchmark.yaml.tftpl", {
    node_name            = each.key
    benchmark_image      = var.benchmark_image
    digitalocean_token   = var.digitalocean_token
    registry_endpoint    = digitalocean_container_registry.benchmark.endpoint
    spaces_region        = var.spaces_region
    spaces_access_key    = var.spaces_access_key
    spaces_secret_key    = var.spaces_secret_key
    bucket_name          = digitalocean_spaces_bucket.benchmark.name
    kafka_brokers        = join(",", [for node in digitalocean_droplet.camu : "${node.ipv4_address}:9092"])
    benchmark_rate       = var.benchmark_rate
    benchmark_topics     = var.benchmark_topics
    benchmark_storage_modes = var.benchmark_storage_modes
    benchmark_partitions = var.benchmark_partitions
    benchmark_message_bytes = var.benchmark_message_bytes
  })

  tags = ["camu", "benchmark-client"]
}

resource "digitalocean_firewall" "benchmark_client" {
  name        = "camu-benchmark-client-firewall"
  droplet_ids = [for node in digitalocean_droplet.benchmark_client : node.id]

  inbound_rule {
    protocol         = "tcp"
    port_range       = "22"
    source_addresses = var.ssh_cidr
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
}
