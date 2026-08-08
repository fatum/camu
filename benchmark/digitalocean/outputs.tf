output "public_ips" {
  value = {
    for name, node in digitalocean_droplet.camu : name => node.ipv4_address
  }
}

output "private_ips" {
  value = {
    for name, node in digitalocean_droplet.camu : name => node.ipv4_address_private
  }
}

output "node_endpoints" {
  value = {
    for name, node in digitalocean_droplet.camu : name => {
      public_ip        = node.ipv4_address
      private_ip       = node.ipv4_address_private
      public_http_url  = "http://${node.ipv4_address}:8080"
      private_http_url = "http://${node.ipv4_address_private}:8080"
      ready_url        = "http://${node.ipv4_address}:8080/v1/ready"
    }
  }
}

output "benchmark_node_urls" {
  value = [
    for name in sort(local.node_names) :
    "http://${digitalocean_droplet.camu[name].ipv4_address}:8080"
  ]
}

output "spaces_endpoint" {
  value = "https://${var.spaces_region}.digitaloceanspaces.com"
}

output "bucket_name" {
  value = digitalocean_spaces_bucket.benchmark.name
}

output "registry_name" {
  value = digitalocean_container_registry.benchmark.name
}

output "registry_endpoint" {
  value = digitalocean_container_registry.benchmark.endpoint
}

output "benchmark_client_ips" {
  value = {
    for name, node in digitalocean_droplet.benchmark_client : name => node.ipv4_address
  }
}

output "kafka_brokers" {
  value = join(",", [for node in digitalocean_droplet.camu : "${node.ipv4_address}:9092"])
}
