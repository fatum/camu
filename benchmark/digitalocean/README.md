# DigitalOcean five-node benchmark

This directory provisions an isolated DigitalOcean benchmark environment:

- one private VPC;
- five Ubuntu droplets running Camu in Docker;
- one private DigitalOcean Spaces bucket for data and coordination;
- one DigitalOcean Container Registry for the benchmark image;
- a firewall for SSH, benchmark HTTP traffic, and private cluster traffic.

The setup builds and publishes the Camu image from this repository before the
droplets are created. Credentials are supplied through Terraform variables and
are not committed to the repository.

## Prerequisites

Install Terraform, `jq`, and Docker, then create the ignored secrets file:

```bash
cp .env.example .env
# edit .env with your credentials
```

Create and edit the ignored variables file:

```bash
cp terraform.tfvars.example terraform.tfvars
terraform init
```

Set `registry_name`, `ssh_key_name`, `spaces_bucket_name`, and restrictive
CIDRs. The Spaces bucket is private and the registry uses the `basic` tier by
default. `spaces_force_destroy` defaults to `false`.

Provision the registry, build and push the image, then create the five nodes:

```bash
./provision.sh
```

Each provisioning run publishes a unique timestamped image tag and applies
that exact tag to the droplets, preventing stale or deleted registry tags from
being reused. Set `IMAGE_TAG` to choose a specific tag.

To build and publish another image after provisioning (`doctl` is optional):

```bash
IMAGE_TAG=benchmark-next ./build-image.sh
```

After provisioning, print the five public/private addresses and HTTP endpoints:

```bash
./ips.sh
```

The same values are available as `terraform output -json node_endpoints`; the
ordered public benchmark URLs are available as `terraform output -json benchmark_node_urls`.

## Run and destroy

```bash
./run.sh 1GiB
./destroy.sh
```

The run script waits for all five nodes, uses their public HTTP addresses, and
writes the report to `/tmp/camu-digitalocean-benchmark.json` unless `OUTPUT`
is set. `TARGET_BYTES` and `MESSAGE_BYTES` can override the workload.
During the run it scrapes each node's internal metrics endpoint every five
seconds and writes raw samples to `/tmp/camu-digitalocean-telemetry.jsonl`
unless `TELEMETRY_OUTPUT` is set.
When the Spaces credentials from `.env` are available, the runner also uploads
both files to `s3://camu/telemetry/<run-id>/`. Set `TELEMETRY_UPLOAD=0` to
disable this.

The default producer/consumer API is HTTP. To benchmark the Kafka protocol,
open port 9092 from `benchmark_cidr`, apply the firewall update, and run:

```bash
BENCHMARK_API=kafka ./run.sh 1GiB
```

Kafka brokers default to all five public droplet addresses on port 9092. Set
`KAFKA_BROKERS` to override them.
