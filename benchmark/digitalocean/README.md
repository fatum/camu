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

DigitalOcean intermittently returns `401 mTLS verification failed` from
registry and droplet API calls. `provision.sh` retries its registry-only step,
which cannot modify droplets. `destroy.sh` retries deletion polling. Both retry
that specific transient failure up to five times; set
`TERRAFORM_RETRY_ATTEMPTS` to change the limit.

Each provisioning run publishes a unique timestamped image tag and creates the
nodes with that exact tag, preventing stale or deleted registry tags from being
reused. Set `IMAGE_TAG` to choose a specific tag.

Benchmark nodes default to `s-4vcpu-8gb` so replication, export, and SQL can
run together without the 4 GiB memory pressure of the previous default. Set
`droplet_size` in `terraform.tfvars` to select a different plan.

To publish a new image and roll it across existing nodes without applying
Terraform, use:

```bash
./deploy.sh
```

`deploy.sh` updates nodes sequentially, waiting for `/v1/ready` before
restarting the next node. Set `IMAGE_TAG` to choose a specific tag or
`SSH_USER` to change the deployment user.

To build and publish an image without deploying it (`doctl` is optional):

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
TOPIC=benchmark-typed-demo ./run.sh produce 1GiB
TOPIC=benchmark-typed-demo ./run.sh consume 1GiB
TOPIC=benchmark-typed-demo ./run.sh sql 1GiB
./destroy.sh
```

The benchmark has separate `produce`, `consume`, and `sql` operations. Use the
same `TOPIC`, target size, and message size for each operation. The script
waits for all five nodes, uses their public HTTP addresses, and writes the
report to `/tmp/camu-digitalocean-benchmark.json` unless `OUTPUT` is set. A
topic is retained by default; set `CLEANUP=1` only when the final operation may
delete it. Each invocation without `TOPIC` creates a unique topic, which is
appropriate for a standalone `produce` run but not a later consume or SQL run.
Repeated `produce` runs with the same `TOPIC` append to that topic; the
benchmark resumes sequence and key numbering from its committed offsets.
`TARGET_BYTES`, `MESSAGE_BYTES`, and `PARTITIONS` can override the workload.
The default is four partitions. Byte values accept a positive byte count or a
binary unit such as `1GiB`, `512MiB`, or `64KiB`.
Set `EXPORT_ENABLED=false` to create a non-exporting topic. This isolates the
native produce/consume path; the `all` operation skips SQL sampling and SQL
verification, and the `sql` operation is unavailable.
`REQUEST_TIMEOUT` bounds every HTTP request (default `30s`); `CONSUME_TIMEOUT`
bounds the whole consume operation (default `10m`).

Start local benchmark monitoring separately when you need live metrics and
logs:

```bash
./monitor.sh start
# run one or more ./run.sh commands
./monitor.sh stop
```

`run.sh` never starts or stops monitoring. The stack runs Prometheus, Loki, and
Grafana locally. Prometheus scrapes Camu on port 8080 and cAdvisor on port 8082
from every benchmark node; the local collector follows each node's Camu
container log and sends it to Loki. It also follows each node's kernel log and
records Docker container state every five seconds, so OOM kills and restart
loops appear in the same Grafana log view. Grafana is available while the stack
is running at <http://localhost:3000> (`admin` / `admin`). The dashboard
provides the per-node CPU/memory, export-lag, S3/export error, and combined-log
view needed to correlate a failure across the cluster.

`consume` reads partition `p` through node `p`, rather than concentrating every
partition on the first public endpoint. It logs the page offset, response size,
and per-partition progress. `sql` logs every visibility poll and its error or
row-count progress. Use `all` only for the legacy sequential flow:

```bash
TOPIC=benchmark-typed-demo ./run.sh all 1GiB
```
During the run it scrapes each node's internal metrics endpoint every five
seconds and writes raw samples to `/tmp/camu-digitalocean-telemetry.jsonl`
unless `TELEMETRY_OUTPUT` is set.
When the Spaces credentials from `.env` are available, the runner also uploads
both files to `s3://camu/telemetry/<run-id>/`. Set `TELEMETRY_UPLOAD=0` to
disable this.

The default producer/consumer API is HTTP. To benchmark the Kafka protocol,
open port 9092 from `benchmark_cidr`, apply the firewall update, and run:

```bash
TOPIC=benchmark-typed-demo BENCHMARK_API=kafka ./run.sh produce 1GiB
```

Kafka brokers default to all five public droplet addresses on port 9092. Set
`KAFKA_BROKERS` to override them. The Kafka benchmark requests up to 16 MiB per
partition and 64 MiB per Fetch response; Camu enforces the 16 MiB per-partition
ceiling.
