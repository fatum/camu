#!/usr/bin/env bash
set -euo pipefail

# ─── Config ──────────────────────────────────────────────────────────────────
CAMU_HTTP_PORT="${CAMU_HTTP_PORT:-8080}"
CAMU_KAFKA_PORT="${CAMU_KAFKA_PORT:-9092}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-camu-data}"
CAMU_CACHE_DIR="${CAMU_CACHE_DIR:-/tmp/camu_dev_cache}"

DEV_DIR="/tmp/camu_dev"
PID_DIR="$DEV_DIR/pids"
MINIO_DATA_DIR="$DEV_DIR/minio_data"
CAMU_CONFIG="$DEV_DIR/camu-dev.yaml"
CAMU_BIN="$(cd "$(dirname "$0")/.." && pwd)/bin/camu"
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

CAMU_URL="http://localhost:${CAMU_HTTP_PORT}"
MINIO_URL="http://localhost:${MINIO_PORT}"

# ─── Helpers ─────────────────────────────────────────────────────────────────
red()    { printf '\033[0;31m%s\033[0m\n' "$*"; }
green()  { printf '\033[0;32m%s\033[0m\n' "$*"; }
yellow() { printf '\033[0;33m%s\033[0m\n' "$*"; }
bold()   { printf '\033[1m%s\033[0m\n' "$*"; }

die() { red "ERROR: $*" >&2; exit 1; }

MINIO_CONTAINER="camu-dev-minio"

check_deps() {
    local missing=()
    for cmd in curl jq kcat mc docker; do
        if ! command -v "$cmd" &>/dev/null; then
            missing+=("$cmd")
        fi
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        die "Missing dependencies: ${missing[*]}. Install them first."
    fi
}

ensure_dirs() {
    mkdir -p "$PID_DIR" "$MINIO_DATA_DIR" "$CAMU_CACHE_DIR"
}

write_config() {
    cat > "$CAMU_CONFIG" <<EOF
server:
  address: "127.0.0.1:${CAMU_HTTP_PORT}"
  internal_address: "127.0.0.1:8081"
  replication_address: "127.0.0.1:8082"
  instance_id: ""
  kafka_port: ${CAMU_KAFKA_PORT}
storage:
  bucket: "${MINIO_BUCKET}"
  region: "us-east-1"
  endpoint: "${MINIO_URL}"
  credentials:
    access_key: "${MINIO_ROOT_USER}"
    secret_key: "${MINIO_ROOT_PASSWORD}"
segments:
  max_size: 67108864
  max_age: "1m"
  compression: "none"
  record_batch_target_size: 16384
  index_interval_bytes: 4096
cache:
  directory: "${CAMU_CACHE_DIR}"
  max_size: 10737418240
maintenance:
  parquet_export:
    temp_directory: "${CAMU_CACHE_DIR}/export-tmp"
coordination:
  lease_ttl: "30s"
  rebalance_delay: "5s"
  heartbeat_interval: "10s"
  isr_expansion_threshold: 1000
  replication_timeout: "30s"
EOF
}

is_running() {
    local pidfile="$PID_DIR/$1.pid"
    if [[ -f "$pidfile" ]]; then
        local pid
        pid=$(cat "$pidfile")
        if kill -0 "$pid" 2>/dev/null; then
            return 0
        fi
        rm -f "$pidfile"
    fi
    return 1
}

wait_for_port() {
    local port=$1 name=$2 timeout=${3:-30}
    local i=0
    while ! (echo > /dev/tcp/localhost/"$port") 2>/dev/null; do
        sleep 0.5
        i=$((i + 1))
        if [[ $i -ge $((timeout * 2)) ]]; then
            die "Timed out waiting for $name on port $port"
        fi
    done
}

wait_for_camu() {
    local timeout=${1:-30} i=0
    while ! curl -sf "${CAMU_URL}/v1/ready" -o /dev/null 2>/dev/null; do
        sleep 0.5
        i=$((i + 1))
        if [[ $i -ge $((timeout * 2)) ]]; then
            die "Timed out waiting for camu to become ready"
        fi
    done
}

# ─── Commands ────────────────────────────────────────────────────────────────

cmd_up() {
    check_deps
    ensure_dirs

    # Start MinIO (Docker)
    if docker inspect "$MINIO_CONTAINER" &>/dev/null 2>&1; then
        if [ "$(docker inspect -f '{{.State.Running}}' "$MINIO_CONTAINER" 2>/dev/null)" = "true" ]; then
            yellow "MinIO already running (container $MINIO_CONTAINER)"
        else
            bold "Starting existing MinIO container..."
            docker start "$MINIO_CONTAINER" > /dev/null
            wait_for_port "$MINIO_PORT" "MinIO"
            green "MinIO started on :${MINIO_PORT} (console :${MINIO_CONSOLE_PORT})"
        fi
    else
        bold "Starting MinIO (Docker)..."
        docker run -d \
            --name "$MINIO_CONTAINER" \
            -p "${MINIO_PORT}:9000" \
            -p "${MINIO_CONSOLE_PORT}:9001" \
            -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
            -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
            -v "${MINIO_DATA_DIR}:/data" \
            minio/minio server /data --console-address ":9001" \
            > /dev/null
        wait_for_port "$MINIO_PORT" "MinIO"
        green "MinIO started on :${MINIO_PORT} (console :${MINIO_CONSOLE_PORT})"
    fi

    # Configure mc alias and wait for MinIO S3 API to be ready
    mc alias set camu-dev "$MINIO_URL" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" --api S3v4 -q 2>/dev/null || true
    local mc_tries=0
    while ! mc ls camu-dev/ &>/dev/null 2>&1; do
        sleep 0.5
        mc_tries=$((mc_tries + 1))
        if [[ $mc_tries -ge 20 ]]; then
            die "MinIO S3 API not responding"
        fi
    done

    # Create bucket
    if ! mc ls camu-dev/"$MINIO_BUCKET" &>/dev/null; then
        mc mb camu-dev/"$MINIO_BUCKET" -q
        green "Created bucket: $MINIO_BUCKET"
    fi

    # Build camu
    bold "Building camu..."
    (cd "$PROJECT_ROOT" && go build -trimpath -ldflags '-s -w' -o bin/camu ./cmd/camu)
    green "Built: $CAMU_BIN"

    # Write config and start camu
    write_config

    if is_running camu; then
        yellow "camu already running (pid $(cat "$PID_DIR/camu.pid"))"
    else
        bold "Starting camu..."
        "$CAMU_BIN" serve --config "$CAMU_CONFIG" \
            > "$DEV_DIR/camu.log" 2>&1 &
        echo $! > "$PID_DIR/camu.pid"
        wait_for_camu
        green "camu started — HTTP :${CAMU_HTTP_PORT}, Kafka :${CAMU_KAFKA_PORT}"
    fi

    green "Ready! Logs at $DEV_DIR/*.log"
}

cmd_down() {
    # Stop camu process
    if is_running camu; then
        local pid
        pid=$(cat "$PID_DIR/camu.pid")
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
        rm -f "$PID_DIR/camu.pid"
        green "Stopped camu (pid $pid)"
    else
        yellow "camu not running"
    fi

    # Stop MinIO container
    if docker inspect "$MINIO_CONTAINER" &>/dev/null 2>&1; then
        docker stop "$MINIO_CONTAINER" > /dev/null 2>&1 || true
        docker rm "$MINIO_CONTAINER" > /dev/null 2>&1 || true
        green "Stopped MinIO container"
    else
        yellow "MinIO not running"
    fi
}

cmd_status() {
    bold "=== Processes ==="
    if [ "$(docker inspect -f '{{.State.Running}}' "$MINIO_CONTAINER" 2>/dev/null)" = "true" ]; then
        green "minio: running (container $MINIO_CONTAINER)"
    else
        red "minio: not running"
    fi
    if is_running camu; then
        green "camu: running (pid $(cat "$PID_DIR/camu.pid"))"
    else
        red "camu: not running"
    fi

    bold "=== camu health ==="
    if curl -sf "${CAMU_URL}/v1/ready" | jq . 2>/dev/null; then
        :
    else
        red "camu not reachable"
    fi

    bold "=== MinIO ==="
    if mc ls camu-dev/"$MINIO_BUCKET" &>/dev/null 2>&1; then
        green "Bucket $MINIO_BUCKET accessible"
    else
        red "MinIO not reachable or bucket missing"
    fi
}

cmd_produce() {
    local topics=1 events=10 api="kafka"

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --topics)  topics="$2"; shift 2 ;;
            --events)  events="$2"; shift 2 ;;
            --api)     api="$2"; shift 2 ;;
            *)         die "Unknown flag: $1" ;;
        esac
    done

    [[ "$api" == "kafka" || "$api" == "http" ]] || die "--api must be kafka or http"

    for i in $(seq 1 "$topics"); do
        local topic="dev-topic-${i}"

        # Create topic via HTTP (idempotent — 409 is fine)
        local create_resp
        create_resp=$(curl -sf -w "\n%{http_code}" -X POST "${CAMU_URL}/v1/topics" \
            -d "{\"name\": \"${topic}\", \"partitions\": 3}" 2>/dev/null) || true
        local http_code
        http_code=$(echo "$create_resp" | tail -1)
        if [[ "$http_code" == "201" ]]; then
            green "Created topic: $topic (3 partitions)"
        elif [[ "$http_code" == "409" ]]; then
            yellow "Topic $topic already exists"
        else
            red "Failed to create topic $topic (HTTP $http_code)"
            echo "$create_resp" | head -1
            continue
        fi

        bold "Producing $events events to $topic via $api..."

        if [[ "$api" == "kafka" ]]; then
            for j in $(seq 1 "$events"); do
                printf '%s\t{"event":%d,"topic":"%s","ts":%d}' \
                    "key-${j}" "$j" "$topic" "$(date +%s)"
                echo
            done | kcat -P -b "localhost:${CAMU_KAFKA_PORT}" -t "$topic" -K '\t'
            green "Produced $events events to $topic via Kafka"

        elif [[ "$api" == "http" ]]; then
            # Build JSON array of messages
            local msgs="["
            for j in $(seq 1 "$events"); do
                [[ $j -gt 1 ]] && msgs+=","
                msgs+="{\"key\":\"key-${j}\",\"value\":\"{\\\"event\\\":${j},\\\"topic\\\":\\\"${topic}\\\",\\\"ts\\\":$(date +%s)}\"}"
            done
            msgs+="]"

            curl -sf -X POST "${CAMU_URL}/v1/topics/${topic}/messages" \
                -d "$msgs" | jq .
            green "Produced $events events to $topic via HTTP"
        fi
    done
}

cmd_consume() {
    local topic="" group="dev-group" api="kafka" limit=10

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --topic)  topic="$2"; shift 2 ;;
            --group)  group="$2"; shift 2 ;;
            --api)    api="$2"; shift 2 ;;
            --limit)  limit="$2"; shift 2 ;;
            *)        die "Unknown flag: $1" ;;
        esac
    done

    [[ -n "$topic" ]] || die "--topic is required"
    [[ "$api" == "kafka" || "$api" == "http" ]] || die "--api must be kafka or http"

    if [[ "$api" == "kafka" ]]; then
        bold "Consuming from $topic (group=$group, limit=$limit) via Kafka..."
        kcat -b "localhost:${CAMU_KAFKA_PORT}" -C -J -c "$limit" -e \
            -G "$group" "$topic" | jq -c '.payload // .'
    elif [[ "$api" == "http" ]]; then
        bold "Consuming from $topic (group=$group, limit=$limit) via HTTP..."

        # Get topic info for partition count
        local topic_info
        topic_info=$(curl -sf "${CAMU_URL}/v1/topics/${topic}") || die "Topic $topic not found"
        local partitions
        partitions=$(echo "$topic_info" | jq '.partitions')

        # Get committed offsets for this group
        local offsets_resp
        offsets_resp=$(curl -sf "${CAMU_URL}/v1/groups/${group}/offsets" 2>/dev/null) || offsets_resp='{"topics":{}}'

        local total=0
        local commit_payload="{\"topics\":{\"${topic}\":{}}}"

        for pid in $(seq 0 $((partitions - 1))); do
            local start_offset
            start_offset=$(echo "$offsets_resp" | jq -r ".topics.\"${topic}\".\"${pid}\" // 0")

            local resp
            resp=$(curl -sf "${CAMU_URL}/v1/topics/${topic}/partitions/${pid}/messages?offset=${start_offset}&limit=${limit}")
            local count
            count=$(echo "$resp" | jq '.messages | length')
            local next_offset
            next_offset=$(echo "$resp" | jq '.next_offset')

            if [[ "$count" -gt 0 ]]; then
                echo "$resp" | jq -c ".messages[] | {partition: ${pid}} + ."
                total=$((total + count))
            fi

            # Build commit payload
            commit_payload=$(echo "$commit_payload" | jq ".topics.\"${topic}\".\"${pid}\" = ${next_offset}")
        done

        # Commit offsets
        if [[ "$total" -gt 0 ]]; then
            curl -sf -X POST "${CAMU_URL}/v1/groups/${group}/commit" \
                -d "$commit_payload" > /dev/null
            green "Consumed $total messages, offsets committed for group=$group"
        else
            yellow "No new messages"
        fi
    fi
}

cmd_inspect() {
    local verbose=false

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --verbose|-v) verbose=true; shift ;;
            *)            die "Unknown flag: $1" ;;
        esac
    done

    # List topics from camu API
    bold "=== Topics ==="
    local topics_resp
    topics_resp=$(curl -sf "${CAMU_URL}/v1/topics") || die "Cannot reach camu"
    echo "$topics_resp" | jq -r '.[] | "  \(.name)  partitions=\(.partitions)  retention=\(.retention)"'

    local topic_names
    topic_names=$(echo "$topics_resp" | jq -r '.[].name')

    if [[ -z "$topic_names" ]]; then
        yellow "No topics found"
        return
    fi

    bold "=== Partition State (from S3) ==="
    for topic in $topic_names; do
        local partitions
        partitions=$(echo "$topics_resp" | jq -r ".[] | select(.name==\"${topic}\") | .partitions")

        for pid in $(seq 0 $((partitions - 1))); do
            local state_key="${topic}/${pid}/state.json"
            local state
            state=$(mc cat "camu-dev/${MINIO_BUCKET}/${state_key}" 2>/dev/null) || state="{}"
            local hw
            hw=$(echo "$state" | jq '.high_watermark // 0')

            # Count segments
            local seg_count
            seg_count=$(mc ls "camu-dev/${MINIO_BUCKET}/${topic}/${pid}/" 2>/dev/null | grep -c '\.log$') || seg_count=0

            printf "  %s/%d  HW=%s  segments=%s\n" "$topic" "$pid" "$hw" "$seg_count"

            if [[ "$verbose" == true ]]; then
                # List individual segments with details
                mc ls "camu-dev/${MINIO_BUCKET}/${topic}/${pid}/" 2>/dev/null | while read -r line; do
                    local fname
                    fname=$(echo "$line" | awk '{print $NF}')
                    case "$fname" in
                        *.log)
                            local size
                            size=$(echo "$line" | awk '{print $4}')
                            # Parse offset range from filename: base-end-epoch.log
                            printf "    segment: %s  size=%s\n" "$fname" "$size"
                            ;;
                        *.meta.json)
                            local meta
                            meta=$(mc cat "camu-dev/${MINIO_BUCKET}/${topic}/${pid}/${fname}" 2>/dev/null)
                            if [[ -n "$meta" ]]; then
                                echo "$meta" | jq -c '
                                    {base_offset, end_offset, min_timestamp, max_timestamp, record_count, size_bytes, compression}
                                ' 2>/dev/null | sed 's/^/    /'
                            fi
                            ;;
                    esac
                done
            fi
        done
    done

    bold "=== Consumer Group Offsets (from S3) ==="
    # List group offset files
    local groups
    groups=$(mc ls "camu-dev/${MINIO_BUCKET}/_coordination/groups/" 2>/dev/null | awk '{print $NF}' | sed 's|/$||') || true
    if [[ -z "$groups" ]]; then
        yellow "No consumer groups found"
    else
        for group in $groups; do
            local offsets
            offsets=$(mc cat "camu-dev/${MINIO_BUCKET}/_coordination/groups/${group}/offsets.json" 2>/dev/null) || continue
            printf "  group=%s\n" "$group"
            echo "$offsets" | jq -r '
                to_entries[] | "    \(.key): " + (
                    .value | to_entries | map("p\(.key)=\(.value)") | join(" ")
                )
            ' 2>/dev/null || echo "$offsets" | jq -c . | sed 's/^/    /'
        done
    fi

    # Also check Kafka-protocol consumer groups
    local kafka_groups
    kafka_groups=$(mc ls "camu-dev/${MINIO_BUCKET}/_coordination/kafka-groups/" 2>/dev/null | awk '{print $NF}' | sed 's|/$||' | sed 's|\.json$||') || true
    if [[ -n "$kafka_groups" ]]; then
        bold "=== Kafka Consumer Groups (from S3) ==="
        for group in $kafka_groups; do
            local gdata
            gdata=$(mc cat "camu-dev/${MINIO_BUCKET}/_coordination/kafka-groups/${group}.json" 2>/dev/null) || continue
            printf "  group=%s  generation=%s  members=%s\n" \
                "$group" \
                "$(echo "$gdata" | jq '.generation // 0')" \
                "$(echo "$gdata" | jq '.members | length // 0')"
        done
    fi
}

cmd_push() {
    local topic="dev-topic-1" count=10

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --topic) topic="$2"; shift 2 ;;
            --count) count="$2"; shift 2 ;;
            *)       die "Unknown flag: $1" ;;
        esac
    done

    # Ensure topic exists (409 is fine)
    local http_code
    http_code=$(curl -sf -o /dev/null -w "%{http_code}" -X POST "${CAMU_URL}/v1/topics" \
        -d "{\"name\": \"${topic}\", \"partitions\": 3}" 2>/dev/null) || true
    if [[ "$http_code" == "201" ]]; then
        green "Created topic: $topic"
    fi

    bold "Pushing $count random messages to $topic via HTTP..."
    local msgs="["
    for i in $(seq 1 "$count"); do
        local uid
        uid=$(od -An -tx1 -N4 /dev/urandom | tr -d ' ')
        [[ $i -gt 1 ]] && msgs+=","
        msgs+="{\"key\":\"key-${uid}\",\"value\":\"{\\\"id\\\":\\\"${uid}\\\",\\\"seq\\\":${i},\\\"rand\\\":$((RANDOM % 10000)),\\\"ts\\\":$(date +%s)}\"}"
    done
    msgs+="]"

    curl -sf -X POST "${CAMU_URL}/v1/topics/${topic}/messages" \
        -d "$msgs" | jq -c '.offsets | length | "\(.) messages delivered"'
    green "Pushed $count messages to $topic"
}

cmd_watch() {
    local topic="dev-topic-1" group="dev-watch-$$" batches=5

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --topic)   topic="$2"; shift 2 ;;
            --group)   group="$2"; shift 2 ;;
            --batches) batches="$2"; shift 2 ;;
            *)         die "Unknown flag: $1" ;;
        esac
    done

    # Get partition count
    local topic_info
    topic_info=$(curl -sf "${CAMU_URL}/v1/topics/${topic}") || die "Topic $topic not found"
    local partitions
    partitions=$(echo "$topic_info" | jq '.partitions')

    # Get initial offsets for this group (or start from 0)
    local offsets_resp
    offsets_resp=$(curl -sf "${CAMU_URL}/v1/groups/${group}/offsets" 2>/dev/null) || offsets_resp='{"topics":{}}'

    bold "Watching $topic ($partitions partitions, group=$group) — $batches batches, 1s interval"
    local batch=0
    while [[ $batch -lt $batches ]]; do
        batch=$((batch + 1))
        yellow "--- batch $batch/$batches ($(date +%H:%M:%S)) ---"

        local total=0
        local commit_payload="{\"topics\":{\"${topic}\":{}}}"

        for pid in $(seq 0 $((partitions - 1))); do
            local start_offset
            start_offset=$(echo "$offsets_resp" | jq -r ".topics.\"${topic}\".\"${pid}\" // 0" 2>/dev/null) || start_offset=0

            local resp
            resp=$(curl -sf "${CAMU_URL}/v1/topics/${topic}/partitions/${pid}/messages?offset=${start_offset}&limit=50" 2>/dev/null) || continue
            local cnt
            cnt=$(echo "$resp" | jq '.messages | length' 2>/dev/null) || continue
            local next_offset
            next_offset=$(echo "$resp" | jq '.next_offset' 2>/dev/null) || continue

            if [[ "$cnt" -gt 0 ]]; then
                echo "$resp" | jq -c ".messages[] | {p: ${pid}, off: .offset, key: .key, val: (.value | fromjson? // .)}" 2>/dev/null || true
                total=$((total + cnt))
            fi

            commit_payload=$(echo "$commit_payload" | jq ".topics.\"${topic}\".\"${pid}\" = ${next_offset}" 2>/dev/null) || true
        done

        # Commit offsets and update local state
        if [[ "$total" -gt 0 ]]; then
            curl -sf -X POST "${CAMU_URL}/v1/groups/${group}/commit" \
                -d "$commit_payload" > /dev/null 2>&1 || true
            offsets_resp=$(curl -sf "${CAMU_URL}/v1/groups/${group}/offsets" 2>/dev/null) || true
            green "  $total messages"
        else
            yellow "  no new messages"
        fi

        if [[ $batch -lt $batches ]]; then sleep 1; fi
    done
}

cmd_logs() {
    local svc="${1:-camu}"
    local logfile="$DEV_DIR/${svc}.log"
    [[ -f "$logfile" ]] || die "No log file: $logfile"
    tail -f "$logfile"
}

cmd_clean() {
    cmd_down 2>/dev/null || true
    docker rm -f "$MINIO_CONTAINER" 2>/dev/null || true
    rm -rf "$DEV_DIR" "$CAMU_CACHE_DIR"
    green "Cleaned $DEV_DIR and $CAMU_CACHE_DIR"
}

# ─── Usage ───────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
$(bold "camu dev tool")

$(bold "Usage:") $(basename "$0") <command> [flags]

$(bold "Lifecycle:")
  up                            Start MinIO + camu
  down                          Stop camu + MinIO
  status                        Show process and health status
  logs [camu|minio]             Tail logs (default: camu)
  clean                         Stop all and delete dev data

$(bold "Produce:")
  produce [flags]               Generate and produce events
    --topics N                  Number of topics to create (default: 1)
    --events N                  Events per topic (default: 10)
    --api kafka|http            Client to use (default: kafka)

$(bold "Consume:")
  consume [flags]               Read messages from a topic
    --topic NAME                Topic to consume (required)
    --group NAME                Consumer group (default: dev-group)
    --api kafka|http            Client to use (default: kafka)
    --limit N                   Max messages to read (default: 10)

$(bold "Quick test:")
  push [flags]                  Push N random messages via Kafka
    --topic NAME                Topic (default: dev-topic-1)
    --count N                   Number of messages (default: 10)
  watch [flags]                 Read N batches with decoded data, 1s interval
    --topic NAME                Topic (default: dev-topic-1)
    --group NAME                Consumer group (default: random)
    --batches N                 Number of batches (default: 5)

$(bold "Inspect:")
  inspect [--verbose]           Browse S3 data: topics, HW, segments, groups

$(bold "Environment variables:")
  CAMU_HTTP_PORT                HTTP port (default: 8080)
  CAMU_KAFKA_PORT               Kafka port (default: 9092)
  MINIO_PORT                    MinIO port (default: 9000)
EOF
}

# ─── Main ────────────────────────────────────────────────────────────────────
cmd="${1:-}"
shift || true

case "$cmd" in
    up)       cmd_up ;;
    down)     cmd_down ;;
    status)   cmd_status ;;
    produce)  cmd_produce "$@" ;;
    consume)  cmd_consume "$@" ;;
    push)     cmd_push "$@" ;;
    watch)    cmd_watch "$@" ;;
    inspect)  cmd_inspect "$@" ;;
    logs)     cmd_logs "$@" ;;
    clean)    cmd_clean ;;
    help|"")  usage ;;
    *)        die "Unknown command: $cmd. Run '$(basename "$0") help' for usage." ;;
esac
