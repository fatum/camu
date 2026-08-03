#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
compose=(docker compose -f "$script_dir/monitoring-compose.yml")
ssh_user="${SSH_USER:-root}"
ssh_opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)

case "${1:-}" in
  start)
    command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
    ips_json="$(terraform -chdir="$script_dir" output -json public_ips)"
    ips=()
    while IFS= read -r ip; do
      ips[${#ips[@]}]="$ip"
    done < <(printf '%s' "$ips_json" | jq -r 'to_entries | sort_by(.key) | .[].value')
    [[ "${#ips[@]}" -eq 5 ]] || { echo "expected five droplet IPs" >&2; exit 1; }
    mkdir -p "$script_dir/monitoring/runtime"
    {
      printf '%s\n' 'global:' '  scrape_interval: 5s' '  evaluation_interval: 5s' '' 'scrape_configs:'
      printf '%s\n' '  - job_name: camu' '    static_configs:' '      - targets:'
      for ip in "${ips[@]}"; do printf '          - "%s:8080"\n' "$ip"; done
      printf '%s\n' '  - job_name: cadvisor' '    static_configs:' '      - targets:'
      for ip in "${ips[@]}"; do printf '          - "%s:8082"\n' "$ip"; done
    } >"$script_dir/monitoring/runtime/prometheus.yml"
    {
      printf '%s\n' 'server:' '  http_listen_port: 9080' '  grpc_listen_port: 0' '' 'positions:' '  filename: /tmp/positions.yaml' '' 'clients:' '  - url: http://loki:3100/loki/api/v1/push' '' 'scrape_configs:'
      for ip in "${ips[@]}"; do
        printf '%s\n' '  - job_name: camu' '    static_configs:' '      - targets: [localhost]' '        labels:' '          job: camu'
        printf '          node: "%s"\n' "$ip"
        printf '          __path__: /var/log/camu/%s.log\n' "$ip"
        printf '%s\n' '  - job_name: kernel' '    static_configs:' '      - targets: [localhost]' '        labels:' '          job: kernel'
        printf '          node: "%s"\n' "$ip"
        printf '          __path__: /var/log/camu/kernel-%s.log\n' "$ip"
        printf '%s\n' '  - job_name: docker-state' '    static_configs:' '      - targets: [localhost]' '        labels:' '          job: docker-state'
        printf '          node: "%s"\n' "$ip"
        printf '          __path__: /var/log/camu/docker-state-%s.log\n' "$ip"
      done
    } >"$script_dir/monitoring/runtime/promtail.yml"
    : >"$script_dir/monitoring/runtime/ssh-pids"
    mkdir -p "$script_dir/monitoring/runtime/logs"
    for ip in "${ips[@]}"; do
      : >"$script_dir/monitoring/runtime/logs/$ip.log"
      : >"$script_dir/monitoring/runtime/logs/kernel-$ip.log"
      : >"$script_dir/monitoring/runtime/logs/docker-state-$ip.log"
      ssh "${ssh_opts[@]}" "${ssh_user}@${ip}" 'docker logs -f --tail 0 --timestamps camu 2>&1' >>"$script_dir/monitoring/runtime/logs/$ip.log" 2>&1 &
      printf '%s\n' "$!" >>"$script_dir/monitoring/runtime/ssh-pids"
      ssh "${ssh_opts[@]}" "${ssh_user}@${ip}" 'journalctl -k -f -n 0 -o short-iso' >>"$script_dir/monitoring/runtime/logs/kernel-$ip.log" 2>&1 &
      printf '%s\n' "$!" >>"$script_dir/monitoring/runtime/ssh-pids"
      ssh "${ssh_opts[@]}" "${ssh_user}@${ip}" 'while :; do date -Is; docker inspect --format "{{json .State}}" camu; sleep 5; done' >>"$script_dir/monitoring/runtime/logs/docker-state-$ip.log" 2>&1 &
      printf '%s\n' "$!" >>"$script_dir/monitoring/runtime/ssh-pids"
    done
    "${compose[@]}" up -d
    echo "Grafana: http://localhost:3000 (admin/admin; metrics and logs from all nodes)" >&2
    ;;
  stop)
    if [[ -f "$script_dir/monitoring/runtime/ssh-pids" ]]; then
      while IFS= read -r pid; do kill "$pid" >/dev/null 2>&1 || true; done <"$script_dir/monitoring/runtime/ssh-pids"
    fi
    "${compose[@]}" down -v
    rm -rf "$script_dir/monitoring/runtime"
    ;;
  *)
    echo "usage: $0 {start|stop}" >&2
    exit 2
    ;;
esac
