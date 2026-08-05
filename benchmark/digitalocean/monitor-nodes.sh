#!/usr/bin/env bash
set -euo pipefail

# monitor-nodes.sh samples host and camu-container CPU/memory on every
# benchmark node directly over SSH and flags anomalies. It does not run the
# local monitoring stack (Prometheus/Grafana/Loki) — each sample is collected
# with one SSH round-trip per node.

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ssh_user="${SSH_USER:-root}"
ssh_opts=(-o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new)
samples="${SAMPLES:-6}"
interval="${INTERVAL:-5}"
watch_interval="${WATCH_INTERVAL:-30}"

host_cpu_avg_threshold="${HOST_CPU_AVG_THRESHOLD:-85}"
host_cpu_max_threshold="${HOST_CPU_MAX_THRESHOLD:-95}"
host_mem_threshold="${HOST_MEM_THRESHOLD:-90}"
host_mem_growth_threshold="${HOST_MEM_GROWTH_THRESHOLD:-15}"
container_cpu_avg_threshold="${CONTAINER_CPU_AVG_THRESHOLD:-95}"
container_cpu_max_threshold="${CONTAINER_CPU_MAX_THRESHOLD:-99}"
container_mem_threshold="${CONTAINER_MEM_THRESHOLD:-90}"

node_ips() {
  command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
  local ips_json ips=()
  ips_json="$(terraform -chdir="$script_dir" output -json public_ips)"
  while IFS= read -r ip; do
    ips[${#ips[@]}]="$ip"
  done < <(printf '%s' "$ips_json" | jq -r 'to_entries | sort_by(.key) | .[].value')
  printf '%s\n' "${ips[@]}"
}

# remote_sampler collects a series of host + container samples in one SSH
# invocation and prints a SUMMARY line plus one ANOMALY line per rule fired.
remote_sampler() {
  cat <<'REMOTE'
#!/usr/bin/env bash
set -u
samples="${1:-6}"
interval="${2:-5}"
cores="$(nproc 2>/dev/null || echo 1)"
since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
proc_stat="${PROC_STAT:-/proc/stat}"
proc_meminfo="${PROC_MEMINFO:-/proc/meminfo}"
HOST_CPU_AVG_THRESHOLD="${HOST_CPU_AVG_THRESHOLD:-85}"
HOST_CPU_MAX_THRESHOLD="${HOST_CPU_MAX_THRESHOLD:-95}"
HOST_MEM_THRESHOLD="${HOST_MEM_THRESHOLD:-90}"
HOST_MEM_GROWTH_THRESHOLD="${HOST_MEM_GROWTH_THRESHOLD:-15}"
CONTAINER_CPU_AVG_THRESHOLD="${CONTAINER_CPU_AVG_THRESHOLD:-95}"
CONTAINER_CPU_MAX_THRESHOLD="${CONTAINER_CPU_MAX_THRESHOLD:-99}"
CONTAINER_MEM_THRESHOLD="${CONTAINER_MEM_THRESHOLD:-90}"

host_cpu_max=0; host_mem_max=0; cont_cpu_max=0; cont_mem_max=0
hc_sum=0; cc_sum=0; n=0
first_host_mem=""; last_host_mem=""
state="unknown"

for ((i=0; i<samples; i++)); do
  prev="$(awk '/^cpu /{print $2,$3,$4,$5,$6,$7,$8,$9,$10; exit}' "$proc_stat")"
  sleep "$interval"
  cur="$(awk '/^cpu /{print $2,$3,$4,$5,$6,$7,$8,$9,$10; exit}' "$proc_stat")"
  hcpu="$(awk -v a="$prev" -v b="$cur" 'function sum(l,t){split(l,x);for(i=1;i<=9;i++)t+=x[i];return t}BEGIN{split(a,x);split(b,y);i1=x[4]+x[5];i2=y[4]+y[5];t1=sum(a);t2=sum(b);dt=t2-t1;di=i2-i1;printf "%.1f",(dt>di?100*(dt-di)/dt:0)}')"
  read -r mt ma < <(awk '/^MemTotal:|^MemAvailable:/{printf "%s ", $2}' "$proc_meminfo")
  hmem="$(awk -v t="$mt" -v a="$ma" 'BEGIN{if(t>0)printf "%.1f",100*(t-a)/t; else printf "0"}')"
  cstats="$(docker stats --no-stream --format '{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}' camu 2>/dev/null || true)"
  ccp=0; cmp=0
  if [[ -n "$cstats" ]]; then
    IFS='|' read -r ccp cusage cmp <<< "$cstats"
    ccp="${ccp%\%}"; cmp="${cmp%\%}"
  fi
  state="$(docker inspect --format '{{.State.Status}}|{{.State.Restarting}}|{{.State.OOMKilled}}' camu 2>/dev/null || echo "ERR")"
  n=$((n+1))
  hc_sum="$(awk -v s="$hc_sum" -v v="$hcpu" 'BEGIN{printf "%.1f", s+v}')"
  cc_sum="$(awk -v s="$cc_sum" -v v="$ccp" 'BEGIN{printf "%.1f", s+v}')"
  host_cpu_max="$(awk -v m="$host_cpu_max" -v v="$hcpu" 'BEGIN{if(v>m)print v; else print m}')"
  host_mem_max="$(awk -v m="$host_mem_max" -v v="$hmem" 'BEGIN{if(v>m)print v; else print m}')"
  cont_cpu_max="$(awk -v m="$cont_cpu_max" -v v="$ccp" 'BEGIN{if(v>m)print v; else print m}')"
  cont_mem_max="$(awk -v m="$cont_mem_max" -v v="$cmp" 'BEGIN{if(v>m)print v; else print m}')"
  [[ -z "$first_host_mem" ]] && first_host_mem="$hmem"
  last_host_mem="$hmem"
done

oom="$(journalctl -k --since "$since" --no-pager 2>/dev/null | grep -ciE 'out of memory|oom-killer|killed process' || true)"
host_cpu_avg="$(awk -v s="$hc_sum" -v n="$n" 'BEGIN{printf "%.1f", (n>0?s/n:0)}')"
cont_cpu_avg="$(awk -v s="$cc_sum" -v n="$n" 'BEGIN{printf "%.1f", (n>0?s/n:0)}')"
mem_growth="$(awk -v f="$first_host_mem" -v l="$last_host_mem" 'BEGIN{printf "%.1f", l-f}')"

run_state="$(awk -F'|' '{print $1}' <<< "$state")"
restarting="$(awk -F'|' '{print $2}' <<< "$state")"
oomkilled="$(awk -F'|' '{print $3}' <<< "$state")"

echo "SUMMARY|$(hostname)|$cores|$host_cpu_avg|$host_cpu_max|$host_mem_max|$mem_growth|$cont_cpu_avg|$cont_cpu_max|$cont_mem_max|$oom|$run_state|$restarting|$oomkilled"

awk -v avg="$host_cpu_avg" -v max="$host_cpu_max" -v ta="$HOST_CPU_AVG_THRESHOLD" -v tm="$HOST_CPU_MAX_THRESHOLD" 'BEGIN{if(avg>ta||max>tm) print "ANOMALY|host_cpu|avg="avg"% max="max"%"}'
awk -v max="$host_mem_max" -v t="$HOST_MEM_THRESHOLD" 'BEGIN{if(max>t) print "ANOMALY|host_mem|max="max"%"}'
awk -v g="$mem_growth" -v t="$HOST_MEM_GROWTH_THRESHOLD" 'BEGIN{if(g>t) print "ANOMALY|host_mem_growth|+"g" points over window"}'
awk -v avg="$cont_cpu_avg" -v max="$cont_cpu_max" -v ta="$CONTAINER_CPU_AVG_THRESHOLD" -v tm="$CONTAINER_CPU_MAX_THRESHOLD" 'BEGIN{if(avg>ta||max>tm) print "ANOMALY|container_cpu|avg="avg"% max="max"%"}'
awk -v max="$cont_mem_max" -v t="$CONTAINER_MEM_THRESHOLD" 'BEGIN{if(max>t) print "ANOMALY|container_mem|max="max"%"}'
if [[ "$oom" -gt 0 ]]; then echo "ANOMALY|oom|${oom} kernel OOM event(s) in window"; fi
if [[ "$run_state" != "running" || "$restarting" != "0" || "$oomkilled" == "true" ]]; then echo "ANOMALY|container_state|state=$run_state restarting=$restarting oomkilled=$oomkilled"; fi
REMOTE
}

run_check() {
  local ips=() ip output anomalies any=0
  mapfile -t ips < <(node_ips)
  printf '%-16s %5s %14s %14s %16s %14s %9s %8s\n' \
    node cores host_cpu_avg/max host_mem_max growth cont_cpu_avg/max cont_mem_max state
  for ip in "${ips[@]}"; do
    output="$(ssh "${ssh_opts[@]}" "${ssh_user}@${ip}" "HOST_CPU_AVG_THRESHOLD=$host_cpu_avg_threshold HOST_CPU_MAX_THRESHOLD=$host_cpu_max_threshold HOST_MEM_THRESHOLD=$host_mem_threshold HOST_MEM_GROWTH_THRESHOLD=$host_mem_growth_threshold CONTAINER_CPU_AVG_THRESHOLD=$container_cpu_avg_threshold CONTAINER_CPU_MAX_THRESHOLD=$container_cpu_max_threshold CONTAINER_MEM_THRESHOLD=$container_mem_threshold bash -s" "$samples" "$interval" <<<"$(remote_sampler)" 2>/dev/null || true)"
    if [[ -z "$output" ]]; then
      printf '%-16s %5s %s\n' "$ip" "-" "UNREACHABLE"
      any=1
      continue
    fi
    summary="$(printf '%s\n' "$output" | grep '^SUMMARY|' | head -1)"
    anomalies="$(printf '%s\n' "$output" | grep '^ANOMALY|' || true)"
    if [[ -n "$anomalies" ]]; then any=1; fi
    IFS='|' read -r _ host cores hcavg hcmax hmmax growth ccavg ccmax cmmax oom run_state restarting oomkilled <<< "$summary"
    printf '%-16s %5s %8s/%-5s %8s%% %6s%% %8s/%-5s %8s%% %8s\n' \
      "$ip" "$cores" "$hcavg" "$hcmax" "$hmmax" "$growth" "$ccavg" "$ccmax" "$cmmax" "$run_state"
    if [[ -n "$anomalies" ]]; then
      while IFS= read -r line; do
        IFS='|' read -r _ kind detail <<< "$line"
        printf '  ! %-18s %s\n' "$kind" "$detail"
      done <<< "$anomalies"
    fi
  done
  return "$any"
}

case "${1:-check}" in
  check)
    run_check
    ;;
  watch)
    trap 'exit 0' INT TERM
    while :; do
      echo "== $(date -u +%Y-%m-%dT%H:%M:%SZ) =="
      run_check || true
      echo
      sleep "$watch_interval"
    done
    ;;
  *)
    echo "usage: $0 {check|watch} [samples] [interval]" >&2
    exit 2
    ;;
esac
