#!/bin/bash
# Smoke test for the Elle queue workload against Camu
lein run -m jepsen.camu.queue test \
  --nodes-file ~/nodes \
  --username root \
  --acks all \
  --idempotence \
  --sub-via assign \
  --nemesis none \
  --time-limit 60 \
  --rate 10
