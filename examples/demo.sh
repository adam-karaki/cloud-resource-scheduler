#!/usr/bin/env bash
set -euo pipefail

BASE="http://localhost:8080"

curl -sS -X POST "$BASE/v1/nodes" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"node-a",
    "region":"us-west-2",
    "zone":"us-west-2a",
    "labels":{"gpu":"false"},
    "capacity":{"cpu_millis":8000,"memory_mb":16384,"disk_gb":100}
  }' | jq .

curl -sS -X POST "$BASE/v1/nodes" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"node-b",
    "region":"us-west-2",
    "zone":"us-west-2b",
    "labels":{"gpu":"true"},
    "capacity":{"cpu_millis":4000,"memory_mb":8192,"disk_gb":100}
  }' | jq .

curl -sS -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"job-1",
    "name":"api",
    "priority":10,
    "resources":{"cpu_millis":1000,"memory_mb":1024,"disk_gb":10}
  }' | jq .

curl -sS -X POST "$BASE/v1/jobs" \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"gpu-job",
    "name":"inference",
    "priority":100,
    "resources":{"cpu_millis":2000,"memory_mb":2048,"disk_gb":10},
    "constraints":{"required_labels":{"gpu":"true"}}
  }' | jq .

sleep 1

echo "Jobs:"
curl -sS "$BASE/v1/jobs" | jq .

echo "Nodes:"
curl -sS "$BASE/v1/nodes" | jq .
