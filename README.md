# Cloud Resource Scheduler

A small cloud resource scheduler written in Go.

The project models the core control-plane problem behind systems such as container/orchestration schedulers:

> Given a queue of workloads and a set of resource-bearing nodes, determine where each workload should run while respecting hard constraints and optimizing placement.

This is intentionally **not** a Kubernetes clone. It is a compact scheduler implementation designed to make the important distributed-systems and infrastructure tradeoffs visible.

## Architecture

```text
                 +----------------------+
                 |      HTTP API        |
                 | jobs / nodes / events|
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 |    Scheduler Engine  |
                 |                      |
                 | 1. order by priority |
                 | 2. filter nodes      |
                 | 3. score candidates  |
                 | 4. bind + lease      |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Thread-safe Store    |
                 | nodes / jobs / events|
                 +----------------------+
```

The scheduler follows the same high-level **filter then score** shape used by Kubernetes: first remove infeasible nodes, then score the remaining candidates and bind the workload. See the Kubernetes scheduler documentation for the corresponding production architecture. 

## What it implements

### Resource accounting
Each node exposes:

- CPU in millicores
- memory in MB
- disk in GB

A node can accept a job only when all requested resources fit inside its currently available capacity.

### Hard constraints

Jobs can require:

- a specific region
- a specific availability zone
- arbitrary node labels

Example:

```json
{
  "required_labels": {
    "gpu": "true"
  }
}
```

This mirrors the general idea of scheduler constraints such as node selectors and affinity. 

### Priority queueing

Jobs are ordered by:

1. higher priority first
2. older creation time as the tie breaker

This gives the scheduler deterministic behavior while making room for future priority/preemption work.

### Scoring

Among feasible nodes, the scheduler prefers tighter fits. This is a simple bin-packing heuristic intended to reduce resource fragmentation.

The implementation also gives a small preference to explicitly requested regions/zones.

### Leases

A successful assignment receives a lease.

If a worker does not renew the lease before it expires, the scheduler releases the resources and retries the job until `max_attempts` is reached.

This models the failure-detection problem that real schedulers face when workers disappear after a placement decision.

### Event log

State transitions append monotonically increasing events:

- `job_submitted`
- `job_assigned`
- `job_released`
- `job_cancelled`

The API exposes events by sequence number, providing a simple basis for rebuilding state or feeding an external event stream later.

### Concurrency

The store is protected by a `sync.RWMutex`. Assignment checks capacity and mutates allocation while holding the same write lock, preventing two concurrent scheduling decisions from oversubscribing a node.

## API

### Health

```bash
curl localhost:8080/healthz
```

### Register a node

```bash
curl -X POST localhost:8080/v1/nodes \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "node-a",
    "region": "us-west-2",
    "zone": "us-west-2a",
    "labels": {"gpu": "false"},
    "capacity": {
      "cpu_millis": 8000,
      "memory_mb": 16384,
      "disk_gb": 100
    }
  }'
```

### Submit a job

```bash
curl -X POST localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "job-1",
    "name": "payments-api",
    "priority": 50,
    "resources": {
      "cpu_millis": 1000,
      "memory_mb": 1024,
      "disk_gb": 10
    },
    "max_attempts": 3
  }'
```

The background scheduler normally places it within the next scheduling tick.

For deterministic demos/tests:

```bash
curl -X POST localhost:8080/v1/scheduler/run
```

### GPU-constrained job

```bash
curl -X POST localhost:8080/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "gpu-job",
    "priority": 100,
    "resources": {
      "cpu_millis": 2000,
      "memory_mb": 4096,
      "disk_gb": 20
    },
    "constraints": {
      "required_labels": {
        "gpu": "true"
      }
    }
  }'
```

### Inspect state

```bash
curl localhost:8080/v1/nodes
curl localhost:8080/v1/jobs
curl localhost:8080/v1/jobs/job-1
curl localhost:8080/v1/events
```

## Run

Requires Go 1.23+.

```bash
go test ./...
go vet ./...
go run ./cmd/scheduler
```

Then:

```bash
curl localhost:8080/healthz
```

## Design decisions

### Why an in-memory store?

The purpose of this repository is the scheduler itself. An external database would add operational complexity without improving the core scheduling algorithm.

The store is deliberately isolated behind a small API so it can later be replaced by PostgreSQL, etcd, or another durable state system.

### Why leases instead of permanent assignments?

A scheduler cannot assume a worker will remain alive after receiving work. A lease gives the control plane a bounded amount of time before it can reclaim resources.

The tradeoff is that lease expiry can produce duplicate work if a worker is merely slow rather than dead. A production system would therefore need fencing tokens or another mechanism to prevent an old worker from continuing after its lease expires.

### Why filter before score?

Scoring every node without first checking hard constraints wastes work and can accidentally turn policy constraints into preferences.

The scheduler therefore performs:

```text
candidate nodes
      |
      v
hard constraint filter
      |
      v
resource capacity filter
      |
      v
scoring
      |
      v
best feasible node
```

This is also conceptually aligned with the filtering/scoring stages in Kubernetes scheduling.

## Failure cases

The implementation intentionally exercises several useful failure modes:

- duplicate node registration
- duplicate job submission
- insufficient capacity
- label/region/zone mismatch
- concurrent assignment races
- worker lease expiration
- retry exhaustion
- cancellation of running work
- node heartbeat recovery

## Metrics

`GET /metrics` exposes Prometheus-compatible basic gauges:

```text
scheduler_pending_jobs
scheduler_running_jobs
scheduler_failed_jobs
scheduler_cpu_capacity_millis
scheduler_cpu_allocated_millis
```

## Scope

This repository is intentionally educational. It does not claim to provide production-grade orchestration, isolation, authentication, durable consensus, or workload execution.

Its value is in making the scheduling control loop concrete:

**observe -> filter -> score -> bind -> lease -> detect failure -> retry**

