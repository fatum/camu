# Partition Maintenance Refactor

This document is the implementation checklist for moving partition-local
maintenance away from the coordination leader while keeping topic deletion
controller-led and restart-safe.

## Target Split

- `CoordinationLeader`
  - leader election
  - assignment planning and publication
  - controller checkpointing
  - topic deletion orchestration and execution
  - cluster-wide admin validation
- `PartitionLeader`
  - partition-local maintenance execution
  - retention
  - future sealed-segment merge
  - future compaction
  - future parquet upload
- `PartitionFollower`
  - replication and catch-up only

## New Models

### `PartitionIdentity`

Shared role resolution model for one topic-partition.

- `topic`
- `partition`
- `role`
- `leader`
- `leader_epoch`
- `replicas`
- `storage_mode`

### `PartitionJob`

Durable partition-leader-executed maintenance record stored under:

- `_coordination/partition_jobs/{topic}/{partition}/{job-id}.json`

Current first job type:

- `retention`

Groundwork also exists for:

- `segment_merge`

## Current Phase Status

### Phase 1

- [x] Add `PartitionIdentity` role abstraction
- [x] Add generic `PartitionJob` model and storage
- [x] Add partition-leader maintenance loop
- [x] Move classic retention off controller GC into partition-leader jobs
- [x] Keep topic deletion controller-led
- [x] Add classic retention local cache/index invalidation
- [x] Move diskless retention off controller GC into partition-leader jobs

### Phase 2

- [x] Add `segment_merge` as a `PartitionJob` type
- [x] Land the classic merge artifact builder
- [x] Add explicit classic sealed-segment merge job execution
- [x] Reuse `PartitionJob` for conservative automatic classic sealed-segment merge discovery/execution
- [ ] Add `compaction` job type
- [ ] Add `parquet_upload` job type

### Phase 3

- [x] Introduce thin `PartitionLeader` and `CoordinationLeader` service layers around existing maintenance / GC paths
- [x] Introduce a thin `PartitionFollower` service layer around follower failover / proxying paths
- [ ] Reduce controller GC to cluster-global cleanup only
- [x] Add bounded partition-maintenance concurrency
- [ ] Add maintenance metrics
- [x] Add reassignment-resume coverage for owner jobs

## Invariants

- Topic deletion must not depend on active partition runtime.
- Partition-local maintenance must be fenced by owner and leader epoch.
- Partition-local maintenance must resume after reassignment by rediscovering
  the same durable job under the new owner/epoch.
- Diskless delete ordering stays: delete S3 data first, then metastore refs.
- Classic retention must evict local sealed-segment cache/index state when it
  deletes retained data.

## Current File Mapping

- [`internal/server/partition_roles.go`](/Users/maksim/Projects/camu/internal/server/partition_roles.go)
  - `PartitionIdentity`
  - leader/follower role resolution
- [`internal/server/partition_jobs.go`](/Users/maksim/Projects/camu/internal/server/partition_jobs.go)
  - durable `PartitionJob` model
- [`internal/server/partition_job_executor.go`](/Users/maksim/Projects/camu/internal/server/partition_job_executor.go)
  - compatibility wrappers delegating to the partition-leader service
- [`internal/server/partition_leader_service.go`](/Users/maksim/Projects/camu/internal/server/partition_leader_service.go)
  - partition-leader maintenance service for retention and merge jobs
- [`internal/server/partition_job_retention.go`](/Users/maksim/Projects/camu/internal/server/partition_job_retention.go)
  - classic and diskless retention discovery and execution
- [`internal/server/partition_job_merge.go`](/Users/maksim/Projects/camu/internal/server/partition_job_merge.go)
  - classic sealed-segment merge artifact builder, auto-discovery, and execution
- [`internal/server/coordination_leader_service.go`](/Users/maksim/Projects/camu/internal/server/coordination_leader_service.go)
  - controller-only GC service wrapper
- [`internal/server/partition_follower_service.go`](/Users/maksim/Projects/camu/internal/server/partition_follower_service.go)
  - follower-side proxying, fetch reconfiguration, and failover self-promotion service
- [`internal/server/topic_deletion.go`](/Users/maksim/Projects/camu/internal/server/topic_deletion.go)
  - controller-led topic deletion

## Immediate Next Work

1. Add maintenance metrics.
2. Add `compaction` and `parquet_upload` job types on the same model.
3. Broaden classic merge policy beyond the current conservative adjacent-pair discovery rule if needed.
