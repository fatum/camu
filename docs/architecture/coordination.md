# Camu Coordination

Camu uses object storage as its coordination backend. There is no separate consensus cluster — all distributed state is managed through S3 conditional writes and leases.

## Coordination Architecture

```
  +-------------+     +-------------+     +-------------+
  |   Node 1    |     |   Node 2    |     |   Node 3    |
  | (controller)|     | (follower)  |     | (follower)  |
  +------+------+     +------+------+     +------+------+
         |                   |                   |
         |    heartbeat      |    heartbeat      |    heartbeat
         |    lease renew    |                   |
         v                   v                   v
  +------------------------------------------------------+
  |                  Object Storage (S3)                  |
  |                                                      |
  |  _coordination/                                      |
  |    leader.json            Controller lease            |
  |    instances/{id}.json    Instance heartbeats         |
  |    assignments/{t}.json   Partition assignments       |
  |    isr/{t}/{p}.json       ISR membership + HW         |
  |    epochs/{t}/{p}.json    Epoch history               |
  |    kafka-groups/{g}.json  Consumer group state        |
  |    groups/                HTTP consumer group offsets  |
  |    consumers/             Per-consumer offsets         |
  +------------------------------------------------------+
```

## Coordination Objects

| Object | Purpose | Write pattern |
|--------|---------|---------------|
| `_coordination/leader.json` | Cluster controller lease with monotonic `lease_epoch` | Conditional write with TTL; renew is a CAS bump of `lease_epoch` |
| `_coordination/assignments/{topic}.json` | Partition leader and replica mapping | Controller-only CAS writes |
| `_coordination/isr/{topic}/{partition}.json` | ISR membership and high watermark | Leader-only conditional writes, guarded by the leader epoch |
| `_coordination/instances/{instanceID}.json` | Instance heartbeat and advertised addresses | Self-registration with TTL |
| `_coordination/epochs/{topic}/{partition}.json` | Epoch history for divergence detection | Controller appends on reassignment |
| `_coordination/kafka-groups/{group}.json` | Kafka consumer group coordination | CAS with ETag-based versioning |

## Roles

### Cluster Controller

One node holds the controller lease and is responsible for:

- Publishing partition assignments with epoch bumps
- Driving cluster-wide topic creation and enqueuing topic deletion
- Acting as the Kafka group coordinator broker
- Garbage-collecting stale ISR entries, expired instances, and pending topic deletions
- Stamping consumer group state with controller epoch for fencing

The controller lease uses S3 conditional writes with a TTL. If the controller fails to renew, another node acquires the lease after expiry.

Topic deletion is resumable. The controller-side GC loop processes
`_coordination/topic_deletions/{topic}.json` markers by deleting topic S3 data
first, then clearing any diskless metastore state, and finally removing the
marker. Controller-only cleanup is now grouped behind a dedicated
coordination-leader service layer rather than being mixed into partition-local
maintenance paths.

### Partition Leader

Each partition leader is responsible for:

- Accepting writes and encoding them as RecordBatch bytes
- Serving replication traffic to followers over h2c
- Tracking ISR follower progress and advancing the high watermark
- Scheduling background segment sealing and object-store publication
- Persisting idempotent producer checkpoints during flush
- Executing retention through durable partition jobs
- Fencing itself if the local epoch falls behind the assignment epoch
- For `rf=1` topics, re-verifying ownership against the assignment store on an
  amortized `coordination.fence_interval` cadence before acknowledging, so a
  fenced owner stops acking instead of losing acknowledged writes

Retention jobs are expected to survive both restart and reassignment. If an
owner loses leadership mid-job, the stale owner stops. The new owner
re-discovers the same durable work and resumes it under the new leader epoch.
This partition-local maintenance path now runs behind a dedicated
partition-leader service layer.

Segment publication is not part of the produce acknowledgement path. A sealed
segment is retained as local pending work until its immutable objects and
metadata are published; retries use that same segment, and follower fetches can
read it while publication is pending.

### Partition Follower

Follower-specific behavior is now grouped behind a dedicated service layer as
well. That service owns:

- resolving and proxying to the current partition leader
- reconfiguring follower fetch loops after controller pushes
- self-promotion as the **primary** failover path: on leader-down detection a
  caught-up ISR member CAS-promotes itself on the assignment store without a
  controller round trip; the controller is the backstop for cases
  self-promotion cannot resolve and reconciles on its periodic publish

## Failover Sequence

```
  Time ──────────────────────────────────────────────────>

  Old Leader    [write write write X─── dead ──────────]
  Follower A    [fetch fetch fetch ... detect ... promote]
  Follower B    [fetch fetch fetch ... detect ... follow A]
  Controller    [─── heartbeat miss ── reassign(epoch++) ─]
  Object Store  [───────────── new assignment published ──]
```

Leader failover works through:

1. **Detection**: Followers detect a hard leader death quickly (connection errors on the fetch loop) and a paused or unresponsive leader once the fetch read deadline (`coordination.replication_read_timeout`, default `10s`) elapses across consecutive fetch attempts. The controller also detects via missed heartbeats or lease expiry.
2. **Reassignment**: The controller publishes a new assignment with a higher epoch (or a follower self-promotes with a CAS bump).
3. **Index refresh**: The promoted leader refreshes its segment index from object storage and continues at `max(local log end, index next offset)`, so an empty local tail still serves committed S3 records.
4. **Local recovery**: The promoted leader recovers its active segment, truncating any divergent tail using epoch history. Promotions persist the new leader epoch to the local `epoch` sidecar, so a later demotion reports the correct epoch of the active tail.
5. **Resume**: Replication and reads resume under the new leader. The high watermark advances once the ISR re-forms.

Epoch history is used to fence stale leaders and to instruct followers to truncate divergent tails when required. The local `epoch` sidecar keeps the follower's reported epoch accurate across demotions; without it a stale epoch made the divergence check fence a demoted leader that actually held committed tail data.

## Consumer Groups

Kafka consumer-group coordination is controller-centric:

- `FindCoordinator` returns the current cluster controller
- Group state (members, assignments, generation) is persisted in object storage as JSON
- All mutations use CAS (Compare-And-Swap) with ETag-based versioning
- Controller epoch stamping fences stale writers during controller failover
- Heartbeat persistence is batched to reduce S3 round-trips (in-memory updates between persists)
- Session timeout expiry removes stale members and triggers generation bumps

## Consistency Model

The system relies on:

- **Single active controller** through a lease with conditional-write acquisition
- **Monotonic lease epochs** as the controller fencing token: renew bumps a
  counter via CAS, so a skewed-clock stale leader is fenced by epoch comparison
  rather than wall-clock expiry alone
- **Conditional writes** for ownership changes where races matter (assignments, ISR, group state); ISR mutations are additionally guarded by the leader epoch
- **Epoch fencing** to reject stale leaders and detect divergence
- **Immutable sealed segments** for shared log history (once written, never modified)
- **Local active segments** for the mutable tail (single-writer per partition)

This keeps the design simple and eliminates the need for a separate consensus cluster, but failover and stale-read timing still depend on object-store round trips and lease intervals.
