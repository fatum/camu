# Camu Engineering Instructions

## Design and compatibility

- Do not add backwards-compatibility shims, legacy fallbacks, migrations, or
  deprecated code paths unless the task explicitly requires them.
- Prefer stable public APIs. Any intentional API change must be small,
  documented, and covered by tests.
- Keep code simple, modular, and clean. Favor focused packages and explicit
  ownership boundaries over broad abstractions or speculative extensibility.
- When multiple designs satisfy the correctness and resiliency requirements
  below, choose the simplest one. Complexity is only justified when it removes
  a concrete failure mode or is required by a test.

## Job idempotency (crucial)

- **All jobs must be idempotent.** Every job type (retention, segment merge,
  parquet export/compaction, ...) must be safe to re-execute any number of
  times: retries, crash recovery, leader changes, and phase replays must never
  duplicate side effects, corrupt state, or double-publish artifacts.
- Job IDs must be deterministic so re-submitting the same work is an upsert,
  not a duplicate.
- Every external effect (object-store writes, segment-ref replacement,
  deletions) must tolerate being repeated, and re-execution must converge to
  the same end state — the `ReplaceSegmentRefs` CAS pattern (retry skips
  already-removed refs, skips already-present added refs) is the model.
- Jobs are phase-based and resumable: a crash at any point leaves the job
  either completable by a later tick or cleanly stale and reclaimed. Never
  write in-memory state that a job acts on before it is persisted.

## Resiliency

- The system must recover from node crashes, leader moves, and object-store
  outages without manual intervention: stale jobs are reclaimed, orphaned
  artifacts are swept, and in-flight work resumes or is safely discarded.
- All decisions that must survive a crash are persisted before they are acted
  upon; never act on state that exists only in memory.
- Failure of one job or one partition must not stall the whole system:
  bounded concurrency, per-(topic, partition) isolation, and no unbounded
  serial work chains.

## Correctness

- Preserve the data invariants under every failure interleaving: dense offset
  space, contiguous refs, atomic ref replacement, publish-before-delete.
- A job that cannot prove it is safe to proceed must not proceed — owner/epoch
  checks, grace periods, and CAS retries exist for exactly this reason; do not
  weaken them for throughput.
- Correctness claims are validated by Jepsen tests for distributed behavior
  (durability, replication, consistency, failure recovery, availability) and
  by integration tests otherwise — never by reasoning alone.

## Verification

- Verify all core functionality with integration tests, not only unit tests.
- For major implementation steps that affect distributed behavior, durability,
  replication, consistency, failure recovery, or availability, run the
  relevant Jepsen tests before considering the work complete.
- Jepsen test correctness is crucial: validate that a test exercises the
  intended fault and assertion, and fix an invalid test rather than treating a
  passing but weak test as evidence of correctness.
