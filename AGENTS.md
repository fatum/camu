# Camu Engineering Instructions

## Design and compatibility

- Do not add backwards-compatibility shims, legacy fallbacks, migrations, or
  deprecated code paths unless the task explicitly requires them.
- Prefer stable public APIs. Any intentional API change must be small,
  documented, and covered by tests.
- Keep code simple, modular, and clean. Favor focused packages and explicit
  ownership boundaries over broad abstractions or speculative extensibility.

## Verification

- Verify all core functionality with integration tests, not only unit tests.
- For major implementation steps that affect distributed behavior, durability,
  replication, consistency, failure recovery, or availability, run the
  relevant Jepsen tests before considering the work complete.
- Jepsen test correctness is crucial: validate that a test exercises the
  intended fault and assertion, and fix an invalid test rather than treating a
  passing but weak test as evidence of correctness.
