# Multiple Indexes Design Note

This is a design note for a possible next step beyond the current `index.json` model.

## Current State

Today Camu already has:

- immutable `.segment` files
- immutable `.offset.idx` sidecars
- immutable `.meta.json` sidecars
- segment readers that use sparse sidecar indexes for in-segment seeks

What it does **not** have yet is a manifest-chain replacement for the partition-level `index.json`. The live partition index is still a single CAS-updated JSON object in S3.

## Goal Of This Note

The proposal here is aimed at reducing the cost and contention of rewriting a monolithic partition index on every flush.

The intended benefits are:

- smaller mutable coordination state
- fewer CAS conflicts on hot partitions
- immutable append-style metadata for segment membership
- cheap bootstrap through manifest checkpoints

## Proposed Layout

```text
{bucket}/
└── {topic}/{partition}/
    ├── manifests/
    │   ├── 00000000000000000001.json
    │   ├── 00000000000000000002.json
    │   └── head.json
    ├── checkpoints/
    │   └── latest.json
    └── segments/
        ├── 00000000000000012345-7.segment
        ├── 00000000000000012345-7.offset.idx
        ├── 00000000000000012345-7.meta.json
        └── ...
```

## Why It Fits Camu

This design keeps the properties Camu already leans on:

- immutable segment assets
- sparse byte-position indexes
- object-storage-friendly writes
- batch-level compression rather than whole-segment compression

It would move the partition-level mutable pointer to a tiny head object and keep heavier membership metadata immutable.

## Still Missing

This proposal is not implemented yet:

- no manifest-chain reader or writer
- no `head.json` pointer management
- no checkpoint compaction flow
- no mmap-backed index loading

So this file should be read as a future-direction sketch, not as current architecture documentation.

## Current Flush Model

The code today still does:

1. build segment and sidecars
2. upload them
3. CAS-update `{topic}/{partition}/index.json`

That is the behavior described in [architecture.md](architecture.md). This note is only about a possible evolution of step 3.
