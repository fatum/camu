# Multiple Indexes Sketch

This note sketches a Kafka-like indexing model for Camu: a small partition-level manifest to locate segments, plus immutable per-segment indexes for fast seeks inside each segment. It assumes the segment format is seekable and does not use whole-segment compression.

## Goals

- Stop rewriting a monolithic `index.json` on every flush.
- Reduce CAS conflicts on hot partitions.
- Keep partition lookup cheap: "which segment covers offset N?"
- Make in-segment reads cheap: "where inside the segment does offset N start?"
- Keep the S3/object layout immutable wherever possible.

## Proposed object layout

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
        ├── 00000000000000013000-7.segment
        ├── 00000000000000013000-7.offset.idx
        └── 00000000000000013000-7.meta.json
```

Files:

- `.segment`: immutable segment payload.
- `.offset.idx`: immutable sparse offset-to-position index for that segment.
- `.meta.json`: immutable segment metadata and integrity information.
- `manifests/{generation}.json`: immutable append-step for partition segment membership.
- `manifests/head.json`: tiny CAS-updated pointer to the latest manifest generation.
- `checkpoints/latest.json`: optional compacted view of the manifest chain.

## Per-segment files

### Segment payload

Keep the data object immutable:

```text
{baseOffset}-{epoch}.segment
```

This holds the actual records or record batches. The segment format should remain directly seekable by byte position, which means no whole-segment compression. If compression is used, it should be applied at the record-batch level so the index can seek to a batch boundary and decode only from there.

Current implementation direction:

- segment header stores the compression mode
- the payload is a sequence of independently readable batches
- each batch is encoded as:

```text
[4B batch_len][4B message_count][batch_payload]
```

- `batch_payload` is the concatenated message frames for that batch
- compression is applied once per batch, never to the whole segment

This makes the natural seek unit a batch boundary, which matches the offset-index design. The batch target size is an internal storage-format setting, not a producer-side networking batch setting.

Implemented so far on this branch:

- segments are written as batch-framed payloads
- compression is batch-level only
- `.offset.idx` sidecars are generated and uploaded beside each segment
- immutable `.meta.json` sidecars are generated and uploaded beside each segment
- `SegmentRef` now carries explicit `offset_index_key` and `meta_key` fields
- readers use the sidecar index to seek to the nearest batch boundary before scanning
- retention and GC treat segment sidecars as part of the same asset set

Still missing:

- manifest/head replacement for `index.json`
- mmap-backed index loading

### Offset index

Kafka's key idea is:

- the partition first picks the right segment by base offset
- then a per-segment sparse index maps relative offsets to byte positions in that segment

Suggested file:

```text
{baseOffset}-{epoch}.offset.idx
```

Kafka-style entry format:

```text
[4B relative_offset][4B position]
```

Where:

- `relative_offset = record_offset - segment_base_offset`
- `position` is the byte offset inside the `.segment` file

This keeps each index entry at 8 bytes total, which is small enough to memory-map efficiently and binary-search directly. In practice, entries should point at record-batch starts, not arbitrary individual message positions.

The index should be sparse, not per-record. For example:

- every 64 KB of segment bytes
- or every 128 messages
- whichever comes first

Read algorithm:

1. Memory-map the segment's `.offset.idx` when it is present in local cache.
2. Binary-search for the greatest indexed offset `<= requested_offset`.
3. Seek the segment to that indexed byte position.
4. Scan forward until the requested offset is reached.

In practice for Camu, step 4 means:

- read the batch header at that position
- decompress that single batch if compression is enabled
- scan message frames inside that batch
- continue to the next batch only if the requested read spans further

That is the Kafka model in simplified form.

Constraints:

- `relative_offset` must fit in `uint32`, so a segment cannot span more than `2^32-1` offsets.
- `position` must fit in `uint32`, so an individual segment file must stay below 4 GiB.
- The indexed `position` must point to a readable batch boundary. Whole-segment compression is incompatible with this model and should not be supported.

### Segment metadata

Suggested file:

```json
{
  "base_offset": 12345,
  "end_offset": 12999,
  "epoch": 7,
  "segment_key": "topic/0/segments/00000000000000012345-7.segment",
  "offset_index_key": "topic/0/segments/00000000000000012345-7.offset.idx",
  "created_at": "2026-03-25T08:00:00Z",
  "record_count": 655,
  "size_bytes": 8388608,
  "compression": "zstd"
}
```

This is immutable and lets readers validate or prefetch segment assets without inflating the partition manifest itself.

Current implementation note:

- `.meta.json` is already emitted during flush
- the current reader path does not need metadata yet, but the key is stored in `SegmentRef`
- cleanup paths delete `.segment`, `.offset.idx`, and `.meta.json` together
- checksum is not included yet; that is still future work

## Partition-level manifest

Per-segment indexes do not replace the partition-level lookup structure. We still need a cheap way to answer:

- which segment contains offset `N`?

The proposed model is a small append-style manifest chain.

### Manifest file

Each flush writes a new immutable manifest generation:

```json
{
  "generation": 42,
  "prev_generation": 41,
  "partition": 0,
  "topic": "orders",
  "high_watermark": 13000,
  "segments": [
    {
      "base_offset": 12345,
      "end_offset": 12999,
      "epoch": 7,
      "meta_key": "orders/0/segments/00000000000000012345-7.meta.json"
    }
  ]
}
```

This can be either:

- one segment entry per manifest generation, or
- a small batch of segment entries if a flush produces more than one segment

### Head pointer

Keep a tiny mutable file:

```json
{
  "generation": 42,
  "checkpoint_generation": 40
}
```

Only `head.json` needs CAS. The heavy segment membership data lives in immutable manifest files.

### Checkpoints

To avoid replaying a long manifest chain forever, periodically write:

```json
{
  "generation": 40,
  "high_watermark": 12345,
  "segments": [
    {"base_offset": 0, "end_offset": 999, "meta_key": "..."},
    {"base_offset": 1000, "end_offset": 1999, "meta_key": "..."}
  ]
}
```

Then the reader rebuild path is:

1. Load `head.json`.
2. Load the latest checkpoint named by `checkpoint_generation`.
3. Apply manifests from `checkpoint_generation+1` through `head.generation`.
4. Build the in-memory ordered segment table.

## Write path

Suggested flush sequence:

1. Build the `.segment` object as a sequence of independently readable batches.
2. Build the `.offset.idx` object from the segment writer's batch start positions.
3. Build the immutable `.meta.json`.
4. Upload all immutable segment files.
5. Write a new immutable manifest generation referencing the new segment metadata.
6. CAS-update `manifests/head.json`.

Benefits:

- no rewrite of the full partition index
- very small mutable state
- immutable segment descriptors are easy to cache and retry

## Read path

Suggested read flow:

1. Load cached partition manifest state.
2. If stale, refresh `head.json` and apply any newer manifest generations.
3. Binary-search the in-memory segment table by `base_offset`.
4. Fetch the chosen segment's `.meta.json` if not cached.
5. Fetch the chosen segment's `.offset.idx` if not cached.
6. Binary-search the mmap'd offset index to get the nearest indexed position `<= target`.
7. Read the `.segment` from that batch boundary and scan forward within the decoded batch stream.

This separates:

- partition-level selection
- segment-level seek

which is the core Kafka idea.

## Why this is better than the current `index.json`

Current shape:

- one partition-level `index.json`
- rewritten on every flush
- contains all segment range metadata
- batch-compressed seekable segment payload
- no in-segment byte-position index yet

Proposed shape:

- immutable segment metadata per segment
- immutable Kafka-style per-segment offset index (`4B relative_offset + 4B position`)
- seekable segment payload with optional batch-level compression only
- tiny mutable partition head pointer
- append-style manifest history

Benefits:

- lower CAS conflict rate
- much smaller mutable writes
- cheaper index refreshes
- direct path to Kafka-style seeks
- easier partial retry: immutable uploads can be retried independently of head publication

## Tradeoffs

- more objects per segment flush
- more moving parts than a single `index.json`
- need checkpoint compaction to keep manifest replay bounded
- readers now maintain two caches:
  - partition manifest state
  - per-segment metadata/index state

## Recommended implementation order

1. Add immutable `.meta.json` beside each segment.
2. Replace `index.json` rewrite with append-style manifests + `head.json`.
3. Keep the current seekable batch-compressed segment payload.
4. Extend `.offset.idx` handling with mmap-backed loading and sparse-index tuning.
5. Change reads to use segment metadata references instead of deriving index keys from segment keys.
6. Compact manifest generations into checkpoints.

## Minimal first version

If we want a lower-risk step first:

- keep the current batch-compressed segment payload
- add `.meta.json`
- add manifest generations + `head.json`
- postpone `.offset.idx` until after the partition manifest rewrite lands

That would already remove the monolithic index bottleneck while keeping the read path mostly unchanged.
