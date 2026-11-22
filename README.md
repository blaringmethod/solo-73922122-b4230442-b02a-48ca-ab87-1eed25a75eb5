# Reach Deduplication Estimator

FastAPI + SQLite service that estimates **de-duplicated cross-device unique
users** over UTC day ranges from HyperLogLog (HLL) summaries uploaded by data
engineers.

The service merges only summaries that share one precision **and** one
encoding version, replaces a whole late-revision batch atomically, invalidates
the snapshots that touched the replaced days, and answers every query from a
single committed snapshot.

Long ranges are served from a **durable fixed-block merge cache**: the UTC
calendar is split into contiguous blocks of `REACH_BLOCK_DAYS` days (default
7) anchored at 1970-01-01, each block stores its register-wise merge plus the
exact revision ids merged, overlapping ranges reuse whole blocks and merge
only the few boundary days, and a backfill invalidates only the blocks that
contain the replaced day. The cache lives in SQLite, so every API instance
shares it and it survives restarts.

## Quick start

### Docker Compose (recommended)

```bash
docker compose up --build
# API instance 1: http://localhost:8000  (docs at /docs)
# API instance 2: http://localhost:8001  (docs at /docs)
# OpenAPI:         http://localhost:8000/openapi.json
```

Two API containers share one SQLite database on the `reach-data` volume
(`REACH_DB_PATH=/data/reach.sqlite3`). Blocks published by one instance are
reused by the other, and a backfill accepted on either instance invalidates
the same blocks for both. Each container runs as a non-root user and defines
a `HEALTHCHECK` on `/healthz`.

End-to-end walkthrough of block reuse, backfill invalidation and restart
recovery against the running stack:

```bash
python examples/compose_demo.py                  # overlapping query + backfill
docker compose restart api-1
python examples/compose_demo.py --post-restart   # cache rebuilt from SQLite
```

### Local Python

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
REACH_DB_PATH=./data/reach.sqlite3 uvicorn app.main:app --reload
pytest -q            # 76 tests
python examples/demo.py          # end-to-end walkthrough against localhost:8000
python examples/compose_demo.py  # block cache walkthrough (needs two instances)
```

`REACH_BLOCK_DAYS` (default `7`, valid 2..366) sets the fixed block size; all
processes sharing a database must use the same value.

`examples/generate_upload.py` prints a ready-to-paste upload body for
`/docs`:

```bash
python examples/generate_upload.py 2026-11-01 2026-11-02 --revision 1 \
  | curl -sS -X POST localhost:8000/v1/revisions \
      -H 'content-type: application/json' --data-binary @-
```

## API

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/revisions` | Upload one revision batch of daily HLL summaries |
| `GET` | `/v1/reach?start_date=&end_date=` | Estimate unique users, error bound, adopted partitions |
| `GET` | `/v1/snapshots/{id}` | Inspect a snapshot and whether it was invalidated |
| `GET` | `/v1/partitions` | Active revision per UTC day |
| `GET` | `/v1/blocks` | Inspect the durable fixed-block merge cache |
| `GET` | `/healthz`, `/readyz` | Liveness / readiness (SQLite round trip, block stats) |

All errors use one envelope:

```json
{"error": {"code": "missing_partitions", "message": "...", "details": {"missing_dates": ["2026-11-02"]}}}
```

### Upload

```json
{
  "revision": 2,
  "precision": 14,
  "idempotency_key": "reach-2026-11-02-r2",
  "partitions": [
    {"partition_date": "2026-11-02", "summary": "<base64 of HLL1 summary>"}
  ]
}
```

* All `partitions` are applied in **one transaction**; a late revision
  replaces exactly the days it carries and leaves other days untouched.
* The first accepted upload pins the store precision and encoding version;
  later uploads at a different precision get `422 precision_conflict`.
* A revision number lower than the active one → `409 stale_revision`. Reusing
  an existing revision number with different bytes → `409 revision_conflict`.
  Re-sending the identical revision is `200 {"status": "unchanged"}`.
* Reusing an `idempotency_key` with an identical payload → `200
  {"status": "duplicate"}`; with a different payload → `409`.
* Status codes: `200` accepted/unchanged/duplicate, `409` revision or
  idempotency conflict, `422` corrupt/colliding summary or bad request.

### Query

```bash
curl 'localhost:8000/v1/reach?start_date=2026-11-01&end_date=2026-11-02'
```

```json
{
  "snapshot_id": 2,
  "reused_snapshot": false,
  "estimated_unique_users": 20944,
  "estimate": 20944.075930203453,
  "precision": 14,
  "encoding_version": 1,
  "error_bound": {
    "confidence_level": 0.95,
    "relative_standard_error": 0.008125,
    "absolute_margin": 333.5,
    "lower": 20610.0,
    "upper": 21277.0
  },
  "partitions_adopted": [
    {"partition_date": "2026-11-01", "revision": 1, "revision_id": 1, "precision": 14},
    {"partition_date": "2026-11-02", "revision": 2, "revision_id": 3, "precision": 14}
  ],
  "partition_count": 2,
  "created_at": "2026-11-21T07:00:00.000000Z"
}
```

Rejected ranges: missing day → `404 missing_partitions`, incompatible
precision/version → `422 precision_mismatch`, inverted range → `400
invalid_range`, more than 1826 days → `400 invalid_range`.

## HLL summary format (v1)

```
offset  size  field
0       4     magic b"HLL1"
4       1     encoding version (1)
5       1     precision p (4..16)
6       2     reserved, must be zero
8       4     register count m = 1 << p (u32)
12      m     dense registers, one u8 each
12+m    4     CRC32 (u32) over all preceding bytes
```

* Hash: SHA-256 of the identity bytes; the high `p` bits pick the register,
  the rank is the position of the first 1 bit in the remaining bits.
* A summary is rejected when the magic, version, precision range, reserved
  bytes, declared register count, exact byte length, CRC32, or register values
  are wrong.
* Encoding version is part of the merge contract: v1 sketches never merge
  with another version, and `/v1/reach` refuses a range that mixes them.

## Design notes

* **Tables**: `submissions` (audit log), `partition_revisions` (immutable
  history, every revision), `active_partitions` (one revision per day),
  `snapshots` + `snapshot_partitions` (copies of the exact register bytes an
  answer used), `query_results` (answer cache), `register_blocks` (durable
  fixed-block merge cache), `service_config` (pinned precision/version).
* **Block cache**: the UTC calendar is divided into a fixed grid — block
  `k` covers `REACH_BLOCK_DAYS` consecutive days starting at
  `1970-01-01 + k * REACH_BLOCK_DAYS` — so the same dates always map to the
  same blocks in every process. Each `register_blocks` row stores the
  register-wise merge of the block's days, the ordered list of the exact
  `partition_revisions.id` values merged (`member_revision_ids`), and a CRC32
  of the merged registers. A query fully covering a block adopts the cached
  row only when it is not invalidated, its signature equals the revision ids
  pinned for every block day, and its checksum matches; otherwise the block is
  rebuilt from the pinned daily partitions and upserted. Only partial blocks
  at the two range edges are merged day by day, so the per-query merge work is
  proportional to at most `2 * (REACH_BLOCK_DAYS - 1)` boundary days plus the
  number of blocks.
* **Backfill invalidation**: inside the same `BEGIN IMMEDIATE` transaction
  that replaces a day's active partition, exactly the grid blocks containing
  a replaced day are marked `invalidated_at`; every other cached block stays
  valid and reusable. The upload response reports their indices in
  `invalidated_blocks`. A later query rebuilds the touched blocks and clears
  the invalidation markers atomically.
* **One revision set per answer**: the block signature check plus the block
  invalidation run inside the query's single writer transaction, so a block
  merge computed against an older revision can never be published as valid
  against a newer committed state — an old compute either finds its row
  invalidated/signature-mismatched and rebuilds, or never commits because
  SQLite's single writer serializes it behind the backfill. The user-visible
  estimate, error-bound math and snapshot format are unchanged; blocks are
  purely an internal acceleration layer.
* **Durability**: `register_blocks` is ordinary SQLite data on the shared
  volume, so both API instances read the same cache and a restart rebuilds
  nothing — the blocks are read back lazily. Databases created before the
  checksum column are upgraded with an additive `ALTER TABLE` migration and
  their unverified rows are rebuilt on first use.
* **Snapshot isolation**: a cold query runs `BEGIN IMMEDIATE`, pins the
  active partitions, merges (reusing blocks), writes the snapshot and result,
  and commits. Since that transaction is the only writer, no revision can
  land between pinning and snapshot storage — an answer never mixes
  revisions. A still-valid cached answer is served from a read-only
  transaction.
* **Invalidation**: a revision marks every snapshot that adopted a replaced
  day with `invalidated_at` + reason; the old answer stays readable via
  `GET /v1/snapshots/{id}` but is no longer reused.
* **Bit rot**: daily register blobs are re-validated (length, header CRC32,
  max rank) when merged directly or when rebuilding a block, and cached block
  registers carry their own CRC32; a mismatch rebuilds instead of reusing, or
  returns `500 stored_summary_corrupt` for the daily partitions, rather than
  producing a wrong estimate.
* **Concurrency**: WAL mode, `busy_timeout=10s`, a process-level writer lock,
  and `BEGIN IMMEDIATE` serialize writers across processes. Reads proceed
  concurrently.
* **Error bound**: nominal 95% normal approximation,
  `1.96 * (1.04 / sqrt(m)) * estimate`, clamped at 0. It is a model-based
  bound, not a measured confidence interval.

## Verified

```
$ pytest -q
76 passed
```

Covered: codec round trip and every corruption mode, register-wise merge,
estimator accuracy, error-bound math, upload/query happy paths, late-revision
batch replacement + snapshot invalidation, stale/conflicting revisions,
idempotency retries, precision pinning, missing-partition/inverted-range
rejections, snapshot inspection, OpenAPI route presence, and concurrent
queries+uploads (each snapshot matches one committed state, and a racing
writer is blocked while a query transaction is open). The block cache adds
grid math, block reuse across overlapping ranges, backfill invalidation
touching only the affected blocks, checksum-based bit-rot rebuild, stale
revision-signature rejection, cache durability across SQLite reopen and
between two instances sharing one database, the legacy-schema migration, and
concurrent query/backfill stress. `examples/demo.py` and
`examples/compose_demo.py` were also run against live uvicorn servers
(two instances on one SQLite file, including a restart).

Measured here with p=12, 365 seeded days, block size 7: a cold full-year
query builds 52 blocks (~330 ms once); a subsequent overlapping 353-day query
reuses 51 interior blocks and merges only 12 boundary days (~85 ms versus
~330 ms for re-merging every day); a one-day backfill invalidates one block
in ~11 ms and the next full-year query rebuilds just that block while reusing
the other 51. Estimates are bit-identical to merging every day directly.

## Not done / limitations

* Docker and Compose files are provided and the YAML/startup configuration was
  reviewed, but the image could not be built here (no Docker daemon in this
  environment); the two-instance scenarios were instead verified with two
  local uvicorn processes sharing one SQLite file.
* Single SQLite database; there is no cross-region replication or read
  replica. The block grid size (`REACH_BLOCK_DAYS`) must be identical for
  every process sharing a database; mismatched values would split the grid.
* No authentication, tenant scoping, or rate limiting — put it behind a
  gateway before exposing it.
* `query_results` is an append-only cache; invalidated rows are retained (by
  design, for auditability) and there is no retention/compaction job. Invalid
  `register_blocks` rows are likewise retained for inspection and overwritten
  in place on rebuild; there is no separate compaction job.
* A 365-day p=14 cold query still pays the one-time block build cost (merge is
  pure Python); warm and overlapping queries are substantially cheaper. p=16
  is slower and memory-heavier; p=12 is the recommended ceiling for very long
  ranges.
* The error bound is the standard theoretical HLL approximation; it is not
  calibrated against ground truth per deployment.
