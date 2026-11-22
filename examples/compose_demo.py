"""Compose walkthrough: overlapping queries, backfill invalidation, restart.

It targets the two API instances defined in docker-compose.yml (both share one
SQLite database on the reach-data volume):

    docker compose up --build
    python examples/compose_demo.py                 # scenarios 1 and 2
    docker compose restart api-1
    python examples/compose_demo.py --post-restart  # scenario 3

Everything uses the standard library plus the project's own HLL codec, so the
same script runs on the host against http://localhost:8000 and :8001.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app.hll import build_summary

# 2026-10-29 is the start of a 7-day fixed grid block, so 14 days line up with
# exactly two cache blocks and the demo shows block reuse unambiguously.
FIRST_DAY = date(2026, 10, 29)
DAY_COUNT = 14
DAYS = [FIRST_DAY + timedelta(days=i) for i in range(DAY_COUNT)]
BACKFILL_DAY = date(2026, 11, 3)
PRECISION = 12


def call(base: str, method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        base + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def partition(day: date, values: list[str]) -> dict:
    return {
        "partition_date": day.isoformat(),
        "summary": base64.b64encode(build_summary(values, PRECISION)).decode("ascii"),
    }


def block_index_map(base: str) -> dict[int, dict]:
    status, body = call(base, "GET", "/v1/blocks")
    assert status == 200, body
    return {entry["block_index"]: entry for entry in body["blocks"]}


def scenario_overlap_and_backfill(base1: str, base2: str) -> None:
    print("== scenario 1: overlapping ranges reuse fixed blocks ==")
    upload = {
        "revision": 1,
        "precision": PRECISION,
        "idempotency_key": "compose-demo-r1",
        "partitions": [
            partition(day, [f"user-{day.isoformat()}-{i}" for i in range(3000)])
            for day in DAYS
        ],
    }
    status, body = call(base1, "POST", "/v1/revisions", upload)
    print(f"[api-1] upload 14 days r1 -> {status} {body['status']}")
    assert status == 200, body

    status, full = call(
        base1,
        "GET",
        f"/v1/reach?start_date={DAYS[0].isoformat()}&end_date={DAYS[-1].isoformat()}",
    )
    print(f"[api-1] full 14-day query -> {status}, snapshot {full['snapshot_id']}")
    assert status == 200, full

    # Instance 2 has no in-memory state: it must see the blocks instance 1 wrote.
    blocks_seen_by_2 = block_index_map(base2)
    print(f"[api-2] sees {len(blocks_seen_by_2)} cached blocks from api-1")
    assert len(blocks_seen_by_2) == 2
    created_before = {index: entry["created_at"] for index, entry in blocks_seen_by_2.items()}

    # Overlapping window: interior block reused, 2 boundary days on each side.
    overlap_start, overlap_end = DAYS[2], DAYS[-3]
    status, overlap = call(
        base2,
        "GET",
        f"/v1/reach?start_date={overlap_start.isoformat()}&end_date={overlap_end.isoformat()}",
    )
    print(
        f"[api-2] overlapping {overlap_start}..{overlap_end} query -> {status}, "
        f"{overlap['partition_count']} days adopted"
    )
    assert status == 200, overlap
    blocks_after = block_index_map(base2)
    assert {i: e["created_at"] for i, e in blocks_after.items()} == created_before
    print("[api-2] no new blocks written: the shared interior block was reused")

    print()
    print("== scenario 2: one-day backfill invalidates only its block ==")
    backfill = {
        "revision": 2,
        "precision": PRECISION,
        "idempotency_key": "compose-demo-r2",
        "partitions": [partition(BACKFILL_DAY, [f"corrected-{i}" for i in range(200)])],
    }
    status, body = call(base2, "POST", "/v1/revisions", backfill)
    print(
        f"[api-2] backfill {BACKFILL_DAY} r2 -> {status}, "
        f"invalidated_blocks={body['invalidated_blocks']}"
    )
    assert status == 200, body
    invalidated = set(body["invalidated_blocks"])

    # Instance 1 observes the durable invalidation without any restart.
    blocks_seen_by_1 = block_index_map(base1)
    for index, entry in blocks_seen_by_1.items():
        print(
            f"[api-1] block {index} {entry['start_date']}..{entry['end_date']} "
            f"still_valid={entry['still_valid']}"
        )
    assert {i for i, e in blocks_seen_by_1.items() if not e["still_valid"]} == invalidated
    assert sum(1 for e in blocks_seen_by_1.values() if e["still_valid"]) == 1

    status, rebuilt = call(
        base1,
        "GET",
        f"/v1/reach?start_date={DAYS[0].isoformat()}&end_date={DAYS[-1].isoformat()}",
    )
    print(f"[api-1] re-query after backfill -> {status}, snapshot {rebuilt['snapshot_id']}")
    assert status == 200, rebuilt
    by_day = {p["partition_date"]: p["revision"] for p in rebuilt["partitions_adopted"]}
    assert by_day[BACKFILL_DAY.isoformat()] == 2
    final_blocks = block_index_map(base1)
    rebuilt_index = next(iter(invalidated))
    assert final_blocks[rebuilt_index]["still_valid"] is True
    assert final_blocks[rebuilt_index]["created_at"] != created_before[rebuilt_index]
    untouched = next(i for i in final_blocks if i not in invalidated)
    assert final_blocks[untouched]["created_at"] == created_before[untouched]
    print("[api-1] touched block rebuilt, the other block kept its cached merge")
    print()
    print("Now restart an instance and re-run with --post-restart:")
    print("  docker compose restart api-1")
    print(f"  python {sys.argv[0]} --post-restart")


def scenario_post_restart(base1: str, base2: str) -> None:
    print("== scenario 3: cache survives a restart (durable in SQLite) ==")
    for name, base in (("api-1", base1), ("api-2", base2)):
        status, ready = call(base, "GET", "/readyz")
        assert status == 200, ready
        blocks = block_index_map(base)
        print(
            f"[{name}] ready: cached_blocks={ready['cached_blocks']} "
            f"valid_blocks={ready['valid_blocks']} block_days={ready['block_days']}"
        )
        for index, entry in sorted(blocks.items()):
            print(
                f"[{name}] block {index} {entry['start_date']}..{entry['end_date']} "
                f"valid={entry['still_valid']} created_at={entry['created_at']}"
            )

    blocks1, blocks2 = block_index_map(base1), block_index_map(base2)
    assert blocks1.keys() == blocks2.keys()
    for index in blocks1:
        assert blocks1[index]["created_at"] == blocks2[index]["created_at"]
        assert blocks1[index]["member_revision_ids"] == blocks2[index]["member_revision_ids"]

    status, answer = call(
        base1,
        "GET",
        f"/v1/reach?start_date={DAYS[0].isoformat()}&end_date={DAYS[-1].isoformat()}",
    )
    assert status == 200, answer
    print(
        f"[api-1] post-restart query -> {status}, reused_snapshot={answer['reused_snapshot']}, "
        f"snapshot {answer['snapshot_id']}"
    )
    by_day = {p["partition_date"]: p["revision"] for p in answer["partitions_adopted"]}
    assert by_day[BACKFILL_DAY.isoformat()] == 2
    print("restart rebuilt nothing: blocks, snapshots and estimates all came from SQLite")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base1", default=os.environ.get("REACH_URL_1", "http://localhost:8000"))
    parser.add_argument("--base2", default=os.environ.get("REACH_URL_2", "http://localhost:8001"))
    parser.add_argument("--post-restart", action="store_true")
    args = parser.parse_args()
    if args.post_restart:
        scenario_post_restart(args.base1, args.base2)
    else:
        scenario_overlap_and_backfill(args.base1, args.base2)


if __name__ == "__main__":
    main()
