"""Fixed-block merge cache: grid math, reuse, invalidation, durability, races."""

from __future__ import annotations

import binascii
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta

import pytest

from app.db import BLOCK_EPOCH, Database
from app.hll import build_registers, estimate_registers, merge_register_bytes

from .conftest import upload_body


def summary(values, precision=12):
    from app.hll import decode_summary, encode_summary

    return decode_summary(encode_summary(build_registers(values, precision), precision))


def seed(db, days, *, revision=1, precision=12, prefix="u"):
    db.submit_revision(
        revision=revision,
        precision=precision,
        summaries=[
            (
                day,
                summary([f"{prefix}-{day.isoformat()}-{i}" for i in range(500)], precision),
            )
            for day in days
        ],
        idempotency_key=f"seed-r{revision}-{prefix}",
    )


def daterange(start: date, days: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(days)]


# 2026-10-29 is the start of a 7-day grid block, so ranges anchored here line
# up exactly with cache blocks (block 2965).
ALIGNED = date(2026, 10, 29)


# -- grid ------------------------------------------------------------------


def test_block_size_environment_variable(monkeypatch, tmp_path):
    from app.db import configured_block_days

    assert configured_block_days() == 7  # default
    monkeypatch.setenv("REACH_BLOCK_DAYS", "30")
    assert configured_block_days() == 30
    db = Database(tmp_path / "db30.sqlite3")
    assert db.block_days == 30
    assert db.block_span(0) == (date(1970, 1, 1), date(1970, 1, 30))

    for bad in ("0", "1", "367", "seven", ""):
        monkeypatch.setenv("REACH_BLOCK_DAYS", bad)
        with pytest.raises(ValueError):
            configured_block_days()
    monkeypatch.delenv("REACH_BLOCK_DAYS", raising=False)
    assert configured_block_days() == 7


def test_legacy_database_without_block_checksum_is_migrated(tmp_path):
    """A register_blocks table from pre-checksum code upgrades lazily."""

    path = tmp_path / "legacy.sqlite3"
    db = Database(path, block_days=7)
    days = daterange(ALIGNED, 14)
    seed(db, days)
    expected = db.estimate(start=days[0], end=days[-1])

    # Simulate the older schema: drop the checksum column and recreate rows.
    with sqlite3.connect(path) as conn:
        rows = conn.execute(
            "SELECT block_index, start_date, end_date, member_revision_ids, "
            "registers, created_at, invalidated_at, invalidation_reason "
            "FROM register_blocks"
        ).fetchall()
        conn.execute("ALTER TABLE register_blocks RENAME TO register_blocks_old")
        conn.execute(
            "CREATE TABLE register_blocks ("
            "block_index INTEGER PRIMARY KEY, start_date TEXT NOT NULL, "
            "end_date TEXT NOT NULL, member_revision_ids TEXT NOT NULL, "
            "registers BLOB NOT NULL, created_at TEXT NOT NULL, "
            "invalidated_at TEXT, invalidation_reason TEXT)"
        )
        conn.executemany(
            "INSERT INTO register_blocks "
            "(block_index, start_date, end_date, member_revision_ids, registers, "
            " created_at, invalidated_at, invalidation_reason) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.execute("DROP TABLE register_blocks_old")
        # Force a cold query so the block path is actually exercised.
        conn.execute("DELETE FROM query_results")
        conn.commit()

    restarted = Database(path, block_days=7)  # runs the additive migration
    with sqlite3.connect(path) as conn:
        columns = {row[1] for row in conn.execute("PRAGMA table_info(register_blocks)")}
    assert "registers_checksum" in columns
    # The legacy rows have NULL checksums, so the query rebuilds them instead
    # of trusting unverified bytes; the answer stays correct.
    rebuilt = restarted.estimate(start=days[0], end=days[-1])
    assert rebuilt["estimate"] == expected["estimate"]
    assert all(block["still_valid"] for block in restarted.blocks())


def test_block_grid_is_fixed_at_epoch(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    assert db.block_index_for_day(BLOCK_EPOCH) == 0
    assert db.block_index_for_day(date(1970, 1, 7)) == 0
    assert db.block_index_for_day(date(1970, 1, 8)) == 1
    assert db.block_span(0) == (date(1970, 1, 1), date(1970, 1, 7))
    assert db.block_span(1) == (date(1970, 1, 8), date(1970, 1, 14))
    # Deterministic from any process: same date always maps to same block.
    again = Database(tmp_path / "other.sqlite3", block_days=7)
    sample = date(2026, 11, 22)
    assert again.block_index_for_day(sample) == db.block_index_for_day(sample)
    # The span of any block is exactly block_days contiguous days.
    start, end = db.block_span(db.block_index_for_day(sample))
    assert (end - start).days + 1 == 7
    assert start <= sample <= end


def test_split_block_coverage_separates_full_blocks_and_boundaries(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    # 2026-11-01 is a Sunday; block boundaries are epoch-based, not weekdays.
    start = date(2026, 11, 1)
    days = daterange(start, 21)
    blocks, boundary = db.split_block_coverage(days)
    assert len(blocks) >= 1
    # Every block day is absent from the boundary list and vice versa.
    covered = set()
    for index in blocks:
        bstart, bend = db.block_span(index)
        span = (bend - bstart).days + 1
        members = daterange(bstart, span)
        assert all(member in days for member in members)
        covered.update(members)
    assert not covered.intersection(boundary)
    assert covered.union(boundary) == set(days)
    # Boundary days are the partial blocks at the two ends only.
    assert boundary == [day for day in days if day not in covered]
    assert len(boundary) <= 2 * (db.block_days - 1)


def test_range_shorter_than_one_block_has_no_full_blocks(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED + timedelta(days=2), 3)
    blocks, boundary = db.split_block_coverage(days)
    assert blocks == []
    assert boundary == days


def test_aligned_full_range_uses_only_blocks_no_boundaries(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 21)  # exactly three grid blocks
    blocks, boundary = db.split_block_coverage(days)
    assert boundary == []
    assert blocks == [db.block_index_for_day(ALIGNED) + i for i in range(3)]


# -- estimate equivalence --------------------------------------------------


def test_block_merge_estimate_matches_direct_day_merge(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    precision = 12
    days = daterange(ALIGNED, 21)
    seed(db, days, precision=precision)

    result = db.estimate(start=days[0], end=days[-1])

    # Recompute the answer the old way: merge every day's registers directly.
    with sqlite3.connect(tmp_path / "db.sqlite3") as conn:
        rows = conn.execute(
            "SELECT registers FROM partition_revisions ORDER BY partition_date"
        ).fetchall()
    direct = merge_register_bytes((row[0] for row in rows), precision)

    blocks = db.blocks()
    assert blocks, "a 21-day query at block size 7 must populate the block cache"
    # HLL max-merge is associative, so grouped merging yields identical bytes
    # and therefore an identical estimate and identical error-bound numbers.
    assert result["estimate"] == pytest.approx(estimate_registers(direct))
    assert result["partition_count"] == 21
    assert result["precision"] == precision
    assert result["encoding_version"] == 1


def test_blocks_are_created_persisted_and_then_reused(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 14)
    seed(db, days)

    first = db.estimate(start=days[0], end=days[-1])
    blocks_after_first = db.blocks()
    assert len(blocks_after_first) == 2
    assert all(block["still_valid"] for block in blocks_after_first)
    created = {b["block_index"]: b["created_at"] for b in blocks_after_first}

    # A second identical query reuses the cached blocks (rows untouched) and
    # serves the answer from the snapshot cache as before.
    second = db.estimate(start=days[0], end=days[-1])
    assert second["reused_snapshot"] is True
    assert {b["block_index"]: b["created_at"] for b in db.blocks()} == created
    assert second["estimate"] == first["estimate"]


def test_overlapping_ranges_reuse_shared_blocks(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 28)
    seed(db, days)

    db.estimate(start=days[0], end=days[-1])
    created = {b["block_index"]: b["created_at"] for b in db.blocks()}
    assert len(created) == 4

    # An overlapping window shifted by a few days shares its interior blocks.
    overlap = db.estimate(start=days[2], end=days[20])
    blocks_after = db.blocks()
    assert len(blocks_after) == 4  # no new blocks were needed
    assert {b["block_index"]: b["created_at"] for b in blocks_after} == created
    assert overlap["partition_count"] == 19

    # And the overlapping answer equals merging its own 19 days directly.
    with sqlite3.connect(tmp_path / "db.sqlite3") as conn:
        rows = conn.execute(
            "SELECT registers FROM partition_revisions "
            "WHERE partition_date BETWEEN ? AND ? ORDER BY partition_date",
            (days[2].isoformat(), days[20].isoformat()),
        ).fetchall()
    direct = merge_register_bytes((row[0] for row in rows), 12)
    assert overlap["estimate"] == pytest.approx(estimate_registers(direct))


# -- invalidation ----------------------------------------------------------


def test_backfill_invalidates_only_blocks_containing_the_day(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 21)
    seed(db, days)
    db.estimate(start=days[0], end=days[-1])
    before = {b["block_index"]: b for b in db.blocks()}
    assert len(before) == 3

    target_day = days[5]  # lives in the first grid block
    target_block = db.block_index_for_day(target_day)
    other_blocks = [index for index in before if index != target_block]

    result = db.submit_revision(
        revision=2,
        precision=12,
        summaries=[(target_day, summary([f"fixed-{i}" for i in range(10)]))],
        idempotency_key="backfill-r2",
    )
    assert result["invalidated_blocks"] == [target_block]
    assert target_block in result["invalidated_snapshots"] or result[
        "invalidated_snapshots"
    ]

    after = {b["block_index"]: b for b in db.blocks()}
    assert after[target_block]["still_valid"] is False
    # Every other block survives untouched and stays valid.
    for index in other_blocks:
        assert after[index]["still_valid"] is True
        assert after[index]["created_at"] == before[index]["created_at"]

    # The next query rebuilds only the touched block; the others are reused.
    rebuilt = db.estimate(start=days[0], end=days[-1])
    final = {b["block_index"]: b for b in db.blocks()}
    assert final[target_block]["still_valid"] is True
    assert final[target_block]["created_at"] != before[target_block]["created_at"]
    for index in other_blocks:
        assert final[index]["created_at"] == before[index]["created_at"]
    assert rebuilt["partitions_adopted"][5]["revision"] == 2


def test_backfill_of_boundary_day_leaves_other_blocks_reusable(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 21)
    seed(db, days)
    # Query a window whose ends are partial blocks; the interior block is
    # cached while the edge days are merged as boundaries.
    db.estimate(start=days[2], end=days[16])
    cached_indices = {b["block_index"] for b in db.blocks()}
    interior_index = db.block_index_for_day(days[10])
    assert cached_indices == {interior_index}

    # Backfilling a boundary day whose grid block was never cached invalidates
    # nothing, while a backfill inside the cached interior block invalidates
    # exactly that block.
    edge = db.submit_revision(
        revision=2,
        precision=12,
        summaries=[(days[2], summary(["x"]))],
        idempotency_key="edge-r2",
    )
    assert edge["invalidated_blocks"] == []
    assert {b["block_index"] for b in db.blocks()} == cached_indices

    interior = db.submit_revision(
        revision=3,
        precision=12,
        summaries=[(days[10], summary(["y"]))],
        idempotency_key="interior-r3",
    )
    assert interior["invalidated_blocks"] == [interior_index]
    assert {
        b["block_index"]: b["still_valid"] for b in db.blocks()
    } == {interior_index: False}


def test_missing_day_is_rejected_before_any_block_is_written(tmp_path):
    from app.db import MissingPartitions

    db = Database(tmp_path / "db.sqlite3", block_days=7)
    seed(db, daterange(date(2026, 11, 1), 7))
    with pytest.raises(MissingPartitions):
        db.estimate(start=date(2026, 11, 1), end=date(2026, 11, 9))
    assert db.blocks() == []


# -- bit rot and stale revision signatures --------------------------------


def test_corrupt_block_registers_are_rebuilt_not_reused(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 14)
    seed(db, days)
    first = db.estimate(start=days[0], end=days[-1])

    path = tmp_path / "db.sqlite3"
    with sqlite3.connect(path) as conn:
        row = conn.execute(
            "SELECT block_index, registers FROM register_blocks "
            "WHERE invalidated_at IS NULL LIMIT 1"
        ).fetchone()
        tampered = bytearray(row[1])
        tampered[0] ^= 0xFF
        conn.execute(
            "UPDATE register_blocks SET registers = ? WHERE block_index = ?",
            (bytes(tampered), row[0]),
        )
        conn.commit()

    # The CRC mismatch disqualifies the row; the query rebuilds it and the
    # answer is still exactly the correct merge.
    second = db.estimate(start=days[0], end=days[-1])
    assert second["estimate"] == first["estimate"]
    blocks = {b["block_index"]: b for b in db.blocks()}
    assert blocks[row[0]]["still_valid"] is True


def test_block_with_stale_revision_signature_is_never_published(tmp_path):
    """Simulate an old merge surviving next to a newer committed state.

    Two API instances share one SQLite file; a block row carrying the previous
    revision id must not be reused even if it is still marked valid.
    """

    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 14)
    seed(db, days)
    first = db.estimate(start=days[0], end=days[-1])

    target = days[3]
    db.submit_revision(
        revision=2,
        precision=12,
        summaries=[(target, summary([f"rev2-{i}" for i in range(20)]))],
        idempotency_key="r2",
    )

    # Force the pre-backfill block row back to "valid" with the OLD signature,
    # imitating a stale compute that tries to publish after the commit.
    path = tmp_path / "db.sqlite3"
    with sqlite3.connect(path) as conn:
        stale_index = db.block_index_for_day(target)
        conn.execute(
            "UPDATE register_blocks SET invalidated_at = NULL, "
            "invalidation_reason = NULL WHERE block_index = ?",
            (stale_index,),
        )
        conn.commit()

    second = db.estimate(start=days[0], end=days[-1])
    row = sqlite3.connect(path).execute(
        "SELECT member_revision_ids FROM register_blocks WHERE block_index = ?",
        (stale_index,),
    ).fetchone()
    active_ids = {
        p["partition_date"]: p["revision_id"]
        for p in db.active_partitions()
    }
    signature = json.loads(row[0])
    block_start, _ = db.block_span(stale_index)
    for offset, revision_id in enumerate(signature):
        assert revision_id == active_ids[(block_start + timedelta(days=offset)).isoformat()]
    assert second["estimate"] != first["estimate"]
    by_day = {p["partition_date"]: p["revision"] for p in second["partitions_adopted"]}
    assert by_day[target.isoformat()] == 2


# -- durability across restarts and instances ------------------------------


def test_block_cache_survives_database_reopen(tmp_path):
    path = tmp_path / "db.sqlite3"
    db = Database(path, block_days=7)
    days = daterange(ALIGNED, 14)
    seed(db, days)
    db.estimate(start=days[0], end=days[-1])
    created = {b["block_index"]: b["created_at"] for b in db.blocks()}

    # A brand-new process opens the same SQLite file and rebuilds its cache
    # view from the durable rows instead of recomputing any merge.
    restarted = Database(path, block_days=7)
    assert {b["block_index"]: b["created_at"] for b in restarted.blocks()} == created
    answer = restarted.estimate(start=days[0], end=days[-1])
    assert {b["block_index"]: b["created_at"] for b in restarted.blocks()} == created
    assert answer["reused_snapshot"] is True


def test_two_instances_sharing_sqlite_see_invalidation(tmp_path):
    path = tmp_path / "db.sqlite3"
    one = Database(path, block_days=7)
    two = Database(path, block_days=7)
    days = daterange(ALIGNED, 14)
    seed(one, days)
    one.estimate(start=days[0], end=days[-1])

    target = days[2]
    one.submit_revision(
        revision=2,
        precision=12,
        summaries=[(target, summary(["cross-instance"]))],
        idempotency_key="r2",
    )

    # The second instance has no in-memory state; it must observe the
    # invalidation durable in SQLite and rebuild the affected block.
    seen = {b["block_index"]: b for b in two.blocks()}
    assert seen[two.block_index_for_day(target)]["still_valid"] is False
    answer = two.estimate(start=days[0], end=days[-1])
    by_day = {p["partition_date"]: p["revision"] for p in answer["partitions_adopted"]}
    assert by_day[target.isoformat()] == 2


# -- concurrency: one revision set per answer ------------------------------


def test_concurrent_queries_and_backfills_never_publish_stale_blocks(tmp_path):
    db = Database(tmp_path / "db.sqlite3", block_days=7)
    days = daterange(ALIGNED, 21)
    seed(db, days)
    db.estimate(start=days[0], end=days[-1])

    stop = threading.Event()
    errors: list[BaseException] = []

    def writer():
        try:
            for revision in range(2, 12):
                target = days[revision % len(days)]
                db.submit_revision(
                    revision=revision,
                    precision=12,
                    summaries=[
                        (target, summary([f"r{revision}-{i}" for i in range(25)]))
                    ],
                    idempotency_key=f"backfill-{revision}",
                )
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)
        finally:
            stop.set()

    def reader(_):
        observed = []
        for _ in range(200):
            result = db.estimate(start=days[0], end=days[-1])
            observed.append(result["snapshot_id"])
            # The answer must come from one committed snapshot whose stored
            # partitions are exactly those reported in the response (it may be
            # an older committed state, never a mix of revisions).
            record = db.snapshot(result["snapshot_id"])
            assert record is not None
            assert [
                (p["partition_date"], p["revision_id"])
                for p in record["partitions_adopted"]
            ] == [
                (p["partition_date"], p["revision_id"])
                for p in result["partitions_adopted"]
            ]
            # One revision per day, all from the immutable revision history.
            with sqlite3.connect(tmp_path / "db.sqlite3") as conn:
                for partition in result["partitions_adopted"]:
                    exists = conn.execute(
                        "SELECT 1 FROM partition_revisions WHERE id = ?",
                        (partition["revision_id"],),
                    ).fetchone()
                    assert exists is not None
            if stop.is_set():
                break
        return observed

    with ThreadPoolExecutor(max_workers=5) as pool:
        writer_future = pool.submit(writer)
        lists = list(pool.map(reader, range(4)))
        writer_future.result(timeout=30)

    assert not errors
    assert any(observed for observed in lists)

    # Final invariant: every still-valid block's signature matches the active
    # revision ids of all of its grid days and its registers match the CRC.
    active = {row["partition_date"]: row["revision_id"] for row in db.active_partitions()}
    with sqlite3.connect(tmp_path / "db.sqlite3") as conn:
        for block in db.blocks():
            row = conn.execute(
                "SELECT registers, registers_checksum FROM register_blocks "
                "WHERE block_index = ?",
                (block["block_index"],),
            ).fetchone()
            if not block["still_valid"]:
                continue
            start = date.fromisoformat(block["start_date"])
            for offset, revision_id in enumerate(block["member_revision_ids"]):
                day = (start + timedelta(days=offset)).isoformat()
                assert revision_id == active[day]
            assert (binascii.crc32(row[0]) & 0xFFFFFFFF) == row[1]


# -- HTTP surface ----------------------------------------------------------


def test_blocks_endpoint_and_readyz_report_cache(client):
    # 2026-10-29..2026-11-11 is exactly two epoch-aligned 7-day blocks.
    days = [ALIGNED + timedelta(days=i) for i in range(14)]
    upload = client.post(
        "/v1/revisions",
        json=upload_body(
            1,
            12,
            {day.isoformat(): [f"u{i}-{day.day}" for i in range(200)] for day in days},
        ),
    )
    assert upload.status_code == 200
    response = client.get(
        "/v1/reach",
        params={"start_date": days[0].isoformat(), "end_date": days[-1].isoformat()},
    )
    assert response.status_code == 200

    blocks = client.get("/v1/blocks").json()
    assert blocks["block_days"] == 7
    assert len(blocks["blocks"]) == 2
    entry = blocks["blocks"][0]
    assert entry["still_valid"] is True
    assert len(entry["member_revision_ids"]) == 7

    ready = client.get("/readyz").json()
    assert ready["cached_blocks"] == 2
    assert ready["valid_blocks"] == 2
    assert ready["block_days"] == 7

    backfill_day = date(2026, 11, 3)
    backfill = client.post(
        "/v1/revisions",
        json=upload_body(2, 12, {backfill_day.isoformat(): [f"fixed-{i}" for i in range(5)]}),
    )
    assert backfill.status_code == 200
    expected_index = client.app.state.db.block_index_for_day(backfill_day)
    assert backfill.json()["invalidated_blocks"] == [expected_index]
    fresh = client.get("/v1/blocks").json()
    valid = [b for b in fresh["blocks"] if b["still_valid"]]
    assert len(valid) == 1
    invalidated = [b for b in fresh["blocks"] if not b["still_valid"]]
    assert [b["block_index"] for b in invalidated] == [expected_index]
