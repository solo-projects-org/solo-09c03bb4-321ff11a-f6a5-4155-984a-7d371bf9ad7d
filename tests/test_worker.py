"""In-process unit tests for the worker's correctness rules.

No HTTP server here: jobs are processed one at a time via worker._process_one,
which makes failure injection and crash-window simulation deterministic.
"""
import sqlite3
from pathlib import Path

import pytest

from app import config, db, fsutil, parquet, worker
from helpers import make_csv


@pytest.fixture()
def isolated(monkeypatch, tmp_path):
    vol = (tmp_path / "data").resolve()
    # config captured constants at import time; repoint them for this test.
    monkeypatch.setattr(config, "DATA_VOLUME", vol)
    monkeypatch.setattr(config, "DB_PATH", vol / "state.db")
    monkeypatch.setattr(config, "INBOX_DIR", vol / "inbox")
    monkeypatch.setattr(config, "DAILY_DIR", vol / "daily")
    monkeypatch.setattr(config, "MONTHLY_DIR", vol / "monthly")
    monkeypatch.setattr(config, "TMP_DIR", vol / "tmp")
    config.ensure_dirs()
    db.init_db()
    return vol


def _seed_upload(station: str, day: str, values=None) -> int:
    """Persist a validated upload exactly like the HTTP handler does."""
    raw = make_csv(station, day, values or {h: 10 for h in range(24)})
    with db.get_conn() as conn:
        with db.tx(conn):
            cur = conn.execute(
                "INSERT INTO daily_versions(station, day, csv_path, status, csv_saved) "
                "VALUES (?, ?, '', 'building', 1)",
                (station, day),
            )
            daily_id = int(cur.lastrowid)
        csv_path = config.inbox_csv_path(station, day, daily_id)
        conn.execute("UPDATE daily_versions SET csv_path=? WHERE id=?",
                     (str(csv_path), daily_id))
    csv_path.write_bytes(raw)
    with db.get_conn() as conn:
        with db.tx(conn):
            worker.enqueue_daily_build(conn, daily_id)
    return daily_id


def _drain(conn, max_jobs: int = 50) -> None:
    for _ in range(max_jobs):
        if not worker._process_one(conn):
            return
    raise AssertionError("queue did not drain")


def _one(conn: sqlite3.Connection, sql: str, args=()):
    row = conn.execute(sql, args).fetchone()
    assert row is not None, f"no row: {sql} {args}"
    return row


def _latest_generation(conn, station: str, month: str = "2026-11"):
    return _one(conn,
        "SELECT * FROM monthly_generations WHERE station=? AND month=? "
        "ORDER BY id DESC LIMIT 1", (station, month))


def test_daily_failure_keeps_previous_day_and_monthly(isolated, monkeypatch):
    conn = db.get_conn()
    # First successful version + its monthly file.
    _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]
    old_monthly = config.monthly_parquet_path("S1", "2026-11", gen1)
    assert old_monthly.is_file()

    # Second version: force the Parquet write to fail.
    v2 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    original_write = parquet.write_daily_parquet

    def boom(*_a, **_k):
        raise OSError("simulated disk failure")

    monkeypatch.setattr(parquet, "write_daily_parquet", boom)
    _drain(conn)

    failed = _one(conn, "SELECT status, error FROM daily_versions WHERE id=?", (v2,))
    assert failed["status"] == "failed" and "disk failure" in failed["error"]
    # v1 remains current and its files are untouched; monthly gen1 still current.
    cur = _one(conn,
        "SELECT id, is_current, status FROM daily_versions WHERE station='S1' "
        "AND day='2026-11-10' AND is_current=1")
    assert cur["id"] != v2 and cur["status"] == "ready"
    m = _one(conn, "SELECT status FROM monthly_generations WHERE id=?", (gen1,))
    assert m["status"] == "current" and old_monthly.is_file()

    # Retry succeeds after the fault clears (restore the original function).
    monkeypatch.setattr(parquet, "write_daily_parquet", original_write)
    job = _one(conn, "SELECT id FROM jobs WHERE ref_id=? AND status='failed'", (v2,))
    conn.execute("UPDATE jobs SET status='pending' WHERE id=?", (job["id"],))
    conn.execute("UPDATE daily_versions SET status='building', error=NULL WHERE id=?", (v2,))
    _drain(conn)
    cur2 = _one(conn,
        "SELECT id FROM daily_versions WHERE station='S1' AND day='2026-11-10' AND is_current=1")
    assert cur2["id"] == v2
    latest = _latest_generation(conn, "S1")
    assert latest["status"] == "current" and latest["id"] > gen1
    # Old monthly still on disk and its row marked superseded.
    assert old_monthly.is_file()
    old = _one(conn, "SELECT status FROM monthly_generations WHERE id=?", (gen1,))
    assert old["status"] == "superseded"


def test_stale_monthly_job_is_never_published(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]

    # Two corrections: let each daily build finish, but hold both monthly jobs
    # in the queue by only processing daily_build entries.
    v2 = _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    worker._process_one(conn)  # v2 daily -> gen2 monthly job pending
    v3 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    worker._process_one(conn)  # v3 daily -> gen3 monthly job pending
    pending_gens = conn.execute(
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='building' "
        "ORDER BY id").fetchall()
    assert len(pending_gens) == 2
    gen2, gen3 = pending_gens[0]["id"], pending_gens[1]["id"]

    # The picker must obsolete gen2's job in favour of the newest gen3 job.
    _drain(conn)
    states = {r["id"]: r["status"] for r in conn.execute(
        "SELECT id, status FROM monthly_generations WHERE station='S1'")}
    assert states[gen2] == "obsolete"
    assert states[gen3] == "current"
    assert states[gen1] == "superseded"
    # Published file corresponds exactly to the current (v3) daily set.
    gen3_row = _one(conn,
        "SELECT signature, parquet_path FROM monthly_generations WHERE id=?", (gen3,))
    deps = conn.execute(
        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?", (gen3,)).fetchall()
    assert [d["daily_id"] for d in deps] == [v3]
    assert f"v{v3}" in gen3_row["signature"]
    # The obsolete generation never got a downloadable file.
    g2 = _one(conn, "SELECT parquet_path FROM monthly_generations WHERE id=?", (gen2,))
    assert g2["parquet_path"] is None


def test_signature_recheck_rejects_publish_after_mid_merge_change(isolated, monkeypatch):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]

    # Correction v2: build its daily so gen2's monthly job is queued.
    _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    worker._process_one(conn)  # v2 daily -> gen2 pending
    gen2 = _latest_generation(conn, "S1")["id"]

    # A further correction (v4) is committed *during* gen2's merge window,
    # after the ready-check passed but before the publishing transaction.
    original_merge = parquet.merge_monthly_parquet
    call = {"n": 0}

    def merge_then_correct(daily_paths, tmp_path):
        result = original_merge(daily_paths, tmp_path)
        call["n"] += 1
        if call["n"] == 1:
            # Merge file is fully written but not yet registered; a correction
            # lands and its daily build commits a new current signature.
            _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
            worker._process_one(conn)  # v4 daily build -> new signature
        return result

    monkeypatch.setattr(parquet, "merge_monthly_parquet", merge_then_correct)
    worker._process_one(conn)  # gen2 merge -> publish re-check must reject it
    monkeypatch.setattr(parquet, "merge_monthly_parquet", original_merge)
    _drain(conn)

    g2 = _one(conn, "SELECT status, error FROM monthly_generations WHERE id=?", (gen2,))
    assert g2["status"] == "obsolete" and "过期" in (g2["error"] or "")
    # A final current generation exists and matches the newest daily version.
    latest = _latest_generation(conn, "S1")
    assert latest["status"] == "current"
    newest_daily = conn.execute(
        "SELECT max(id) AS id FROM daily_versions").fetchone()["id"]
    cur_daily = _one(conn,
        "SELECT id FROM daily_versions WHERE station='S1' AND is_current=1")
    assert cur_daily["id"] == newest_daily
    deps = conn.execute(
        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
        (latest["id"],)).fetchall()
    assert [d["daily_id"] for d in deps] == [newest_daily]
    # gen1 row: superseded, never lost; its file remains downloadable.
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (gen1,))["status"] == "superseded"


def test_monthly_build_waits_for_pending_daily(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10")
    # Build the first daily, but leave its monthly job pending.
    worker._process_one(conn)
    # A second correction: its daily job must run before any monthly publish.
    _seed_upload("S1", "2026-11-10", {h: 5 for h in range(24)})
    _drain(conn)
    latest = _latest_generation(conn, "S1")
    assert latest["status"] == "current"
    # The published monthly depends on the newest daily only.
    deps = conn.execute(
        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
        (latest["id"],)).fetchall()
    newest = conn.execute("SELECT max(id) AS id FROM daily_versions").fetchone()["id"]
    assert [d["daily_id"] for d in deps] == [newest]


def test_recovery_after_crash_resets_running_and_removes_orphans(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10")
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]
    good = config.monthly_parquet_path("S1", "2026-11", gen1)

    # A correction in flight: daily v2 + its monthly generation, all 'running'.
    v2 = _seed_upload("S1", "2026-11-10", {h: 9 for h in range(24)})
    worker._process_one(conn)  # v2 daily builds and enqueues a new generation
    gen2 = _latest_generation(conn, "S1")["id"]
    assert gen2 > gen1
    conn.execute("UPDATE jobs SET status='running'")

    # Stray files a real crash could leave behind.
    stray_month = config.monthly_parquet_path("S1", "2026-11", gen2)
    stray_month.parent.mkdir(parents=True, exist_ok=True)
    stray_month.write_bytes(b"PAR1partial")
    stray_tmp = config.TMP_DIR / "leftover.tmp"
    stray_tmp.write_bytes(b"x")
    unregistered = config.DAILY_DIR / "S1" / "daily-2026-11-11-v999.parquet"
    unregistered.parent.mkdir(parents=True, exist_ok=True)
    unregistered.write_bytes(b"PAR1ghost")
    # Emulate the daily row being interrupted between parquet rename and commit.
    conn.execute("UPDATE daily_versions SET status='building' WHERE id=?", (v2,))
    conn.close()

    conn = db.get_conn()
    # Every interrupted ('running') job is reset; already-finished jobs stay.
    running = conn.execute("SELECT COUNT(*) AS c FROM jobs WHERE status='running'").fetchone()["c"]
    assert running > 0
    report = worker.recover()
    assert stray_tmp.name in [p.split("/")[-1] for p in report["tmp_removed"]]
    assert not stray_month.exists()
    assert not unregistered.exists()
    assert good.is_file()  # last complete monthly survives

    conn = db.get_conn()
    leftover_running = conn.execute(
        "SELECT COUNT(*) AS c FROM jobs WHERE status='running'").fetchone()["c"]
    assert leftover_running == 0
    # Every previously-running job is back in the queue.
    reset_rows = conn.execute(
        "SELECT status FROM jobs WHERE id IN (%s)" %
        ",".join(str(i) for i in range(1, 5))).fetchall()
    assert all(r["status"] in ("pending", "done", "obsolete", "failed") for r in reset_rows)
    # v2 is still the newest accepted upload, so recovery auto-resumes it:
    # the daily row stays 'building' and its job is queued again (no manual
    # retry needed) instead of being parked as a failure.
    v2_row = _one(conn, "SELECT status FROM daily_versions WHERE id=?", (v2,))
    assert v2_row["status"] == "building"
    v2_job = conn.execute(
        "SELECT status FROM jobs WHERE kind='daily_build' AND ref_id=?", (v2,)
    ).fetchall()
    assert any(r["status"] == "pending" for r in v2_job)
    gen2_row = _one(conn,
        "SELECT status, parquet_path FROM monthly_generations WHERE id=?", (gen2,))
    assert gen2_row["status"] == "building" and gen2_row["parquet_path"] is None

    # The queue now drains without any manual retry and converges on the
    # newest daily set; gen1's complete file remains available throughout.
    _drain(conn)
    assert _latest_generation(conn, "S1")["status"] == "current"
    assert good.is_file()


def test_only_changed_month_is_rebuilt(isolated):
    conn = db.get_conn()
    _seed_upload("S1", "2026-10-31")
    _seed_upload("S1", "2026-11-10")
    _drain(conn)
    oct_gen = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND month='2026-10' "
        "AND status='current'")["id"]

    _seed_upload("S1", "2026-11-10", {h: 33 for h in range(24)})
    _drain(conn)
    # October generation untouched; November got a new one.
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (oct_gen,))["status"] == "current"
    nov_gens = conn.execute(
        "SELECT id FROM monthly_generations WHERE station='S1' AND month='2026-11'").fetchall()
    assert len(nov_gens) == 2


def test_late_old_daily_job_never_becomes_current(isolated, monkeypatch):
    """旧更正作业迟到完成/重试，不得把已发布的新日档顶回旧值。"""
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]

    # v2 fails its transcription (disk fault); v1 stays current.
    v2 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    original_write = parquet.write_daily_parquet

    def boom(*_a, **_k):
        raise OSError("disk full")

    monkeypatch.setattr(parquet, "write_daily_parquet", boom)
    _drain(conn)
    monkeypatch.setattr(parquet, "write_daily_parquet", original_write)
    assert _one(conn, "SELECT status FROM daily_versions WHERE id=?", (v2,))["status"] == "failed"
    assert _one(conn, "SELECT id FROM daily_versions WHERE is_current=1")["id"] == v1

    # v3 is accepted and fully published: it becomes the current day file and
    # forces a fresh monthly generation.
    v3 = _seed_upload("S1", "2026-11-10", {h: 14 for h in range(24)})
    _drain(conn)
    assert _one(conn, "SELECT id FROM daily_versions WHERE is_current=1")["id"] == v3
    gen3 = _latest_generation(conn, "S1")
    assert gen3["status"] == "current" and gen3["id"] > gen1

    # Now retry the stale v2 job exactly like the HTTP endpoint would: it must
    # be retired as obsolete and never flip is_current back to the old value.
    conn.execute("UPDATE daily_versions SET status='building', error=NULL WHERE id=?", (v2,))
    conn.execute(
        "INSERT INTO jobs(kind, ref_id, status) VALUES ('daily_build', ?, 'pending')", (v2,))
    _drain(conn)
    v2_row = _one(conn, "SELECT status, is_current FROM daily_versions WHERE id=?", (v2,))
    assert v2_row["status"] == "obsolete" and v2_row["is_current"] == 0
    assert _one(conn, "SELECT id FROM daily_versions WHERE is_current=1")["id"] == v3
    # The current monthly generation is untouched (no rollback rebuild).
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (gen3["id"],))["status"] == "current"
    # v2's job was obsoleted, not marked done.
    old_jobs = conn.execute(
        "SELECT DISTINCT status FROM jobs WHERE kind='daily_build' AND ref_id=?", (v2,)
    ).fetchall()
    assert all(r["status"] in ("failed", "obsolete") for r in old_jobs)


def test_old_daily_retry_after_crash_does_not_revive(isolated, monkeypatch):
    """重启恢复后，被新更正取代的旧 building/failed 作业不再续算。"""
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)

    # v2 crashes mid-build (left 'building' with a pending/running job).
    v2 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    conn.execute("UPDATE jobs SET status='running' WHERE ref_id=?", (v2,))
    conn.close()

    # A newer correction v3 is accepted in a later process lifetime.
    conn = db.get_conn()
    v3 = _seed_upload("S1", "2026-11-10", {h: 14 for h in range(24)})
    worker.recover()
    _drain(conn)

    v2_row = _one(conn, "SELECT status, is_current FROM daily_versions WHERE id=?", (v2,))
    assert v2_row["status"] == "obsolete" and v2_row["is_current"] == 0
    assert _one(conn, "SELECT id FROM daily_versions WHERE is_current=1")["id"] == v3
    assert _latest_generation(conn, "S1")["status"] == "current"


def test_monthly_waits_and_never_publishes_while_newest_daily_unfinished(isolated, monkeypatch):
    """最新更正尚未补算完：旧月文件保留，但月作业等待，绝不标最新。"""
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]

    # v2 accepted; block its parquet write so it stays in building/failed flow.
    v2 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    original_write = parquet.write_daily_parquet

    def boom(*_a, **_k):
        raise OSError("simulated transient fault")

    monkeypatch.setattr(parquet, "write_daily_parquet", boom)
    # Drain: v2 fails, and no monthly build may run/publish old content.
    _drain(conn)
    monkeypatch.setattr(parquet, "write_daily_parquet", original_write)
    v2_row = _one(conn, "SELECT status FROM daily_versions WHERE id=?", (v2,))
    assert v2_row["status"] == "failed"
    # gen1 remains the stored current and its file is intact.
    g1 = _one(conn, "SELECT status, parquet_path FROM monthly_generations WHERE id=?", (gen1,))
    assert g1["status"] == "current" and Path(g1["parquet_path"]).is_file()
    # No extra monthly generation is enqueued while the newest day failed.
    assert conn.execute(
        "SELECT COUNT(*) AS c FROM monthly_generations").fetchone()["c"] == 1
    # The read model must NOT present the old file as 最新: it derives a
    # failed-latest effective state while still offering the old download.
    from app import status as status_mod
    overview = status_mod.list_overview()
    month = next(m for m in overview["months"] if m["station"] == "S1")
    assert month["effective_status"] == "failed_latest"
    assert month["is_latest"] is False and month["download_ready"] is True
    day = next(d for d in overview["days"] if d["station"] == "S1")
    assert day["badge"][0] == "失败" and day["is_latest"] is False
    assert day["fallback_id"] == v1  # old complete day file stays downloadable

    # Retrying the failed daily build succeeds; the pipeline then publishes a
    # monthly covering the newest day.
    conn.execute("UPDATE daily_versions SET status='building', error=NULL WHERE id=?", (v2,))
    conn.execute("UPDATE jobs SET status='pending', error=NULL "
                 "WHERE kind='daily_build' AND ref_id=?", (v2,))
    _drain(conn)
    final_gen = _latest_generation(conn, "S1")
    assert final_gen["status"] == "current"
    deps = conn.execute(
        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
        (final_gen["id"],)).fetchall()
    assert [d["daily_id"] for d in deps] == [v2]
    # gen1's file is still on disk (superseded, downloadable).
    assert Path(g1["parquet_path"]).is_file()


def test_monthly_pending_while_daily_still_building(isolated):
    """日档补算中：月作业保持 pending（页面显示待补算），不失败也不发布。"""
    conn = db.get_conn()
    v1 = _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]

    # Accept v2 but keep its daily job pending (no drain yet), then register a
    # monthly generation+job the way the pipeline will once v2 publishes.
    v2 = _seed_upload("S1", "2026-11-10", {h: 12 for h in range(24)})
    head = _one(conn, "SELECT status FROM daily_versions WHERE id=?", (v2,))
    assert head["status"] == "building"
    with db.tx(conn):
        monthly_gen = worker.enqueue_monthly_build(conn, "S1", "2026-11")
    monthly_job = _one(conn,
        "SELECT id FROM jobs WHERE kind='monthly_build' AND ref_id=?", (monthly_gen,))

    # The picker prefers daily jobs, so mark the daily one running to force the
    # monthly job to be picked while the newest day is still not built.
    conn.execute(
        "UPDATE jobs SET status='running' WHERE kind='daily_build' AND ref_id=?", (v2,))

    processed = worker._process_one(conn)  # monthly picked -> must wait
    assert processed is False
    job_row = _one(conn, "SELECT status, error FROM jobs WHERE id=?", (monthly_job["id"],))
    assert job_row["status"] == "pending" and job_row["error"] is None
    # Nothing new published; gen1 stays the stored current and downloadable.
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (gen1,))["status"] == "current"
    gen2_row = _one(conn,
        "SELECT status, parquet_path FROM monthly_generations WHERE id=?", (monthly_gen,))
    assert gen2_row["status"] == "building" and gen2_row["parquet_path"] is None
    # The read model reports 待补算 (not 最新) while keeping the old download.
    from app import status as status_mod
    overview = status_mod.list_overview()
    month = next(m for m in overview["months"] if m["station"] == "S1")
    assert month["effective_status"] == "pending_latest"
    assert month["is_latest"] is False and month["download_ready"] is True

    # Once the daily build finishes, the parked monthly job publishes normally.
    conn.execute(
        "UPDATE jobs SET status='pending' WHERE kind='daily_build' AND ref_id=?", (v2,))
    _drain(conn)
    final_gen = _latest_generation(conn, "S1")
    assert final_gen["status"] == "current" and final_gen["id"] > gen1
    assert _one(conn, "SELECT id FROM daily_versions WHERE is_current=1")["id"] == v2


def test_monthly_must_cover_all_newest_days(isolated):
    """月文件只有覆盖同站当月全部最新已接收日档才能发布。"""
    conn = db.get_conn()
    _seed_upload("S1", "2026-11-10", {h: 10 for h in range(24)})
    _seed_upload("S1", "2026-11-11", {h: 20 for h in range(24)})
    _drain(conn)
    gen1 = _one(conn,
        "SELECT id FROM monthly_generations WHERE station='S1' AND status='current'")["id"]
    # Correct day 10 only; the new monthly must contain BOTH day 10 (new v)
    # and day 11 (reused current).
    v2 = _seed_upload("S1", "2026-11-10", {h: 11 for h in range(24)})
    _drain(conn)
    gen2 = _latest_generation(conn, "S1")
    assert gen2["status"] == "current"
    deps = sorted(
        r["daily_id"] for r in conn.execute(
            "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
            (gen2["id"],)).fetchall())
    day11 = _one(conn,
        "SELECT id FROM daily_versions WHERE station='S1' AND day='2026-11-11'")["id"]
    assert deps == sorted([v2, day11])
    assert _one(conn, "SELECT status FROM monthly_generations WHERE id=?",
                (gen1,))["status"] == "superseded"
