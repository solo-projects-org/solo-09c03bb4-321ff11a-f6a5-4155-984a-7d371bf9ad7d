"""Background worker: durable job queue, crash recovery and stale-job rules.

Guarantees implemented here
---------------------------
1. Daily Parquet is built and atomically published *before* any metadata says
   "current"; a failed replacement leaves the previous day file untouched.
2. A monthly job re-reads the set of current daily files both at dequeue time
   and again inside the publishing transaction. A job that was enqueued against
   an older state ("过期作业") is marked ``obsolete`` and never published; the
   newest pending job for that station-month continues the recomputation.
3. The last complete monthly file is only marked ``superseded`` inside the same
   transaction that publishes the new one. If the process dies at any earlier
   point (write failure, registration failure, SIGKILL), the old monthly file
   stays ``current`` and downloadable; on restart the interrupted job goes back
   to ``pending`` and resumes.
4. Nothing ever downloads a ``.tmp`` or a missing file: download paths are
   built only from registered rows and re-checked on disk.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path

from . import config, db, fsutil, parquet

# One worker thread per process; the service is deployed as a single instance.
_wakeup = threading.Event()
_worker_started = False

# Demo/test only: when enabled (DEMO_FAULT=1), the next daily transcription
# raises a simulated I/O error once, so the UI can show 失败/待补算 and retry.
# Disabled by default; nothing in the normal deployment sets this flag.
_fault_next_daily = False


def arm_daily_fault() -> bool:
    """Arm a one-shot daily-build failure. Returns False when disabled."""
    global _fault_next_daily
    if not config.DEMO_FAULT_ENABLED:
        return False
    _fault_next_daily = True
    return True


def wakeup() -> None:
    _wakeup.set()


# --------------------------------------------------------------------------- #
# Version rule helpers
# --------------------------------------------------------------------------- #
def latest_saved_daily_id(conn: sqlite3.Connection, station: str, day: str) -> int | None:
    """Newest accepted (CSV saved + registered) version for one station-day.

    This id defines "最新更正". Rows whose save failed are deleted at upload
    time and therefore never participate in the ordering.
    """
    row = conn.execute(
        "SELECT MAX(id) AS id FROM daily_versions "
        "WHERE station=? AND day=? AND csv_saved=1",
        (station, day),
    ).fetchone()
    return row["id"]


# --------------------------------------------------------------------------- #
# Enqueue helpers (called from request handlers)
# --------------------------------------------------------------------------- #
def enqueue_daily_build(conn: sqlite3.Connection, daily_id: int) -> int:
    cur = conn.execute(
        "INSERT INTO jobs(kind, ref_id, status) VALUES ('daily_build', ?, 'pending')",
        (daily_id,),
    )
    return int(cur.lastrowid)


def enqueue_monthly_build(conn: sqlite3.Connection, station: str, month: str) -> int:
    """Register a new monthly generation attempt and its job."""
    cur = conn.execute(
        "INSERT INTO monthly_generations(station, month, status) "
        "VALUES (?, ?, 'building')",
        (station, month),
    )
    gen_id = int(cur.lastrowid)
    conn.execute(
        "INSERT INTO jobs(kind, ref_id, status) VALUES ('monthly_build', ?, 'pending')",
        (gen_id,),
    )
    return gen_id


def ensure_daily_job_pending(conn: sqlite3.Connection, daily_id: int) -> None:
    """Make sure a saved daily version has a queued build job.

    Used by startup recovery: a crash between the version registration and the
    job enqueue must not strand an accepted correction forever.
    """
    row = conn.execute(
        "SELECT id FROM jobs WHERE kind='daily_build' AND ref_id=? "
        "AND status IN ('pending','running')",
        (daily_id,),
    ).fetchone()
    if row is None:
        existing = conn.execute(
            "SELECT id, status FROM jobs WHERE kind='daily_build' AND ref_id=?",
            (daily_id,),
        ).fetchone()
        if existing is not None and existing["status"] == "failed":
            conn.execute(
                "UPDATE jobs SET status='pending', error=NULL, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (existing["id"],),
            )
        else:
            conn.execute(
                "INSERT INTO jobs(kind, ref_id, status) "
                "VALUES ('daily_build', ?, 'pending')",
                (daily_id,),
            )


# --------------------------------------------------------------------------- #
# Startup recovery
# --------------------------------------------------------------------------- #
def recover() -> dict[str, object]:
    """Rebuild a consistent workable state after a clean start or a crash."""
    report: dict[str, object] = {"tmp_removed": [], "reset_jobs": [], "orphans_removed": []}
    report["tmp_removed"] = fsutil.cleanup_tmp_dir()

    with db.get_conn() as conn:
        with db.tx(conn):
            # 1. Jobs interrupted mid-flight go back to the queue.
            rows = conn.execute(
                "SELECT id, kind, ref_id FROM jobs WHERE status = 'running'"
            ).fetchall()
            for row in rows:
                conn.execute(
                    "UPDATE jobs SET status='pending', updated_at="
                    "strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                    (row["id"],),
                )
                report["reset_jobs"].append(row["id"])

            # 2. Daily rows left in 'building' after a crash. The parquet was
            #    never registered, so remove any half-published file. If the
            #    version is still the newest accepted upload it is auto-resumed
            #    (its job is already back to 'pending' from step 1); a version
            #    superseded by a newer correction can never be current again and
            #    is retired as obsolete together with its job.
            for row in conn.execute(
                "SELECT id, station, day, parquet_path FROM daily_versions "
                "WHERE status='building'"
            ).fetchall():
                stale_file = config.daily_parquet_path(
                    row["station"], row["day"], row["id"]
                )
                if stale_file.exists():
                    try:
                        stale_file.unlink()
                        report["orphans_removed"].append(str(stale_file))
                    except OSError:
                        pass
                newest_id = latest_saved_daily_id(conn, row["station"], row["day"])
                if newest_id == row["id"]:
                    # Auto-resume: keep status='building'; the pending/running
                    # daily_build job (reset in step 1) will pick it up.
                    ensure_daily_job_pending(conn, row["id"])
                else:
                    conn.execute(
                        "UPDATE daily_versions SET status='obsolete', parquet_path=NULL, "
                        "error='进程退出前已被更新的更正取代，不再续算' WHERE id=?",
                        (row["id"],),
                    )
                    conn.execute(
                        "UPDATE jobs SET status='obsolete', "
                        "error='旧版本作业，已被更新的更正取代' "
                        "WHERE kind='daily_build' AND ref_id=? AND status IN "
                        "('pending','running','failed')",
                        (row["id"],),
                    )

            # 2b. Already-failed daily jobs that were the newest version must
            #     be auto-resumed after restart as well (the task was saved but
            #     its transcription never finished). Older failed versions are
            #     retired instead.
            for row in conn.execute(
                "SELECT id, station, day FROM daily_versions WHERE status='failed'"
            ).fetchall():
                newest_id = latest_saved_daily_id(conn, row["station"], row["day"])
                if newest_id == row["id"]:
                    conn.execute(
                        "UPDATE daily_versions SET status='building', error=NULL WHERE id=?",
                        (row["id"],),
                    )
                    ensure_daily_job_pending(conn, row["id"])
                else:
                    conn.execute(
                        "UPDATE daily_versions SET status='obsolete', "
                        "error='已被更新的更正取代' WHERE id=? AND status='failed'",
                        (row["id"],),
                    )
                    conn.execute(
                        "UPDATE jobs SET status='obsolete', "
                        "error='旧版本作业，已被更新的更正取代' "
                        "WHERE kind='daily_build' AND ref_id=? AND status IN "
                        "('pending','running','failed')",
                        (row["id"],),
                    )

            # 3. Monthly generations left in 'building': keep them as pending
            #    work (their job was reset above); remove any orphaned file.
            for row in conn.execute(
                "SELECT id, parquet_path FROM monthly_generations WHERE status='building'"
            ).fetchall():
                if row["parquet_path"]:
                    p = Path(row["parquet_path"])
                    if p.exists():
                        try:
                            p.unlink()
                        except OSError:
                            pass
                        report["orphans_removed"].append(str(p))
                    conn.execute(
                        "UPDATE monthly_generations SET parquet_path=NULL WHERE id=?",
                        (row["id"],),
                    )

            # 4. Database rows pointing at files that vanished from the volume.
            for table in ("daily_versions", "monthly_generations"):
                for row in conn.execute(
                    f"SELECT id AS rid, parquet_path FROM {table} "
                    "WHERE parquet_path IS NOT NULL"
                ).fetchall():
                    if not Path(row["parquet_path"]).is_file():
                        conn.execute(
                            f"UPDATE {table} SET parquet_path=NULL, "
                            "error=COALESCE(error, '数据卷上的文件丢失') WHERE id=?",
                            (row["rid"],),
                        )

            # 5. Parquet files on disk that no registered row owns: they can
            #    never be reached through a download entry and are deleted.
            registered = {
                str(r[0])
                for r in conn.execute(
                    "SELECT parquet_path FROM daily_versions "
                    "WHERE parquet_path IS NOT NULL "
                    "UNION ALL "
                    "SELECT parquet_path FROM monthly_generations "
                    "WHERE parquet_path IS NOT NULL"
                ).fetchall()
            }
            for base in (config.DAILY_DIR, config.MONTHLY_DIR):
                if not base.exists():
                    continue
                for p in base.rglob("*.parquet"):
                    if str(p) not in registered:
                        try:
                            p.unlink()
                            report["orphans_removed"].append(str(p))
                        except OSError:
                            pass
    return report


# --------------------------------------------------------------------------- #
# Job selection
# --------------------------------------------------------------------------- #
def _pick_job(conn: sqlite3.Connection) -> sqlite3.Row | None:
    """Pick the oldest pending job, coalescing redundant monthly jobs.

    For a given station-month only the newest pending monthly job can ever be
    publishable; older ones are marked obsolete immediately (they are the
    "过期作业" that must not be published).
    """
    pending = conn.execute(
        "SELECT id, kind, ref_id FROM jobs WHERE status='pending' ORDER BY id"
    ).fetchall()
    if not pending:
        return None

    newest_monthly: dict[tuple[str, str], int] = {}
    monthly_rows: dict[int, sqlite3.Row] = {}
    for job in pending:
        if job["kind"] != "monthly_build":
            continue
        gen = conn.execute(
            "SELECT id, station, month FROM monthly_generations WHERE id=?",
            (job["ref_id"],),
        ).fetchone()
        if gen is None:
            continue
        monthly_rows[job["id"]] = job
        key = (gen["station"], gen["month"])
        if key not in newest_monthly or job["id"] > newest_monthly[key]:
            newest_monthly[key] = job["id"]

    for job_id, job in monthly_rows.items():
        gen = conn.execute(
            "SELECT station, month FROM monthly_generations WHERE id=?",
            (job["ref_id"],),
        ).fetchone()
        key = (gen["station"], gen["month"])
        if newest_monthly.get(key) != job_id:
            conn.execute(
                "UPDATE jobs SET status='obsolete', "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (job_id,),
            )
            conn.execute(
                "UPDATE monthly_generations SET status='obsolete', "
                "error='被更新的补算作业取代' WHERE id=? AND status='building'",
                (job["ref_id"],),
            )

    # Daily builds always go first: a monthly merge whose daily inputs are
    # still pending would fail, and a newer correction may obsolete it anyway.
    chosen = conn.execute(
        "SELECT id, kind, ref_id FROM jobs WHERE status='pending' "
        "ORDER BY CASE kind WHEN 'daily_build' THEN 0 ELSE 1 END, id LIMIT 1"
    ).fetchone()
    return chosen


# --------------------------------------------------------------------------- #
# Daily build
# --------------------------------------------------------------------------- #
def _run_daily_build(conn: sqlite3.Connection, job_id: int, daily_id: int) -> None:
    row = conn.execute(
        "SELECT * FROM daily_versions WHERE id=?", (daily_id,)
    ).fetchone()
    if row is None:
        raise RuntimeError(f"日档记录 {daily_id} 不存在")

    # Version gate (rule: newest accepted upload wins). If a newer correction
    # was already accepted, this late/retried/restarted job can never become
    # the current day file again — regardless of whether its build succeeds.
    newest_id = latest_saved_daily_id(conn, row["station"], row["day"])
    if newest_id is not None and newest_id != daily_id:
        with db.tx(conn):
            conn.execute(
                "UPDATE daily_versions SET status='obsolete', is_current=0, "
                "error='已被更新的更正取代，本作业迟到完成，不得成为当前日档' "
                "WHERE id=? AND status IN ('building','failed')",
                (daily_id,),
            )
            conn.execute(
                "UPDATE jobs SET status='obsolete', error='旧版本作业，已被更新的更正取代', "
                "attempts=attempts+1, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (job_id,),
            )
        return

    csv_path = Path(row["csv_path"])
    if not csv_path.is_file():
        raise RuntimeError(f"原始 CSV 已丢失: {csv_path}")

    # Re-parse from the persisted CSV: the build must not depend on request
    # memory, and a re-run after restart takes exactly the same input.
    from .csv_validation import parse_csv

    parsed = parse_csv(csv_path.read_bytes())

    final_path = config.daily_parquet_path(parsed.station, parsed.day, daily_id)
    tmp_path = fsutil.tmp_target(final_path, f"daily{daily_id}")
    fsutil.discard_tmp(tmp_path)
    if _fault_next_daily:  # demo-only one-shot simulated write failure
        globals()["_fault_next_daily"] = False
        raise OSError("模拟转写故障（演示用，DEMO_FAULT 已开启）")
    written = parquet.write_daily_parquet(parsed.rows, tmp_path)
    parquet.verify_daily_parquet(tmp_path, parsed.row_count)
    fsutil.atomic_publish(tmp_path, final_path)
    # From here the new Parquet is durably on disk; metadata transaction next.
    # If we crash between the rename and the commit, startup recovery treats
    # this version as a failed retry and never serves it as current. Its file
    # remains harmless (orphan-cleanup keeps files owned by registered rows).

    with db.tx(conn):
        # Re-check inside the write lock: a correction accepted while the
        # parquet was being written would make this version non-current.
        if latest_saved_daily_id(conn, parsed.station, parsed.day) != daily_id:
            conn.execute(
                "UPDATE daily_versions SET status='obsolete', is_current=0, "
                "error='发布前已被更新的更正取代' WHERE id=? AND status='building'",
                (daily_id,),
            )
            conn.execute(
                "UPDATE jobs SET status='obsolete', error='发布前已被更新的更正取代', "
                "attempts=attempts+1, "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (job_id,),
            )
            return
        conn.execute(
            "UPDATE daily_versions SET parquet_path=?, row_count=?, status='ready', "
            "error=NULL, is_current=1 WHERE id=?",
            (str(final_path), written, daily_id),
        )
        conn.execute(
            "UPDATE daily_versions SET is_current=0, superseded_at="
            "strftime('%Y-%m-%dT%H:%M:%fZ','now') "
            "WHERE station=? AND day=? AND id<>? AND is_current=1",
            (parsed.station, parsed.day, daily_id),
        )
        # The day changed: any monthly file covering it is now stale. It stays
        # downloadable, but is flagged until a fresh generation is published.
        month = parsed.day[:7]
        conn.execute(
            "UPDATE monthly_generations SET status='superseded', "
            "published_at=COALESCE(published_at, created_at) "
            "WHERE station=? AND month=? AND status='current'",
            (parsed.station, month),
        )
        enqueue_monthly_build(conn, parsed.station, month)
        conn.execute(
            "UPDATE jobs SET status='done', error=NULL, attempts=attempts+1, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (job_id,),
        )
    # A daily build finishing may unblock a monthly job parked in _WaitForDaily.
    wakeup()


# --------------------------------------------------------------------------- #
# Monthly build
# --------------------------------------------------------------------------- #
def _current_daily_deps(conn: sqlite3.Connection, station: str, month: str):
    return conn.execute(
        "SELECT id, day, parquet_path, row_count FROM daily_versions "
        "WHERE station=? AND substr(day,1,7)=? AND is_current=1 AND status='ready' "
        "ORDER BY day",
        (station, month),
    ).fetchall()


class _WaitForDaily(Exception):
    """Raised when the newest accepted day files are not all built yet.

    The job stays ``pending`` (shown as 补算中) and is retried automatically;
    nothing is marked failed and no monthly file is published in the meantime.
    """


def _latest_saved_dailies(conn: sqlite3.Connection, station: str, month: str):
    """One row per station-day: its newest *accepted* (csv_saved) version."""
    return conn.execute(
        "SELECT d.id, d.day, d.status, d.is_current, d.parquet_path, "
        "       (SELECT COUNT(*) FROM jobs j WHERE j.kind='daily_build' "
        "          AND j.ref_id=d.id AND j.status IN ('pending','running')) AS has_active_job "
        "FROM daily_versions d "
        "JOIN (SELECT day, MAX(id) AS id FROM daily_versions "
        "      WHERE station=? AND substr(day,1,7)=? AND csv_saved=1 GROUP BY day) m "
        "  ON m.id = d.id ORDER BY d.day",
        (station, month),
    ).fetchall()


def _month_signature(conn: sqlite3.Connection, station: str, month: str):
    """Canonical signature of *all current* daily files of a station-month.

    Computed through the same SQLite connection that holds the transaction, so
    the re-check under the write lock observes exactly the committed state.
    """
    deps = _current_daily_deps(conn, station, month)
    signature = ";".join(
        f"{r['day']}=v{r['id']}:rows{r['row_count']}" for r in deps
    )
    return signature, [dict(r) for r in deps]


def _obsolete_monthly(conn: sqlite3.Connection, job_id: int, gen_id: int,
                      reason: str) -> None:
    conn.execute(
        "UPDATE jobs SET status='obsolete', "
        "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
        (job_id,),
    )
    conn.execute(
        "UPDATE monthly_generations SET status='obsolete', error=? "
        "WHERE id=? AND status='building'",
        (reason, gen_id),
    )


def _run_monthly_build(conn: sqlite3.Connection, job_id: int, gen_id: int) -> None:
    gen = conn.execute(
        "SELECT * FROM monthly_generations WHERE id=?", (gen_id,)
    ).fetchone()
    if gen is None:
        raise RuntimeError(f"月生成记录 {gen_id} 不存在")
    station, month = gen["station"], gen["month"]

    # A newer pending monthly job for the same station-month means this one is
    # already stale even before it starts.
    newer = conn.execute(
        "SELECT j.id FROM jobs j JOIN monthly_generations g ON g.id=j.ref_id "
        "WHERE j.kind='monthly_build' AND j.status='pending' "
        "AND g.station=? AND g.month=? AND j.id<>? ORDER BY j.id DESC LIMIT 1",
        (station, month, job_id),
    ).fetchone()
    if newer is not None:
        _obsolete_monthly(conn, job_id, gen_id, "排队期间日档再次变更，由更新的作业接算")
        return

    # A monthly file may be published only when EVERY newest accepted daily
    # version of the station-month is fully built and current. If the newest
    # version is still being transcribed, keep waiting (never publish old
    # content as latest); if it has failed, fail this job with a clear reason.
    latest_days = _latest_saved_dailies(conn, station, month)
    if not latest_days:
        raise RuntimeError("本月没有任何已接收日档，无法合并月文件")
    not_ready = [
        r for r in latest_days
        if not (r["status"] == "ready" and r["is_current"] == 1)
    ]
    if not_ready:
        if any(r["has_active_job"] or r["status"] == "building" for r in not_ready):
            raise _WaitForDaily(
                "最新更正日档尚未补算完成，月文件等待日档就绪（旧完整文件仍可下载）"
            )
        raise RuntimeError(
            "最新更正日档转写失败，月文件不能以旧内容冒充最新；请先重试该日档作业"
        )
    deps = _current_daily_deps(conn, station, month)
    if any(not Path(r["parquet_path"]).is_file() for r in deps):
        raise _WaitForDaily("日档文件尚未落盘完成，月文件等待日档就绪")

    signature, dep_list = _month_signature(conn, station, month)
    daily_paths = [r["parquet_path"] for r in dep_list]

    final_path = config.monthly_parquet_path(station, month, gen_id)
    # A previous attempt of this same generation may have left an orphan file.
    if final_path.exists():
        final_path.unlink()
    tmp_path = fsutil.tmp_target(final_path, f"monthly{gen_id}")
    fsutil.discard_tmp(tmp_path)
    written = parquet.merge_monthly_parquet(daily_paths, tmp_path)
    if written != sum(r["row_count"] for r in dep_list):
        fsutil.discard_tmp(tmp_path)
        raise RuntimeError("月文件行数与日档合计不一致，拒绝发布")
    fsutil.atomic_publish(tmp_path, final_path)
    # New file is fully on disk. Publishing metadata transactionally next;
    # until this COMMIT, the previous monthly file remains 'current'.

    with db.tx(conn):
        # Re-check under the write lock: a correction committed while we were
        # merging makes this job stale and it must not be published.
        current_sig, current_deps = _month_signature(conn, station, month)
        if current_sig != signature:
            try:
                final_path.unlink()
            except OSError:
                pass
            _obsolete_monthly(
                conn, job_id, gen_id,
                "合并期间日档发生变更，本作业已过期，由后续作业接算",
            )
            return
        if any(not Path(r["parquet_path"]).is_file() for r in current_deps):
            try:
                final_path.unlink()
            except OSError:
                pass
            raise RuntimeError("发布前发现日档文件缺失，拒绝发布")

        conn.execute(
            "UPDATE monthly_generations SET status='superseded' "
            "WHERE station=? AND month=? AND status='current'",
            (station, month),
        )
        conn.execute(
            "UPDATE monthly_generations SET parquet_path=?, signature=?, row_count=?, "
            "status='current', error=NULL, "
            "published_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (str(final_path), signature, written, gen_id),
        )
        conn.execute("DELETE FROM monthly_dependencies WHERE generation_id=?", (gen_id,))
        conn.executemany(
            "INSERT INTO monthly_dependencies(generation_id, daily_id, day) "
            "VALUES (?, ?, ?)",
            [(gen_id, r["id"], r["day"]) for r in dep_list],
        )
        conn.execute(
            "UPDATE jobs SET status='done', error=NULL, attempts=attempts+1, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (job_id,),
        )


# --------------------------------------------------------------------------- #
# Worker loop
# --------------------------------------------------------------------------- #
def _mark_failed(conn: sqlite3.Connection, job: sqlite3.Row, exc: Exception) -> None:
    msg = f"{type(exc).__name__}: {exc}"[:1000]
    with db.tx(conn):
        conn.execute(
            "UPDATE jobs SET status='failed', error=?, attempts=attempts+1, "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (msg, job["id"]),
        )
        if job["kind"] == "daily_build":
            conn.execute(
                "UPDATE daily_versions SET status='failed', error=? WHERE id=? "
                "AND status='building'",
                (msg, job["ref_id"]),
            )
        else:
            conn.execute(
                "UPDATE monthly_generations SET status='failed', error=? WHERE id=? "
                "AND status='building'",
                (msg, job["ref_id"]),
            )


def _process_one(conn: sqlite3.Connection) -> bool:
    with db.tx(conn):
        job = _pick_job(conn)
        if job is None:
            return False
        conn.execute(
            "UPDATE jobs SET status='running', "
            "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
            (job["id"],),
        )
    try:
        if job["kind"] == "daily_build":
            _run_daily_build(conn, job["id"], job["ref_id"])
        else:
            _run_monthly_build(conn, job["id"], job["ref_id"])
    except _WaitForDaily:
        # Not a failure: the newest daily correction is still building. Return
        # the job to the queue; the dashboard keeps showing 补算中 and the
        # previous complete monthly file stays downloadable.
        with db.tx(conn):
            conn.execute(
                "UPDATE jobs SET status='pending', "
                "updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id=?",
                (job["id"],),
            )
        return False  # let the worker loop pause before re-picking
    except Exception as exc:  # recorded as retryable failure, worker survives
        # The worker runs one task at a time, so every staging file left by
        # this attempt is safe to remove; the registered final paths are never
        # in tmp and therefore never touched.
        for child in config.TMP_DIR.glob("*.tmp"):
            fsutil.discard_tmp(child)
        _mark_failed(conn, job, exc)
    return True


def worker_loop(stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            conn = db.get_conn()
            try:
                did_work = _process_one(conn)
            finally:
                conn.close()
        except Exception:
            # A SQLite hiccup must not kill the worker; retry after a pause.
            did_work = False
            time.sleep(1.0)
        if not did_work:
            _wakeup.wait(timeout=2.0)
            _wakeup.clear()


def start_worker() -> tuple[threading.Thread, threading.Event]:
    global _worker_started
    if _worker_started:
        raise RuntimeError("worker already started")
    _worker_started = True
    db.init_db()
    recover()
    stop_event = threading.Event()
    thread = threading.Thread(target=worker_loop, args=(stop_event,),
                              name="monthly-worker", daemon=True)
    thread.start()
    return thread, stop_event
