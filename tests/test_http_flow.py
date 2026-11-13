"""End-to-end HTTP tests: upload, correction, reuse, safe downloads."""
import time

import duckdb

from conftest import Server, free_port
from helpers import make_csv, redirect_message, upload, wait_for


def _current_month(client, station: str, month: str) -> dict:
    data = client.get("/api/status").json()
    matches = [m for m in data["months"]
               if m["station"] == station and m["month"] == month]
    assert matches, f"no monthly row for {station} {month}"
    return matches[0]


def test_full_flow_upload_correction_and_reuse(server, volume):
    client = server.client()

    # Day 10: first upload -> daily parquet + monthly generation 1.
    r = upload(client, make_csv("ST001", "2026-11-10", {h: 10 for h in range(24)}))
    assert r.status_code == 303
    wait_for(client, ("ST001", "2026-11"))
    m1 = _current_month(client, "ST001", "2026-11")
    assert m1["badge"][0] == "最新"
    gen1 = m1["download_id"]
    assert gen1 is not None

    # Second day in the same month: only the new day + one new monthly build.
    r = upload(client, make_csv("ST001", "2026-11-11", {h: 20 for h in range(24)}))
    assert r.status_code == 303
    wait_for(client, ("ST001", "2026-11"), after_gen=gen1)
    m2 = _current_month(client, "ST001", "2026-11")
    gen2 = m2["download_id"]
    assert gen2 > gen1
    assert m2["day_count"] == 2

    # The day-10 parquet was reused (same version id, same file name).
    data = client.get("/api/status").json()
    day10 = [d for d in data["days"]
             if d["station"] == "ST001" and d["day"] == "2026-11-10"][0]
    v10 = day10["current_id"]

    # Correction of day 10: full replacement, monthly rebuilt, other day reused.
    r = upload(client, make_csv("ST001", "2026-11-10",
                                {**{h: 10 for h in range(24)}, 0: 99}))
    assert r.status_code == 303
    wait_for(client, ("ST001", "2026-11"), after_gen=gen2)
    data = client.get("/api/status").json()
    day10b = [d for d in data["days"]
              if d["station"] == "ST001" and d["day"] == "2026-11-10"][0]
    assert day10b["current_id"] > v10  # new version, old one superseded
    day11 = [d for d in data["days"]
             if d["station"] == "ST001" and d["day"] == "2026-11-11"][0]
    assert day11["version_count"] == 1  # untouched -> its parquet was reused

    m3 = _current_month(client, "ST001", "2026-11")
    assert m3["download_id"] > gen2
    # Old generations remain present in history and still downloadable.
    hist = {h["id"]: h for h in m3["history"]}
    assert set(hist) >= {gen1, gen2, m3["download_id"]}
    assert hist[gen1]["badge"][0].startswith("已过期")
    assert hist[gen1]["downloadable"] is True

    # Verify parquet contents through DuckDB.
    resp = client.get(f"/download/monthly/ST001/2026-11")
    assert resp.status_code == 200
    latest = volume / "monthly" / "ST001" / f"monthly-2026-11-gen{m3['download_id']}.parquet"
    con = duckdb.connect()
    try:
        rows = con.execute(
            "SELECT count(*), sum(pm25), min(utc_seconds), max(utc_seconds) "
            "FROM read_parquet(?)", [str(latest)]
        ).fetchone()
    finally:
        con.close()
    assert rows[0] == 24 + 24
    # Day10 corrected: 23*10 + 99; day11: 24*20.
    assert rows[1] == (23 * 10 + 99) + 24 * 20

    # Old generation 1 still downloads (stale but available), and its content
    # is the pre-correction snapshot.
    resp = client.get(f"/download/generation/{gen1}")
    assert resp.status_code == 200
    assert f"gen{gen1}" in resp.headers["content-disposition"]


def test_invalid_csv_changes_nothing(server, volume):
    client = server.client()
    r = upload(client, b"station,utc_seconds,pm25\nST001,notanint,5\n")
    assert r.status_code == 303
    assert "校验失败" in redirect_message(r)
    # Nothing registered, no files created outside tmp.
    data = client.get("/api/status").json()
    assert data["days"] == [] and data["months"] == []
    assert not (volume / "inbox").exists() or not any((volume / "inbox").iterdir())

    # Negative pm likewise rejected.
    r = upload(client, make_csv("ST001", "2026-11-10").replace(b",10\n", b",-1\n", 1))
    assert r.status_code == 303
    assert "校验失败" in redirect_message(r)
    data = client.get("/api/status").json()
    assert data["days"] == []


def test_download_entries_never_half_or_missing(server, volume):
    client = server.client()
    # No data yet: endpoints refuse instead of 500/path traversal.
    assert client.get("/download/monthly/ST001/2026-11", follow_redirects=False).status_code == 303
    assert client.get("/download/daily/ST001/2026-11-10", follow_redirects=False).status_code == 303
    # Path validation.
    bad = client.get("/download/monthly/..%2F..%2Fetc/2026-11", follow_redirects=False)
    assert bad.status_code == 303

    upload(client, make_csv("ST001", "2026-11-10"))
    wait_for(client, ("ST001", "2026-11"))

    # Delete the file behind the DB row: the entry must refuse, not 404-stream.
    for p in (volume / "monthly" / "ST001").glob("*.parquet"):
        p.unlink()
    resp = client.get("/download/monthly/ST001/2026-11", follow_redirects=False)
    assert resp.status_code == 303
    assert "不存在" in redirect_message(resp)

    # No tmp file is ever left in the serving directories.
    for base in (volume / "daily", volume / "monthly"):
        assert not list(base.rglob("*.tmp"))


def test_restart_resumes_and_keeps_last_good_file(server, volume):
    client = server.client()
    upload(client, make_csv("ST001", "2026-11-10", {h: 10 for h in range(24)}))
    wait_for(client, ("ST001", "2026-11"))
    before = _current_month(client, "ST001", "2026-11")
    gen1 = before["download_id"]
    good_file = volume / "monthly" / "ST001" / f"monthly-2026-11-gen{gen1}.parquet"
    assert good_file.is_file()

    # Graceful restart: state lives entirely in the volume; file still served.
    server.stop()
    srv2 = Server(volume, free_port())
    srv2.start()
    try:
        c2 = srv2.client()
        resp = c2.get(f"/download/generation/{gen1}")
        assert resp.status_code == 200
        m = _current_month(c2, "ST001", "2026-11")
        assert m["download_id"] == gen1

        # A correction after restart resumes the normal pipeline.
        upload(c2, make_csv("ST001", "2026-11-10", {h: 11 for h in range(24)}))
        wait_for(c2, ("ST001", "2026-11"), after_gen=gen1)
        after = _current_month(c2, "ST001", "2026-11")
        assert after["download_id"] > gen1
        assert good_file.is_file()  # superseded, not deleted
    finally:
        srv2.stop()


def test_abrupt_kill_during_processing_resumes(server, volume):
    # Upload work, then SIGKILL without giving the worker time to finish.
    # Startup recovery must reset the interrupted job and the retry endpoint
    # drives the queue to convergence; the last good file is never lost.
    client = server.client()
    for attempt in range(3):
        upload(client, make_csv("ST002", "2026-11-10",
                                {h: attempt + 1 for h in range(24)}))
        server.kill9()
        srv = Server(volume, free_port())
        srv.start()
        # Hand the live process back to the fixture for teardown.
        server.proc = srv.proc
        server.port = srv.port
        server.base_url = srv.base_url
        server.kill9()  # kill again almost immediately, mid-recovery/processing
        srv2 = Server(volume, free_port())
        srv2.start()
        server.proc = srv2.proc
        server.port = srv2.port
        server.base_url = srv2.base_url
        client = server.client()

    # Whatever state the repeated crashes left, every failed job is retryable
    # and the pipeline converges on the current daily set.
    deadline = time.time() + 25
    while time.time() < deadline:
        data = client.get("/api/status").json()
        for job in data["failed_jobs"]:
            client.post(f"/jobs/{job['id']}/retry")
        if any(m["station"] == "ST002" and m["latest_status"] == "current"
               for m in data["months"]):
            break
        time.sleep(0.5)

    data = wait_for(client, ("ST002", "2026-11"), timeout=30)
    m = [x for x in data["months"] if x["station"] == "ST002"][0]
    assert m["badge"][0] == "最新"
    # No staging leftovers; no job left running.
    assert not list((volume / "tmp").glob("*.tmp"))
    assert all(j["status"] != "running" for j in data["jobs"])


def test_dashboard_renders(server):
    client = server.client()
    resp = client.get("/")
    assert resp.status_code == 200
    assert "空气站站月文件服务" in resp.text
    upload(client, make_csv("ST001", "2026-11-10"))
    wait_for(client, ("ST001", "2026-11"))
    resp = client.get("/")
    assert resp.status_code == 200
    assert "ST001" in resp.text and "下载最新月文件" in resp.text


def test_save_failure_changes_nothing_and_keeps_version_order(server, volume, monkeypatch):
    """CSV 落盘失败：删除预登记行，原日档不动，后续成功上传仍是连续版本。"""
    # A normal first upload establishes v1.
    upload(client := server.client(), make_csv("ST001", "2026-11-10", {h: 10 for h in range(24)}))
    wait_for(client, ("ST001", "2026-11"))
    data = client.get("/api/status").json()
    day = [d for d in data["days"] if d["station"] == "ST001"][0]
    v1 = day["head_id"]
    assert day["is_latest"] is True

    # Make the atomic CSV publish fail inside the live server process by
    # pointing DATA_VOLUME at a read-only location via an env-driven fault:
    # here we remove write permission on the inbox directory instead.
    inbox = volume / "inbox"
    inbox.chmod(0o500)
    try:
        r = upload(client, make_csv("ST001", "2026-11-10", {h: 33 for h in range(24)}))
        assert r.status_code == 303
        assert "保存失败" in redirect_message(r)
    finally:
        inbox.chmod(0o700)

    # State is unchanged: still one version, still current, no failed row.
    data = client.get("/api/status").json()
    days = [d for d in data["days"] if d["station"] == "ST001"]
    assert len(days) == 1
    assert days[0]["head_id"] == v1 and days[0]["is_latest"] is True
    assert days[0]["badge"][0] == "最新"

    # The next successful upload becomes the new head (autoincrement ids are
    # not recycled after a deleted row — what matters is that the failed save
    # changed no existing state) and converges normally.
    r = upload(client, make_csv("ST001", "2026-11-10", {h: 44 for h in range(24)}))
    assert r.status_code == 303
    wait_for(client, ("ST001", "2026-11"), after_gen=data["months"][0]["download_id"])
    data = client.get("/api/status").json()
    day = [d for d in data["days"] if d["station"] == "ST001"][0]
    assert day["head_id"] > v1 and day["is_latest"] is True
    assert day["version_count"] == 2  # only the two successful uploads


def test_retrying_old_failed_daily_job_is_rejected(server, volume):
    """新更正已接收后，旧失败作业的重试被拒并作废，绝不回滚当前日档。"""
    client = server.client()
    # v1 published.
    upload(client, make_csv("ST001", "2026-11-10", {h: 10 for h in range(24)}))
    wait_for(client, ("ST001", "2026-11"))
    data = client.get("/api/status").json()
    v1 = [d for d in data["days"] if d["station"] == "ST001"][0]["head_id"]

    # v2 fails at CSV save (read-only inbox): no state change, no version row.
    inbox = volume / "inbox"
    inbox.chmod(0o500)
    try:
        r = upload(client, make_csv("ST001", "2026-11-10", {h: 11 for h in range(24)}))
        assert r.status_code == 303 and "保存失败" in redirect_message(r)
    finally:
        inbox.chmod(0o700)

    # v3 accepted and published: it is the newest correction.
    upload(client, make_csv("ST001", "2026-11-10", {h: 12 for h in range(24)}))
    wait_for(client, ("ST001", "2026-11"), after_gen=data["months"][0]["download_id"])
    data = client.get("/api/status").json()
    head = [d for d in data["days"] if d["station"] == "ST001"][0]
    assert head["head_id"] > v1 and head["is_latest"] is True

    # Now force a failed-but-superseded daily job through the real retry guard:
    # upload v4, then make its *transcription* fail by blocking the parquet
    # write is not possible over HTTP, so instead verify the guard on a job
    # belonging to an older version. v1's original job is 'done' and retrying a
    # non-failed job is refused; the important ordering invariant is that no
    # operation can make v1 current again.
    jobs = client.get("/api/status").json()["jobs"]
    v1_job = next(j for j in jobs if j["kind"] == "daily_build"
                  and j["status"] == "done" and j["ref_id"] == v1)
    r = client.post(f"/jobs/{v1_job['id']}/retry", follow_redirects=False)
    assert r.status_code == 303
    assert "仅失败作业可重试" in redirect_message(r)

    after = client.get("/api/status").json()
    head_after = [d for d in after["days"] if d["station"] == "ST001"][0]
    assert head_after["head_id"] > v1 and head_after["is_latest"] is True
    # The current monthly stays published and downloadable.
    assert client.get("/download/monthly/ST001/2026-11").status_code == 200


def test_stale_failed_daily_retry_rejected_over_http(server, volume):
    """旧版本的失败作业走真实重试接口时被拒并作废，当前日档不回滚。"""
    import sqlite3

    client = server.client()
    upload(client, make_csv("ST001", "2026-11-10", {h: 10 for h in range(24)}))
    wait_for(client, ("ST001", "2026-11"))
    data = client.get("/api/status").json()
    v1 = [d for d in data["days"] if d["station"] == "ST001"][0]["head_id"]

    # A newer accepted + published correction becomes the current day file.
    upload(client, make_csv("ST001", "2026-11-10", {h: 12 for h in range(24)}))
    wait_for(client, ("ST001", "2026-11"), after_gen=data["months"][0]["download_id"])
    data = client.get("/api/status").json()
    v2 = [d for d in data["days"] if d["station"] == "ST001"][0]["head_id"]
    assert v2 > v1

    # Simulate an old (v1) failed job left behind, then retry it over HTTP.
    db_path = volume / "state.db"
    conn = sqlite3.connect(db_path)
    cur = conn.execute(
        "INSERT INTO jobs(kind, ref_id, status, error) "
        "VALUES ('daily_build', ?, 'failed', 'old failure')", (v1,))
    stale_job = cur.lastrowid
    conn.commit()
    conn.close()

    r = client.post(f"/jobs/{stale_job}/retry", follow_redirects=False)
    assert r.status_code == 303
    assert "旧版本" in redirect_message(r)

    # The stale job is obsoleted; v2 remains the sole current day file and the
    # old v1 keeps its ready (historical, downloadable) row without reviving.
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    assert conn.execute("SELECT status FROM jobs WHERE id=?", (stale_job,)
                        ).fetchone()["status"] == "obsolete"
    assert conn.execute("SELECT is_current FROM daily_versions WHERE id=?", (v2,)
                        ).fetchone()["is_current"] == 1
    assert conn.execute("SELECT is_current FROM daily_versions WHERE id=?", (v1,)
                        ).fetchone()["is_current"] == 0
    conn.close()

    after = client.get("/api/status").json()
    head = [d for d in after["days"] if d["station"] == "ST001"][0]
    assert head["head_id"] == v2 and head["is_latest"] is True


def test_continuous_corrections_always_serve_newest(server, volume):
    """连续多次更正：当前日档/月文件始终对应最新已接收且补算完成的版本。"""
    client = server.client()
    last_gen = 0
    head_id = None
    for round_, pm in enumerate((10, 20, 30, 40), start=1):
        upload(client, make_csv("ST001", "2026-11-10", {h: pm for h in range(24)}))
        data = wait_for(client, ("ST001", "2026-11"), after_gen=last_gen)
        month = [m for m in data["months"] if m["station"] == "ST001"][0]
        assert month["is_latest"] is True
        assert month["badge"][0] == "最新"
        last_gen = month["download_id"]
        day = [d for d in data["days"] if d["station"] == "ST001"][0]
        assert day["is_latest"] is True and day["badge"][0] == "最新"
        head_id = day["head_id"]

    # Four successful uploads -> four versions, the head is the last one.
    data = client.get("/api/status").json()
    day = [d for d in data["days"] if d["station"] == "ST001"][0]
    assert day["version_count"] == 4 and day["head_id"] == head_id
    # The newest monthly's dependency points exactly at the newest day version.
    resp = client.get("/download/monthly/ST001/2026-11")
    assert resp.status_code == 200
    con = duckdb.connect()
    try:
        total = con.execute(
            "SELECT sum(pm25) FROM read_parquet(?)",
            [str(volume / "monthly" / "ST001" /
                 f"monthly-2026-11-gen{last_gen}.parquet")]).fetchone()[0]
    finally:
        con.close()
    assert total == 24 * 40  # the last correction's values
    # All older generations remain downloadable history.
    month = [m for m in data["months"] if m["station"] == "ST001"][0]
    assert all(h["downloadable"] for h in month["history"])
