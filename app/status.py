"""Read models powering the web dashboard and the JSON status API.

Freshness is *derived*, never trusted from a stored flag alone:

* a station-day's head version is the newest **accepted** upload
  (``csv_saved=1``); it is 最新 only when that exact version is ``ready`` and
  current, otherwise it shows 待补算 / 失败 / 已被取代;
* a station-month's stored ``current`` generation is 最新 only while it covers
  every newest accepted day version of that month. While a newer correction is
  still building (or failed) the old complete file stays downloadable but the
  page shows 待补算 / 失败 and never marks it 最新.
"""
from __future__ import annotations

from pathlib import Path

from . import db

# UI badge order matters for the colour coding in the template.
MONTHLY_BADGE = {
    "current": ("最新", "ok"),
    "building": ("补算中", "busy"),
    "failed": ("失败", "bad"),
    "superseded": ("已过期(可下载旧版)", "stale"),
    "obsolete": ("作业过期(由后续接算)", "stale"),
    # Derived effective states (a stored generation seen through newer data).
    "pending_latest": ("待补算(旧完整文件可下载)", "busy"),
    "failed_latest": ("最新更正失败(旧完整文件可下载)", "bad"),
}
DAILY_BADGE = {
    "ready": ("最新", "ok"),
    "building": ("待补算", "busy"),
    "failed": ("失败", "bad"),
    "obsolete": ("已被取代", "stale"),
}
JOB_BADGE = {
    "pending": ("排队", "busy"),
    "running": ("运行中", "busy"),
    "done": ("完成", "ok"),
    "failed": ("失败", "bad"),
    "obsolete": ("过期作废", "stale"),
}


def _file_ready(path: str | None) -> bool:
    return bool(path and Path(path).is_file())


def _job_for(jobs: list[dict], kind: str, ref_id: int | None) -> dict | None:
    if ref_id is None:
        return None
    return next(
        (j for j in jobs if j["kind"] == kind and j["ref_id"] == ref_id
         and j["status"] in ("pending", "running", "failed")),
        None,
    )


def list_overview() -> dict:
    with db.get_conn() as conn:
        dailies = [dict(r) for r in conn.execute(
            "SELECT * FROM daily_versions WHERE csv_saved=1 "
            "ORDER BY station, day, id DESC"
        ).fetchall()]
        monthlies = [dict(r) for r in conn.execute(
            "SELECT * FROM monthly_generations ORDER BY station, month, id DESC"
        ).fetchall()]
        jobs = [dict(r) for r in conn.execute(
            "SELECT * FROM jobs ORDER BY id DESC LIMIT 50"
        ).fetchall()]
        # Newest accepted version id per station-day (the "最新更正" anchor).
        head_ids = {
            (r["station"], r["day"]): r["id"]
            for r in conn.execute(
                "SELECT station, day, MAX(id) AS id FROM daily_versions "
                "WHERE csv_saved=1 GROUP BY station, day"
            ).fetchall()
        }

    # Group generations per station-month (rows already id-desc).
    gens_by_sm: dict[tuple[str, str], list[dict]] = {}
    for g in monthlies:
        gens_by_sm.setdefault((g["station"], g["month"]), []).append(g)

    # Newest accepted daily version per station-day, plus current versions.
    versions_by_sd: dict[tuple[str, str], list[dict]] = {}
    for d in dailies:  # id-desc per station/day
        versions_by_sd.setdefault((d["station"], d["day"]), []).append(d)

    days_by_sm: dict[tuple[str, str], list[dict]] = {}
    for d in dailies:
        days_by_sm.setdefault((d["station"], d["day"][:7]), []).append(d)

    month_rows: list[dict] = []
    for (station, month), gens in sorted(gens_by_sm.items()):
        latest_gen = gens[0]
        stored_current = next((g for g in gens if g["status"] == "current"), None)

        # The newest accepted version of every station-day in this month.
        month_versions = days_by_sm.get((station, month), [])
        head_versions = [
            versions_by_sd[(st, dy)][0]
            for (st, dy), hid in head_ids.items()
            if st == station and dy[:7] == month
            and (st, dy) in versions_by_sd
        ]
        head_versions.sort(key=lambda v: v["day"])
        head_id_set = {v["id"] for v in head_versions}
        heads_ready = all(v["status"] == "ready" and v["is_current"] == 1
                          for v in head_versions) and bool(head_versions)
        any_head_failed = any(v["status"] == "failed" for v in head_versions)
        any_head_obsolete = any(v["status"] == "obsolete" for v in head_versions)

        # Which stored generation (if any) actually covers all head versions.
        covering_gen = None
        if stored_current is not None and head_versions:
            with db.get_conn() as conn:
                dep_ids = {
                    r["daily_id"] for r in conn.execute(
                        "SELECT daily_id FROM monthly_dependencies WHERE generation_id=?",
                        (stored_current["id"],),
                    ).fetchall()
                }
            if head_id_set and head_id_set <= dep_ids:
                covering_gen = stored_current

        is_latest = covering_gen is not None and heads_ready
        if is_latest:
            effective_status = "current"
        elif any_head_failed or latest_gen["status"] == "failed":
            effective_status = "failed_latest"
        else:
            effective_status = "pending_latest"

        # The downloadable file: prefer the true latest, otherwise offer the
        # newest complete (stored current/superseded) file as a fallback.
        download_row = covering_gen if covering_gen is not None else (
            next((g for g in gens
                  if g["status"] in ("current", "superseded")
                  and _file_ready(g["parquet_path"])), None)
        )
        current_days = [
            d for d in month_versions if d["is_current"] and d["status"] == "ready"
        ]
        active_job = _job_for(jobs, "monthly_build", latest_gen["id"])
        month_rows.append({
            "station": station,
            "month": month,
            "badge": MONTHLY_BADGE[effective_status],
            "effective_status": effective_status,
            "stored_status": latest_gen["status"],
            "latest_status": effective_status,  # JSON-friendly alias for pollers
            "latest_id": latest_gen["id"],
            "error": latest_gen["error"],
            "is_latest": is_latest,
            "download_id": download_row["id"] if download_row else None,
            "download_is_latest": download_row is covering_gen and is_latest,
            "download_ready": _file_ready(
                download_row["parquet_path"] if download_row else None),
            "job": active_job,
            "day_count": len(current_days),
            "row_count": (covering_gen or stored_current or latest_gen)["row_count"],
            "history": [
                {
                    "id": g["id"],
                    "status": g["status"],
                    "badge": MONTHLY_BADGE[g["status"]],
                    "error": g["error"],
                    "created_at": g["created_at"],
                    "published_at": g["published_at"],
                    "downloadable": _file_ready(g["parquet_path"])
                        and g["status"] in ("current", "superseded"),
                }
                for g in gens
            ],
        })

    daily_rows: list[dict] = []
    for (station, day), versions in sorted(versions_by_sd.items()):
        head = versions[0]  # newest accepted upload (id-desc)
        head_is_current = head["status"] == "ready" and head["is_current"] == 1
        cur = next((v for v in versions if v["is_current"] == 1), None)
        # Fallback download: an older complete file while the head is pending.
        fallback = None if head_is_current else next(
            (v for v in versions
             if v["status"] == "ready" and _file_ready(v["parquet_path"])), None)
        daily_rows.append({
            "station": station,
            "day": day,
            "version_count": len(versions),
            "head_id": head["id"],
            "current_id": cur["id"] if cur else None,
            "badge": DAILY_BADGE[head["status"]],
            "head_status": head["status"],
            "is_latest": head_is_current,
            "row_count": head["row_count"] if head["status"] == "ready" else (
                cur["row_count"] if cur else None),
            "error": head["error"],
            "download_ready": head_is_current
                and _file_ready(head["parquet_path"]),
            "fallback_id": fallback["id"] if fallback else None,
            "fallback_ready": _file_ready(
                fallback["parquet_path"] if fallback else None),
            "job": _job_for(jobs, "daily_build", head["id"]),
        })

    return {
        "months": month_rows,
        "days": daily_rows,
        "jobs": jobs,
        "job_badges": JOB_BADGE,
        "failed_jobs": [j for j in jobs if j["status"] == "failed"],
    }
