"""End-to-end demo against a running service: continuous corrections + retry.

Prerequisite: start the stack first (``docker compose up`` or
``DATA_VOLUME=./data uvicorn app.main:app``), then run::

    python3 scripts/demo.py
    # or against another base URL:
    BASE_URL=http://localhost:8000 python3 scripts/demo.py

The script never touches the data volume directly; it only uses the same HTTP
interface the web page uses, so every step is reproducible by hand in the UI.
It walks through:

1. original upload -> first daily/monthly file published;
2. an invalid upload -> rejected, no state change;
3. first correction, then immediately a second correction (continuous
   corrections); only the newest accepted version can be current;
4. forcing a failure and retrying: an old failed job can never roll back a
   newer, already-accepted correction.
"""
from __future__ import annotations

import io
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

BASE_URL = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")
ROOT = Path(__file__).resolve().parents[1]
SAMPLES = ROOT / "samples"


def _post_file(path: Path) -> tuple[int, str]:
    boundary = f"----demo{uuid.uuid4().hex}"
    data = path.read_bytes()
    body = b"".join([
        f"--{boundary}\r\n".encode(),
        f'Content-Disposition: form-data; name="file"; filename="{path.name}"\r\n'.encode(),
        b"Content-Type: text/csv\r\n\r\n",
        data,
        b"\r\n",
        f"--{boundary}--\r\n".encode(),
    ])
    req = urllib.request.Request(
        BASE_URL + "/upload", data=body, method="POST",
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
    )
    # Success and validation/save failures both answer with a 303 redirect.
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.headers.get("location", "")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers.get("location", "")


def _post(path: str) -> tuple[int, str]:
    req = urllib.request.Request(BASE_URL + path, data=b"", method="POST")
    # A 303 redirect is the normal response; urllib treats it as an error only
    # without a handler, so read it explicitly.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, *a, **k):
            return None

    opener = urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(req, timeout=15) as resp:
            return resp.status, resp.headers.get("location", "")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.headers.get("location", "")


def _status() -> dict:
    with urllib.request.urlopen(BASE_URL + "/api/status", timeout=15) as resp:
        import json
        return json.loads(resp.read().decode())


def wait_latest(station: str, month: str, after_gen: int | None = None,
                timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        last = _status()
        for m in last.get("months", []):
            if m["station"] == station and m["month"] == month and m["is_latest"]:
                if after_gen is None or (m["download_id"] or 0) > after_gen:
                    return last
        time.sleep(0.4)
    raise SystemExit(f"timeout waiting for {station} {month}: {last}")


def show(tag: str) -> None:
    data = _status()
    print(f"\n--- {tag} ---")
    for d in data.get("days", []):
        print(f"  日档 {d['station']} {d['day']}: 最新接收 v{d['head_id']} "
              f"状态={d['badge'][0]} 当前可用 v{d.get('current_id')} "
              f"最新={d['is_latest']}")
    for m in data.get("months", []):
        print(f"  月档 {m['station']} {m['month']}: 状态={m['badge'][0]} "
              f"可下载 gen={m['download_id']} 最新={m['is_latest']} "
              f"日档数={m['day_count']}")


def main() -> int:
    try:
        urllib.request.urlopen(BASE_URL + "/health", timeout=5)
    except OSError as exc:
        print(f"无法连接 {BASE_URL}（/health 失败）: {exc}\n"
              "请先启动服务：docker compose up --build", file=sys.stderr)
        return 1

    station, day, month = "ST001", "2026-11-10", "2026-11"
    before = SAMPLES / "before" / f"{station}-{day}.csv"
    after = SAMPLES / "after" / f"{station}-{day}.csv"
    recheck = SAMPLES / "recheck" / f"{station}-{day}.csv"
    invalid = SAMPLES / "invalid" / "ST001-bad.csv"

    print("== 1. 上传原始日档 ==")
    print("  upload", before.name, _post_file(before))
    wait_latest(station, month)
    show("原始日档已发布")

    print("\n== 2. 上传非法 CSV（重复时间戳 + 非整数），应被拒绝且状态不变 ==")
    code, loc = _post_file(invalid)
    print("  upload", invalid.name, "->", code, loc)
    show("非法上传后（状态应与上一步完全一致）")

    print("\n== 3. 连续两次更正：after 之后立即 recheck ==")
    print("  upload", after.name, _post_file(after))
    print("  upload", recheck.name, _post_file(recheck), "（不等待，模拟连续更正）")
    data = wait_latest(station, month)
    show("连续更正收敛后：只有最新接收版本能成为当前日档")
    head = next(d for d in data["days"] if d["station"] == station and d["day"] == day)
    assert head["is_latest"] and head["badge"][0] == "最新", head

    print("\n== 4. 重试失败作业：旧作业不得让日档/月文件退回旧值 ==")
    # Arm a one-shot transcription fault (only works when the server was
    # started with DEMO_FAULT=1). Then upload another correction: it will be
    # accepted (CSV saved) but its transcription fails once.
    fault_req = urllib.request.Request(BASE_URL + "/demo/arm-fault", data=b"", method="POST")
    fault_on = False
    try:
        with urllib.request.urlopen(fault_req, timeout=5) as resp:
            fault_on = resp.status == 200
    except urllib.error.HTTPError:
        fault_on = False
    except OSError:
        fault_on = False

    if fault_on:
        r = _post_file(recheck)
        print("  已开启演示故障，上传更正 ->", r)
        # Wait until the newest day shows a failed/pending state.
        deadline = time.time() + 15
        failed_job = None
        while time.time() < deadline:
            status = _status()
            head = next((d for d in status["days"]
                         if d["station"] == station and d["day"] == day), None)
            if head and head["badge"][0] == "失败":
                failed_job = head.get("job")
                break
            time.sleep(0.3)
        show("转写失败后：页面显示失败，旧完整日档/月文件仍可下载，但不标最新")
        if failed_job is None:
            failed_job = next((j for j in _status().get("failed_jobs", [])
                               if j["kind"] == "daily_build"), None)
        if failed_job:
            print(f"  重试失败的日作业 #{failed_job['id']} ...")
            print("  retry ->", _post(f"/jobs/{failed_job['id']}/retry"))
            wait_latest(station, month)
            show("重试成功后：当前日档/月文件为最新更正，未回退")
    else:
        print("  服务未开启 DEMO_FAULT，跳过故障注入。")
        failed = _status().get("failed_jobs", [])
        if not failed:
            print("  当前也没有失败作业（正常流程下更正都成功了）。")
        for job in failed:
            print(f"  retry job {job['id']} ->", _post(f"/jobs/{job['id']}/retry"))
        show("重试后：当前日档/月文件仍是最新更正，不回退")

    print("\n演示完成。打开", BASE_URL, "查看仪表盘，历史版本仍可下载。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
