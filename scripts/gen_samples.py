"""Generate the correction sample series for station ST001.

Run:  python3 scripts/gen_samples.py
All files cover the same UTC day (2026-11-10). Each later file is a full
replacement, exactly matching the upload contract:

* ``samples/before``  - the original upload (24 hourly rows)
* ``samples/after``   - first correction (4 readings fixed + 2 half-hour rows)
* ``samples/recheck`` - second correction on top of "after" (evening peak
  re-checked after instrument recalibration), used to demo continuous
  corrections and the rule that an old job can never roll the day file back.
"""
from __future__ import annotations

import csv
import datetime as dt
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAY = dt.datetime(2026, 11, 10, tzinfo=dt.timezone.utc)
STATION = "ST001"


def ts(hour: int, minute: int = 0) -> int:
    return int((DAY.replace(hour=hour, minute=minute)).timestamp())


# Hourly readings; the first two and the evening peak values are wrong in the
# original file (instrument zero-drift), corrected below.
before_values = {
    0: 8, 1: 9, 2: 7, 3: 6, 4: 7, 5: 12, 6: 28, 7: 45,
    8: 52, 9: 47, 10: 38, 11: 33, 12: 30, 13: 29, 14: 31,
    15: 36, 16: 44, 17: 61, 18: 72, 19: 68, 20: 55, 21: 39,
    22: 24, 23: 15,
}
corrections = {0: 12, 1: 13, 18: 64, 19: 60}
after_values = {**before_values, **corrections}
# Two half-hour samples missing from the original upload.
after_extra = {ts(7, 30): 48, ts(12, 30): 31}
# Second correction: recalibration trims the evening peak a little more.
recheck_values = {**after_values, 17: 58, 18: 62, 19: 57, 20: 52}


def write_csv(path: Path, values: dict[int, int]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["station", "utc_seconds", "pm25"])
        for sec, pm in sorted(values.items()):
            writer.writerow([STATION, sec, pm])


def main() -> None:
    before = {ts(h): pm for h, pm in before_values.items()}
    after = {ts(h): pm for h, pm in after_values.items()}
    after.update(after_extra)
    recheck = {ts(h): pm for h, pm in recheck_values.items()}
    recheck.update(after_extra)

    before_path = ROOT / "samples" / "before" / f"{STATION}-2026-11-10.csv"
    after_path = ROOT / "samples" / "after" / f"{STATION}-2026-11-10.csv"
    recheck_path = ROOT / "samples" / "recheck" / f"{STATION}-2026-11-10.csv"
    write_csv(before_path, before)
    write_csv(after_path, after)
    write_csv(recheck_path, recheck)
    print(f"wrote {before_path} ({len(before)} rows)")
    print(f"wrote {after_path} ({len(after)} rows)")
    print(f"wrote {recheck_path} ({len(recheck)} rows)")


if __name__ == "__main__":
    main()
