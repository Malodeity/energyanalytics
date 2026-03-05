import json
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def main() -> None:
    random.seed(42)

    start_date = date(2025, 1, 1)
    end_date = date(2025, 12, 31)
    days = (end_date - start_date).days + 1  # 365

    total_rows = 10_000
    egregious_rows = 1_959  # REQUIRED

    # We generate exactly total_rows as "base valid" rows, with:
    # 1 reading per meter per day.
    # Distribute rows across days:
    per_day = total_rows // days
    remainder = total_rows % days

    # days 1..remainder get (per_day + 1) rows, the rest get per_day rows
    meters_high = per_day + 1
    meters_low = per_day
    days_high = remainder  # number of days with meters_high

    # Build meter+customer catalog up to meters_high
    # m-001..m-{meters_high}, c-001..c-{meters_high}
    meter_defs = []
    for i in range(1, meters_high + 1):
        meter_id = f"m-{i:03d}"
        customer_id = f"c-{i:03d}"
        plan_code = "RES-2025-A" if i % 2 == 1 else "RES-2025-B"
        hh, mm = (2, 10) if plan_code == "RES-2025-A" else (3, 30)
        meter_defs.append((meter_id, customer_id, plan_code, hh, mm))

    readings = []
    reading_num = 1001

    # ---- 1) Generate BASE rows (valid) ----
    day_index = 0
    for d in daterange(start_date, end_date):
        day_index += 1
        meters_today = meters_high if day_index <= days_high else meters_low

        for i in range(meters_today):
            meter_id, customer_id, plan_code, hh, mm = meter_defs[i]

            # kwh: float with 1 decimal
            if plan_code == "RES-2025-A":
                kwh = round(random.uniform(8.0, 35.0), 1)
            else:
                kwh = round(random.uniform(6.0, 18.0), 1)

            # occasional spike for anomaly testing
            if (reading_num - 1001 + 1) % 120 == 0:
                kwh = round(kwh * 3.0, 1)

            ts = datetime(d.year, d.month, d.day, hh, mm, 0, tzinfo=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

            readings.append(
                {
                    "reading_id": f"r-{reading_num}",
                    "meter_id": meter_id,
                    "customer_id": customer_id,
                    "plan_code": plan_code,
                    "reading_date": d.strftime("%Y-%m-%d"),
                    "kwh": kwh,
                    "source_ts": ts,
                }
            )
            reading_num += 1

    assert len(readings) == total_rows, f"Expected total_rows={total_rows}, got {len(readings)}"

    # ---- 2) Replace EXACTLY 7,959 rows with egregious rows ----
    # These are designed to be:
    # - valid JSON objects
    # - still stageable in Postgres (no invalid date strings)
    # - rejected by finalize rules (missing fields / nulls / negative kwh)
    bad_indices = random.sample(range(total_rows), egregious_rows)

    bad_templates = [
        # Missing reading_date (NULL) -> rejected
        lambda rid: {
            "reading_id": rid,
            "meter_id": "m-004",
            "customer_id": "c-04",
            "plan_code": "RES-2025-B",
            "reading_date": None,
            "kwh": 7.2,
            "source_ts": "2025-12-31T03:30:00Z",
        },
        # Negative kwh -> rejected
        lambda rid: {
            "reading_id": rid,
            "meter_id": "m-005",
            "customer_id": "c-05",
            "plan_code": "RES-2025-A",
            "reading_date": "2025-12-31",
            "kwh": -1.0,
            "source_ts": "2025-12-31T02:10:00Z",
        },
        # Missing meter_id -> rejected (empty string is stageable)
        lambda rid: {
            "reading_id": rid,
            "meter_id": "",
            "customer_id": "c-01",
            "plan_code": "RES-2025-A",
            "reading_date": "2025-12-31",
            "kwh": 10.0,
            "source_ts": "2025-12-31T02:10:00Z",
        },
        # Missing customer_id -> rejected
        lambda rid: {
            "reading_id": rid,
            "meter_id": "m-001",
            "customer_id": "",
            "plan_code": "RES-2025-A",
            "reading_date": "2025-12-31",
            "kwh": 10.0,
            "source_ts": "2025-12-31T02:10:00Z",
        },
        # Missing plan_code -> rejected
        lambda rid: {
            "reading_id": rid,
            "meter_id": "m-002",
            "customer_id": "c-02",
            "plan_code": "",
            "reading_date": "2025-12-31",
            "kwh": 11.4,
            "source_ts": "2025-12-31T03:30:00Z",
        },
        # Missing kwh -> rejected (NULL is stageable)
        lambda rid: {
            "reading_id": rid,
            "meter_id": "m-003",
            "customer_id": "c-03",
            "plan_code": "RES-2025-A",
            "reading_date": "2025-12-31",
            "kwh": None,
            "source_ts": "2025-12-31T02:10:00Z",
        },
    ]

    for idx_i, idx in enumerate(bad_indices):
        rid = readings[idx]["reading_id"]  # keep reading_id unique + stable
        tmpl = bad_templates[idx_i % len(bad_templates)]
        readings[idx] = tmpl(rid)

    assert len(readings) == total_rows, f"Expected total_rows={total_rows}, got {len(readings)}"

    payload = {
        "readings": readings,
        "page": 1,
        "page_size": total_rows,
        "total_pages": 1,
    }

    out_path = Path(__file__).resolve().parent / "meter_readings_200k_2025_7959_bad.json"
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Wrote {total_rows} total rows to: {out_path}")
    print(f"Egregious rows: {egregious_rows} (replaced within total)")
    print(f"Date range (base rows): {start_date} to {end_date}")
    print(f"Distribution: per_day={per_day}, remainder_days={remainder}")
    print("Guarantee: no duplicate (meter_id, reading_date) in the base valid data.")


if __name__ == "__main__":
    main()