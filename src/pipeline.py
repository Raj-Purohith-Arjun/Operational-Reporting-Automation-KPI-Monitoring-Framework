from __future__ import annotations

import csv
import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


RAW_CASES = Path("data/raw/cases_raw.csv")
RAW_AGENTS = Path("data/raw/agents_raw.csv")
INGEST_MANIFEST = Path("data/raw/ingestion_manifest.json")
PROCESSED_DIR = Path("data/processed")

TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%d-%m-%Y %H:%M",
    "%Y-%m-%d",
]

REQUIRED_CASE_COLUMNS = {
    "case_id",
    "customer_id",
    "site_code",
    "channel",
    "priority",
    "created_at",
    "handle_seconds",
    "sla_breached",
}

REQUIRED_AGENT_COLUMNS = {"agent_id", "team", "manager", "tenure_months"}


class DataReadinessError(Exception):
    pass


def parse_mixed_ts(val: str):
    if not val:
        return None
    for fmt in TIMESTAMP_FORMATS:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            pass
    return None


def to_int_safe(val: str):
    try:
        return int(float(val))
    except Exception:
        return None


def ingest():
    if not RAW_CASES.exists() or not RAW_AGENTS.exists():
        raise FileNotFoundError("Raw input files missing. Run src/generate_data.py first.")

    with open(RAW_CASES, newline="", encoding="utf-8") as f:
        case_reader = csv.DictReader(f)
        case_cols = set(case_reader.fieldnames or [])
        missing_case_cols = REQUIRED_CASE_COLUMNS - case_cols
        if missing_case_cols:
            raise DataReadinessError(f"Schema drift in cases file, missing columns: {sorted(missing_case_cols)}")
        cases = list(case_reader)

    with open(RAW_AGENTS, newline="", encoding="utf-8") as f:
        agent_reader = csv.DictReader(f)
        agent_cols = set(agent_reader.fieldnames or [])
        missing_agent_cols = REQUIRED_AGENT_COLUMNS - agent_cols
        if missing_agent_cols:
            raise DataReadinessError(f"Schema drift in agents file, missing columns: {sorted(missing_agent_cols)}")
        agents = list(agent_reader)

    return cases, agents


def assess_data_readiness(cases, agents):
    warnings = []

    # 1) delayed files check
    max_lag_hours = int(os.getenv("MAX_FILE_LAG_HOURS", "30"))
    now_utc = datetime.now(timezone.utc).timestamp()
    case_lag_hours = round((now_utc - RAW_CASES.stat().st_mtime) / 3600, 2)
    agent_lag_hours = round((now_utc - RAW_AGENTS.stat().st_mtime) / 3600, 2)
    if case_lag_hours > max_lag_hours or agent_lag_hours > max_lag_hours:
        raise DataReadinessError(
            f"Delayed file arrival: cases lag={case_lag_hours}h, agents lag={agent_lag_hours}h, limit={max_lag_hours}h"
        )

    # 2) partial arrival check (manifest-driven when available)
    partial_policy = os.getenv("PARTIAL_ARRIVAL_POLICY", "warn").lower()  # warn | fail
    if INGEST_MANIFEST.exists():
        manifest = json.loads(INGEST_MANIFEST.read_text(encoding="utf-8"))
        expected_case_rows = manifest.get("expected_case_rows")
        if isinstance(expected_case_rows, int) and len(cases) < expected_case_rows * 0.90:
            msg = f"Partial arrival likely: got {len(cases)} rows, expected around {expected_case_rows}"
            if partial_policy == "fail":
                raise DataReadinessError(msg)
            warnings.append("WARN: " + msg)
    else:
        # fallback heuristic if no manifest from upstream
        if len(cases) < 1500:
            msg = f"Low case volume ({len(cases)}) and no ingestion manifest; may be partial load"
            if partial_policy == "fail":
                raise DataReadinessError(msg)
            warnings.append("WARN: " + msg)

    # 3) basic non-empty check
    if not cases or not agents:
        raise DataReadinessError("Empty raw extract received")

    return {
        "case_file_lag_hours": case_lag_hours,
        "agent_file_lag_hours": agent_lag_hours,
        "readiness_warnings": warnings,
    }


def clean(cases, agents):
    audit = {
        "raw_case_rows": len(cases),
        "raw_case_missing_resolved": sum(1 for r in cases if not r.get("resolved_at")),
        "raw_case_missing_agent": sum(1 for r in cases if not r.get("agent_id")),
    }

    deduped = []
    seen = set()
    for row in cases:
        key = (row.get("case_id"), row.get("created_at"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row.copy())

    audit["raw_case_duplicates"] = len(cases) - len(deduped)

    agents_map = {a["agent_id"]: a for a in agents}
    agents_map["A999"] = {
        "agent_id": "A999",
        "team": "unknown",
        "shift": "unknown",
        "manager": "unknown",
        "tenure_months": "0",
    }

    cleaned = []
    now_ref = datetime.now()

    handle_values = [to_int_safe(r.get("handle_seconds", "")) for r in deduped]
    handle_values = [v for v in handle_values if v is not None]
    handle_values.sort()
    p99_idx = int(0.99 * (len(handle_values) - 1)) if handle_values else 0
    p99 = handle_values[p99_idx] if handle_values else 0

    for r in deduped:
        row = r.copy()
        created_ts = parse_mixed_ts(row.get("created_at", ""))
        resolved_ts = parse_mixed_ts(row.get("resolved_at", ""))

        if not row.get("agent_id"):
            row["agent_id"] = "A999"

        open_case = 0
        if resolved_ts is None:
            resolved_ts = now_ref
            open_case = 1

        resolution_seconds = None
        invalid_duration = 0
        if created_ts is None:
            invalid_duration = 1
        else:
            delta = (resolved_ts - created_ts).total_seconds()
            if delta < 0:
                invalid_duration = 1
            else:
                resolution_seconds = int(delta)

        handle_seconds = to_int_safe(row.get("handle_seconds", ""))
        handle_capped = min(handle_seconds, p99) if handle_seconds is not None else None

        if not row.get("issue_type"):
            row["issue_type"] = "unknown"

        row["created_ts"] = created_ts.strftime("%Y-%m-%d %H:%M:%S") if created_ts else ""
        row["resolved_ts"] = resolved_ts.strftime("%Y-%m-%d %H:%M:%S") if resolved_ts else ""
        row["resolution_seconds"] = "" if resolution_seconds is None else str(resolution_seconds)
        row["invalid_duration_flag"] = str(invalid_duration)
        row["is_open_case"] = str(open_case)
        row["handle_seconds_capped"] = "" if handle_capped is None else str(handle_capped)

        agent = agents_map.get(row["agent_id"], agents_map["A999"])
        row["team"] = agent.get("team", "unknown")
        row["manager"] = agent.get("manager") or "unknown"

        cleaned.append(row)

    audit.update(
        {
            "clean_case_rows": len(cleaned),
            "clean_missing_resolution_seconds": sum(1 for r in cleaned if r["resolution_seconds"] == ""),
            "clean_invalid_duration_flag": sum(1 for r in cleaned if r["invalid_duration_flag"] == "1"),
            "open_case_count": sum(1 for r in cleaned if r["is_open_case"] == "1"),
        }
    )

    return cleaned, audit


def transform(cases):
    daily_grp = defaultdict(
        lambda: {
            "total_cases": 0,
            "sla_sum": 0,
            "handle_sum": 0,
            "handle_cnt": 0,
            "frt_sum": 0,
            "frt_cnt": 0,
            "open_cases": 0,
            "p1_cases": 0,
        }
    )

    agent_grp = defaultdict(lambda: {"handled_cases": 0, "handle_sum": 0, "handle_cnt": 0, "sla_sum": 0})

    for r in cases:
        if not r.get("created_ts"):
            continue
        created_date = r["created_ts"][:10]
        site = r.get("site_code", "unknown")
        dkey = (created_date, site)

        daily_grp[dkey]["total_cases"] += 1
        daily_grp[dkey]["sla_sum"] += to_int_safe(r.get("sla_breached", "0")) or 0
        daily_grp[dkey]["open_cases"] += to_int_safe(r.get("is_open_case", "0")) or 0
        if r.get("priority") == "P1":
            daily_grp[dkey]["p1_cases"] += 1

        h = to_int_safe(r.get("handle_seconds_capped", ""))
        if h is not None:
            daily_grp[dkey]["handle_sum"] += h
            daily_grp[dkey]["handle_cnt"] += 1

        f = to_int_safe(r.get("first_response_seconds", ""))
        if f is not None:
            daily_grp[dkey]["frt_sum"] += f
            daily_grp[dkey]["frt_cnt"] += 1

        akey = (created_date, r.get("agent_id", "A999"), r.get("team", "unknown"), r.get("manager", "unknown"))
        agent_grp[akey]["handled_cases"] += 1
        agent_grp[akey]["sla_sum"] += to_int_safe(r.get("sla_breached", "0")) or 0
        if h is not None:
            agent_grp[akey]["handle_sum"] += h
            agent_grp[akey]["handle_cnt"] += 1

    daily_rows = []
    for (day, site), v in sorted(daily_grp.items()):
        total = v["total_cases"]
        sla_pct = (100 * v["sla_sum"] / total) if total else 0
        open_pct = (100 * v["open_cases"] / total) if total else 0

        daily_rows.append(
            {
                "created_date": day,
                "site_code": site,
                "total_cases": str(total),
                "p1_case_mix_pct": f"{(100 * v['p1_cases'] / total):.2f}" if total else "0.00",
                "sla_breach_pct": f"{sla_pct:.2f}",
                "sla_status": "red" if sla_pct >= 12 else ("yellow" if sla_pct >= 8 else "green"),
                "avg_handle_seconds": f"{(v['handle_sum'] / v['handle_cnt']):.2f}" if v["handle_cnt"] else "",
                "avg_first_response_seconds": f"{(v['frt_sum'] / v['frt_cnt']):.2f}" if v["frt_cnt"] else "",
                "open_cases": str(v["open_cases"]),
                "open_case_pct": f"{open_pct:.2f}",
                "backlog_status": "red" if open_pct >= 18 else ("yellow" if open_pct >= 10 else "green"),
            }
        )

    agent_rows = []
    for (day, agent_id, team, manager), v in sorted(agent_grp.items()):
        handled = v["handled_cases"]
        agent_rows.append(
            {
                "created_date": day,
                "agent_id": agent_id,
                "team": team,
                "manager": manager,
                "handled_cases": str(handled),
                "avg_handle_seconds": f"{(v['handle_sum'] / v['handle_cnt']):.2f}" if v["handle_cnt"] else "",
                "sla_breach_pct": f"{(100 * v['sla_sum'] / handled):.2f}" if handled else "0.00",
            }
        )

    return daily_rows, agent_rows


def validate(cases, daily_rows, audit):
    checks = []

    neg_handle = any((to_int_safe(r.get("handle_seconds_capped", "0")) or 0) < 0 for r in cases)
    checks.append("FAIL: negative handle seconds found" if neg_handle else "PASS: no negative handle seconds")

    in_range = True
    for r in daily_rows:
        try:
            x = float(r["sla_breach_pct"])
            y = float(r["open_case_pct"])
            if x < 0 or x > 100 or y < 0 or y > 100:
                in_range = False
                break
        except Exception:
            in_range = False
            break
    checks.append("PASS: SLA/open-case percent in range 0-100" if in_range else "FAIL: percent metrics out of range")

    checks.append(
        "PASS: duplicate drop did not increase row count"
        if audit["raw_case_rows"] >= audit["clean_case_rows"]
        else "FAIL: row count increased unexpectedly"
    )

    created_null = sum(1 for r in cases if not r.get("created_ts"))
    null_rate = created_null / len(cases) if cases else 0
    if null_rate > 0.02:
        checks.append(f"WARN: created_ts parse null rate high at {null_rate:.2%}")
    else:
        checks.append(f"PASS: created_ts parse null rate acceptable at {null_rate:.2%}")

    return checks


def simulate_failures(cases):
    mode = os.getenv("SIMULATE_FAILURE", "none").lower()
    if mode == "missing_input":
        raise FileNotFoundError("Simulated missing input from upstream extract")
    if mode == "schema_change":
        if not cases or "priority" not in cases[0]:
            raise ValueError("Simulated schema mismatch: priority column missing")
        raise ValueError("Simulated schema mismatch: unexpected datatype in priority")
    if mode == "zero_rows":
        if not cases:
            raise ValueError("No rows available after ingest")
        raise ValueError("Simulated empty partition received from source")


def write_csv(path: Path, rows):
    if not rows:
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def export(cases, daily, agent_perf, audit, checks, readiness):
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    write_csv(PROCESSED_DIR / "cases_cleaned.csv", cases)
    write_csv(PROCESSED_DIR / "kpi_daily_snapshot.csv", daily)
    write_csv(PROCESSED_DIR / "agent_productivity_daily.csv", agent_perf)

    payload = {"audit": audit, "validation_checks": checks, "readiness": readiness}
    with open(PROCESSED_DIR / "data_quality_audit.json", "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    # status file used by Airflow branch task
    has_fail = any(x.startswith("FAIL") for x in checks)
    with open(PROCESSED_DIR / "publish_decision.json", "w", encoding="utf-8") as f:
        json.dump({"publish_allowed": not has_fail, "generated_at_utc": datetime.utcnow().isoformat()}, f)


def run_pipeline():
    cases_raw, agents_raw = ingest()
    simulate_failures(cases_raw)
    readiness = assess_data_readiness(cases_raw, agents_raw)
    cases_clean, audit = clean(cases_raw, agents_raw)
    daily, agent_perf = transform(cases_clean)
    checks = validate(cases_clean, daily, audit)
    checks.extend(readiness.get("readiness_warnings", []))
    export(cases_clean, daily, agent_perf, audit, checks, readiness)

    print("Pipeline done")
    print(json.dumps(audit, indent=2))
    for c in checks:
        print(f" - {c}")


if __name__ == "__main__":
    run_pipeline()
