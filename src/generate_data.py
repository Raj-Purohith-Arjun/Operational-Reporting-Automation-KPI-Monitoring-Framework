import csv
import json
import os
import random
from datetime import datetime, timedelta
from pathlib import Path

SEED = 42
random.seed(SEED)


def make_timestamp(base_dt: datetime, style: str) -> str:
    if style == "iso":
        return base_dt.strftime("%Y-%m-%d %H:%M:%S")
    if style == "slash":
        return base_dt.strftime("%m/%d/%Y %H:%M")
    if style == "dayfirst":
        return base_dt.strftime("%d-%m-%Y %H:%M")
    if style == "date_only":
        return base_dt.strftime("%Y-%m-%d")
    return base_dt.strftime("%Y-%m-%d %H:%M:%S")


def weighted_choice(items, weights):
    return random.choices(items, weights=weights, k=1)[0]


def build_cases(start_date: str = "2026-01-01", days: int = 60):
    start = datetime.strptime(start_date, "%Y-%m-%d")

    channels = ["chat", "email", "phone", "callback"]
    issue_types = ["delivery_delay", "refund", "damaged_item", "wrong_item", "account", "payment"]
    priorities = ["P1", "P2", "P3"]
    sites = ["PHX1", "DFW2", "JFK5", "SEA3"]

    rows = []
    case_counter = 100000

    for d in range(days):
        day = start + timedelta(days=d)
        weekday = day.weekday()
        volume = random.randint(60, 95) if weekday < 5 else random.randint(22, 45)

        for _ in range(volume):
            created = day + timedelta(
                hours=random.randint(6, 22),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59),
            )
            channel = weighted_choice(channels, [0.35, 0.28, 0.30, 0.07])
            issue = random.choice(issue_types)
            priority = weighted_choice(priorities, [0.15, 0.5, 0.35])
            site = random.choice(sites)

            base_ranges = {
                "chat": (220, 720),
                "email": (520, 1600),
                "phone": (300, 980),
                "callback": (420, 1280),
            }
            handle_seconds = random.randint(*base_ranges[channel])

            if random.random() < 0.018:
                handle_seconds = random.randint(6000, 22000)
            
            first_response_seconds = max(5, int(handle_seconds * random.uniform(0.15, 0.65)))
            backlog_pressure = 1.0 if weekday < 5 else 0.7
            sla_seconds = 24 * 3600 if priority == "P1" else (48 * 3600 if priority == "P2" else 72 * 3600)
            resolution_seconds = int(handle_seconds * random.uniform(1.1, 2.8) * backlog_pressure)
            if random.random() < 0.12:
                resolution_seconds = int(sla_seconds * random.uniform(1.1, 1.8))

            resolved_at = created + timedelta(seconds=resolution_seconds)

            breach = 1 if resolution_seconds > sla_seconds else 0
            

            rows.append(
                {
                    "case_id": f"C{case_counter}",
                    "customer_id": f"U{random.randint(5000, 19000)}",
                    "site_code": site,
                    "channel": channel,
                    "issue_type": issue,
                    "priority": priority,
                    "created_at": created,
                    "first_response_seconds": str(first_response_seconds),
                    "handle_seconds": str(handle_seconds),
                    "resolved_at": resolved_at,
                    "sla_target_seconds": str(sla_seconds),
                    "sla_breached": str(breach),
                    "agent_id": f"A{random.randint(100, 179)}",
                }
            )
            case_counter += 1

    for row in rows:
        if random.random() < 0.08:
            row["first_response_seconds"] = ""
        if random.random() < 0.06:
            row["resolved_at"] = ""
        if random.random() < 0.05:
            row["agent_id"] = ""
        if random.random() < 0.05:
            row["issue_type"] = ""

    stamp_styles = ["iso", "slash", "dayfirst", "date_only"]
    style_weights = [0.72, 0.14, 0.09, 0.05]
    for row in rows:
        if isinstance(row["created_at"], datetime):
            row["created_at"] = make_timestamp(row["created_at"], weighted_choice(stamp_styles, style_weights))
        if isinstance(row["resolved_at"], datetime):
            row["resolved_at"] = make_timestamp(row["resolved_at"], weighted_choice(stamp_styles, style_weights))

    dup_count = int(len(rows) * 0.04)
    duplicates = random.sample(rows, dup_count)
    rows.extend(duplicates)

    # optional scenario: mimic partial upstream arrival (for ops testing)
    if os.getenv("SIMULATE_PARTIAL_ARRIVAL", "false").lower() == "true":
        rows = rows[: int(len(rows) * 0.62)]

    random.shuffle(rows)
    return rows


def build_agent_roster():
    manager_pool = ["M-Rita", "M-Javier", "M-Kim", "M-Anika"]
    rows = []
    for a in range(100, 180):
        manager = random.choice(manager_pool)
        if random.random() < 0.06:
            manager = ""
        rows.append(
            {
                "agent_id": f"A{a}",
                "team": random.choice(["T1_returns", "T2_delivery", "T3_accounts"]),
                "shift": random.choice(["day", "swing", "night"]),
                "manager": manager,
                "tenure_months": str(max(1, int(random.gauss(16, 9)))),
            }
        )
    return rows


def write_csv(path: Path, rows, fieldnames):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    cases = build_cases()
    agents = build_agent_roster()

    write_csv(out_dir / "cases_raw.csv", cases, list(cases[0].keys()))
    write_csv(out_dir / "agents_raw.csv", agents, list(agents[0].keys()))

    # manifest helps pipeline detect partial file arrivals
    # intentionally set expected rows from historical estimate, not exact current rows
    simulated_partial = os.getenv("SIMULATE_PARTIAL_ARRIVAL", "false").lower() == "true"
    if simulated_partial:
        expected_case_rows = int((len(cases) / 0.62) / 0.96)  # mimic upstream expecting full batch
    else:
        expected_case_rows = int(len(cases) / 0.96)  # rough estimate before dedupe noise
    manifest = {
        "generated_at_utc": datetime.utcnow().isoformat(),
        "expected_case_rows": expected_case_rows,
        "notes": "Expected rows are estimate, not exact. Small variance is normal.",
    }
    (out_dir / "ingestion_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Wrote {len(cases)} rows to data/raw/cases_raw.csv")
    print(f"Wrote {len(agents)} rows to data/raw/agents_raw.csv")
    print("Wrote data/raw/ingestion_manifest.json")


if __name__ == "__main__":
    main()
