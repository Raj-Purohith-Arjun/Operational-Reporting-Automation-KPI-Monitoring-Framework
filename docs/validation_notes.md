# Validation Notes

Run date: 2026-03-27 UTC

## 1) Before vs after cleaning

- Raw case rows: 3927
- Duplicate rows removed: 151
- Clean case rows: 3776
- Missing `resolved_at` in raw: 218
- Open case count after clean logic: 212
- Invalid duration records after parse checks: 193

## 2) Readiness checks

- Case file lag hours: 0.0
- Agent file lag hours: 0.0
- Readiness warnings on standard run: none

## 3) Sanity checks from pipeline

- PASS: no negative handle seconds
- PASS: SLA/open-case percent in range 0-100
- PASS: duplicate drop did not increase row count
- PASS: created_ts parse null rate acceptable at 0.00%

## 4) Failure simulation tests

- `SIMULATE_PARTIAL_ARRIVAL=true python src/generate_data.py` + `PARTIAL_ARRIVAL_POLICY=warn python src/pipeline.py`
  - expected result: pipeline runs with warning and writes warning into checks.
- `PARTIAL_ARRIVAL_POLICY=fail python src/pipeline.py`
  - expected result: `DataReadinessError` for partial arrival and non-zero exit.
- schema drift test by removing `priority` column from `cases_raw.csv`
  - expected result: `DataReadinessError` for missing required column.
- `MAX_FILE_LAG_HOURS=-1 python src/pipeline.py`
  - expected result: delayed file readiness failure.

## 5) KPI range reasonability

- `sla_breach_pct` and `open_case_pct` constrained to 0-100 and validated.
- `avg_handle_seconds` remains positive because invalid/negative durations are flagged and outlier-capped values are non-negative.
- weekend volume stays lower by generation design.
