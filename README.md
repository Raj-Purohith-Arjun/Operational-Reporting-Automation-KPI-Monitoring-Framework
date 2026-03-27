# Operational Reporting Automation - RCX KPI Monitoring Framework

This project is a practical RCX operations analytics setup for case handling performance.
It is focused on daily decisions: SLA risk, backlog pressure, and agent productivity.

---

##  Internal analysis (business first)

### 1) Business problem
RCX managers are seeing customer case queues fluctuate by day and channel, but they do not have one daily view that ties **SLA breaches + response delays + productivity** together. The result is late staffing reactions. Breaches are noticed after escalation, not before.

### 2) Stakeholders
- **RCX site managers (PHX1 / DFW2 / JFK5 / SEA3):** decide same-day staffing shifts.
- **Team managers:** rebalance case assignments when one queue overloads.
- **Ops program manager:** tracks week-over-week SLA stability and exception patterns.
- **Workforce planning analyst:** adjusts weekend staffing because weekend volume is lower but still variable.

### 3) Decisions this system supports
- Move agents between queues when SLA breach % crosses threshold.
- Trigger overtime or callback deflection when open-case % spikes.
- Assign complex cases to tenured agents when complexity mix shifts.
- Escalate tool/system incidents when response time jumps suddenly.

### 4) Metrics that really matter
- **SLA Breach %** (customer promise metric).
- **First Response Time** (leading signal of queue stress).
- **Open Case %** (backlog and repeat-contact risk).
- **Handled Cases per Agent + Team Rank** (productivity balance).
- **Case Complexity Mix** (workload quality, not just volume).

---

##  KPI definitions with action meaning

1. **KPI: SLA Breach % by site + priority**  
   **SQL logic:** breached cases / total by day/site/priority segment with threshold table join.  
   **Why:** direct signal of delayed customer resolution with local context (site standards differ).  
   **Action:** if red in P1 segment, manager reassigns senior agents same shift.

2. **KPI: First Response Trend Signal (leading KPI)**  
   **SQL logic:** daily avg first response + 7-day moving average + day delta using window functions.  
   **Why:** response lag increases before SLA breach spikes in many queues.  
   **Action:** if `avg_frt > 1.2 * frt_7d_ma`, trigger same-day queue triage before breach escalates.

3. **KPI: Backlog Risk % by site/channel/priority**  
   **SQL logic:** open-case percent segmented by site + channel + priority bands.  
   **Why:** open backlog growth predicts tomorrow's workload shock.  
   **Action:** if backlog red in realtime channels, pause non-critical tasks and clear queue.

4. **KPI: Productivity Rank (site/team fair ranking)**  
   **SQL logic:** handled count + DENSE_RANK partitioned by site/team/day.  
   **Why:** avoids unfair global ranking and gives manager actionable peer comparison.  
   **Action:** rebalance assignment inside same site/team, not across different sites.

5. **KPI: Complexity Mix %**  
   **SQL logic:** CASE segmentation into simple/medium/complex with site-day share.  
   **Why:** avoids wrong performance conclusions on high-complexity days.  
   **Action:** if complex share >35%, staffing plan shifts to higher tenure ratio.

---

##  Data design (realistic imperfections)

Raw datasets in `data/raw/`:
- `cases_raw.csv`
- `agents_raw.csv`
- `ingestion_manifest.json`

Imperfections intentionally included:
- **Missing values (5-10%)** on `resolved_at`, `first_response_seconds`, `agent_id`, `issue_type`.
- **Duplicate rows (~4%)** from repeated upstream extract pulls.
- **Inconsistent timestamp formats** (ISO, slash, day-first, date-only).
- **Outliers** in `handle_seconds` (6k-22k sec) from escalated investigations/system waits.
- **Weekly pattern** with lower weekend volume.
- **Manifest expected row estimate is intentionally rough**, not exact.

Why these happen in real life:
- partial sync failures,
- re-runs without dedupe keys,
- multiple tools exporting dates differently,
- true long-handle outliers during escalations,
- weekday/weekend demand swings,
- ingestion control file often based on estimates not final reconciled totals.

---

##  Architecture and production thinking

### Data sources
- Case handling extract (daily CSV snapshot)
- Agent roster export
- Ingestion manifest (expected row estimate)

### Transformation layer
- Python pipeline (`src/pipeline.py`) for ingest/clean/readiness checks.
- SQL layer (`sql/kpi_queries.sql`) as business logic authority for KPI semantics and enrichment joins.

### Storage layer
- Processed CSV tables + JSON audit in `data/processed/`

### Output
- Dashboard-ready KPI snapshots with status bands.

### Why move logic into SQL
- SQL is easier to audit by analysts + BI developers.
- One metric definition can be reused by dashboards and scheduled jobs.
- Joins and window trends are clearer and easier to review in SQL than hidden in app code.

### What could break
- upstream schema changes,
- delayed files beyond readiness lag window,
- partial arrivals (manifest mismatch),
- timestamp parse drift with unseen format.

### How outputs are validated
- readiness checks (file lag + partial arrival),
- schema checks,
- metric sanity checks,
- publish decision file used by Airflow branching.

---

##  SQL design (deeper)

Full SQL: `sql/kpi_queries.sql`.

What is improved:
- **Complex join** to `dim_site_sla_thresholds` on site+priority+effective date range.
- **Agent enrichment join** to `dim_agents` for team/manager/tenure segments.
- **Window function trend KPI** (7-day moving average + LAG delta).
- **Business segmentation** in SQL for priority/channel/tenure groups.
- **Threshold status flags** (green/yellow/red) in query output.

This keeps KPI meaning in one place and reduces dashboard-side logic drift.

---

##  Python pipeline realism

`src/pipeline.py` stages:
1. ingest
2. data readiness checks
3. clean
4. transform
5. validate
6. export

### Real-world issue simulation and handling
1. **Partial data arrival**
   - pipeline checks row count vs `ingestion_manifest.json` expected rows.
   - behavior controlled by `PARTIAL_ARRIVAL_POLICY` (`warn` or `fail`).

2. **Delayed files**
   - pipeline computes file lag and fails if above `MAX_FILE_LAG_HOURS` (default 30).

3. **Schema drift**
   - required columns validated at ingest; missing critical columns fail fast with clear message.

4. **Simulated failure modes**
   - `SIMULATE_FAILURE=missing_input|schema_change|zero_rows`.

### Why this is realistic
It does not assume perfect upstream delivery and separates **data readiness failures** from **metric quality failures**.

---

##  Airflow DAG (improved)

File: `dags/rcx_kpi_daily_dag.py`
- schedule: daily `03:30 UTC`
- retries: 2, retry delay: 15 min
- tasks:
  1. generate raw data
  2. run pipeline
  3. branch on `publish_decision.json`
  4. either `publish_outputs` or `skip_publish`

If validation fails, branch sends run to `skip_publish` so reporting refresh is blocked intentionally.

---

##  Validation and DQ

Checks implemented:
- no negative handle values
- SLA/open-case % between 0 and 100
- duplicate drop row-count sanity
- created_ts parse null-rate check
- readiness warnings (partial/load quality)

Checks still missing:
- site-level reconciliation to source system control totals
- outlier rate monitor by channel
- late-arriving update correction for previous dates
- automated incident notification integration

---

##  Dashboard logic (decision-driven)

### First panel (manager sees first)
`SLA status`, `Backlog status`, `First response signal`, `P1 mix` by site.

### Thresholds
- **SLA status:** green <8, yellow 8-12, red >=12
- **Backlog status:** green <10, yellow 10-18, red >=18
- **Response signal:** alert when today >120% of 7-day MA

### Alert conditions
- SLA red for any site in P1 segment
- backlog red in realtime channels
- response alert for 2 consecutive days

### Example scenario
If site DFW2 SLA breach rises **5% -> 12%** and response signal already alert:
1. manager pauses low-priority callback queue,
2. reassigns 2 senior agents to realtime queue,
3. flags program manager for next-shift staffing adjustment.

---

##  Key learnings + limitations

### Key learnings
- Most real effort is in readiness and data quality checks, not chart building.
- Segment-level KPIs reduce wrong blanket actions.
- SQL-owned metric logic lowers cross-tool definition drift.

### Limitations
- still synthetic data, not live RCX sources.
- no warehouse orchestration here (CSV outputs only).
- no true BI artifact included, only dashboard design logic.
- threshold values are operational starting points, not final calibrated policy.

---

## STEP 10 - Diagrams

- Architecture diagram: `docs/architecture_diagram.md`
- Pipeline flow diagram: `docs/pipeline_workflow.md`

### Architecture diagram (embedded)

```text
+-------------------+        +-------------------------+        +------------------------------+
| RCX Case Tool     |        | Agent Roster Export     |        | Ingestion Manifest           |
| (raw cases feed)  |        | (daily csv)             |        | (expected row estimate)      |
+---------+---------+        +------------+------------+        +--------------+---------------+
          |                               |                                      |
          +---------------+---------------+----------------------+---------------+
                          |                                      |
                          v                                      v
      +----------------------------------------+    +------------------------------+
      | Ingest + Readiness Layer (Python csv)  |    | SQL KPI Layer                |
      | - schema check                          |    | - joins with agents/targets  |
      | - delay check                           |    | - segments + trend windows   |
      | - partial arrival check                 |    +------------------------------+
      +-------------------+--------------------+                  |
                          |                                       |
                          v                                       v
      +----------------------------------------+      +-----------------------------+
      | Clean/Transform Layer                  |----->| KPI Outputs                 |
      | - dedupe                               |      | daily snapshot + productivity|
      | - mixed timestamp parse                |      +--------------+--------------+
      | - open case handling                   |                     |
      | - outlier cap                          |                     v
      +-------------------+--------------------+      +-----------------------------+
                          |                           | Dashboard + Alerts          |
                          v                           | red/yellow/green statuses   |
      +----------------------------------------+      +-----------------------------+
      | Storage Layer                          |
      | data/processed/*.csv + audit json      |
      +----------------------------------------+
```

### Pipeline workflow diagram (embedded)

```text
[Start Daily Run]
      |
      v
[Ingest raw CSV + manifest]
      |
      v
[Readiness checks]
 - delayed files?
 - partial arrival?
 - schema drift?
      |----> fail hard -> [Retry in 15 min] -> [Fail DAG after retry limit]
      |----> warn -> continue with warning in audit
      v
[Clean + Standardize]
  - remove duplicates
  - parse mixed timestamps
  - fill missing agent with A999
  - cap outliers
      |
      v
[Transform KPI Tables]
      |
      v
[Validation checks]
  - metric ranges
  - row count sanity
      |
      v
[Write publish_decision.json]
      |
      v
[Airflow branch]
  |---- publish_allowed=true  -> [Publish outputs]
  |---- publish_allowed=false -> [Skip publish + investigate]
```

---

## Risk analysis and self-review

### Most dangerous KPI if wrong
**SLA Breach %** is highest-risk if wrong. Underreported breach leads to delayed staffing response and customer promise failures.

### Wrong decisions if data is wrong
- false green SLA -> managers do not reallocate and queue degrades.
- false high backlog -> overreaction with unnecessary overtime costs.
- wrong productivity rank -> unfair performance coaching actions.

### What still looks imperfect (intentionally)
- readiness manifest is an estimate and can produce warnings.
- trend KPI is noisy for first week of data.
- fallback unknown agent bucket may hide roster sync issues if overused.

---

## Quickstart

```bash
python src/generate_data.py
python src/pipeline.py
```

Optional scenario tests:

```bash
SIMULATE_PARTIAL_ARRIVAL=true python src/generate_data.py
PARTIAL_ARRIVAL_POLICY=fail python src/pipeline.py
SIMULATE_FAILURE=schema_change python src/pipeline.py
```

Outputs in `data/processed/`:
- `cases_cleaned.csv`
- `kpi_daily_snapshot.csv`
- `agent_productivity_daily.csv`
- `data_quality_audit.json`
- `publish_decision.json`

---

## Tradeoffs and why we chose this design

### 1) Why SQL vs Python for KPI logic

**What we chose:** keep cleaning/readiness in Python, but keep KPI definition logic in SQL.

**Why:**
- SQL is easier for analysts, BI developers, and ops managers to inspect and challenge.
- Window trends and segmentation in SQL reduce dashboard-side duplicate logic.
- SQL definitions are easier to version and compare when metric meaning changes.

**Alternatives considered:**
- **All KPI logic in Python only**: not chosen because KPI meaning can get hidden in code and drift between dashboard and pipeline.
- **All KPI logic in BI tool**: not chosen because logic becomes hard to test, and multiple dashboards can diverge.

### 2) Why daily pipeline instead of hourly

**What we chose:** daily run at 03:30 UTC.

**Why:**
- source feeds in this project are daily snapshots, not event streams.
- manager actions here are mostly same-shift/day planning, not minute-by-minute routing.
- daily cadence keeps run stability and reduces alert fatigue while still being useful.

**Alternatives considered:**
- **Hourly micro-batches**: not chosen yet; higher infra/monitoring cost and higher false-alert risk before source reliability matures.
- **Weekly**: too slow for SLA/backlog interventions.

### 3) Why these thresholds now

**What we chose:**
- SLA status: <8 green, 8-12 yellow, >=12 red
- Backlog status: <10 green, 10-18 yellow, >=18 red
- Response signal alert: >120% of 7-day moving average

**Why:**
- these are operating guardrails to drive action quickly, not final statistical truth.
- calibrated to be sensitive enough to trigger action without paging every normal variation.
- they match current synthetic data behavior and expected day-to-day volatility.

**Alternatives considered:**
- **Very strict thresholds (e.g., SLA red at 8%)**: too noisy in early rollout.
- **Very loose thresholds (e.g., SLA red at 20%)**: slow detection, late intervention.
- **Fully dynamic anomaly model only**: not chosen for v1 because it is harder to explain to ops leads.

---

## KPI risk analysis (if KPI is wrong)

### SLA Breach %
- **If wrong low:** managers delay staffing moves; customer promise failures continue.
- **Failed decision:** no same-shift reallocation when queue is actually degrading.
- **How to detect:** compare daily aggregate against random sample of case-level audit and source control totals.

### First Response Trend Signal
- **If wrong high:** false alarm and unnecessary overtime/queue shuffling.
- **Failed decision:** team burns capacity on a fake queue problem.
- **How to detect:** backtest against actual next-day SLA movement; monitor alert precision/false positive rate.

### Open Case % / Backlog Status
- **If wrong low:** backlog risk hidden; next-day load shock not staffed.
- **Failed decision:** manager keeps normal staffing when carryover is high.
- **How to detect:** reconcile open-case count against next-day opening queue counts and unresolved ticket snapshots.

### Productivity Rank
- **If wrong:** unfair coaching/escalation on agents.
- **Failed decision:** wrong people moved off queues, damaging throughput and morale.
- **How to detect:** validate joins for agent/team mappings, and review rank shifts vs complexity mix before actioning.

### Complexity Mix %
- **If wrong low:** performance looks worse than reality on heavy-complexity days.
- **Failed decision:** manager applies productivity pressure instead of skill-based staffing.
- **How to detect:** weekly manual QA sample of issue categories and priority mapping rules.

---

## Iteration story (how this evolved)

### Initial version
- had core ETL flow and basic KPIs, but decisions were still somewhat generic.
- SQL existed but business alert framing was light.

### Issues discovered during review
- metric logic split between layers could still confuse ownership.
- readiness checks needed to reflect real delivery failures (partial, delayed, schema drift).
- dashboard guidance needed explicit action playbook per threshold.

### Improvements made
- moved deeper KPI semantics into SQL with joins, segments, thresholds, trend windows.
- added readiness gates and publish branching behavior.
- added clearer threshold/action language and risk framing in docs.

### Current state
- technically stable daily flow with explicit operational tradeoffs.
- still intentionally imperfect (manifest estimate noise, threshold tuning not final) to reflect real deployment maturity.

---

## Decision playbook (dashboard to action)

| KPI / Threshold | Trigger | Who acts | Action | Expected response time |
|---|---|---|---|---|
| SLA Breach % (red, >=12) | 1 day red in P1 segment OR 2 days yellow | Site manager | Reallocate ~15% workforce to affected queue; pause non-critical backlog tasks | Same shift (within 2-4 hours) |
| First Response Signal = alert | today >120% of 7d MA for realtime channels | Team manager | Pull cross-trained agents for queue triage and reduce callback commitments | Within 1-2 hours |
| Backlog status red (>=18) | backlog red at site/channel level | Site manager + workforce planning | Add overtime/shift extension and next-day staffing adjustment | Same day + next-day planning |
| Productivity rank anomaly | agent/team rank drops sharply with stable complexity | Team manager | Review routing and coaching only after data quality/complexity check | Within 1 business day |
| Complexity mix >35% complex | sustained 2 days | Ops program manager + team managers | Increase senior coverage, adjust expected throughput target | Next shift planning window |

---

## Efficiency and impact breakdown (manual effort reduction)

Before, manual effort was spread across:
- pulling files from multiple exports,
- manually deduping and standardizing timestamps,
- hand-calculating SLA/backlog ratios in sheets,
- building ad-hoc daily status emails/slides.

Now automated:
- raw generation/ingest checks,
- dedupe + parsing + quality flags,
- KPI table production + status bands,
- publish/skip decision output for orchestration.

Where time savings happen:
- less time cleaning data every day,
- less time re-validating formulas in spreadsheets,
- faster manager brief creation because status fields are pre-built.

Note: impact estimate should still be validated after live rollout with time tracking, not treated as final.

---

## Final realism check

Two things that can still fail in production:
1. **Threshold drift:** fixed thresholds may become stale during peak season changes and trigger wrong signal frequency.
2. **Upstream ID mapping issues:** if agent/team mappings lag, productivity rank decisions can be biased before anyone notices.

One thing I would redesign fully with more time:
- move from CSV artifacts to warehouse-native incremental models (with history/versioned dimensions) and make SQL models the single production source for dashboard + alerting + reconciliation.
