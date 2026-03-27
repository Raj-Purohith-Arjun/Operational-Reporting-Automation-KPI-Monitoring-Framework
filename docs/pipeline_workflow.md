# Pipeline Workflow Diagram

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
