# Architecture Diagram (simple)

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
