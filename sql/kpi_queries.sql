-- RCX KPI SQL pack (production-leaning version)
-- Main idea: push business logic into SQL so dashboards + downstream models read same definitions.
-- Assumed source objects:
--   fact_cases_raw            : cleaned event-level case table landed daily
--   dim_agents                : agent attributes (team, manager, tenure)
--   dim_site_sla_thresholds   : site+priority SLA bad/warn thresholds by effective date

-- =====================================================================
-- Base model: enrich and standardize in SQL (join with agent + thresholds)
-- =====================================================================
WITH case_enriched AS (
    SELECT
        f.case_id,
        f.customer_id,
        f.site_code,
        f.channel,
        COALESCE(NULLIF(f.issue_type, ''), 'unknown') AS issue_type,
        f.priority,
        CAST(f.created_ts AS TIMESTAMP) AS created_ts,
        CAST(f.resolved_ts AS TIMESTAMP) AS resolved_ts,
        CAST(f.first_response_seconds AS DOUBLE) AS first_response_seconds,
        CAST(f.handle_seconds_capped AS DOUBLE) AS handle_seconds_capped,
        CAST(f.sla_breached AS INTEGER) AS sla_breached,
        CAST(f.is_open_case AS INTEGER) AS is_open_case,
        COALESCE(NULLIF(f.agent_id, ''), 'A999') AS agent_id,
        COALESCE(a.team, 'unknown') AS team,
        COALESCE(a.manager, 'unknown') AS manager,
        COALESCE(a.tenure_months, 0) AS tenure_months,
        t.warn_threshold_pct,
        t.bad_threshold_pct,

        -- segmentation done in SQL instead of Python for consistency across tools
        CASE
            WHEN f.priority = 'P1' THEN 'critical'
            WHEN f.priority = 'P2' THEN 'high'
            ELSE 'standard'
        END AS priority_segment,

        CASE
            WHEN f.channel IN ('chat', 'phone') THEN 'realtime'
            WHEN f.channel = 'email' THEN 'async'
            ELSE 'scheduled'
        END AS channel_segment,

        CASE
            WHEN COALESCE(a.tenure_months, 0) < 6 THEN 'new_hire'
            WHEN COALESCE(a.tenure_months, 0) < 18 THEN 'mid_tenure'
            ELSE 'senior'
        END AS tenure_segment
    FROM fact_cases_raw f
    LEFT JOIN dim_agents a
        ON COALESCE(NULLIF(f.agent_id, ''), 'A999') = a.agent_id
    -- complex join: threshold selected by site + priority + effective date range
    LEFT JOIN dim_site_sla_thresholds t
        ON f.site_code = t.site_code
       AND f.priority = t.priority
       AND DATE(f.created_ts) BETWEEN t.effective_start_dt AND COALESCE(t.effective_end_dt, DATE '2999-12-31')
)
SELECT *
FROM case_enriched;


-- =====================================================================
-- KPI 1: SLA Breach % by site/priority/day + status bands
-- =====================================================================
WITH daily_sla AS (
    SELECT
        DATE(created_ts) AS report_date,
        site_code,
        priority_segment,
        COUNT(*) AS total_cases,
        ROUND(100.0 * AVG(CASE WHEN sla_breached = 1 THEN 1 ELSE 0 END), 2) AS sla_breach_pct,
        MAX(COALESCE(warn_threshold_pct, 8.0)) AS warn_threshold_pct,
        MAX(COALESCE(bad_threshold_pct, 12.0)) AS bad_threshold_pct
    FROM case_enriched
    GROUP BY 1,2,3
)
SELECT
    report_date,
    site_code,
    priority_segment,
    total_cases,
    sla_breach_pct,
    CASE
        WHEN sla_breach_pct >= bad_threshold_pct THEN 'red'
        WHEN sla_breach_pct >= warn_threshold_pct THEN 'yellow'
        ELSE 'green'
    END AS sla_status
FROM daily_sla;


-- =====================================================================
-- KPI 2: Leading indicator - 7d first response trend with acceleration
-- Why leading: response delays usually increase before SLA misses show up
-- =====================================================================
WITH daily_frt AS (
    SELECT
        DATE(created_ts) AS report_date,
        site_code,
        channel_segment,
        ROUND(AVG(first_response_seconds), 2) AS avg_frt_sec
    FROM case_enriched
    WHERE first_response_seconds IS NOT NULL
    GROUP BY 1,2,3
),
trend AS (
    SELECT
        report_date,
        site_code,
        channel_segment,
        avg_frt_sec,
        ROUND(
            AVG(avg_frt_sec) OVER (
                PARTITION BY site_code, channel_segment
                ORDER BY report_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ), 2
        ) AS frt_7d_ma,
        LAG(avg_frt_sec, 1) OVER (
            PARTITION BY site_code, channel_segment
            ORDER BY report_date
        ) AS prev_day_frt_sec
    FROM daily_frt
)
SELECT
    report_date,
    site_code,
    channel_segment,
    avg_frt_sec,
    frt_7d_ma,
    ROUND(avg_frt_sec - prev_day_frt_sec, 2) AS frt_day_delta_sec,
    CASE
        WHEN avg_frt_sec > frt_7d_ma * 1.20 THEN 'alert'
        WHEN avg_frt_sec > frt_7d_ma * 1.10 THEN 'watch'
        ELSE 'normal'
    END AS frt_signal
FROM trend;


-- =====================================================================
-- KPI 3: Backlog risk by site/channel/priority (decision KPI)
-- =====================================================================
SELECT
    DATE(created_ts) AS report_date,
    site_code,
    channel_segment,
    priority_segment,
    COUNT(*) AS total_cases,
    SUM(CASE WHEN is_open_case = 1 THEN 1 ELSE 0 END) AS open_cases,
    ROUND(100.0 * SUM(CASE WHEN is_open_case = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS open_case_pct,
    CASE
        WHEN ROUND(100.0 * SUM(CASE WHEN is_open_case = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) >= 18 THEN 'red'
        WHEN ROUND(100.0 * SUM(CASE WHEN is_open_case = 1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) >= 10 THEN 'yellow'
        ELSE 'green'
    END AS backlog_status
FROM case_enriched
GROUP BY 1,2,3,4;


-- =====================================================================
-- KPI 4: Productivity with fairness segmentation
-- =====================================================================
SELECT
    DATE(created_ts) AS report_date,
    site_code,
    team,
    tenure_segment,
    agent_id,
    COUNT(*) AS handled_cases,
    ROUND(AVG(handle_seconds_capped), 2) AS avg_handle_sec,
    ROUND(100.0 * AVG(CASE WHEN sla_breached = 1 THEN 1 ELSE 0 END), 2) AS sla_breach_pct,
    DENSE_RANK() OVER (
        PARTITION BY DATE(created_ts), site_code, team
        ORDER BY COUNT(*) DESC
    ) AS handled_rank_in_team_site
FROM case_enriched
GROUP BY 1,2,3,4,5;


-- =====================================================================
-- KPI 5: Complexity mix (to avoid wrong action on throughput)
-- =====================================================================
SELECT
    DATE(created_ts) AS report_date,
    site_code,
    CASE
        WHEN issue_type IN ('payment', 'account') OR priority_segment = 'critical' THEN 'complex'
        WHEN issue_type IN ('refund', 'delivery_delay') THEN 'medium'
        ELSE 'simple'
    END AS case_complexity,
    COUNT(*) AS case_count,
    ROUND(
        100.0 * COUNT(*)
        / NULLIF(SUM(COUNT(*)) OVER (PARTITION BY DATE(created_ts), site_code), 0),
        2
    ) AS pct_of_site_day
FROM case_enriched
GROUP BY 1,2,3;

-- Notes / edge cases:
-- 1) NULLIF protects division for zero-volume edge days.
-- 2) COALESCE/NULLIF protects sparse agent and issue fields.
-- 3) Threshold defaults applied when site threshold dim is missing.
-- 4) Trend KPI can be noisy first 6 days (short MA window by design).
