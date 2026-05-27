/* ==================================================================================
Query Title: GA4 sessionization model comparison — Model 1 vs Model 2
Description: This query compares daily session counts between two sessionization 
models on the same GA4 BigQuery export — an existing first-event-based session table 
(Model 1) and a freshly-built session_start-based aggregation (Model 2) — and 
reports the relative difference per day.
==================================================================================
Anna Horáková | MeasureDesign | www.measuredesign.cz | anna.horakova@measuredesign.cz

Usage: This is a validation query used when evaluating or migrating between 
sessionization approaches. Model 1 (first_event) reads from an existing sessionized 
table that derives traffic source from the first event in each session. Model 2 
(session_start) is built from scratch directly in this query and reads traffic 
source straight from the session_start event — viable for data after 2023-11-02, 
when session_start began carrying the same attribution parameters as the first 
client-triggered event. Model 2 deduplicates rare duplicate session_start events 
via ROW_NUMBER, applies click-ID overrides (gclid, fbclid, msclid, hgtid), falls 
back to direct / (none) when no source/medium is present, and fills campaign from 
the URL utm_campaign when missing. 
The final SELECT joins both models on session_date only (channel breakdown 
intentionally omitted) and reports diff_pct = (Model 2 / Model 1 - 1) * 100. 
A positive diff means Model 2 sees more sessions than Model 1 on that day. Use a 
short static date window for ad-hoc validation; switch to the dynamic DECLARE block 
for scheduled monitoring. Replace the project, dataset and analytics property 
references before running. 
For questions or contributions, please send us an e-mail: info@measuredesign.cz 
Or submit an issue on our GitHub repository.
================================================================================== */

-- Date window: switch between dynamic (for scheduled monitoring) and static 
-- (for ad-hoc validation on a fixed period).

/* DYNAMIC DATE — for scheduled runs */
-- DECLARE start_date DATE DEFAULT CURRENT_DATE() - 2;
-- DECLARE end_date   DATE DEFAULT CURRENT_DATE() - 1;

/* STATIC DATE — for ad-hoc validation */
DECLARE start_date DATE DEFAULT DATE '2026-01-01';
DECLARE end_date   DATE DEFAULT DATE '2026-01-31';


WITH

-- Model 1: read pre-aggregated session counts from existing sessionized table.
-- Source attribution here comes from the first event in each session.
first_event AS (
  SELECT
    session_date,
    session_source,
    session_medium,
    session_campaign,
    COUNT(DISTINCT unique_sessionID) AS cnt_sessions_fev
  FROM `PROJECT_ID.DATASET_ID.GA4_export_sessions_consent`
  WHERE session_date BETWEEN start_date AND end_date
  GROUP BY ALL
),

-- Model 2 — step 1: extract raw session_start events from GA4 export and rank them
-- per unique_session_id. Ranking guards against rare duplicate session_start events
-- (GA4 occasionally emits more than one per session).
ss_raw_data AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY unique_session_id
      ORDER BY event_timestamp ASC, event_bundle_sequence_id ASC
    ) AS session_rank
  FROM (
    SELECT
      PARSE_DATE('%Y%m%d', event_date) AS session_date,
      -- unique_session_id = user_pseudo_id + ga_session_id; stable across the session
      CONCAT(
        user_pseudo_id, '-',
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
      ) AS unique_session_id,
      event_bundle_sequence_id,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'N/A') AS page_location,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source'),        'N/A') AS session_source,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium'),        'N/A') AS session_medium,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign'),      'N/A') AS session_campaign
    FROM `PROJECT_ID.analytics_ID.events_*`
    WHERE REGEXP_EXTRACT(_TABLE_SUFFIX, r'[0-9]+')
            BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
      AND event_name = 'session_start'
      AND user_pseudo_id IS NOT NULL
  )
),

-- Model 2 — step 2: apply attribution transformations.
-- Click-ID overrides take precedence over GA4's reported source/medium because
-- the click-ID is the most reliable signal of the actual paid channel.
ss_traffic_transform AS (
  SELECT
    session_date,
    unique_session_id,

    -- Source: click-ID wins; direct fallback when no source/medium present.
    CASE
      WHEN REGEXP_CONTAINS(page_location, r'gclid=')                           THEN 'google'
      WHEN REGEXP_CONTAINS(page_location, r'fbclid=')                          THEN 'facebook'
      WHEN REGEXP_CONTAINS(page_location, r'msclid=')                          THEN 'bing'
      WHEN REGEXP_CONTAINS(page_location, r'hgtid=')                           THEN 'heureka.cz'
      WHEN session_source = 'N/A' AND session_medium = 'N/A'                   THEN 'direct'
      ELSE session_source
    END AS session_source,

    -- Medium: gclid -> cpc; fbclid without campaign -> referral (organic share);
    -- empty source/medium -> (none) per GA4 convention.
    CASE
      WHEN REGEXP_CONTAINS(page_location, r'gclid=')                                          THEN 'cpc'
      WHEN REGEXP_CONTAINS(page_location, r'fbclid=') AND session_campaign = 'N/A'            THEN 'referral'
      WHEN session_source = 'N/A' AND session_medium = 'N/A'                                  THEN '(none)'
      ELSE session_medium
    END AS session_medium,

    -- Campaign: clear "(organic)" / "(referral)" when gclid is present (gclid means
    -- paid traffic, so an organic/referral campaign value is misattributed and we
    -- prefer to fill from utm_campaign in the next CTE).
    CASE
      WHEN REGEXP_CONTAINS(page_location, r'gclid=')
       AND session_campaign IN ('(organic)', '(referral)')                                    THEN 'N/A'
      ELSE session_campaign
    END AS session_campaign,

    -- Fallback campaign source: utm_campaign extracted from page URL.
    IFNULL(REGEXP_EXTRACT(page_location, r'(?i)utm_campaign=([^&]*)'), 'N/A') AS utm_campaign_url

  FROM ss_raw_data
  WHERE session_rank = 1  -- deduplicate sessions with multiple session_start events
),

-- Model 2 — step 3: final daily aggregation.
-- Campaign falls back to utm_campaign from URL when GA4-reported campaign is missing.
ss_final AS (
  SELECT
    session_date,
    session_source,
    session_medium,
    CASE
      WHEN session_campaign = 'N/A' THEN utm_campaign_url
      ELSE session_campaign
    END AS session_campaign,
    COUNT(DISTINCT unique_session_id) AS cnt_sessions_ss
  FROM ss_traffic_transform
  GROUP BY ALL
)

-- Final comparison: daily totals from both models, with relative difference.
-- Join is intentionally on session_date only — comparing total daily volumes,
-- not per-channel breakdowns. To compare per channel, add source/medium/campaign
-- to the JOIN clause.
SELECT
  FORMAT_DATE('%Y-%m-%d', ss.session_date) AS date,
  SUM(ss.cnt_sessions_ss)                  AS cnt_sessions_model2_session_start,
  SUM(fe.cnt_sessions_fev)                 AS cnt_sessions_model1_first_event,
  ROUND(
    ((SUM(ss.cnt_sessions_ss) / SUM(fe.cnt_sessions_fev)) - 1) * 100,
    2
  ) AS diff_pct
FROM ss_final ss
LEFT JOIN first_event fe
  ON ss.session_date = fe.session_date
GROUP BY ALL
ORDER BY date;
