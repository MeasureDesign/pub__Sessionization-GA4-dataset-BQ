/* ================================================================================== 
Query Title: GA4 L0 sessions — Model 1: First Event Available (FEV)
Description: This query builds a session-level table from the GA4 BigQuery export, 
taking traffic source attribution from the first event in a session that carries 
attribution parameters.
==================================================================================
Anna Horáková | MeasureDesign | www.measuredesign.cz | anna.horakova@measuredesign.cz

Usage: Use this model to sessionize GA4 export data when the session_start event 
cannot be relied on — typically for historical data before 2023-11 (when session_start 
did not yet carry attribution parameters) or for properties with a high share of 
sessions without a session_start event. Attribution (source / medium / campaign / 
content / term) is taken from the first event that contains any non-empty value, 
ordered by actual_timestamp with event_bundle_sequence_id as a tie-breaker. Sessions 
with no attribution signal fall back to direct / none.

The query writes into a partitioned target table via MERGE INTO — the target table 
must be created beforehand (see the commented-out CREATE block). When scheduling, do 
NOT set a destination table; the MERGE handles writes internally. Replace the project, 
dataset and UDF references before running.

For questions or contributions, please send us an e-mail: info@measuredesign.cz 
Or submit an issue on our GitHub repository.
================================================================================== */
-- Your SQL query starts here


/* ============================================================================
   GA4 L0 SESSIONS — Model 1: First Event Available (FEV)
   ============================================================================
   Builds a session-level table from GA4 raw events by extracting traffic source 
   attribution from the FIRST event in the session that contains attribution 
   parameters (source / medium / campaign / content / term).

   USE CASE:
     - Historical data before 2023-11-02 (session_start event had no params)
     - Clients with high % of sessions without session_start 
     - Mid-session attribution changes (e.g. SPA URL changes with UTM)

   ATTRIBUTION LOGIC:
     - Order events chronologically by actual_timestamp (with safe fallback to 
       event_timestamp) and event_bundle_sequence_id as tie-breaker.
     - Take the first event whose concatenated (source*medium*campaign*content*term)
       has at least one non-empty value.
     - If no event has attribution → direct / none fallback.

   OUTPUT TABLE: GA4_L0_web_sessions_consent (partitioned by session_date)

   NOTES:
     - This query uses MERGE INTO DML; the destination table must be created 
       beforehand (see initial CREATE OR REPLACE block, commented out).
     - When scheduling, do NOT set a destination table — MERGE handles writes 
       internally.
   ============================================================================ */

/* DYNAMIC DATE FOR ALL QUERIES — defaults: yesterday and the day before
   (2-day window handles sessions crossing midnight + late-arriving data) */
DECLARE start_date DATE DEFAULT CURRENT_DATE()-2;
DECLARE end_date DATE DEFAULT CURRENT_DATE()-1;

/* STATIC DATE FOR ALL QUERIES — uncomment for backfill / one-off runs */
--DECLARE start_date DATE DEFAULT DATE '2024-05-16';
--DECLARE end_date DATE DEFAULT DATE '2025-05-17';


/* ----------------------------------------------------------------------------
   INITIAL TABLE CREATION (run once, then comment out)
   ----------------------------------------------------------------------------
   Uncomment and run this section ONLY when creating the table for the first time.
   After initial creation, comment it out and use the MERGE statement below.
   ---------------------------------------------------------------------------- */
/* CREATE TABLE `<PROJECT_ID>.<DATASET_ID>.GA4_L0_web_sessions_consent`
   PARTITION BY session_date
   AS
*/


/* ============================================================================
   MAIN MERGE STATEMENT
   ----------------------------------------------------------------------------
   Inserts new sessions and updates existing ones if attribution changed 
   (handles late-arriving data and corrections).
   ============================================================================ */
MERGE INTO `<PROJECT_ID>.<DATASET_ID>.GA4_L0_web_sessions_consent` t USING (

  /* --------------------------------------------------------------------------
     CTE 1: RAW_DATA
     --------------------------------------------------------------------------
     Reads raw GA4 events from the export tables and extracts the fields needed
     for sessionization. Key design decisions:
       - event_timestamp kept as INT64 microseconds for accurate sorting 
         (string-formatted timestamps lose microsecond resolution).
       - actual_timestamp resolved via safe_actual_ts_ms UDF, which falls back 
         to event_timestamp if the client-side timestamp is invalid.
       - event_traffic_sources is a concatenation of (source*medium*campaign*
         content*term); empty concat ("****") is replaced with NULL so we can 
         later pick the first event with actual attribution.
     -------------------------------------------------------------------------- */
  WITH raw_data AS (
    SELECT
      PARSE_DATE('%Y%m%d', event_date) AS session_date,

      -- Keep as INT64 microseconds for deterministic ordering; format only on output
      event_timestamp AS event_timestamp_micros,

      -- UDF needs to be created before
      -- Safe actual_timestamp: returns NULL/event_timestamp fallback if client
      -- timestamp is implausible (timestamp drift on user devices)
      `<PROJECT_ID>.UDF_all.safe_actual_ts_ms`(
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'actual_timestamp' LIMIT 1),
        event_timestamp
      ) AS actual_timestamp_micros,

      event_bundle_sequence_id,

      user_pseudo_id,

      -- Composite unique session ID — ga_session_id alone is NOT unique across users
      CONCAT(
        user_pseudo_id, "-",
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
      ) AS unique_session_id,

      event_name,

      -- Concatenate attribution params with '*' delimiter for easy "has any value" detection.
      -- '****' (all empty) is normalized to NULL in the next CTE.
      CONCAT(
        IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source'),   ''), '*',
        IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium'),   ''), '*',
        IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign'), ''), '*',
        IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content'),  ''), '*',
        IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term'),     '')
      ) AS event_traffic_sources

    FROM `<PROJECT_ID>.analytics_12345.events_*`
    WHERE 1=1
      AND REGEXP_EXTRACT(_table_suffix, r'[0-9]+') 
          BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
      AND user_pseudo_id IS NOT NULL
  ),


  /* --------------------------------------------------------------------------
     CTE 2: TRAFFIC_SOURCES
     --------------------------------------------------------------------------
     Normalize empty concat ('****' = all five attribution params empty) to 
     NULL. This allows the next CTE to find the first event WITH attribution 
     via standard NULL-aware window functions.
     -------------------------------------------------------------------------- */
  traffic_sources AS (
    SELECT
      * EXCEPT(event_traffic_sources),
      CASE 
        WHEN event_traffic_sources = '****' THEN NULL 
        ELSE event_traffic_sources 
      END AS event_traffic_sources
    FROM raw_data
  ),


  /* --------------------------------------------------------------------------
     CTE 3: SESSION_ATTRIBUTION
     --------------------------------------------------------------------------
     For each session, pick the first event (chronologically) that has any 
     attribution params. Uses FIRST_VALUE with IGNORE NULLS for clean 
     "first non-null in window" semantics.

     Ordering priority:
       1. actual_timestamp (with event_timestamp fallback inside safe UDF)
       2. event_bundle_sequence_id (tie-breaker for events in the same batch)

     Then deduplicates to one row per session via ROW_NUMBER ordered the same way.
     -------------------------------------------------------------------------- */
  session_attribution AS (
    SELECT
      *,
      FIRST_VALUE(event_traffic_sources IGNORE NULLS) OVER (
        PARTITION BY unique_session_id 
        ORDER BY 
          IFNULL(actual_timestamp_micros, event_timestamp_micros) ASC,
          event_bundle_sequence_id ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ) AS session_traffic_concat,
      ROW_NUMBER() OVER (
        PARTITION BY unique_session_id 
        ORDER BY 
          IFNULL(actual_timestamp_micros, event_timestamp_micros) ASC,
          event_bundle_sequence_id ASC
      ) AS rn
    FROM traffic_sources
  ),


  /* --------------------------------------------------------------------------
     CTE 4: SPLIT_DATA
     --------------------------------------------------------------------------
     Split the '*'-delimited attribution concat back into individual columns.
     Keep only one row per session (rn = 1 = chronologically first event).
     -------------------------------------------------------------------------- */
  split_data AS (
    SELECT
      session_date,
      unique_session_id,
      event_timestamp_micros,
      actual_timestamp_micros,
      SPLIT(session_traffic_concat, '*')[SAFE_OFFSET(0)] AS session_source_raw,
      SPLIT(session_traffic_concat, '*')[SAFE_OFFSET(1)] AS session_medium_raw,
      SPLIT(session_traffic_concat, '*')[SAFE_OFFSET(2)] AS session_campaign_raw,
      SPLIT(session_traffic_concat, '*')[SAFE_OFFSET(3)] AS session_content_raw,
      SPLIT(session_traffic_concat, '*')[SAFE_OFFSET(4)] AS session_term_raw
    FROM session_attribution
    WHERE rn = 1
  ),


  /* --------------------------------------------------------------------------
     CTE 5: FINAL_ATTRIBUTION
     --------------------------------------------------------------------------
     Apply direct/none fallback for sessions with no attribution at all.
     Format timestamps for output (string, Europe/Prague timezone).
     -------------------------------------------------------------------------- */
  final_attribution AS (
    SELECT
      session_date,
      unique_session_id,

      -- Format timestamps for output (timezone-aware)
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(event_timestamp_micros), 'Europe/Prague') AS event_timestamp,
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(actual_timestamp_micros), 'Europe/Prague') AS actual_timestamp,

      -- Direct/none fallback when session has no attribution at all
      CASE WHEN session_source_raw IS NULL OR session_source_raw = '' THEN '(direct)' ELSE session_source_raw END AS session_source,
      CASE WHEN session_medium_raw IS NULL OR session_medium_raw = '' THEN '(none)'   ELSE session_medium_raw END AS session_medium,
      CASE WHEN session_campaign_raw IS NULL OR session_campaign_raw = '' THEN 'N/A'  ELSE session_campaign_raw END AS session_campaign,
      CASE WHEN session_content_raw IS NULL OR session_content_raw = ''  THEN 'N/A'   ELSE session_content_raw END AS session_content,
      CASE WHEN session_term_raw IS NULL OR session_term_raw = ''        THEN 'N/A'   ELSE session_term_raw END AS session_term,

      CURRENT_TIMESTAMP() AS export_timestamp,
      CURRENT_TIMESTAMP() AS last_update_timestamp

    FROM split_data
  )

  SELECT * FROM final_attribution

) s

ON t.unique_session_id = s.unique_session_id 
   AND t.session_date BETWEEN start_date AND end_date


/* ----------------------------------------------------------------------------
   WHEN MATCHED: Update existing sessions if attribution changed
   ----------------------------------------------------------------------------
   Uses IS DISTINCT FROM to correctly handle NULL comparisons.
   Only updates the final attribution columns + last_update_timestamp.
   ---------------------------------------------------------------------------- */
WHEN MATCHED AND (
       t.session_source   IS DISTINCT FROM s.session_source
    OR t.session_medium   IS DISTINCT FROM s.session_medium
    OR t.session_campaign IS DISTINCT FROM s.session_campaign
    OR t.session_content  IS DISTINCT FROM s.session_content
    OR t.session_term     IS DISTINCT FROM s.session_term
)
THEN
  UPDATE SET
    t.session_source         = s.session_source,
    t.session_medium         = s.session_medium,
    t.session_campaign       = s.session_campaign,
    t.session_content        = s.session_content,
    t.session_term           = s.session_term,
    t.last_update_timestamp  = CURRENT_TIMESTAMP()


/* ----------------------------------------------------------------------------
   WHEN NOT MATCHED: Insert new sessions
   ---------------------------------------------------------------------------- */
WHEN NOT MATCHED THEN INSERT (
  session_date, unique_session_id, event_timestamp, actual_timestamp,
  session_source, session_medium, session_campaign, session_content, session_term,
  export_timestamp, last_update_timestamp
)
VALUES (
  s.session_date, s.unique_session_id, s.event_timestamp, s.actual_timestamp,
  s.session_source, s.session_medium, s.session_campaign, s.session_content, s.session_term,
  s.export_timestamp, s.last_update_timestamp
);
