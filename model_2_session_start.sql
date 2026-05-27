/* ================================================================================== 
Query Title: GA4 L0 sessions — Model 2: Session Start
Description: This query builds a session-level table from the GA4 BigQuery export, 
taking traffic source attribution directly from the session_start event.
==================================================================================
Anna Horáková | MeasureDesign | www.measuredesign.cz | anna.horakova@measuredesign.cz
Created On: 2026-05-16 | Last Updated: 2026-05-16
Usage: This is the default sessionization model for new implementations. Since 
2023-11-02 the automatically collected session_start event carries the same 
attribution parameters as the first client-triggered event, so attribution can be 
read straight from session_start without scanning every event in the session. 
Suitable for data after 2023-11-02 and for properties with a low share of sessions 
without a session_start event. The query deduplicates rare duplicate session_start 
events, applies click-ID overrides (gclid, fbclid, msclid, hgtid), falls back to 
direct / none when no source/medium is present, and fills campaign from the URL 
utm_campaign when missing.
The query writes into a partitioned target table via MERGE INTO — the target table 
must be created beforehand (see the commented-out CREATE block). When scheduling, do 
NOT set a destination table; the MERGE handles writes internally. Replace the project, 
dataset and UDF references before running.
For questions or contributions, please send us an e-mail: info@measuredesign.cz 
Or submit an issue on our GitHub repository.
================================================================================== */
-- Your SQL query starts here

/* ============================================================================
   GA4 L0 SESSIONS — Model 2: Session Start
   ============================================================================
   Builds a session-level table from GA4 raw events by extracting traffic source 
   attribution directly from the session_start event. Available for data after 
   2023-11-02 (when session_start started carrying the same event params as the 
   first client-triggered event in the session).

   USE CASE:
     - Default model for new client implementations
     - Data after 2023-11-02
     - Clients with low % of sessions without session_start 

   ATTRIBUTION LOGIC:
     - Take attribution params (source / medium / campaign / content / term) 
       directly from the session_start event.
     - Deduplicate via ROW_NUMBER ordered by actual_timestamp (with safe fallback 
       to event_timestamp) and event_bundle_sequence_id as tie-breaker.
     - Apply click-ID overrides (gclid, fbclid, msclid, hgtid) to fix common GA4 
       misattribution patterns.
     - Apply direct/none fallback when both source and medium are missing.
     - Fall back to utm_campaign from URL when GA4 reports campaign as N/A.

   OUTPUT TABLE: GA4_L0_web_sessions_consent (partitioned by session_date)

   NOTES:
     - This query uses MERGE INTO DML; the destination table must be created 
       beforehand (see initial CREATE OR REPLACE block, commented out).
     - When scheduling, do NOT set a destination table — MERGE handles writes 
       internally.
     - Compared to Model 1, this query only scans session_start events, which 
       is significantly cheaper.
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
     Reads session_start events only (filter pushed down to scan) and extracts 
     attribution params + URL for click-ID detection. Key design decisions:
       - event_timestamp kept as INT64 microseconds for accurate sorting 
         (string-formatted timestamps lose microsecond resolution).
       - actual_timestamp resolved via safe_actual_ts_ms UDF, which falls back 
         to event_timestamp if the client-side timestamp is invalid.
       - ga_session_number uses -1 as sentinel for missing values (intentional —
         filter out in downstream queries via WHERE ga_session_number > 0).
     -------------------------------------------------------------------------- */
  WITH raw_data AS (
    SELECT
      PARSE_DATE('%Y%m%d', event_date) AS session_date,

      -- Keep as INT64 microseconds for deterministic ordering; format only on output
      event_timestamp AS event_timestamp_micros,

      -- Safe actual_timestamp: returns event_timestamp fallback if client 
      -- timestamp is implausible (timestamp drift on user devices)
      `<PROJECT_ID>.UDF_all.safe_actual_ts_ms`(
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'actual_timestamp' LIMIT 1),
        event_timestamp
      ) AS actual_timestamp_micros,

      event_bundle_sequence_id,

      -- Composite unique session ID — ga_session_id alone is NOT unique across users
      CONCAT(
        user_pseudo_id, "-",
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
      ) AS unique_session_id,

      -- ga_session_number with -1 sentinel for missing values (intentional)
      IFNULL(
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number'),
        -1
      ) AS ga_session_number,

      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'N/A') AS page_location,

      -- Raw attribution params from session_start event (N/A sentinel for missing)
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source'),   'N/A') AS session_source,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium'),   'N/A') AS session_medium,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign'), 'N/A') AS session_campaign,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'term'),     'N/A') AS session_term,
      IFNULL((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content'),  'N/A') AS session_content

    FROM `<PROJECT_ID>.analytics_12345.events_*`
    WHERE 1=1
      AND REGEXP_EXTRACT(_table_suffix, r'[0-9]+') 
          BETWEEN FORMAT_DATE('%Y%m%d', start_date) AND FORMAT_DATE('%Y%m%d', end_date)
      AND event_name = "session_start"
      AND user_pseudo_id IS NOT NULL
  ),


  /* --------------------------------------------------------------------------
     CTE 2: SESSION_RANKED
     --------------------------------------------------------------------------
     Deduplicate sessions with multiple session_start events (rare but happens 
     in ~0.1 % of sessions). Keep only the first session_start per session.

     Ordering priority:
       1. actual_timestamp (with event_timestamp fallback inside safe UDF)
       2. event_bundle_sequence_id (tie-breaker for events in the same batch)
     -------------------------------------------------------------------------- */
  session_ranked AS (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY unique_session_id
        ORDER BY 
          IFNULL(actual_timestamp_micros, event_timestamp_micros) ASC,
          event_bundle_sequence_id ASC
      ) AS session_rank
    FROM raw_data
  ),


  /* --------------------------------------------------------------------------
     CTE 3: TRAFFIC_TRANSFORM
     --------------------------------------------------------------------------
     Apply business logic to correct/override traffic source attribution based 
     on URL click IDs. This handles cases where:
       - GA4 misattributes paid traffic as organic/referral
       - Click IDs in URL provide more accurate attribution
       - Direct traffic needs to be properly labeled

     Source override priority:
       1. gclid   → "google"
       2. fbclid  → "facebook"
       3. msclid  → "bing"
       4. hgtid   → "heureka.cz"
       5. No source AND no medium → "direct"
       6. Otherwise → keep original GA4 source
     -------------------------------------------------------------------------- */
  traffic_transform AS (
    SELECT
      session_date,
      event_timestamp_micros,
      actual_timestamp_micros,
      unique_session_id,
      ga_session_number,
      page_location,

      -- Keep original (untransformed) values for audit/debugging
      session_source   AS session_source_orig,
      session_medium   AS session_medium_orig,
      session_campaign AS session_campaign_orig,

      -- SOURCE transformation
      CASE
        WHEN REGEXP_CONTAINS(page_location, r'gclid=')  THEN "google"
        WHEN REGEXP_CONTAINS(page_location, r'fbclid=') THEN "facebook"
        WHEN REGEXP_CONTAINS(page_location, r'msclid=') THEN "bing"
        WHEN REGEXP_CONTAINS(page_location, r'hgtid=')  THEN "heureka.cz"
        WHEN session_source = "N/A" AND session_medium = "N/A" THEN "direct"
        ELSE session_source
      END AS session_source,

      -- MEDIUM transformation
      CASE
        WHEN REGEXP_CONTAINS(page_location, r'gclid=')  THEN "cpc"
        WHEN REGEXP_CONTAINS(page_location, r'fbclid=') AND session_campaign = "N/A" THEN "referral"
        WHEN session_source = "N/A" AND session_medium = "N/A" THEN "(none)"
        ELSE session_medium
      END AS session_medium,

      -- CAMPAIGN transformation — clear campaign when gclid is present but 
      -- campaign shows organic/referral (indicates GA4 misattribution)
      CASE
        WHEN REGEXP_CONTAINS(page_location, r'gclid=') 
             AND (session_campaign = "(organic)" OR session_campaign = "(referral)") 
        THEN "N/A"
        ELSE session_campaign
      END AS session_campaign,

      session_term,
      session_content,

      -- URL-extracted params for fallback / debugging
      IFNULL(REGEXP_EXTRACT(page_location, r'(?i)utm_campaign=([^&]*)'), "N/A") AS utm_campaign_url,
      IFNULL(REGEXP_EXTRACT(page_location, r'[?&]gclid=([^&]+)'),       "N/A") AS gclid_url
      -- ,IFNULL(REGEXP_EXTRACT(page_location, r'[?&]fbclid=([^&]+)'),    "N/A") AS fbclid_url
      -- ,IFNULL(REGEXP_EXTRACT(page_location, r'[?&]msclid=([^&]+)'),    "N/A") AS msclid_url

    FROM session_ranked
    WHERE session_rank = 1   -- keep only the first session_start per session
  ),


  /* --------------------------------------------------------------------------
     CTE 4: FINAL_ATTRIBUTION
     --------------------------------------------------------------------------
     Assemble final output:
       - URL fallback for campaign (when session_campaign is N/A but URL has utm_campaign).
         Note: fallback is applied only to campaign, not to source/medium. 
         Rationale: GA4 auto-collects utm_source/utm_medium into event_params, 
         so missing source/medium in event_params usually means they were not 
         in the URL either. Campaign fallback handles edge cases of legitimate 
         campaign tags that didn't propagate.
       - Format timestamps for output (string, Europe/Prague timezone).
     -------------------------------------------------------------------------- */
  final_attribution AS (
    SELECT
      session_date,
      unique_session_id,

      -- Format timestamps for output (timezone-aware)
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(event_timestamp_micros), 'Europe/Prague') AS event_timestamp,
      FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', TIMESTAMP_MICROS(actual_timestamp_micros), 'Europe/Prague') AS actual_timestamp,

      ga_session_number,
      page_location,

      session_source_orig,
      session_source,

      session_medium_orig,
      session_medium,

      session_campaign_orig,
      -- URL fallback for campaign only
      CASE
        WHEN session_campaign = "N/A" THEN utm_campaign_url
        ELSE session_campaign
      END AS session_campaign,

      session_term,
      session_content,

      utm_campaign_url,
      gclid_url,

      CURRENT_TIMESTAMP() AS export_timestamp,
      CURRENT_TIMESTAMP() AS last_update_timestamp

    FROM traffic_transform
  )

  SELECT * FROM final_attribution

) s

ON t.unique_session_id = s.unique_session_id 
   AND t.session_date BETWEEN start_date AND end_date


/* ----------------------------------------------------------------------------
   WHEN MATCHED: Update existing sessions if attribution changed
   ----------------------------------------------------------------------------
   Uses IS DISTINCT FROM to correctly handle NULL comparisons.
   Updates both transformed and original (_orig) columns to keep audit 
   trail consistent.
   ---------------------------------------------------------------------------- */
WHEN MATCHED AND (
       t.session_source   IS DISTINCT FROM s.session_source
    OR t.session_medium   IS DISTINCT FROM s.session_medium
    OR t.session_campaign IS DISTINCT FROM s.session_campaign
)
THEN
  UPDATE SET
    t.session_source           = s.session_source,
    t.session_source_orig      = s.session_source_orig,
    t.session_medium           = s.session_medium,
    t.session_medium_orig      = s.session_medium_orig,
    t.session_campaign         = s.session_campaign,
    t.session_campaign_orig    = s.session_campaign_orig,
    t.last_update_timestamp    = CURRENT_TIMESTAMP()


/* ----------------------------------------------------------------------------
   WHEN NOT MATCHED: Insert new sessions
   ---------------------------------------------------------------------------- */
WHEN NOT MATCHED THEN INSERT (
  session_date, event_timestamp, actual_timestamp, unique_session_id, page_location,
  session_source_orig, session_source,
  session_medium_orig, session_medium,
  session_campaign_orig, session_campaign,
  session_term, session_content,
  export_timestamp
)
VALUES (
  s.session_date, s.event_timestamp, s.actual_timestamp, s.unique_session_id, s.page_location,
  s.session_source_orig, s.session_source,
  s.session_medium_orig, s.session_medium,
  s.session_campaign_orig, s.session_campaign,
  s.session_term, s.session_content,
  s.export_timestamp
);
