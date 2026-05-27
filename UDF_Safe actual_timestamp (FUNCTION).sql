/* ==================================================================================
UDF Title: safe_actual_ts_ms — GA4 event timestamp normalization
Description: Scalar SQL UDF that returns a normalized event timestamp in milliseconds, 
preferring the client-side actual_timestamp event parameter but falling back to the 
GA4 server-side event_timestamp when the two diverge beyond a 2-day tolerance window.
==================================================================================
Anna Horáková | MeasureDesign | www.measuredesign.cz | anna.horakova@measuredesign.cz

Usage: This UDF guards downstream sessionization and attribution logic against 
corrupted client-side timestamps — clock skew on user devices, replayed events from 
offline retry queues, and bugged client implementations that emit timestamps from 
1970 or far in the future. 

The function takes two inputs: param_actual_ts_ms (the 
client-reported timestamp in milliseconds, typically extracted from the GA4 
event_params array under the key actual_timestamp) and event_ts_micros (the 
server-side event_timestamp from the GA4 BigQuery export, in microseconds). 
When the two values agree within ±2 days, the client-reported value is returned; 
otherwise the server-side event_timestamp is returned (converted from microseconds 
to milliseconds). The 2-day tolerance is symmetric, catching both ahead-of-server 
and behind-server skew. NULL param_actual_ts_ms is handled via COALESCE and falls 
back to event_ts_micros automatically.

The UDF is created as a persistent function in a dedicated UDF dataset (commonly 
named UDF_all) and called by its fully qualified name in backticks. Note: argument 
order matters — pass actual_timestamp first, event_timestamp second. The UDF must 
reside in the same BigQuery region as the data it operates on; cross-region calls 
will fail. Replace the project and dataset references before running.
For questions or contributions, please send us an e-mail: info@measuredesign.cz 
Or submit an issue on our GitHub repository.
================================================================================== */
-- Your SQL query starts here

CREATE FUNCTION `your_project.your_dataset.safe_actual_ts_ms`(
  param_actual_ts_ms INT64,
  event_ts_micros INT64
)
RETURNS INT64
OPTIONS(description="Returns actual_timestamp if within 2 days of event_ts, else event_ts.")

AS (
(
   (
  -- Normalize event timestamp: prefer client-sent actual_timestamp,
  -- but fall back to event_ts if the two diverge by more than 2 days
  -- (guards against clock skew / replayed events / corrupted client time).
  WITH base AS (
    SELECT
      -- Candidate = client-reported timestamp; falls back to event_ts if NULL
      COALESCE(param_actual_ts_ms, DIV(event_ts_micros, 1000)) AS candidate_ms,

      -- Event timestamp from GA4 export (micros -> millis for comparison)
      DIV(event_ts_micros, 1000) AS event_ms
  )
  SELECT
    CASE
      -- Symmetric 2-day tolerance window (candidate may be ahead or behind)
      WHEN ABS(
        TIMESTAMP_DIFF(
          TIMESTAMP_MILLIS(candidate_ms),
          TIMESTAMP_MILLIS(event_ms),
          DAY
        )
      ) > 2
        THEN event_ms       -- Too far off -> trust server-side event_ts
        ELSE candidate_ms   -- Within tolerance -> trust client-side value
    END
  FROM base
)
)

/* Use in SELECT:
SELECT
  `project.UDF_all.safe_actual_ts_ms`(param_actual_ts_ms, event_ts_micros) AS normalized_ts_ms
FROM `project.dataset.table`

*/
