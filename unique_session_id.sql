/* ================================================================================== 
Query Title: GA4 unique session ID
Description: This query builds a unique session identifier from the GA4 BigQuery export.
==================================================================================
MeasureDesign | www.measuredesign.cz | info@measuredesign.cz

Usage: The output is useful whenever you need to identify a session in the GA4 BigQuery 
export. The ga_session_id field alone is NOT a unique session identifier — it is derived 
from a timestamp, so two different users (different user_pseudo_id) can be assigned the 
same ga_session_id value. Concatenating user_pseudo_id with ga_session_id guarantees the 
identifier is unique per session.

Replace the table reference with your own project (analytics_12345), dataset and date suffix (events_20240121) before running.

For questions or contributions, please send us an e-mail: info@measuredesign.cz 
Or submit an issue on our GitHub repository.
================================================================================== */
-- Your SQL query starts here

SELECT
  CONCAT(
    user_pseudo_id, "-",
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')
  ) AS unique_session_id
FROM `project_name.analytics_12345.events_20240121`
