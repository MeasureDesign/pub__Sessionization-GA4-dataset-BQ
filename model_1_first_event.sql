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
