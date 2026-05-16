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
