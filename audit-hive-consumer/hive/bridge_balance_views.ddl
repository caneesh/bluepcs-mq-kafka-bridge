-- ============================================================================
-- Views supporting the Audit-Balance-Control framework
-- (scripts/abc-balance-check.sh, docs/AUDIT_BALANCE_CONTROL.md).
--
-- Run once per environment, after bridge_audit_event.ddl.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated audit events.
--
-- Audit rows arrive at-least-once (Spark commits offsets only after the Hive
-- write, so a retried batch re-inserts). EVERY balance query must read this
-- view rather than the raw table, or a replayed batch inflates counts and
-- fabricates a variance.
--
-- COALESCE matters: rows whose value failed to parse have a NULL
-- audit_event_id, and would otherwise all collapse into a single row.
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS bluepcs.bridge_audit_event_deduped AS
SELECT * FROM (
  SELECT *,
         ROW_NUMBER() OVER (
           PARTITION BY COALESCE(audit_event_id,
                                 CONCAT(kafka_partition, '-', kafka_offset))
           ORDER BY event_timestamp) AS rn
  FROM bluepcs.bridge_audit_event) t
WHERE rn = 1;

-- ----------------------------------------------------------------------------
-- 2. Hourly stage funnel — the balance snapshot.
--
-- One row per UTC hour with the distinct-message count at each pipeline stage.
-- COUNT(DISTINCT event_id) is deliberate: a redelivered message re-emits
-- MESSAGE_RECEIVED with the SAME deterministic event_id, so distinct-counting
-- collapses redeliveries to the one message they represent.
--
-- MESSAGE_DISCARDED is counted as ROWS, not distinct event_id: the listener
-- emits it with a null event_id (both discard paths run before the orchestrator
-- assigns one), so distinct-counting would collapse every discard into one.
-- Discards are also NOT a drain from MESSAGE_RECEIVED — both discard paths
-- return before any MESSAGE_RECEIVED is emitted, so those messages never enter
-- the audited funnel at all. They are reported separately.
--
-- Quarantine is split by stage using the errorCode metadata key, falling back
-- to the description text for rows written before that key existed.
-- ----------------------------------------------------------------------------
CREATE VIEW IF NOT EXISTS bluepcs.bridge_stage_funnel_hourly AS
SELECT
  event_dt,
  substr(event_timestamp, 1, 13) AS event_hour,   -- 'yyyy-MM-ddTHH'
  COUNT(DISTINCT CASE WHEN event_type = 'MESSAGE_RECEIVED'        THEN event_id END) AS received,
  COUNT(DISTINCT CASE WHEN event_type = 'MESSAGE_PARSED'          THEN event_id END) AS parsed,
  COUNT(DISTINCT CASE WHEN event_type = 'ENRICHMENT_COMPLETED'    THEN event_id END) AS enriched,
  COUNT(DISTINCT CASE WHEN event_type IN ('HDFS_WRITE_COMPLETED',
                                          'HDFS_WRITE_SKIPPED')   THEN event_id END) AS hdfs_written,
  COUNT(DISTINCT CASE WHEN event_type = 'KAFKA_PUBLISH_COMPLETED' THEN event_id END) AS kafka_published,
  COUNT(DISTINCT CASE WHEN event_type = 'PROCESSING_COMPLETED'    THEN event_id END) AS completed,
  COUNT(DISTINCT CASE WHEN event_type = 'CLAIM_CHECK_RESOLVED'    THEN event_id END) AS claim_check_resolved,
  COUNT(DISTINCT CASE WHEN event_type = 'CLAIM_CHECK_SKIPPED'     THEN event_id END) AS claim_check_skipped,
  COUNT(DISTINCT CASE WHEN event_type = 'HIVE_LOAD_COMPLETED'     THEN event_id END) AS hive_loaded,
  COUNT(DISTINCT
        CASE WHEN event_type = 'MESSAGE_QUARANTINED'
              AND COALESCE(get_json_object(metadata_json, '$.errorCode'),
                           CASE WHEN description LIKE 'Unparseable%' THEN 'PARSE_ERROR' END)
                  = 'PARSE_ERROR'
             THEN event_id END)                                                      AS quarantined_parse,
  COUNT(DISTINCT
        CASE WHEN event_type = 'MESSAGE_QUARANTINED'
              AND COALESCE(get_json_object(metadata_json, '$.errorCode'),
                           CASE WHEN description LIKE 'Non-retryable enrichment%'
                                THEN 'ENRICHMENT_ERROR' END)
                  = 'ENRICHMENT_ERROR'
             THEN event_id END)                                                      AS quarantined_enrichment,
  SUM(CASE WHEN event_type = 'MESSAGE_DISCARDED' THEN 1 ELSE 0 END)                  AS discarded_rows
FROM bluepcs.bridge_audit_event_deduped
GROUP BY event_dt, substr(event_timestamp, 1, 13);
