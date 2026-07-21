-- ============================================================================
-- Hive audit table for the MQ-Kafka bridge audit trail (source of truth DDL).
-- Written by AuditHiveConsumer; column order MUST match the AuditRow case
-- class in AuditHiveConsumer.scala. Run once per environment before the
-- first job submit; adjust database/location to the environment's standards.
--
-- Event catalog and field semantics: docs/AUDIT.md in the bridge repo.
-- Rows arrive at-least-once: dedupe on audit_event_id.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bluepcs.bridge_audit_event (
  audit_event_id         STRING,   -- unique per event: dedupe key
  event_id               STRING,   -- joins notification + HDFS payload file
  bridge_event_id        STRING,   -- groups one bridge processing attempt (null on consumer events)
  original_mq_message_id STRING,
  transaction_id         STRING,
  event_type             STRING,   -- MESSAGE_RECEIVED ... HIVE_LOAD_COMPLETED; null if unparseable
  description            STRING,
  error_message          STRING,
  metadata_json          STRING,   -- JSON map; get_json_object(metadata_json,'$.deliveryCount')
  event_timestamp        STRING,   -- ISO-8601 UTC as emitted
  kafka_partition        INT,      -- lineage / debugging
  kafka_offset           BIGINT,
  raw_value              STRING    -- set ONLY when the value failed to parse
)
PARTITIONED BY (event_dt STRING)   -- yyyy-MM-dd from event_timestamp (UTC);
                                   -- ingestion date when unparseable
STORED AS ORC;
