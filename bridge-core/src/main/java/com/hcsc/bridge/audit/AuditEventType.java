package com.hcsc.bridge.audit;

public enum AuditEventType {
    MESSAGE_RECEIVED,
    MESSAGE_PARSED,
    ENRICHMENT_COMPLETED,
    ENRICHMENT_FAILED,
    HDFS_WRITE_COMPLETED,
    HDFS_WRITE_FAILED,
    HDFS_WRITE_SKIPPED,
    KAFKA_PUBLISH_COMPLETED,
    KAFKA_PUBLISH_FAILED,
    PROCESSING_COMPLETED,
    PROCESSING_FAILED,
    MESSAGE_DISCARDED,
    MESSAGE_QUARANTINED,
    // Web-service call stage of pipelines that POST a request and land the raw
    // response (mq-pmm-bridge). Every event from such a pipeline also carries
    // metadata.pipeline (e.g. "pmm") so the balance checks can partition the topic;
    // the PMM+ bridge's stage-2 counters remain ENRICHMENT_*.
    API_CALL_COMPLETED,
    API_CALL_FAILED,
    // Consumer-stage events: emitted by the downstream DStream job (via
    // ConsumerAuditEmitter, see docs/consumer/), never by the bridge itself.
    // Declared here so this enum stays the single source of truth for the
    // audit contract on the topic.
    CLAIM_CHECK_RESOLVED,
    CLAIM_CHECK_SKIPPED,
    HIVE_LOAD_COMPLETED,
    HIVE_LOAD_FAILED,
    DUPLICATE_DETECTED
}
