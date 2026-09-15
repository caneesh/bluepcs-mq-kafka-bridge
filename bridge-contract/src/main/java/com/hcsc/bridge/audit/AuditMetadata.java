package com.hcsc.bridge.audit;

/**
 * The metadata keys the audit stream carries, in one place.
 *
 * <p>These strings are a published contract, not local detail: the Hive consumer stores the
 * map as {@code metadata_json}, and {@code scripts/abc-balance-check.sh} and
 * {@code scripts/audit-gap-check.sh} read individual keys out of it with
 * {@code get_json_object}. A branch that invents its own spelling silently breaks a
 * reconciliation equation instead of failing a build, so emitters use these constants and
 * {@code AuditContractTest} checks that the scripts read only keys declared here.
 */
public final class AuditMetadata {

    private AuditMetadata() {
    }

    /** Which bridge emitted the event ("pmm"); absent on PMM+ events, where it reads as "bridge". */
    public static final String PIPELINE = "pipeline";

    /** Attributes a quarantine or discard to the stage that produced it (PARSE_ERROR, API_ERROR, POISON). */
    public static final String ERROR_CODE = "errorCode";

    /** Where the payload is durably stored. */
    public static final String HDFS_PATH = "hdfsPath";

    /** SHA-256 of the stored bytes, as advertised to the consumer. */
    public static final String CHECKSUM = "checksum";

    public static final String BYTES_WRITTEN = "bytesWritten";
    public static final String PAYLOAD_BYTES = "payloadBytes";
    public static final String KAFKA_OFFSET = "kafkaOffset";

    /** Why a stage was skipped (resumed-from-landing, resumed-from-archive, target-exists-before-api-call). */
    public static final String REASON = "reason";

    public static final String STATUS_CODE = "statusCode";
    public static final String RESPONSE_BYTES = "responseBytes";
    public static final String DURATION_MS = "durationMs";
    public static final String RETRYABLE = "retryable";

    /** PMM: whether the 4-hour window came from the broker's put time or the receive time. */
    public static final String ANCHOR_SOURCE = "anchorSource";

    /** PMM: the window a payload landed in, as date/hour. */
    public static final String WINDOW = "window";

    public static final String DELIVERY_COUNT = "deliveryCount";
    public static final String MAX_DELIVERY_ATTEMPTS = "maxDeliveryAttempts";
    public static final String SOURCE_QUEUE = "sourceQueue";
    public static final String CORRELATION_ID = "correlationId";
    public static final String MESSAGE_CLASS = "messageClass";

    /** Why a discard carries no preserved copy. */
    public static final String NO_EVIDENCE_REASON = "noEvidenceReason";
}
