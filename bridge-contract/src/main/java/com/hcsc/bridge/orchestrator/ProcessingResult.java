package com.hcsc.bridge.orchestrator;

import java.util.Objects;

/**
 * The outcome of processing one MQ message, and the single place that decides whether
 * acknowledging it is safe.
 *
 * <p>Acknowledging an MQ message throws away the broker's copy, so it is only ever safe
 * when the message will not need to be processed again:
 *
 * <ul>
 *   <li>{@code SUCCESS} - the work is durable downstream (the payload landed, and for
 *       pipelines that publish, a notification was accepted).</li>
 *   <li>{@code QUARANTINED} - the work can never succeed, and the raw payload has been
 *       durably preserved somewhere an operator can replay it from.</li>
 *   <li>{@code DISCARDED} - the message is being given up on (poison guard, unsupported
 *       type) with its payload preserved, or with an explicit statement that there was no
 *       payload to preserve.</li>
 *   <li>{@code FAILURE} - anything else. The message stays on the queue and the broker
 *       redelivers it.</li>
 * </ul>
 *
 * <p>The evidence requirement is enforced by the factory methods, not left to the caller:
 * a QUARANTINED or DISCARDED result cannot be constructed without either a durable path or
 * an explicit "nothing to preserve" reason. That is what stops an acknowledgement from
 * being issued on the strength of an intention rather than a fact.
 *
 * <p>Both pipelines produce this type and both listeners settle it through the same rule
 * ({@code JmsMessageSupport.settle}), so a disposition decision cannot drift between them.
 */
public final class ProcessingResult {

    private final String eventId;
    private final Status status;
    private final String hdfsPath;
    private final String kafkaOffset;
    private final String errorCode;
    private final String errorMessage;
    private final String noEvidenceReason;

    private ProcessingResult(String eventId, Status status, String hdfsPath, String kafkaOffset,
                             String errorCode, String errorMessage, String noEvidenceReason) {
        this.eventId = Objects.requireNonNull(eventId, "eventId must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.hdfsPath = hdfsPath;
        this.kafkaOffset = kafkaOffset;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
        this.noEvidenceReason = noEvidenceReason;
    }

    /** Terminal success for a pipeline that publishes a notification after landing the payload. */
    public static ProcessingResult success(String eventId, String hdfsPath, String kafkaOffset) {
        return new ProcessingResult(eventId, Status.SUCCESS, requirePath(hdfsPath, "SUCCESS"),
                kafkaOffset, null, null, null);
    }

    /** Terminal success for a pipeline whose last durable step is the HDFS write. */
    public static ProcessingResult success(String eventId, String hdfsPath) {
        return new ProcessingResult(eventId, Status.SUCCESS, requirePath(hdfsPath, "SUCCESS"),
                null, null, null, null);
    }

    /** The message stays on the queue: the broker redelivers it. */
    public static ProcessingResult failure(String eventId, String errorCode, String errorMessage) {
        return new ProcessingResult(eventId, Status.FAILURE, null, null, errorCode, errorMessage, null);
    }

    /**
     * The message can never be processed (unparseable payload, a permanently rejected
     * request) but its raw payload is durably preserved at {@code hdfsPath}, so the
     * acknowledgement loses nothing.
     *
     * @throws IllegalArgumentException if no durable path is given - a quarantine without a
     *         preserved copy must be reported as a {@link #failure} so the message is redelivered
     */
    public static ProcessingResult quarantined(String eventId, String hdfsPath,
                                               String errorCode, String errorMessage) {
        return new ProcessingResult(eventId, Status.QUARANTINED, requirePath(hdfsPath, "QUARANTINED"),
                null, errorCode, errorMessage, null);
    }

    /**
     * The message is given up on (poison guard, unsupported message type) with its payload
     * preserved at {@code hdfsPath}.
     *
     * @throws IllegalArgumentException if no durable path is given - use
     *         {@link #discardedWithoutPayload} when there was nothing to preserve
     */
    public static ProcessingResult discarded(String eventId, String hdfsPath,
                                             String errorCode, String errorMessage) {
        return new ProcessingResult(eventId, Status.DISCARDED, requirePath(hdfsPath, "DISCARDED"),
                null, errorCode, errorMessage, null);
    }

    /**
     * The message is given up on and there was no payload to preserve - the only case in
     * which an acknowledgement is issued without durable evidence. {@code reason} records why
     * (typically a body the client could not convert on any delivery), and is required so the
     * absence of evidence is always a stated fact rather than an oversight.
     */
    public static ProcessingResult discardedWithoutPayload(String eventId, String reason,
                                                           String errorCode, String errorMessage) {
        if (reason == null || reason.trim().isEmpty()) {
            throw new IllegalArgumentException(
                    "discardedWithoutPayload requires a reason: acknowledging without a durable copy "
                            + "must always be explained");
        }
        return new ProcessingResult(eventId, Status.DISCARDED, null, null, errorCode, errorMessage, reason);
    }

    private static String requirePath(String hdfsPath, String status) {
        if (hdfsPath == null || hdfsPath.trim().isEmpty()) {
            throw new IllegalArgumentException(status + " requires the path the payload is durably "
                    + "stored at; without one the message must stay on the queue");
        }
        return hdfsPath;
    }

    /**
     * Whether the MQ message may be acknowledged. THE acknowledgement rule for every bridge:
     * anything not terminal stays on the queue.
     */
    public boolean isAcknowledgeable() {
        return status != Status.FAILURE;
    }

    public String getEventId() {
        return eventId;
    }

    public Status getStatus() {
        return status;
    }

    /** Where the payload is durably stored; null only for FAILURE and a payload-less discard. */
    public String getHdfsPath() {
        return hdfsPath;
    }

    public String getKafkaOffset() {
        return kafkaOffset;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    /** Why no durable copy exists, for a payload-less discard; null otherwise. */
    public String getNoEvidenceReason() {
        return noEvidenceReason;
    }

    public boolean isSuccessful() {
        return status == Status.SUCCESS;
    }

    public boolean isFailed() {
        return status == Status.FAILURE;
    }

    public boolean isQuarantined() {
        return status == Status.QUARANTINED;
    }

    public boolean isDiscarded() {
        return status == Status.DISCARDED;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "eventId='" + eventId + '\'' +
                ", status=" + status +
                ", hdfsPath='" + hdfsPath + '\'' +
                ", kafkaOffset='" + kafkaOffset + '\'' +
                ", errorCode='" + errorCode + '\'' +
                (noEvidenceReason != null ? ", noEvidenceReason='" + noEvidenceReason + '\'' : "") +
                '}';
    }

    public enum Status {
        SUCCESS,
        FAILURE,
        QUARANTINED,
        DISCARDED
    }
}
