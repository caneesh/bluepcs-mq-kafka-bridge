package com.hcsc.bridge.pmm.orchestrator;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.core.ProcessingContext;
import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsWriteException;
import com.hcsc.bridge.hdfs.SafeHdfsWriter;
import com.hcsc.bridge.model.HdfsWriteResult;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import com.hcsc.bridge.pmm.api.PmmApiClient;
import com.hcsc.bridge.pmm.api.PmmApiException;
import com.hcsc.bridge.pmm.api.PmmApiResponse;
import com.hcsc.bridge.pmm.hdfs.WindowedPathResolver;
import com.hcsc.bridge.pmm.template.PmmRequestTemplate;
import com.hcsc.bridge.pmm.xml.PmmExtractedValues;
import com.hcsc.bridge.pmm.xml.PmmXmlException;
import com.hcsc.bridge.pmm.xml.PmmXmlExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * PMM pipeline: extract two values → render request → POST → land the raw response.
 *
 * <p>Same invariants as the PMM+ orchestrator: a deterministic eventId (SHA-256 of the
 * JMS message id) names the HDFS file; QUARANTINED is returned only when the raw payload
 * was durably written; every audit event carries {@code metadata.pipeline = "pmm"} so the
 * shared audit topic can be partitioned by the balance checks.
 *
 * <p>Redelivery rule: if the target file already exists the web service is NOT called
 * again. The response may carry volatile fields (timestamps, request ids); re-fetching it
 * would produce different bytes, fail the writer's checksum comparison and wedge the
 * message. A file at the deterministic path already went through temp → checksum →
 * rename, so it is the response for this message.
 */
@Component
public class PmmOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(PmmOrchestrator.class);
    public static final String PIPELINE = "pmm";

    private final PmmXmlExtractor extractor;
    private final PmmRequestTemplate template;
    private final PmmApiClient apiClient;
    private final SafeHdfsWriter hdfsWriter;
    private final HdfsFileOperations hdfsFileOperations;
    private final WindowedPathResolver pathResolver;
    private final EventIdGenerator eventIdGenerator;
    private final AuditPublisher auditPublisher;
    private final boolean skipApiWhenTargetExists;

    public PmmOrchestrator(PmmXmlExtractor extractor,
                           PmmRequestTemplate template,
                           PmmApiClient apiClient,
                           SafeHdfsWriter hdfsWriter,
                           HdfsFileOperations hdfsFileOperations,
                           WindowedPathResolver pathResolver,
                           EventIdGenerator eventIdGenerator,
                           AuditPublisher auditPublisher,
                           @Value("${bridge.pmm.hdfs.skip-api-when-target-exists:true}")
                           boolean skipApiWhenTargetExists) {
        this.extractor = extractor;
        this.template = template;
        this.apiClient = apiClient;
        this.hdfsWriter = hdfsWriter;
        this.hdfsFileOperations = hdfsFileOperations;
        this.pathResolver = pathResolver;
        this.eventIdGenerator = eventIdGenerator;
        this.auditPublisher = auditPublisher;
        this.skipApiWhenTargetExists = skipApiWhenTargetExists;
    }

    public ProcessingResult process(MqMessage mqMessage) {
        String originalMqMessageId = mqMessage.getMessageId();
        String eventId = deriveEventId(originalMqMessageId, mqMessage.getPayload());
        ProcessingContext ctx = new ProcessingContext(eventId, originalMqMessageId, mqMessage.getReceivedAt());

        // JMSTimestamp is the broker's put time and identical on every redelivery; the
        // receive time is not. Only the former keeps the window (and so the target path)
        // stable, which is what the exists()-based idempotency relies on.
        Instant anchor = mqMessage.getJmsTimestamp() != null ? mqMessage.getJmsTimestamp() : mqMessage.getReceivedAt();
        String anchorSource = mqMessage.getJmsTimestamp() != null ? "jmsTimestamp" : "receivedAt";
        if (mqMessage.getJmsTimestamp() == null) {
            logger.warn("Message {} carries no JMSTimestamp; window derived from receive time — a redelivery "
                    + "across a window boundary would land a second copy", eventId);
        }

        logger.info("Processing PMM message: originalMqMessageId={}, eventId={}, bridgeMessageId={}, window={}",
                originalMqMessageId, eventId, ctx.getBridgeMessageId(), pathResolver.windowLabel(anchor));

        publishAudit(ctx, AuditEventType.MESSAGE_RECEIVED, "Message received from MQ", null,
                metadata("payloadBytes", payloadBytes(mqMessage.getPayload()),
                        "anchorSource", anchorSource,
                        "window", pathResolver.windowLabel(anchor)));

        try {
            PmmExtractedValues values = extractor.extract(mqMessage.getPayload(), originalMqMessageId);
            publishAudit(ctx, AuditEventType.MESSAGE_PARSED, "Request values extracted", null, metadata());

            String targetPath = pathResolver.resolve(eventId, anchor);

            if (skipApiWhenTargetExists && targetExists(targetPath, originalMqMessageId)) {
                logger.info("Target already exists for eventId {} ({}); skipping the web-service call",
                        eventId, targetPath);
                publishAudit(ctx, AuditEventType.HDFS_WRITE_SKIPPED,
                        "Target file already present before the API call: " + targetPath, null,
                        metadata("hdfsPath", targetPath, "reason", "target-exists-before-api-call"));
                publishAudit(ctx, AuditEventType.PROCESSING_COMPLETED,
                        "Message already landed (redelivery)", null, metadata());
                return ProcessingResult.success(eventId, targetPath);
            }

            String requestXml = template.render(values);
            PmmApiResponse response = apiClient.submit(requestXml, eventId);
            publishAudit(ctx, AuditEventType.API_CALL_COMPLETED,
                    "PMM API responded " + response.getStatusCode(), null,
                    metadata("statusCode", response.getStatusCode(),
                            "responseBytes", payloadBytes(response.getBody()),
                            "durationMs", response.getDurationMs()));

            HdfsWriteResult hdfsResult = hdfsWriter.write(targetPath, response.getBody(), originalMqMessageId);
            AuditEventType hdfsEventType = hdfsResult.isAlreadyExists()
                    ? AuditEventType.HDFS_WRITE_SKIPPED
                    : AuditEventType.HDFS_WRITE_COMPLETED;
            publishAudit(ctx, hdfsEventType, "HDFS write completed: " + hdfsResult.getHdfsPath(), null,
                    metadata("hdfsPath", hdfsResult.getHdfsPath(),
                            "checksum", hdfsResult.getChecksum() != null ? hdfsResult.getChecksum() : "",
                            "bytesWritten", hdfsResult.getBytesWritten(),
                            "window", pathResolver.windowLabel(anchor)));

            publishAudit(ctx, AuditEventType.PROCESSING_COMPLETED, "Message processed successfully", null,
                    metadata());

            logger.info("Successfully processed PMM message: eventId={}, hdfsPath={}", eventId, hdfsResult.getHdfsPath());
            return ProcessingResult.success(eventId, hdfsResult.getHdfsPath());

        } catch (PmmXmlException e) {
            return handleParseFailure(ctx, mqMessage, e);
        } catch (PmmApiException e) {
            return handleApiFailure(ctx, mqMessage, e);
        } catch (HdfsWriteException e) {
            return handleHdfsFailure(ctx, e);
        } catch (RuntimeException e) {
            // Anything escaping the typed handlers would otherwise propagate to the
            // listener with no audit trail and no ProcessingResult — an invisible
            // redelivery loop.
            return handleUnexpectedFailure(ctx, e);
        }
    }

    private boolean targetExists(String targetPath, String messageId) {
        try {
            return hdfsFileOperations.exists(targetPath);
        } catch (IOException e) {
            // Same class of failure as a write: retryable, no ack
            throw new HdfsWriteException("Failed to check target existence before the API call",
                    targetPath, messageId, e);
        }
    }

    /**
     * Deterministic eventId. JMSMessageID can legally be null or empty; the payload-hash
     * fallback keeps the id (and so the HDFS path) stable across redeliveries.
     */
    private String deriveEventId(String messageId, String payload) {
        if (messageId != null && !messageId.isEmpty()) {
            return eventIdGenerator.generateEventId(messageId);
        }
        logger.warn("Message has no JMSMessageID; deriving eventId from payload hash");
        String basis = (payload != null && !payload.isEmpty()) ? payload : "<empty-mq-message>";
        return eventIdGenerator.generateEventId(basis);
    }

    /** Parse failures are permanent: quarantine-then-ack, or FAILURE if the quarantine write fails. */
    private ProcessingResult handleParseFailure(ProcessingContext ctx, MqMessage mqMessage, PmmXmlException e) {
        logger.error("Parse failure for eventId {}: {}", ctx.getEventId(), e.getMessage());
        ProcessingResult quarantined = quarantineOrNull(ctx, mqMessage, "PARSE_ERROR",
                "Unparseable message", e.getMessage());
        if (quarantined != null) {
            return quarantined;
        }
        publishAudit(ctx, AuditEventType.PROCESSING_FAILED,
                "Parse failure (quarantine write also failed)", e.getMessage(), metadata());
        return ProcessingResult.failure(ctx.getEventId(), "PARSE_ERROR", e.getMessage());
    }

    /**
     * API_CALL_FAILED records the fact for every failure; a permanent one additionally
     * quarantines (with the ack invariant), a retryable one returns FAILURE so MQ redelivers.
     */
    private ProcessingResult handleApiFailure(ProcessingContext ctx, MqMessage mqMessage, PmmApiException e) {
        publishAudit(ctx, AuditEventType.API_CALL_FAILED, "PMM API call failure", e.getMessage(),
                metadata("statusCode", e.getStatusCode(), "retryable", e.isRetryable()));

        if (!e.isRetryable()) {
            logger.error("Non-retryable API failure for eventId {}: {} — quarantining", ctx.getEventId(), e.getMessage());
            ProcessingResult quarantined = quarantineOrNull(ctx, mqMessage, "API_ERROR",
                    "Non-retryable API failure", e.getMessage());
            if (quarantined != null) {
                return quarantined;
            }
        } else {
            logger.error("API failure for eventId {} (retryable): {}", ctx.getEventId(), e.getMessage());
        }
        return ProcessingResult.failure(ctx.getEventId(), "API_ERROR", e.getMessage());
    }

    private ProcessingResult handleHdfsFailure(ProcessingContext ctx, HdfsWriteException e) {
        logger.error("HDFS write failure for eventId {}: {}", ctx.getEventId(), e.getMessage());
        publishAudit(ctx, AuditEventType.HDFS_WRITE_FAILED, "HDFS write failure", e.getMessage(), metadata());
        return ProcessingResult.failure(ctx.getEventId(), "HDFS_ERROR", e.getMessage());
    }

    private ProcessingResult handleUnexpectedFailure(ProcessingContext ctx, RuntimeException e) {
        logger.error("Unexpected failure for eventId {}", ctx.getEventId(), e);
        publishAudit(ctx, AuditEventType.PROCESSING_FAILED,
                "Unexpected failure: " + e.getClass().getSimpleName(), e.getMessage(), metadata());
        return ProcessingResult.failure(ctx.getEventId(), "UNEXPECTED_ERROR", e.getMessage());
    }

    /**
     * Quarantine-then-ack for permanent failures. QUARANTINED (which the listener acks) is
     * returned only when the raw payload was durably preserved at
     * {@code <error-path>/<eventId>.xml}; a failed quarantine write returns null and the
     * caller falls back to FAILURE — no ack, redelivery retries the quarantine.
     */
    private ProcessingResult quarantineOrNull(ProcessingContext ctx, MqMessage mqMessage, String errorCode,
                                              String description, String errorMessage) {
        try {
            String payload = mqMessage.getPayload() != null ? mqMessage.getPayload() : "";
            HdfsWriteResult result = hdfsWriter.write(pathResolver.quarantinePath(ctx.getEventId()),
                    payload, ctx.getOriginalMqMessageId());
            logger.warn("Quarantined PMM message: eventId={}, reason={}, path={}",
                    ctx.getEventId(), errorCode, result.getHdfsPath());
            publishAudit(ctx, AuditEventType.MESSAGE_QUARANTINED,
                    description + "; raw payload quarantined to " + result.getHdfsPath(), errorMessage,
                    metadata("errorCode", errorCode, "hdfsPath", result.getHdfsPath()));
            return ProcessingResult.quarantined(ctx.getEventId(), result.getHdfsPath(), errorCode, errorMessage);
        } catch (RuntimeException quarantineFailure) {
            logger.error("Quarantine write failed for eventId {} — message will stay on the queue for redelivery",
                    ctx.getEventId(), quarantineFailure);
            return null;
        }
    }

    /**
     * Metadata map for audit events. Always carries {@code pipeline=pmm}: the audit topic is
     * shared with the PMM+ bridge and the balance checks partition on this key. Never put the
     * extracted values here — they may be PHI.
     */
    static Map<String, Object> metadata(Object... keyValues) {
        Map<String, Object> map = new HashMap<>();
        map.put("pipeline", PIPELINE);
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            map.put(String.valueOf(keyValues[i]), keyValues[i + 1]);
        }
        return map;
    }

    private void publishAudit(ProcessingContext ctx, AuditEventType eventType, String description,
                              @Nullable String errorMessage, Map<String, Object> metadata) {
        AuditEvent event = AuditEvent.builder()
                .auditEventId(UUID.randomUUID().toString())
                .eventId(ctx.getEventId())
                .bridgeEventId(ctx.getBridgeMessageId())
                .originalMqMessageId(ctx.getOriginalMqMessageId())
                .messageId(ctx.getOriginalMqMessageId())
                .eventType(eventType)
                .description(description)
                .errorMessage(errorMessage)
                .metadata(metadata)
                .timestamp(Instant.now())
                .build();
        auditPublisher.publishAsync(event);
    }

    private static int payloadBytes(@Nullable String payload) {
        return payload == null ? 0 : payload.getBytes(StandardCharsets.UTF_8).length;
    }
}
