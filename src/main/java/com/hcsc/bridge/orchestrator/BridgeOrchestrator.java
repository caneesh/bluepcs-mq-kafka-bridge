package com.hcsc.bridge.orchestrator;

import com.hcsc.bridge.api.EnrichmentException;
import com.hcsc.bridge.api.EnrichmentWrapperFactory;
import com.hcsc.bridge.api.MarketingPlanApiClient;
import com.hcsc.bridge.api.MarketingPlanApiClient.EnrichmentResult;
import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.core.ProcessingContext;
import com.hcsc.bridge.hdfs.HdfsSafePayloadWriter;
import com.hcsc.bridge.hdfs.HdfsWriteException;
import com.hcsc.bridge.kafka.KafkaEnvelopePublisher;
import com.hcsc.bridge.kafka.KafkaNotificationFactory;
import com.hcsc.bridge.kafka.KafkaPublishException;
import com.hcsc.bridge.model.EnrichedPayload;
import com.hcsc.bridge.model.HdfsWriteResult;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.model.ParsedPayload;
import com.hcsc.bridge.parser.MessageParseException;
import com.hcsc.bridge.parser.MessageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

@Component
public class BridgeOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BridgeOrchestrator.class);

    private final MessageParser messageParser;
    private final MarketingPlanApiClient apiClient;
    private final HdfsSafePayloadWriter hdfsWriter;
    private final EnrichmentWrapperFactory wrapperFactory;
    private final KafkaNotificationFactory notificationFactory;
    private final KafkaEnvelopePublisher kafkaPublisher;
    private final EventIdGenerator eventIdGenerator;
    private final AuditPublisher auditPublisher;

    public BridgeOrchestrator(
            MessageParser messageParser,
            MarketingPlanApiClient apiClient,
            HdfsSafePayloadWriter hdfsWriter,
            EnrichmentWrapperFactory wrapperFactory,
            KafkaNotificationFactory notificationFactory,
            KafkaEnvelopePublisher kafkaPublisher,
            EventIdGenerator eventIdGenerator,
            AuditPublisher auditPublisher) {
        this.messageParser = messageParser;
        this.apiClient = apiClient;
        this.hdfsWriter = hdfsWriter;
        this.wrapperFactory = wrapperFactory;
        this.notificationFactory = notificationFactory;
        this.kafkaPublisher = kafkaPublisher;
        this.eventIdGenerator = eventIdGenerator;
        this.auditPublisher = auditPublisher;
    }

    public ProcessingResult process(MqMessage mqMessage) {
        String originalMqMessageId = mqMessage.getMessageId();
        String eventId = deriveEventId(originalMqMessageId, mqMessage.getPayload());
        ProcessingContext ctx = new ProcessingContext(eventId, originalMqMessageId, mqMessage.getReceivedAt());

        logger.info("Processing message: originalMqMessageId={}, eventId={}, bridgeMessageId={}",
                originalMqMessageId, eventId, ctx.getBridgeMessageId());

        publishAudit(ctx, null, AuditEventType.MESSAGE_RECEIVED, "Message received from MQ", null,
                Map.of("payloadBytes", payloadBytes(mqMessage.getPayload())));

        try {
            ParsedPayload parsedPayload = messageParser.parse(mqMessage);
            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.MESSAGE_PARSED, "Message parsed successfully", null);

            EnrichmentResult enrichmentResult = apiClient.enrich(parsedPayload);
            EnrichedPayload enrichedPayload = buildEnrichedPayload(parsedPayload, ctx, enrichmentResult);
            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.ENRICHMENT_COMPLETED, "Payload enriched successfully", null);

            // Cross-check: the notification advertises the MQ-supplied plan id; the API's
            // response carries its own. They should always agree — a mismatch means the
            // gateway routed to (or returned) the wrong plan, which would otherwise ship
            // silently because only the MQ value is published downstream.
            String apiPlanId = enrichmentResult.getMarketingPlanId();
            if (apiPlanId != null && !apiPlanId.isEmpty()
                    && !apiPlanId.equals(parsedPayload.getEntityId())) {
                logger.warn("Enrichment plan-id mismatch for eventId {}: MQ says '{}' but the "
                                + "API response says '{}' — verify gateway routing",
                        ctx.getEventId(), parsedPayload.getEntityId(), apiPlanId);
            }

            // The full wrapper document goes to HDFS (it can exceed the broker's ~1 MB
            // message limit); Kafka carries only a small claim-check notification with
            // the HDFS path. The wrapper is built from the unmodified API response; the
            // MQ notification's changeEvent.typeName serves as the fallback for
            // changeEventTypeName when the API response carries none.
            EnrichmentWrapperFactory.WrapperResult wrapper = wrapperFactory.build(
                    enrichmentResult.getRawResponse(),
                    extractMqChangeEventTypeName(parsedPayload));

            HdfsWriteResult hdfsResult = hdfsWriter.write(enrichedPayload, wrapper.getWrapperJson());
            AuditEventType hdfsEventType = hdfsResult.isAlreadyExists()
                    ? AuditEventType.HDFS_WRITE_SKIPPED
                    : AuditEventType.HDFS_WRITE_COMPLETED;
            publishAudit(ctx, enrichedPayload.getTransactionId(), hdfsEventType,
                    "HDFS write completed: " + hdfsResult.getHdfsPath(), null,
                    Map.of("hdfsPath", hdfsResult.getHdfsPath(),
                           "checksum", hdfsResult.getChecksum() != null ? hdfsResult.getChecksum() : "",
                           "bytesWritten", hdfsResult.getBytesWritten()));

            String notification = notificationFactory.buildNotification(
                    wrapper,
                    parsedPayload.getEntityId(),
                    hdfsResult.getHdfsPath(),
                    hdfsResult.getChecksum(),
                    enrichedPayload.getEventId());
            String kafkaOffset = kafkaPublisher.publish(enrichedPayload.getEventId(), notification);
            publishAudit(ctx, enrichedPayload.getTransactionId(),
                    AuditEventType.KAFKA_PUBLISH_COMPLETED, "Published to Kafka, offset: " + kafkaOffset, null,
                    Map.of("kafkaOffset", kafkaOffset));

            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.PROCESSING_COMPLETED, "Message processed successfully", null);

            logger.info("Successfully processed message: eventId={}, hdfsPath={}, kafkaOffset={}",
                    eventId, hdfsResult.getHdfsPath(), kafkaOffset);

            return ProcessingResult.success(eventId, hdfsResult.getHdfsPath(), kafkaOffset);

        } catch (MessageParseException e) {
            return handleParseFailure(ctx, mqMessage, e);
        } catch (EnrichmentException e) {
            return handleEnrichmentFailure(ctx, mqMessage, e);
        } catch (HdfsWriteException e) {
            return handleHdfsFailure(ctx, e);
        } catch (KafkaPublishException e) {
            return handleKafkaFailure(ctx, e);
        } catch (RuntimeException e) {
            // Anything escaping the typed handlers would otherwise propagate to the
            // listener with no audit trail and no ProcessingResult — an invisible
            // redelivery loop. Audit it and return a failure so redelivery stays visible.
            return handleUnexpectedFailure(ctx, e);
        }
    }

    /**
     * Derives the deterministic eventId. JMSMessageID can legally be null or empty
     * (e.g. messages bridged from another JMS provider); falling back to a hash of the
     * payload keeps the eventId deterministic across redeliveries so HDFS idempotency
     * and downstream dedup still work, instead of throwing before any audit is emitted.
     */
    private String deriveEventId(String messageId, String payload) {
        if (messageId != null && !messageId.isEmpty()) {
            return eventIdGenerator.generateEventId(messageId);
        }
        logger.warn("Message has no JMSMessageID; deriving eventId from payload hash");
        String basis = (payload != null && !payload.isEmpty()) ? payload : "<empty-mq-message>";
        return eventIdGenerator.generateEventId(basis);
    }

    /**
     * Reads changeEvent.typeName from the parsed MQ notification data (e.g. "New" or
     * "Update"). Returns null when absent, letting the wrapper factory apply its default.
     */
    private String extractMqChangeEventTypeName(ParsedPayload parsedPayload) {
        Object changeEvent = parsedPayload.getData().get("changeEvent");
        if (changeEvent instanceof Map) {
            Object typeName = ((Map<?, ?>) changeEvent).get("typeName");
            if (typeName instanceof String && !((String) typeName).isEmpty()) {
                return (String) typeName;
            }
        }
        return null;
    }

    private EnrichedPayload buildEnrichedPayload(ParsedPayload parsedPayload, ProcessingContext ctx,
                                                 EnrichmentResult result) {
        return new EnrichedPayload(
                parsedPayload,
                ctx,
                result.getAdditionalData(),
                result.getMarketingPlanId(),
                result.getCampaignId(),
                Instant.now()
        );
    }

    /**
     * A parse failure is PERMANENT: redelivering the same payload can never succeed, and an
     * unacknowledged message blocks the queue forever. Instead, quarantine the raw payload to
     * the HDFS error directory and return QUARANTINED so the listener acknowledges it.
     *
     * <p>Safety invariant: the message is only acknowledged if its payload was durably
     * preserved. If the quarantine write itself fails (e.g. HDFS outage), fall back to a
     * FAILURE result — the message stays on the queue and redelivery retries the quarantine.
     */
    private ProcessingResult handleParseFailure(ProcessingContext ctx, MqMessage mqMessage,
                                                MessageParseException e) {
        logger.error("Parse failure for eventId {}: {}", ctx.getEventId(), e.getMessage());

        ProcessingResult quarantined = quarantineOrNull(ctx, mqMessage, "PARSE_ERROR",
                "Unparseable message", e.getMessage());
        if (quarantined != null) {
            return quarantined;
        }
        publishAudit(ctx, null, AuditEventType.PROCESSING_FAILED,
                "Parse failure (quarantine write also failed)", e.getMessage());
        return ProcessingResult.failure(ctx.getEventId(), "PARSE_ERROR", e.getMessage());
    }

    /**
     * Shared quarantine-then-ack path for permanent failures. Safety invariant: the
     * QUARANTINED result (which the listener ACKs) is returned only when the raw payload
     * was durably preserved; a failed quarantine write returns null and the caller falls
     * back to FAILURE — no ack, redelivery retries the quarantine.
     */
    private ProcessingResult quarantineOrNull(ProcessingContext ctx, MqMessage mqMessage,
                                              String errorCode, String description,
                                              String errorMessage) {
        try {
            HdfsWriteResult quarantineResult = hdfsWriter.writeQuarantine(
                    ctx.getEventId(), mqMessage.getPayload(), ctx.getOriginalMqMessageId());
            logger.warn("Quarantined message: eventId={}, reason={}, path={}",
                    ctx.getEventId(), errorCode, quarantineResult.getHdfsPath());
            // errorCode attributes this quarantine to the stage that produced it, so the
            // balance check can subtract it from the right equation
            // (docs/AUDIT_BALANCE_CONTROL.md, equations 1 and 2)
            publishAudit(ctx, null, AuditEventType.MESSAGE_QUARANTINED,
                    description + "; raw payload quarantined to " + quarantineResult.getHdfsPath(),
                    errorMessage,
                    Map.of("errorCode", errorCode,
                           "hdfsPath", quarantineResult.getHdfsPath()));
            return ProcessingResult.quarantined(ctx.getEventId(), quarantineResult.getHdfsPath(),
                    errorCode, errorMessage);
        } catch (RuntimeException quarantineFailure) {
            logger.error("Quarantine write failed for eventId {} — message will stay on the queue "
                    + "for redelivery", ctx.getEventId(), quarantineFailure);
            return null;
        }
    }

    /**
     * Retryable enrichment failures (5xx, timeouts, token trouble) return FAILURE so MQ
     * redelivers. Non-retryable ones (4xx — e.g. a plan identifier that will never resolve)
     * are PERMANENT: redelivery repeats the identical 4xx and, with concurrency=1, the
     * unacknowledged message head-of-line-blocks the whole queue. Those take the same
     * quarantine-then-ack path as parse failures, with the same safety invariant: ack only
     * if the raw payload was durably preserved.
     */
    private ProcessingResult handleEnrichmentFailure(ProcessingContext ctx, MqMessage mqMessage,
                                                     EnrichmentException e) {
        if (!e.isRetryable()) {
            logger.error("Non-retryable enrichment failure for eventId {}: {} — quarantining",
                    ctx.getEventId(), e.getMessage());
            ProcessingResult quarantined = quarantineOrNull(ctx, mqMessage, "ENRICHMENT_ERROR",
                    "Non-retryable enrichment failure", e.getMessage());
            if (quarantined != null) {
                return quarantined;
            }
            // fall through to the FAILURE path below
        } else {
            logger.error("Enrichment failure for eventId {} (retryable): {}",
                    ctx.getEventId(), e.getMessage());
        }
        publishAudit(ctx, null,
                AuditEventType.ENRICHMENT_FAILED, "Enrichment failure", e.getMessage());
        return ProcessingResult.failure(ctx.getEventId(), "ENRICHMENT_ERROR", e.getMessage());
    }

    private ProcessingResult handleHdfsFailure(ProcessingContext ctx, HdfsWriteException e) {
        logger.error("HDFS write failure for eventId {}: {}", ctx.getEventId(), e.getMessage());
        publishAudit(ctx, null,
                AuditEventType.HDFS_WRITE_FAILED, "HDFS write failure", e.getMessage());
        return ProcessingResult.failure(ctx.getEventId(), "HDFS_ERROR", e.getMessage());
    }

    private ProcessingResult handleKafkaFailure(ProcessingContext ctx, KafkaPublishException e) {
        logger.error("Kafka publish failure for eventId {}: {}", ctx.getEventId(), e.getMessage());
        publishAudit(ctx, null,
                AuditEventType.KAFKA_PUBLISH_FAILED, "Kafka publish failure", e.getMessage());
        return ProcessingResult.failure(ctx.getEventId(), "KAFKA_ERROR", e.getMessage());
    }

    private ProcessingResult handleUnexpectedFailure(ProcessingContext ctx, RuntimeException e) {
        logger.error("Unexpected failure for eventId {}", ctx.getEventId(), e);
        publishAudit(ctx, null, AuditEventType.PROCESSING_FAILED,
                "Unexpected failure: " + e.getClass().getSimpleName(), e.getMessage());
        return ProcessingResult.failure(ctx.getEventId(), "UNEXPECTED_ERROR", e.getMessage());
    }

    private void publishAudit(ProcessingContext ctx, @Nullable String transactionId,
                              AuditEventType eventType, String description, @Nullable String errorMessage) {
        publishAudit(ctx, transactionId, eventType, description, errorMessage, Map.of());
    }

    /**
     * Audit emission with balance metadata. The map lands in the Hive audit table's
     * {@code metadata_json} column as-is, so quantities put here are queryable with
     * {@code get_json_object} without any DDL or consumer change — this is what the
     * Audit-Balance-Control checks read (see docs/AUDIT_BALANCE_CONTROL.md).
     *
     * <p>Keys must stay stable: {@code scripts/abc-balance-check.sh} depends on
     * {@code errorCode} to attribute a quarantine to the stage that produced it.
     */
    private void publishAudit(ProcessingContext ctx, @Nullable String transactionId,
                              AuditEventType eventType, String description,
                              @Nullable String errorMessage, Map<String, Object> metadata) {
        AuditEvent event = AuditEvent.builder()
                .auditEventId(UUID.randomUUID().toString())
                .eventId(ctx.getEventId())
                .bridgeEventId(ctx.getBridgeMessageId())
                .originalMqMessageId(ctx.getOriginalMqMessageId())
                .messageId(ctx.getOriginalMqMessageId())
                .transactionId(transactionId)
                .eventType(eventType)
                .description(description)
                .errorMessage(errorMessage)
                .metadata(metadata)
                .timestamp(Instant.now())
                .build();
        auditPublisher.publishAsync(event);
    }

    /** UTF-8 byte size of a payload, 0 when absent — the volume term of the balance. */
    private static int payloadBytes(@Nullable String payload) {
        return payload == null ? 0 : payload.getBytes(StandardCharsets.UTF_8).length;
    }
}
