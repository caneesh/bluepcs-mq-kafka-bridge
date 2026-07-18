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
        String eventId = eventIdGenerator.generateEventId(originalMqMessageId);
        ProcessingContext ctx = new ProcessingContext(eventId, originalMqMessageId, mqMessage.getReceivedAt());

        logger.info("Processing message: originalMqMessageId={}, eventId={}, bridgeMessageId={}",
                originalMqMessageId, eventId, ctx.getBridgeMessageId());

        publishAudit(ctx, null, AuditEventType.MESSAGE_RECEIVED, "Message received from MQ", null);

        try {
            ParsedPayload parsedPayload = messageParser.parse(mqMessage);
            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.MESSAGE_PARSED, "Message parsed successfully", null);

            EnrichmentResult enrichmentResult = apiClient.enrich(parsedPayload);
            EnrichedPayload enrichedPayload = buildEnrichedPayload(parsedPayload, ctx, enrichmentResult);
            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.ENRICHMENT_COMPLETED, "Payload enriched successfully", null);

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
                    "HDFS write completed: " + hdfsResult.getHdfsPath(), null);

            String notification = notificationFactory.buildNotification(
                    wrapper,
                    parsedPayload.getEntityId(),
                    hdfsResult.getHdfsPath(),
                    hdfsResult.getChecksum(),
                    enrichedPayload.getEventId());
            String kafkaOffset = kafkaPublisher.publish(enrichedPayload.getEventId(), notification);
            publishAudit(ctx, enrichedPayload.getTransactionId(),
                    AuditEventType.KAFKA_PUBLISH_COMPLETED, "Published to Kafka, offset: " + kafkaOffset, null);

            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.PROCESSING_COMPLETED, "Message processed successfully", null);

            logger.info("Successfully processed message: eventId={}, hdfsPath={}, kafkaOffset={}",
                    eventId, hdfsResult.getHdfsPath(), kafkaOffset);

            return ProcessingResult.success(eventId, hdfsResult.getHdfsPath(), kafkaOffset);

        } catch (MessageParseException e) {
            return handleParseFailure(ctx, mqMessage, e);
        } catch (EnrichmentException e) {
            return handleEnrichmentFailure(ctx, e);
        } catch (HdfsWriteException e) {
            return handleHdfsFailure(ctx, e);
        } catch (KafkaPublishException e) {
            return handleKafkaFailure(ctx, e);
        }
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

        try {
            HdfsWriteResult quarantineResult = hdfsWriter.writeQuarantine(
                    ctx.getEventId(), mqMessage.getPayload(), ctx.getOriginalMqMessageId());
            logger.warn("Quarantined unparseable message: eventId={}, path={}",
                    ctx.getEventId(), quarantineResult.getHdfsPath());
            publishAudit(ctx, null, AuditEventType.MESSAGE_QUARANTINED,
                    "Unparseable message quarantined to " + quarantineResult.getHdfsPath(),
                    e.getMessage());
            return ProcessingResult.quarantined(ctx.getEventId(), quarantineResult.getHdfsPath(),
                    "PARSE_ERROR", e.getMessage());
        } catch (RuntimeException quarantineFailure) {
            logger.error("Quarantine write failed for eventId {} — message will stay on the queue "
                    + "for redelivery", ctx.getEventId(), quarantineFailure);
            publishAudit(ctx, null, AuditEventType.PROCESSING_FAILED,
                    "Parse failure (quarantine write also failed)", e.getMessage());
            return ProcessingResult.failure(ctx.getEventId(), "PARSE_ERROR", e.getMessage());
        }
    }

    private ProcessingResult handleEnrichmentFailure(ProcessingContext ctx, EnrichmentException e) {
        logger.error("Enrichment failure for eventId {}: {}", ctx.getEventId(), e.getMessage());
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

    private void publishAudit(ProcessingContext ctx, @Nullable String transactionId,
                              AuditEventType eventType, String description, @Nullable String errorMessage) {
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
                .timestamp(Instant.now())
                .build();
        auditPublisher.publishAsync(event);
    }
}
