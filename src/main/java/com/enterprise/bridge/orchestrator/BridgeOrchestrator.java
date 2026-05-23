package com.enterprise.bridge.orchestrator;

import com.enterprise.bridge.api.EnrichmentException;
import com.enterprise.bridge.api.MarketingPlanApiClient;
import com.enterprise.bridge.api.MarketingPlanApiClient.EnrichmentResult;
import com.enterprise.bridge.audit.AuditEvent;
import com.enterprise.bridge.audit.AuditEventType;
import com.enterprise.bridge.audit.AuditPublisher;
import com.enterprise.bridge.core.EventIdGenerator;
import com.enterprise.bridge.core.ProcessingContext;
import com.enterprise.bridge.hdfs.HdfsSafePayloadWriter;
import com.enterprise.bridge.hdfs.HdfsWriteException;
import com.enterprise.bridge.kafka.KafkaEnvelopeFactory;
import com.enterprise.bridge.kafka.KafkaEnvelopePublisher;
import com.enterprise.bridge.kafka.KafkaPublishException;
import com.enterprise.bridge.model.EnrichedPayload;
import com.enterprise.bridge.model.HdfsWriteResult;
import com.enterprise.bridge.model.KafkaEnvelope;
import com.enterprise.bridge.model.MqMessage;
import com.enterprise.bridge.model.ParsedPayload;
import com.enterprise.bridge.parser.MessageParseException;
import com.enterprise.bridge.parser.MessageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.UUID;

@Component
public class BridgeOrchestrator {

    private static final Logger logger = LoggerFactory.getLogger(BridgeOrchestrator.class);

    private final MessageParser messageParser;
    private final MarketingPlanApiClient apiClient;
    private final HdfsSafePayloadWriter hdfsWriter;
    private final KafkaEnvelopeFactory envelopeFactory;
    private final KafkaEnvelopePublisher kafkaPublisher;
    private final EventIdGenerator eventIdGenerator;
    private final AuditPublisher auditPublisher;

    public BridgeOrchestrator(
            MessageParser messageParser,
            MarketingPlanApiClient apiClient,
            HdfsSafePayloadWriter hdfsWriter,
            KafkaEnvelopeFactory envelopeFactory,
            KafkaEnvelopePublisher kafkaPublisher,
            EventIdGenerator eventIdGenerator,
            AuditPublisher auditPublisher) {
        this.messageParser = messageParser;
        this.apiClient = apiClient;
        this.hdfsWriter = hdfsWriter;
        this.envelopeFactory = envelopeFactory;
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

            EnrichedPayload enrichedPayload = enrichPayload(parsedPayload, ctx);
            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.ENRICHMENT_COMPLETED, "Payload enriched successfully", null);

            HdfsWriteResult hdfsResult = hdfsWriter.write(enrichedPayload);
            AuditEventType hdfsEventType = hdfsResult.isAlreadyExists()
                    ? AuditEventType.HDFS_WRITE_SKIPPED
                    : AuditEventType.HDFS_WRITE_COMPLETED;
            publishAudit(ctx, enrichedPayload.getTransactionId(), hdfsEventType,
                    "HDFS write completed: " + hdfsResult.getHdfsPath(), null);

            String kafkaOffset = publishToKafka(enrichedPayload, hdfsResult);
            publishAudit(ctx, enrichedPayload.getTransactionId(),
                    AuditEventType.KAFKA_PUBLISH_COMPLETED, "Published to Kafka, offset: " + kafkaOffset, null);

            publishAudit(ctx, parsedPayload.getTransactionId(),
                    AuditEventType.PROCESSING_COMPLETED, "Message processed successfully", null);

            logger.info("Successfully processed message: eventId={}, hdfsPath={}, kafkaOffset={}",
                    eventId, hdfsResult.getHdfsPath(), kafkaOffset);

            return ProcessingResult.success(eventId, hdfsResult.getHdfsPath(), kafkaOffset);

        } catch (MessageParseException e) {
            return handleParseFailure(ctx, e);
        } catch (EnrichmentException e) {
            return handleEnrichmentFailure(ctx, e);
        } catch (HdfsWriteException e) {
            return handleHdfsFailure(ctx, e);
        } catch (KafkaPublishException e) {
            return handleKafkaFailure(ctx, e);
        }
    }

    private EnrichedPayload enrichPayload(ParsedPayload parsedPayload, ProcessingContext ctx) {
        EnrichmentResult result = apiClient.enrich(parsedPayload);
        return new EnrichedPayload(
                parsedPayload,
                ctx,
                result.getAdditionalData(),
                result.getMarketingPlanId(),
                result.getCampaignId(),
                Instant.now()
        );
    }

    private String publishToKafka(EnrichedPayload payload, HdfsWriteResult hdfsResult) {
        KafkaEnvelope envelope = envelopeFactory.createEnvelope(payload, hdfsResult);
        return kafkaPublisher.publish(envelope);
    }

    private ProcessingResult handleParseFailure(ProcessingContext ctx, MessageParseException e) {
        logger.error("Parse failure for eventId {}: {}", ctx.getEventId(), e.getMessage());
        publishAudit(ctx, null, AuditEventType.PROCESSING_FAILED,
                "Parse failure", e.getMessage());
        return ProcessingResult.failure(ctx.getEventId(), "PARSE_ERROR", e.getMessage());
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
