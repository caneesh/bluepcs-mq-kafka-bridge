package com.enterprise.bridge.orchestrator;

import com.enterprise.bridge.api.EnrichmentException;
import com.enterprise.bridge.api.MarketingPlanApiClient;
import com.enterprise.bridge.api.MarketingPlanApiClient.EnrichmentResult;
import com.enterprise.bridge.audit.AuditEvent;
import com.enterprise.bridge.audit.AuditEventType;
import com.enterprise.bridge.audit.AuditPublisher;
import com.enterprise.bridge.hdfs.HdfsSafePayloadWriter;
import com.enterprise.bridge.hdfs.HdfsWriteException;
import com.enterprise.bridge.kafka.KafkaEnvelopeFactory;
import com.enterprise.bridge.kafka.KafkaEnvelopePublisher;
import com.enterprise.bridge.kafka.KafkaPublishException;
import com.enterprise.bridge.ledger.LedgerEntry;
import com.enterprise.bridge.ledger.LedgerRepository;
import com.enterprise.bridge.ledger.LedgerState;
import com.enterprise.bridge.model.EnrichedPayload;
import com.enterprise.bridge.model.HdfsWriteResult;
import com.enterprise.bridge.model.KafkaEnvelope;
import com.enterprise.bridge.model.MqMessage;
import com.enterprise.bridge.model.ParsedPayload;
import com.enterprise.bridge.parser.MessageParseException;
import com.enterprise.bridge.parser.MessageParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private final KafkaEnvelopeFactory envelopeFactory;
    private final KafkaEnvelopePublisher kafkaPublisher;
    private final LedgerRepository ledgerRepository;
    private final AuditPublisher auditPublisher;

    public BridgeOrchestrator(
            MessageParser messageParser,
            MarketingPlanApiClient apiClient,
            HdfsSafePayloadWriter hdfsWriter,
            KafkaEnvelopeFactory envelopeFactory,
            KafkaEnvelopePublisher kafkaPublisher,
            LedgerRepository ledgerRepository,
            AuditPublisher auditPublisher) {
        this.messageParser = messageParser;
        this.apiClient = apiClient;
        this.hdfsWriter = hdfsWriter;
        this.envelopeFactory = envelopeFactory;
        this.kafkaPublisher = kafkaPublisher;
        this.ledgerRepository = ledgerRepository;
        this.auditPublisher = auditPublisher;
    }

    public ProcessingResult process(MqMessage mqMessage) {
        String messageId = mqMessage.getMessageId();
        logger.info("Processing message: {}", messageId);

        if (isDuplicateMessage(messageId)) {
            logger.info("Duplicate message detected: {}", messageId);
            publishAudit(messageId, null, AuditEventType.DUPLICATE_DETECTED, "Duplicate message skipped", null);
            return ProcessingResult.duplicate(messageId);
        }

        LedgerEntry ledgerEntry = createInitialLedgerEntry(mqMessage);

        try {
            ParsedPayload parsedPayload = parseMessage(mqMessage, ledgerEntry);
            EnrichedPayload enrichedPayload = enrichPayload(parsedPayload, ledgerEntry);
            HdfsWriteResult hdfsResult = writeToHdfs(enrichedPayload, ledgerEntry);
            String kafkaOffset = publishToKafka(enrichedPayload, hdfsResult, ledgerEntry);

            completeProcessing(ledgerEntry, kafkaOffset);
            publishAudit(messageId, parsedPayload.getTransactionId(),
                    AuditEventType.PROCESSING_COMPLETED, "Message processed successfully", null);

            logger.info("Successfully processed message: {}", messageId);
            return ProcessingResult.success(messageId, hdfsResult.getHdfsPath(), kafkaOffset);

        } catch (MessageParseException e) {
            return handleParseFailure(ledgerEntry, e);
        } catch (EnrichmentException e) {
            return handleEnrichmentFailure(ledgerEntry, e);
        } catch (HdfsWriteException e) {
            return handleHdfsFailure(ledgerEntry, e);
        } catch (KafkaPublishException e) {
            return handleKafkaFailure(ledgerEntry, e);
        }
    }

    private boolean isDuplicateMessage(String messageId) {
        return ledgerRepository.findByMessageId(messageId)
                .map(entry -> entry.getState().isTerminal())
                .orElse(false);
    }

    private LedgerEntry createInitialLedgerEntry(MqMessage mqMessage) {
        LedgerEntry entry = LedgerEntry.builder()
                .messageId(mqMessage.getMessageId())
                .state(LedgerState.RECEIVED)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
        ledgerRepository.save(entry);
        publishAudit(mqMessage.getMessageId(), null, AuditEventType.MESSAGE_RECEIVED, "Message received from MQ", null);
        return entry;
    }

    private ParsedPayload parseMessage(MqMessage mqMessage, LedgerEntry ledgerEntry) {
        ParsedPayload payload = messageParser.parse(mqMessage);
        LedgerEntry updated = ledgerEntry.toBuilder()
                .transactionId(payload.getTransactionId())
                .state(LedgerState.PARSED)
                .updatedAt(Instant.now())
                .build();
        ledgerRepository.update(updated);
        publishAudit(mqMessage.getMessageId(), payload.getTransactionId(), AuditEventType.MESSAGE_PARSED, "Message parsed successfully", null);
        return payload;
    }

    private EnrichedPayload enrichPayload(ParsedPayload parsedPayload, LedgerEntry ledgerEntry) {
        EnrichmentResult result = apiClient.enrich(parsedPayload);

        EnrichedPayload enrichedPayload = new EnrichedPayload(
                parsedPayload,
                result.getAdditionalData(),
                result.getMarketingPlanId(),
                result.getCampaignId(),
                Instant.now()
        );

        LedgerEntry updated = ledgerEntry.withState(LedgerState.ENRICHED);
        ledgerRepository.update(updated);
        publishAudit(parsedPayload.getMessageId(), parsedPayload.getTransactionId(),
                AuditEventType.ENRICHMENT_COMPLETED, "Payload enriched successfully", null);

        return enrichedPayload;
    }

    private HdfsWriteResult writeToHdfs(EnrichedPayload payload, LedgerEntry ledgerEntry) {
        HdfsWriteResult result = hdfsWriter.write(payload);

        LedgerEntry updated = ledgerEntry.withHdfsPath(result.getHdfsPath(), result.getChecksum())
                .withState(LedgerState.HDFS_WRITTEN);
        ledgerRepository.update(updated);

        AuditEventType eventType = result.isAlreadyExists()
                ? AuditEventType.HDFS_WRITE_SKIPPED
                : AuditEventType.HDFS_WRITE_COMPLETED;
        publishAudit(payload.getMessageId(), payload.getTransactionId(), eventType,
                "HDFS write completed: " + result.getHdfsPath(), null);

        return result;
    }

    private String publishToKafka(EnrichedPayload payload, HdfsWriteResult hdfsResult, LedgerEntry ledgerEntry) {
        KafkaEnvelope envelope = envelopeFactory.createEnvelope(payload, hdfsResult);
        String offset = kafkaPublisher.publish(envelope);

        LedgerEntry updated = ledgerEntry.withKafkaOffset(offset).withState(LedgerState.KAFKA_PUBLISHED);
        ledgerRepository.update(updated);
        publishAudit(payload.getMessageId(), payload.getTransactionId(),
                AuditEventType.KAFKA_PUBLISH_COMPLETED, "Published to Kafka, offset: " + offset, null);

        return offset;
    }

    private void completeProcessing(LedgerEntry ledgerEntry, String kafkaOffset) {
        LedgerEntry completed = ledgerEntry.toBuilder()
                .state(LedgerState.COMPLETED)
                .kafkaOffset(kafkaOffset)
                .updatedAt(Instant.now())
                .build();
        ledgerRepository.update(completed);
    }

    private ProcessingResult handleParseFailure(LedgerEntry entry, MessageParseException e) {
        logger.error("Parse failure for message {}: {}", entry.getMessageId(), e.getMessage());
        LedgerEntry updated = entry.withState(LedgerState.FAILED_PARSE).withError(e.getMessage());
        ledgerRepository.update(updated);
        publishAudit(entry.getMessageId(), null, AuditEventType.PROCESSING_FAILED,
                "Parse failure", e.getMessage());
        return ProcessingResult.failure(entry.getMessageId(), "PARSE_ERROR", e.getMessage());
    }

    private ProcessingResult handleEnrichmentFailure(LedgerEntry entry, EnrichmentException e) {
        logger.error("Enrichment failure for message {}: {}", entry.getMessageId(), e.getMessage());
        LedgerEntry updated = entry.withState(LedgerState.FAILED_ENRICHMENT).withError(e.getMessage());
        ledgerRepository.update(updated);
        publishAudit(entry.getMessageId(), entry.getTransactionId(),
                AuditEventType.ENRICHMENT_FAILED, "Enrichment failure", e.getMessage());
        return ProcessingResult.failure(entry.getMessageId(), "ENRICHMENT_ERROR", e.getMessage());
    }

    private ProcessingResult handleHdfsFailure(LedgerEntry entry, HdfsWriteException e) {
        logger.error("HDFS write failure for message {}: {}", entry.getMessageId(), e.getMessage());
        LedgerEntry updated = entry.withState(LedgerState.FAILED_HDFS).withError(e.getMessage());
        ledgerRepository.update(updated);
        publishAudit(entry.getMessageId(), entry.getTransactionId(),
                AuditEventType.HDFS_WRITE_FAILED, "HDFS write failure", e.getMessage());
        return ProcessingResult.failure(entry.getMessageId(), "HDFS_ERROR", e.getMessage());
    }

    private ProcessingResult handleKafkaFailure(LedgerEntry entry, KafkaPublishException e) {
        logger.error("Kafka publish failure for message {}: {}", entry.getMessageId(), e.getMessage());
        LedgerEntry updated = entry.withState(LedgerState.FAILED_KAFKA).withError(e.getMessage());
        ledgerRepository.update(updated);
        publishAudit(entry.getMessageId(), entry.getTransactionId(),
                AuditEventType.KAFKA_PUBLISH_FAILED, "Kafka publish failure", e.getMessage());
        return ProcessingResult.failure(entry.getMessageId(), "KAFKA_ERROR", e.getMessage());
    }

    private void publishAudit(String messageId, String transactionId, AuditEventType eventType,
                              String description, String errorMessage) {
        AuditEvent event = AuditEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .messageId(messageId)
                .transactionId(transactionId)
                .eventType(eventType)
                .description(description)
                .errorMessage(errorMessage)
                .timestamp(Instant.now())
                .build();
        auditPublisher.publishAsync(event);
    }
}
