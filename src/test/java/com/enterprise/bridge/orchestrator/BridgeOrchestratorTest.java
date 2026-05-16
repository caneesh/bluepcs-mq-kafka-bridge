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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("BridgeOrchestrator")
class BridgeOrchestratorTest {

    @Mock
    private MessageParser messageParser;
    @Mock
    private MarketingPlanApiClient apiClient;
    @Mock
    private HdfsSafePayloadWriter hdfsWriter;
    @Mock
    private KafkaEnvelopeFactory envelopeFactory;
    @Mock
    private KafkaEnvelopePublisher kafkaPublisher;
    @Mock
    private LedgerRepository ledgerRepository;
    @Mock
    private AuditPublisher auditPublisher;

    @Captor
    private ArgumentCaptor<LedgerEntry> ledgerEntryCaptor;
    @Captor
    private ArgumentCaptor<AuditEvent> auditEventCaptor;

    private BridgeOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        orchestrator = new BridgeOrchestrator(
                messageParser,
                apiClient,
                hdfsWriter,
                envelopeFactory,
                kafkaPublisher,
                ledgerRepository,
                auditPublisher
        );
    }

    @Nested
    @DisplayName("happy path processing")
    class HappyPath {

        @Test
        @DisplayName("should process message successfully through all stages")
        void shouldProcessMessageSuccessfully() {
            MqMessage mqMessage = createMqMessage("MSG-001");
            ParsedPayload parsedPayload = createParsedPayload("MSG-001", "TXN-001");
            EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);
            KafkaEnvelope envelope = createEnvelope("MSG-001", "TXN-001");

            when(ledgerRepository.findByMessageId("MSG-001")).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class))).thenReturn(hdfsResult);
            when(envelopeFactory.createEnvelope(any(), any())).thenReturn(envelope);
            when(kafkaPublisher.publish(envelope)).thenReturn("12345");
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getMessageId()).isEqualTo("MSG-001");
            assertThat(result.getHdfsPath()).isEqualTo("/path/file.json");
            assertThat(result.getKafkaOffset()).isEqualTo("12345");
        }

        @Test
        @DisplayName("should create ledger entry at start")
        void shouldCreateLedgerEntryAtStart() {
            MqMessage mqMessage = createMqMessage("MSG-002");
            setupSuccessfulFlow("MSG-002", "TXN-002");

            orchestrator.process(mqMessage);

            verify(ledgerRepository).save(ledgerEntryCaptor.capture());
            LedgerEntry entry = ledgerEntryCaptor.getValue();
            assertThat(entry.getMessageId()).isEqualTo("MSG-002");
            assertThat(entry.getState()).isEqualTo(LedgerState.RECEIVED);
        }

        @Test
        @DisplayName("should publish audit events for each stage")
        void shouldPublishAuditEventsForEachStage() {
            MqMessage mqMessage = createMqMessage("MSG-003");
            setupSuccessfulFlow("MSG-003", "TXN-003");

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(6)).publishAsync(auditEventCaptor.capture());
        }
    }

    @Nested
    @DisplayName("parser failure handling")
    class ParserFailure {

        @Test
        @DisplayName("should return failure result on parse error")
        void shouldReturnFailureOnParseError() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-001");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid JSON", "MSG-PARSE-001", "{}"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
        }

        @Test
        @DisplayName("should update ledger to FAILED_PARSE state")
        void shouldUpdateLedgerToFailedParseState() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-002");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid JSON", "MSG-PARSE-002", "{}"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(ledgerRepository, times(1)).update(ledgerEntryCaptor.capture());
            LedgerEntry entry = ledgerEntryCaptor.getValue();
            assertThat(entry.getState()).isEqualTo(LedgerState.FAILED_PARSE);
        }

        @Test
        @DisplayName("should not call downstream services on parse failure")
        void shouldNotCallDownstreamServicesOnParseFailure() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-003");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid", "MSG-PARSE-003", "{}"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(apiClient, never()).enrich(any());
            verify(hdfsWriter, never()).write(any());
            verify(kafkaPublisher, never()).publish(any());
        }
    }

    @Nested
    @DisplayName("enrichment failure handling")
    class EnrichmentFailure {

        @Test
        @DisplayName("should return failure result on enrichment error")
        void shouldReturnFailureOnEnrichmentError() {
            MqMessage mqMessage = createMqMessage("MSG-ENR-001");
            ParsedPayload parsedPayload = createParsedPayload("MSG-ENR-001", "TXN-ENR-001");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload))
                    .thenThrow(new EnrichmentException("API Error", "ENT-001", 500));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("ENRICHMENT_ERROR");
        }

        @Test
        @DisplayName("should update ledger to FAILED_ENRICHMENT state")
        void shouldUpdateLedgerToFailedEnrichmentState() {
            MqMessage mqMessage = createMqMessage("MSG-ENR-002");
            ParsedPayload parsedPayload = createParsedPayload("MSG-ENR-002", "TXN-ENR-002");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload))
                    .thenThrow(new EnrichmentException("API Error", "ENT-001"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(ledgerRepository, times(2)).update(ledgerEntryCaptor.capture());
            LedgerEntry finalEntry = ledgerEntryCaptor.getAllValues().get(1);
            assertThat(finalEntry.getState()).isEqualTo(LedgerState.FAILED_ENRICHMENT);
        }
    }

    @Nested
    @DisplayName("HDFS failure handling")
    class HdfsFailure {

        @Test
        @DisplayName("should return failure result on HDFS error")
        void shouldReturnFailureOnHdfsError() {
            MqMessage mqMessage = createMqMessage("MSG-HDFS-001");
            ParsedPayload parsedPayload = createParsedPayload("MSG-HDFS-001", "TXN-HDFS-001");
            EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class)))
                    .thenThrow(new HdfsWriteException("Write failed", "/path/file.json", "MSG-HDFS-001"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("HDFS_ERROR");
        }

        @Test
        @DisplayName("should update ledger to FAILED_HDFS state")
        void shouldUpdateLedgerToFailedHdfsState() {
            MqMessage mqMessage = createMqMessage("MSG-HDFS-002");
            ParsedPayload parsedPayload = createParsedPayload("MSG-HDFS-002", "TXN-HDFS-002");
            EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class)))
                    .thenThrow(new HdfsWriteException("Write failed", "/path", "MSG-HDFS-002"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(ledgerRepository, times(3)).update(ledgerEntryCaptor.capture());
            LedgerEntry finalEntry = ledgerEntryCaptor.getAllValues().get(2);
            assertThat(finalEntry.getState()).isEqualTo(LedgerState.FAILED_HDFS);
        }

        @Test
        @DisplayName("should not call Kafka on HDFS failure")
        void shouldNotCallKafkaOnHdfsFailure() {
            MqMessage mqMessage = createMqMessage("MSG-HDFS-003");
            ParsedPayload parsedPayload = createParsedPayload("MSG-HDFS-003", "TXN-HDFS-003");
            EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class)))
                    .thenThrow(new HdfsWriteException("Write failed", "/path", "MSG-HDFS-003"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(kafkaPublisher, never()).publish(any());
        }
    }

    @Nested
    @DisplayName("Kafka failure handling")
    class KafkaFailure {

        @Test
        @DisplayName("should return failure result on Kafka error")
        void shouldReturnFailureOnKafkaError() {
            MqMessage mqMessage = createMqMessage("MSG-KFK-001");
            ParsedPayload parsedPayload = createParsedPayload("MSG-KFK-001", "TXN-KFK-001");
            EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);
            KafkaEnvelope envelope = createEnvelope("MSG-KFK-001", "TXN-KFK-001");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class))).thenReturn(hdfsResult);
            when(envelopeFactory.createEnvelope(any(), any())).thenReturn(envelope);
            when(kafkaPublisher.publish(envelope))
                    .thenThrow(new KafkaPublishException("Publish failed", "MSG-KFK-001", "topic"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("KAFKA_ERROR");
        }

        @Test
        @DisplayName("should update ledger to FAILED_KAFKA state")
        void shouldUpdateLedgerToFailedKafkaState() {
            MqMessage mqMessage = createMqMessage("MSG-KFK-002");
            ParsedPayload parsedPayload = createParsedPayload("MSG-KFK-002", "TXN-KFK-002");
            EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);
            KafkaEnvelope envelope = createEnvelope("MSG-KFK-002", "TXN-KFK-002");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class))).thenReturn(hdfsResult);
            when(envelopeFactory.createEnvelope(any(), any())).thenReturn(envelope);
            when(kafkaPublisher.publish(envelope))
                    .thenThrow(new KafkaPublishException("Publish failed", "MSG-KFK-002", "topic"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(ledgerRepository, times(4)).update(ledgerEntryCaptor.capture());
            LedgerEntry finalEntry = ledgerEntryCaptor.getAllValues().get(3);
            assertThat(finalEntry.getState()).isEqualTo(LedgerState.FAILED_KAFKA);
        }
    }

    @Nested
    @DisplayName("duplicate message handling")
    class DuplicateHandling {

        @Test
        @DisplayName("should return duplicate result for already completed message")
        void shouldReturnDuplicateForCompletedMessage() {
            MqMessage mqMessage = createMqMessage("MSG-DUP-001");
            LedgerEntry existingEntry = LedgerEntry.builder()
                    .messageId("MSG-DUP-001")
                    .state(LedgerState.COMPLETED)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            when(ledgerRepository.findByMessageId("MSG-DUP-001")).thenReturn(Optional.of(existingEntry));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isDuplicate()).isTrue();
            assertThat(result.getMessageId()).isEqualTo("MSG-DUP-001");
        }

        @Test
        @DisplayName("should not process duplicate message")
        void shouldNotProcessDuplicateMessage() {
            MqMessage mqMessage = createMqMessage("MSG-DUP-002");
            LedgerEntry existingEntry = LedgerEntry.builder()
                    .messageId("MSG-DUP-002")
                    .state(LedgerState.COMPLETED)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            when(ledgerRepository.findByMessageId("MSG-DUP-002")).thenReturn(Optional.of(existingEntry));
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(messageParser, never()).parse(any());
            verify(apiClient, never()).enrich(any());
            verify(hdfsWriter, never()).write(any());
            verify(kafkaPublisher, never()).publish(any());
        }

        @Test
        @DisplayName("should publish audit event for duplicate")
        void shouldPublishAuditEventForDuplicate() {
            MqMessage mqMessage = createMqMessage("MSG-DUP-003");
            LedgerEntry existingEntry = LedgerEntry.builder()
                    .messageId("MSG-DUP-003")
                    .state(LedgerState.COMPLETED)
                    .createdAt(Instant.now())
                    .updatedAt(Instant.now())
                    .build();

            when(ledgerRepository.findByMessageId("MSG-DUP-003")).thenReturn(Optional.of(existingEntry));
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(auditPublisher).publishAsync(auditEventCaptor.capture());
            AuditEvent event = auditEventCaptor.getValue();
            assertThat(event.getEventType()).isEqualTo(AuditEventType.DUPLICATE_DETECTED);
        }
    }

    @Nested
    @DisplayName("ledger transitions")
    class LedgerTransitions {

        @Test
        @DisplayName("should transition through all states on success")
        void shouldTransitionThroughAllStatesOnSuccess() {
            MqMessage mqMessage = createMqMessage("MSG-TRANS-001");
            setupSuccessfulFlow("MSG-TRANS-001", "TXN-TRANS-001");

            orchestrator.process(mqMessage);

            verify(ledgerRepository).save(ledgerEntryCaptor.capture());
            verify(ledgerRepository, times(5)).update(ledgerEntryCaptor.capture());

            java.util.List<LedgerEntry> entries = ledgerEntryCaptor.getAllValues();
            assertThat(entries.get(0).getState()).isEqualTo(LedgerState.RECEIVED);
            assertThat(entries.get(1).getState()).isEqualTo(LedgerState.PARSED);
            assertThat(entries.get(2).getState()).isEqualTo(LedgerState.ENRICHED);
            assertThat(entries.get(3).getState()).isEqualTo(LedgerState.HDFS_WRITTEN);
            assertThat(entries.get(4).getState()).isEqualTo(LedgerState.KAFKA_PUBLISHED);
            assertThat(entries.get(5).getState()).isEqualTo(LedgerState.COMPLETED);
        }

        @Test
        @DisplayName("should update hdfs path and checksum in ledger")
        void shouldUpdateHdfsPathAndChecksumInLedger() {
            MqMessage mqMessage = createMqMessage("MSG-TRANS-002");
            setupSuccessfulFlow("MSG-TRANS-002", "TXN-TRANS-002");

            orchestrator.process(mqMessage);

            verify(ledgerRepository, times(5)).update(ledgerEntryCaptor.capture());
            LedgerEntry hdfsEntry = ledgerEntryCaptor.getAllValues().get(2);
            assertThat(hdfsEntry.getHdfsPath()).isEqualTo("/path/file.json");
            assertThat(hdfsEntry.getChecksum()).isEqualTo("checksum");
        }

        @Test
        @DisplayName("should update kafka offset in ledger")
        void shouldUpdateKafkaOffsetInLedger() {
            MqMessage mqMessage = createMqMessage("MSG-TRANS-003");
            setupSuccessfulFlow("MSG-TRANS-003", "TXN-TRANS-003");

            orchestrator.process(mqMessage);

            verify(ledgerRepository, times(5)).update(ledgerEntryCaptor.capture());
            LedgerEntry kafkaEntry = ledgerEntryCaptor.getAllValues().get(3);
            assertThat(kafkaEntry.getKafkaOffset()).isEqualTo("12345");
        }
    }

    @Nested
    @DisplayName("audit publishing")
    class AuditPublishing {

        @Test
        @DisplayName("should publish MESSAGE_RECEIVED audit event")
        void shouldPublishMessageReceivedAuditEvent() {
            MqMessage mqMessage = createMqMessage("MSG-AUD-001");
            setupSuccessfulFlow("MSG-AUD-001", "TXN-AUD-001");

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(6)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();
            assertThat(events.get(0).getEventType()).isEqualTo(AuditEventType.MESSAGE_RECEIVED);
        }

        @Test
        @DisplayName("should publish PROCESSING_COMPLETED audit event on success")
        void shouldPublishProcessingCompletedAuditEvent() {
            MqMessage mqMessage = createMqMessage("MSG-AUD-002");
            setupSuccessfulFlow("MSG-AUD-002", "TXN-AUD-002");

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(6)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();
            assertThat(events.get(5).getEventType()).isEqualTo(AuditEventType.PROCESSING_COMPLETED);
        }

        @Test
        @DisplayName("should publish PROCESSING_FAILED audit event on failure")
        void shouldPublishProcessingFailedAuditEvent() {
            MqMessage mqMessage = createMqMessage("MSG-AUD-003");

            when(ledgerRepository.findByMessageId(anyString())).thenReturn(Optional.empty());
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid", "MSG-AUD-003", "{}"));
            doNothing().when(ledgerRepository).save(any());
            doNothing().when(ledgerRepository).update(any());
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(2)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();
            assertThat(events.get(1).getEventType()).isEqualTo(AuditEventType.PROCESSING_FAILED);
        }
    }

    private void setupSuccessfulFlow(String messageId, String transactionId) {
        ParsedPayload parsedPayload = createParsedPayload(messageId, transactionId);
        EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());
        HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);
        KafkaEnvelope envelope = createEnvelope(messageId, transactionId);

        when(ledgerRepository.findByMessageId(messageId)).thenReturn(Optional.empty());
        when(messageParser.parse(any())).thenReturn(parsedPayload);
        when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
        when(hdfsWriter.write(any(EnrichedPayload.class))).thenReturn(hdfsResult);
        when(envelopeFactory.createEnvelope(any(), any())).thenReturn(envelope);
        when(kafkaPublisher.publish(envelope)).thenReturn("12345");
        doNothing().when(ledgerRepository).save(any());
        doNothing().when(ledgerRepository).update(any());
        doNothing().when(auditPublisher).publishAsync(any());
    }

    private MqMessage createMqMessage(String messageId) {
        return new MqMessage(
                messageId,
                "CORR-" + messageId,
                "{\"test\":\"payload\"}",
                Instant.now(),
                "TEST.QUEUE"
        );
    }

    private ParsedPayload createParsedPayload(String messageId, String transactionId) {
        return new ParsedPayload(
                messageId,
                transactionId,
                "ORDER_CREATED",
                "ENT-001",
                Map.of("key", "value"),
                Instant.now(),
                "{\"test\":\"payload\"}"
        );
    }

    private KafkaEnvelope createEnvelope(String messageId, String transactionId) {
        return KafkaEnvelope.builder()
                .messageId(messageId)
                .transactionId(transactionId)
                .eventType("ORDER_CREATED")
                .entityId("ENT-001")
                .hdfsPath("/path/file.json")
                .checksum("checksum")
                .eventTimestamp(Instant.now())
                .processedAt(Instant.now())
                .schemaVersion("1.0")
                .build();
    }
}
