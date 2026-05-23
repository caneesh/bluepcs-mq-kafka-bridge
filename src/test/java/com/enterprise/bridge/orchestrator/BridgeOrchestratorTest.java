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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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
    private EventIdGenerator eventIdGenerator;
    @Mock
    private AuditPublisher auditPublisher;

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
                eventIdGenerator,
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
            KafkaEnvelope envelope = createEnvelope("event-id-001", "MSG-001", "TXN-001");

            when(eventIdGenerator.generateEventId("MSG-001")).thenReturn("event-id-001");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class))).thenReturn(hdfsResult);
            when(envelopeFactory.createEnvelope(any(), any())).thenReturn(envelope);
            when(kafkaPublisher.publish(envelope)).thenReturn("12345");
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getEventId()).isEqualTo("event-id-001");
            assertThat(result.getHdfsPath()).isEqualTo("/path/file.json");
            assertThat(result.getKafkaOffset()).isEqualTo("12345");
        }

        @Test
        @DisplayName("should generate deterministic event ID from JMS message ID")
        void shouldGenerateDeterministicEventId() {
            MqMessage mqMessage = createMqMessage("JMS-MSG-123");
            setupSuccessfulFlow("JMS-MSG-123", "TXN-001", "deterministic-event-id");

            ProcessingResult result = orchestrator.process(mqMessage);

            verify(eventIdGenerator).generateEventId("JMS-MSG-123");
            assertThat(result.getEventId()).isEqualTo("deterministic-event-id");
        }

        @Test
        @DisplayName("should publish audit events for each stage")
        void shouldPublishAuditEventsForEachStage() {
            MqMessage mqMessage = createMqMessage("MSG-003");
            setupSuccessfulFlow("MSG-003", "TXN-003", "event-id-003");

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

            when(eventIdGenerator.generateEventId("MSG-PARSE-001")).thenReturn("event-id-parse-001");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid JSON", "MSG-PARSE-001", "{}"));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
        }

        @Test
        @DisplayName("should not call downstream services on parse failure")
        void shouldNotCallDownstreamServicesOnParseFailure() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-003");

            when(eventIdGenerator.generateEventId("MSG-PARSE-003")).thenReturn("event-id-parse-003");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid", "MSG-PARSE-003", "{}"));
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

            when(eventIdGenerator.generateEventId("MSG-ENR-001")).thenReturn("event-id-enr-001");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload))
                    .thenThrow(new EnrichmentException("API Error", "ENT-001", 500));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("ENRICHMENT_ERROR");
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

            when(eventIdGenerator.generateEventId("MSG-HDFS-001")).thenReturn("event-id-hdfs-001");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class)))
                    .thenThrow(new HdfsWriteException("Write failed", "/path/file.json", "MSG-HDFS-001"));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("HDFS_ERROR");
        }

        @Test
        @DisplayName("should not call Kafka on HDFS failure")
        void shouldNotCallKafkaOnHdfsFailure() {
            MqMessage mqMessage = createMqMessage("MSG-HDFS-003");
            ParsedPayload parsedPayload = createParsedPayload("MSG-HDFS-003", "TXN-HDFS-003");
            EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());

            when(eventIdGenerator.generateEventId("MSG-HDFS-003")).thenReturn("event-id-hdfs-003");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class)))
                    .thenThrow(new HdfsWriteException("Write failed", "/path", "MSG-HDFS-003"));
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
            KafkaEnvelope envelope = createEnvelope("event-id-kfk-001", "MSG-KFK-001", "TXN-KFK-001");

            when(eventIdGenerator.generateEventId("MSG-KFK-001")).thenReturn("event-id-kfk-001");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
            when(hdfsWriter.write(any(EnrichedPayload.class))).thenReturn(hdfsResult);
            when(envelopeFactory.createEnvelope(any(), any())).thenReturn(envelope);
            when(kafkaPublisher.publish(envelope))
                    .thenThrow(new KafkaPublishException("Publish failed", "MSG-KFK-001", "topic"));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("KAFKA_ERROR");
        }
    }

    @Nested
    @DisplayName("audit publishing")
    class AuditPublishing {

        @Test
        @DisplayName("should publish MESSAGE_RECEIVED audit event")
        void shouldPublishMessageReceivedAuditEvent() {
            MqMessage mqMessage = createMqMessage("MSG-AUD-001");
            setupSuccessfulFlow("MSG-AUD-001", "TXN-AUD-001", "event-id-aud-001");

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(6)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();
            assertThat(events.get(0).getEventType()).isEqualTo(AuditEventType.MESSAGE_RECEIVED);
        }

        @Test
        @DisplayName("should publish PROCESSING_COMPLETED audit event on success")
        void shouldPublishProcessingCompletedAuditEvent() {
            MqMessage mqMessage = createMqMessage("MSG-AUD-002");
            setupSuccessfulFlow("MSG-AUD-002", "TXN-AUD-002", "event-id-aud-002");

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(6)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();
            assertThat(events.get(5).getEventType()).isEqualTo(AuditEventType.PROCESSING_COMPLETED);
        }

        @Test
        @DisplayName("should publish PROCESSING_FAILED audit event on failure")
        void shouldPublishProcessingFailedAuditEvent() {
            MqMessage mqMessage = createMqMessage("MSG-AUD-003");

            when(eventIdGenerator.generateEventId("MSG-AUD-003")).thenReturn("event-id-aud-003");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid", "MSG-AUD-003", "{}"));
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(2)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();
            assertThat(events.get(1).getEventType()).isEqualTo(AuditEventType.PROCESSING_FAILED);
        }

        @Test
        @DisplayName("should include eventId and bridgeEventId in audit events")
        void shouldIncludeEventIdAndBridgeEventIdInAuditEvents() {
            MqMessage mqMessage = createMqMessage("MSG-AUD-004");
            setupSuccessfulFlow("MSG-AUD-004", "TXN-AUD-004", "event-id-aud-004");

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(6)).publishAsync(auditEventCaptor.capture());
            AuditEvent event = auditEventCaptor.getAllValues().get(0);
            assertThat(event.getEventId()).isEqualTo("event-id-aud-004");
            assertThat(event.getBridgeEventId()).isNotNull();
            assertThat(event.getOriginalMqMessageId()).isEqualTo("MSG-AUD-004");
        }
    }

    private void setupSuccessfulFlow(String messageId, String transactionId, String eventId) {
        ParsedPayload parsedPayload = createParsedPayload(messageId, transactionId);
        EnrichmentResult enrichmentResult = new EnrichmentResult("MP-001", "CAMP-001", Map.of());
        HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);
        KafkaEnvelope envelope = createEnvelope(eventId, messageId, transactionId);

        when(eventIdGenerator.generateEventId(messageId)).thenReturn(eventId);
        when(messageParser.parse(any())).thenReturn(parsedPayload);
        when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult);
        when(hdfsWriter.write(any(EnrichedPayload.class))).thenReturn(hdfsResult);
        when(envelopeFactory.createEnvelope(any(), any())).thenReturn(envelope);
        when(kafkaPublisher.publish(envelope)).thenReturn("12345");
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

    private KafkaEnvelope createEnvelope(String eventId, String messageId, String transactionId) {
        return KafkaEnvelope.builder()
                .eventId(eventId)
                .bridgeMessageId("bridge-" + messageId)
                .originalMqMessageId(messageId)
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
