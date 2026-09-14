package com.hcsc.bridge.orchestrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcsc.bridge.api.EnrichmentException;
import com.hcsc.bridge.api.EnrichmentWrapperFactory;
import com.hcsc.bridge.api.MarketingPlanApiClient;
import com.hcsc.bridge.api.MarketingPlanApiClient.EnrichmentResult;
import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;
import com.hcsc.bridge.core.EventIdGenerator;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("BridgeOrchestrator")
class BridgeOrchestratorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private MessageParser messageParser;
    @Mock
    private MarketingPlanApiClient apiClient;
    @Mock
    private HdfsSafePayloadWriter hdfsWriter;
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
                new EnrichmentWrapperFactory(),
                new KafkaNotificationFactory(),
                kafkaPublisher,
                eventIdGenerator,
                auditPublisher
        );
    }

    private static EnrichmentResult enrichmentResult() {
        return new EnrichmentResult("MP-001", null, Map.of(), rawResponse());
    }

    private static JsonNode rawResponse() {
        try {
            return MAPPER.readTree("{\"PlanResponse\":{"
                    + "\"planIdentification\":{\"marketingPlanIdentifier\":\"MP-001\"},"
                    + "\"changeEvent\":{\"typeName\":\"Update\",\"timestamp\":\"20260710T162108.143 CDT\"}"
                    + "}}");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    @DisplayName("redelivery after acknowledgement failure (at-least-once)")
    class RedeliveryAfterAckFailure {

        /**
         * The sequence behind review finding P0-2: HDFS write succeeds, Kafka publish
         * succeeds, the MQ acknowledge fails (the listener deliberately does not undo any
         * work — see MqMessageListener.acknowledgeProcessedMessage), and the broker
         * redelivers the same JMS message id. The redelivery must be idempotent on HDFS
         * and explicitly duplicate on Kafka: this bridge is at-least-once, not exactly-once.
         */
        @Test
        @DisplayName("redelivery reuses the eventId, skips the HDFS rewrite, and republishes to Kafka")
        void redeliveryIsIdempotentOnHdfsAndDuplicateOnKafka() {
            MqMessage first = createMqMessage("MSG-REDELIVER-001");
            MqMessage redelivered = createMqMessage("MSG-REDELIVER-001"); // same JMS message id
            ParsedPayload parsed = createParsedPayload("MSG-REDELIVER-001", "TXN-001");

            when(eventIdGenerator.generateEventId("MSG-REDELIVER-001")).thenReturn("stable-event-id");
            when(messageParser.parse(any(MqMessage.class))).thenReturn(parsed);
            when(apiClient.enrich(parsed)).thenReturn(enrichmentResult());
            // First delivery writes the file; the redelivery finds the identical file in place
            when(hdfsWriter.write(any(EnrichedPayload.class), anyString()))
                    .thenReturn(HdfsWriteResult.success("/path/stable-event-id.json", "checksum", 1024))
                    .thenReturn(HdfsWriteResult.alreadyExists("/path/stable-event-id.json", "checksum"));
            when(kafkaPublisher.publish(anyString(), anyString())).thenReturn("100", "101");
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult firstResult = orchestrator.process(first);
            ProcessingResult redeliveredResult = orchestrator.process(redelivered);

            assertThat(firstResult.isSuccessful()).isTrue();
            assertThat(redeliveredResult.isSuccessful()).isTrue();
            // Event id is stable across deliveries of the same JMS message id
            assertThat(firstResult.getEventId()).isEqualTo("stable-event-id");
            assertThat(redeliveredResult.getEventId()).isEqualTo("stable-event-id");
            // Same advertised HDFS path both times; the second write was the idempotent skip
            assertThat(redeliveredResult.getHdfsPath()).isEqualTo(firstResult.getHdfsPath());
            // Kafka IS published twice with the same key: duplicates are explicit and expected;
            // downstream consumers dedupe on eventId
            ArgumentCaptor<String> keys = ArgumentCaptor.forClass(String.class);
            verify(kafkaPublisher, times(2)).publish(keys.capture(), anyString());
            assertThat(keys.getAllValues()).containsExactly("stable-event-id", "stable-event-id");
        }
    }

    @Nested
    @DisplayName("happy path processing")
    class HappyPath {

        @Test
        @DisplayName("should process message successfully through all stages")
        void shouldProcessMessageSuccessfully() {
            MqMessage mqMessage = createMqMessage("MSG-001");
            ParsedPayload parsedPayload = createParsedPayload("MSG-001", "TXN-001");
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);

            when(eventIdGenerator.generateEventId("MSG-001")).thenReturn("event-id-001");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult());
            when(hdfsWriter.write(any(EnrichedPayload.class), anyString())).thenReturn(hdfsResult);
            when(kafkaPublisher.publish(anyString(), anyString())).thenReturn("12345");
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getEventId()).isEqualTo("event-id-001");
            assertThat(result.getHdfsPath()).isEqualTo("/path/file.json");
            assertThat(result.getKafkaOffset()).isEqualTo("12345");
        }

        @Test
        @DisplayName("should write wrapper to HDFS and publish claim-check notification to Kafka")
        void shouldPublishWrapperToHdfsAndNotificationToKafka() {
            MqMessage mqMessage = createMqMessage("MSG-WRAP-001");
            setupSuccessfulFlow("MSG-WRAP-001", "TXN-001", "event-id-wrap-001");

            orchestrator.process(mqMessage);

            ArgumentCaptor<String> hdfsContent = ArgumentCaptor.forClass(String.class);
            verify(hdfsWriter).write(any(EnrichedPayload.class), hdfsContent.capture());

            ArgumentCaptor<String> kafkaKey = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> kafkaValue = ArgumentCaptor.forClass(String.class);
            verify(kafkaPublisher).publish(kafkaKey.capture(), kafkaValue.capture());

            assertThat(kafkaKey.getValue()).isEqualTo("event-id-wrap-001");
            // HDFS gets the full wrapper; Kafka gets only the small claim-check notification.
            assertThat(hdfsContent.getValue()).contains("RestAPIResponse");
            assertThat(kafkaValue.getValue()).doesNotContain("RestAPIResponse");
            assertThat(kafkaValue.getValue()).contains("\"hdfsPath\":\"/path/file.json\"");
            assertThat(kafkaValue.getValue()).contains("\"eventId\":\"event-id-wrap-001\"");
            assertThat(kafkaValue.getValue()).contains("\"changeEventTypeName\":\"Update\"");
            assertThat(kafkaValue.getValue()).contains("\"changeEventTimeStamp\":\"20260710T162108.143 CDT\"");
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
        @DisplayName("should quarantine raw payload and return quarantined result on parse error")
        void shouldQuarantineOnParseError() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-001");

            when(eventIdGenerator.generateEventId("MSG-PARSE-001")).thenReturn("event-id-parse-001");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid JSON", "MSG-PARSE-001", "{}"));
            when(hdfsWriter.writeQuarantine("event-id-parse-001", "{\"test\":\"payload\"}", "MSG-PARSE-001"))
                    .thenReturn(HdfsWriteResult.success("/errors/event-id-parse-001.json", "checksum", 20));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isQuarantined()).isTrue();
            assertThat(result.isFailed()).isFalse();
            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
            assertThat(result.getHdfsPath()).isEqualTo("/errors/event-id-parse-001.json");
        }

        @Test
        @DisplayName("should return failure (not quarantined) when quarantine write fails")
        void shouldReturnFailureWhenQuarantineWriteFails() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-002");

            when(eventIdGenerator.generateEventId("MSG-PARSE-002")).thenReturn("event-id-parse-002");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid JSON", "MSG-PARSE-002", "{}"));
            when(hdfsWriter.writeQuarantine(anyString(), anyString(), anyString()))
                    .thenThrow(new HdfsWriteException("HDFS down", "/errors/x.json", "MSG-PARSE-002"));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            // Payload was NOT preserved — must stay a failure so the message is redelivered
            assertThat(result.isFailed()).isTrue();
            assertThat(result.isQuarantined()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
        }

        @Test
        @DisplayName("should publish MESSAGE_QUARANTINED audit event on quarantine")
        void shouldPublishMessageQuarantinedAudit() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-004");

            when(eventIdGenerator.generateEventId("MSG-PARSE-004")).thenReturn("event-id-parse-004");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid", "MSG-PARSE-004", "{}"));
            when(hdfsWriter.writeQuarantine(anyString(), anyString(), anyString()))
                    .thenReturn(HdfsWriteResult.success("/errors/event-id-parse-004.json", "checksum", 20));
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(2)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();
            assertThat(events.get(0).getEventType()).isEqualTo(AuditEventType.MESSAGE_RECEIVED);
            assertThat(events.get(1).getEventType()).isEqualTo(AuditEventType.MESSAGE_QUARANTINED);
        }

        @Test
        @DisplayName("should not call downstream services on parse failure")
        void shouldNotCallDownstreamServicesOnParseFailure() {
            MqMessage mqMessage = createMqMessage("MSG-PARSE-003");

            when(eventIdGenerator.generateEventId("MSG-PARSE-003")).thenReturn("event-id-parse-003");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid", "MSG-PARSE-003", "{}"));
            when(hdfsWriter.writeQuarantine(anyString(), anyString(), anyString()))
                    .thenReturn(HdfsWriteResult.success("/errors/event-id-parse-003.json", "checksum", 20));
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(apiClient, never()).enrich(any());
            // The landing-directory write never happens; only the quarantine write does
            verify(hdfsWriter, never()).write(any(), anyString());
            verify(hdfsWriter).writeQuarantine("event-id-parse-003", "{\"test\":\"payload\"}", "MSG-PARSE-003");
            verify(kafkaPublisher, never()).publish(anyString(), anyString());
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

        @Test
        @DisplayName("should quarantine on non-retryable (4xx) enrichment error")
        void shouldQuarantineOnNonRetryableEnrichmentError() {
            MqMessage mqMessage = createMqMessage("MSG-ENR-002");
            ParsedPayload parsedPayload = createParsedPayload("MSG-ENR-002", "TXN-ENR-002");

            when(eventIdGenerator.generateEventId("MSG-ENR-002")).thenReturn("event-id-enr-002");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            // 404: a plan identifier that will never resolve — redelivery can never succeed
            when(apiClient.enrich(parsedPayload))
                    .thenThrow(new EnrichmentException("Plan not found", "ENT-002", 404));
            when(hdfsWriter.writeQuarantine(eq("event-id-enr-002"), anyString(), eq("MSG-ENR-002")))
                    .thenReturn(HdfsWriteResult.success("/errors/event-id-enr-002.json", "cs", 10));
            doNothing().when(auditPublisher).publishAsync(any());

            ProcessingResult result = orchestrator.process(mqMessage);

            assertThat(result.isQuarantined()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("ENRICHMENT_ERROR");
            assertThat(result.getHdfsPath()).isEqualTo("/errors/event-id-enr-002.json");
            verify(kafkaPublisher, never()).publish(anyString(), anyString());
        }

        @Test
        @DisplayName("should fall back to failure when quarantine of a non-retryable enrichment error fails")
        void shouldFailWhenEnrichmentQuarantineWriteFails() {
            MqMessage mqMessage = createMqMessage("MSG-ENR-003");
            ParsedPayload parsedPayload = createParsedPayload("MSG-ENR-003", "TXN-ENR-003");

            when(eventIdGenerator.generateEventId("MSG-ENR-003")).thenReturn("event-id-enr-003");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload))
                    .thenThrow(new EnrichmentException("Plan not found", "ENT-003", 404));
            when(hdfsWriter.writeQuarantine(anyString(), anyString(), anyString()))
                    .thenThrow(new HdfsWriteException("HDFS down", "/errors/x", "MSG-ENR-003"));
            doNothing().when(auditPublisher).publishAsync(any());

            // Safety invariant: no ack unless the payload was durably preserved
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

            when(eventIdGenerator.generateEventId("MSG-HDFS-001")).thenReturn("event-id-hdfs-001");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult());
            when(hdfsWriter.write(any(EnrichedPayload.class), anyString()))
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

            when(eventIdGenerator.generateEventId("MSG-HDFS-003")).thenReturn("event-id-hdfs-003");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult());
            when(hdfsWriter.write(any(EnrichedPayload.class), anyString()))
                    .thenThrow(new HdfsWriteException("Write failed", "/path", "MSG-HDFS-003"));
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(kafkaPublisher, never()).publish(anyString(), anyString());
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
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);

            when(eventIdGenerator.generateEventId("MSG-KFK-001")).thenReturn("event-id-kfk-001");
            when(messageParser.parse(mqMessage)).thenReturn(parsedPayload);
            when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult());
            when(hdfsWriter.write(any(EnrichedPayload.class), anyString())).thenReturn(hdfsResult);
            when(kafkaPublisher.publish(anyString(), anyString()))
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
        @DisplayName("balance metadata: stage events carry the keys the ABC check reads")
        void shouldCarryBalanceMetadata() {
            // scripts/abc-balance-check.sh reads these via get_json_object(metadata_json, ...).
            // Renaming a key here silently breaks the balance equations, so they are pinned.
            MqMessage mqMessage = createMqMessage("MSG-ABC-001");
            setupSuccessfulFlow("MSG-ABC-001", "TXN-ABC-001", "event-id-abc-001");

            orchestrator.process(mqMessage);

            verify(auditPublisher, times(6)).publishAsync(auditEventCaptor.capture());
            java.util.List<AuditEvent> events = auditEventCaptor.getAllValues();

            AuditEvent received = events.stream()
                    .filter(e -> e.getEventType() == AuditEventType.MESSAGE_RECEIVED)
                    .findFirst().orElseThrow();
            assertThat(received.getMetadata()).containsKey("payloadBytes");
            assertThat((Integer) received.getMetadata().get("payloadBytes")).isPositive();

            AuditEvent hdfs = events.stream()
                    .filter(e -> e.getEventType() == AuditEventType.HDFS_WRITE_COMPLETED
                              || e.getEventType() == AuditEventType.HDFS_WRITE_SKIPPED)
                    .findFirst().orElseThrow();
            assertThat(hdfs.getMetadata()).containsKeys("hdfsPath", "checksum", "bytesWritten");

            AuditEvent kafka = events.stream()
                    .filter(e -> e.getEventType() == AuditEventType.KAFKA_PUBLISH_COMPLETED)
                    .findFirst().orElseThrow();
            assertThat(kafka.getMetadata()).containsKey("kafkaOffset");
        }

        @Test
        @DisplayName("balance metadata: quarantine carries the errorCode that attributes it to a stage")
        void shouldCarryQuarantineErrorCode() {
            MqMessage mqMessage = createMqMessage("MSG-ABC-002");
            when(eventIdGenerator.generateEventId("MSG-ABC-002")).thenReturn("event-id-abc-002");
            when(messageParser.parse(mqMessage))
                    .thenThrow(new MessageParseException("Invalid JSON", "MSG-ABC-002", "{bad"));
            when(hdfsWriter.writeQuarantine(anyString(), anyString(), anyString()))
                    .thenReturn(HdfsWriteResult.success("/errors/event-id-abc-002.json", "cs", 4));
            doNothing().when(auditPublisher).publishAsync(any());

            orchestrator.process(mqMessage);

            verify(auditPublisher, atLeastOnce()).publishAsync(auditEventCaptor.capture());
            AuditEvent quarantined = auditEventCaptor.getAllValues().stream()
                    .filter(e -> e.getEventType() == AuditEventType.MESSAGE_QUARANTINED)
                    .findFirst().orElseThrow();
            // Equation 1 subtracts PARSE_ERROR quarantines; equation 2 subtracts ENRICHMENT_ERROR
            assertThat(quarantined.getMetadata()).containsEntry("errorCode", "PARSE_ERROR");
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
            // Quarantine write also fails → the parse failure surfaces as PROCESSING_FAILED
            when(hdfsWriter.writeQuarantine(anyString(), anyString(), anyString()))
                    .thenThrow(new HdfsWriteException("HDFS down", "/errors/x.json", "MSG-AUD-003"));
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
        HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 1024);

        when(eventIdGenerator.generateEventId(messageId)).thenReturn(eventId);
        when(messageParser.parse(any())).thenReturn(parsedPayload);
        when(apiClient.enrich(parsedPayload)).thenReturn(enrichmentResult());
        when(hdfsWriter.write(any(EnrichedPayload.class), anyString())).thenReturn(hdfsResult);
        when(kafkaPublisher.publish(anyString(), anyString())).thenReturn("12345");
        doNothing().when(auditPublisher).publishAsync(any());
    }

    @Nested
    @DisplayName("unexpected failure handling")
    class UnexpectedFailureHandling {

        @Test
        @DisplayName("should derive eventId from payload hash when JMSMessageID is null")
        void shouldDeriveEventIdFromPayloadWhenMessageIdNull() {
            MqMessage message = new MqMessage(null, "CORR-NULL", "{\"test\":\"payload\"}",
                    Instant.now(), "TEST.QUEUE");
            when(eventIdGenerator.generateEventId("{\"test\":\"payload\"}"))
                    .thenReturn("payload-derived-event-id");
            when(messageParser.parse(any())).thenThrow(
                    new MessageParseException("bad", null, "{\"test\":\"payload\"}"));
            when(hdfsWriter.writeQuarantine(anyString(), anyString(), any()))
                    .thenReturn(HdfsWriteResult.success("/errors/payload-derived-event-id.json", "cs", 10));

            ProcessingResult result = orchestrator.process(message);

            // No exception escaped, and the deterministic payload-hash eventId was used
            assertThat(result.getEventId()).isEqualTo("payload-derived-event-id");
            verify(eventIdGenerator).generateEventId("{\"test\":\"payload\"}");
        }

        @Test
        @DisplayName("should audit PROCESSING_FAILED and return UNEXPECTED_ERROR on an escaping RuntimeException")
        void shouldHandleEscapingRuntimeException() {
            MqMessage message = createMqMessage("MSG-UNEXPECTED-001");
            when(eventIdGenerator.generateEventId("MSG-UNEXPECTED-001")).thenReturn("event-unexpected-001");
            when(messageParser.parse(any())).thenReturn(createParsedPayload("MSG-UNEXPECTED-001", "TXN-1"));
            when(apiClient.enrich(any())).thenThrow(new IllegalStateException("defensive serialization failure"));

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("UNEXPECTED_ERROR");
            verify(auditPublisher, times(3)).publishAsync(auditEventCaptor.capture());
            assertThat(auditEventCaptor.getAllValues())
                    .extracting(AuditEvent::getEventType)
                    .contains(AuditEventType.PROCESSING_FAILED);
        }
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
                "2026-01-01",
                Map.of("key", "value"),
                Instant.now(),
                "{\"test\":\"payload\"}"
        );
    }
}
