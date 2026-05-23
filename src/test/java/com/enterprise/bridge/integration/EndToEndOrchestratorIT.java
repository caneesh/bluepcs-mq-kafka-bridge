package com.enterprise.bridge.integration;

import com.enterprise.bridge.audit.AuditEventType;
import com.enterprise.bridge.core.EventIdGenerator;
import com.enterprise.bridge.kafka.KafkaEnvelopeFactory;
import com.enterprise.bridge.kafka.KafkaEnvelopePublisher;
import com.enterprise.bridge.mock.FakeMqMessageGenerator;
import com.enterprise.bridge.mock.InMemoryAuditPublisher;
import com.enterprise.bridge.mock.LocalFileSystemHdfsOperations;
import com.enterprise.bridge.mock.MockMarketingPlanApiClient;
import com.enterprise.bridge.model.MqMessage;
import com.enterprise.bridge.hdfs.HdfsSafePayloadWriter;
import com.enterprise.bridge.orchestrator.BridgeOrchestrator;
import com.enterprise.bridge.orchestrator.ProcessingResult;
import com.enterprise.bridge.parser.JsonMessageParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import com.enterprise.bridge.model.KafkaEnvelope;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EndToEndOrchestratorIT {

    private BridgeOrchestrator orchestrator;
    private InMemoryAuditPublisher auditPublisher;
    private MockMarketingPlanApiClient apiClient;
    private LocalFileSystemHdfsOperations hdfsOperations;
    private FakeMqMessageGenerator messageGenerator;
    private KafkaEnvelopePublisher kafkaPublisher;
    private EventIdGenerator eventIdGenerator;
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("hdfs-test");
        hdfsOperations = new LocalFileSystemHdfsOperations(tempDir);
        auditPublisher = new InMemoryAuditPublisher();
        apiClient = new MockMarketingPlanApiClient();
        messageGenerator = new FakeMqMessageGenerator();
        kafkaPublisher = Mockito.mock(KafkaEnvelopePublisher.class);
        eventIdGenerator = new EventIdGenerator();

        when(kafkaPublisher.publish(any(KafkaEnvelope.class))).thenReturn("12345");

        JsonMessageParser parser = new JsonMessageParser();
        HdfsSafePayloadWriter hdfsWriter = new HdfsSafePayloadWriter(hdfsOperations, "/data/bridge/payloads", ".tmp");
        KafkaEnvelopeFactory envelopeFactory = new KafkaEnvelopeFactory();

        orchestrator = new BridgeOrchestrator(
                parser,
                apiClient,
                hdfsWriter,
                envelopeFactory,
                kafkaPublisher,
                eventIdGenerator,
                auditPublisher
        );
    }

    @AfterEach
    void tearDown() throws IOException {
        hdfsOperations.cleanup();
    }

    @Nested
    @DisplayName("Full E2E Flow Tests")
    class FullE2EFlowTests {

        @Test
        @DisplayName("should complete full processing flow successfully")
        void shouldCompleteFullFlow() {
            MqMessage message = messageGenerator.generateMessage();

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getEventId()).isNotNull();
            assertThat(result.getHdfsPath()).isNotNull();
            assertThat(result.getKafkaOffset()).isEqualTo("12345");
        }

        @Test
        @DisplayName("should generate deterministic event ID from message ID")
        void shouldGenerateDeterministicEventId() {
            MqMessage message = messageGenerator.generateMessageWithId("MSG-EVENTID-001");

            ProcessingResult result1 = orchestrator.process(message);
            String expectedEventId = eventIdGenerator.generateEventId("MSG-EVENTID-001");

            assertThat(result1.getEventId()).isEqualTo(expectedEventId);
        }

        @Test
        @DisplayName("should publish all audit events")
        void shouldPublishAllAuditEvents() {
            MqMessage message = messageGenerator.generateMessageWithId("MSG-AUDIT-001");

            orchestrator.process(message);

            assertThat(auditPublisher.hasEventOfType(AuditEventType.MESSAGE_RECEIVED)).isTrue();
            assertThat(auditPublisher.hasEventOfType(AuditEventType.MESSAGE_PARSED)).isTrue();
            assertThat(auditPublisher.hasEventOfType(AuditEventType.ENRICHMENT_COMPLETED)).isTrue();
            assertThat(auditPublisher.hasEventOfType(AuditEventType.HDFS_WRITE_COMPLETED)).isTrue();
            assertThat(auditPublisher.hasEventOfType(AuditEventType.KAFKA_PUBLISH_COMPLETED)).isTrue();
            assertThat(auditPublisher.hasEventOfType(AuditEventType.PROCESSING_COMPLETED)).isTrue();
        }

        @Test
        @DisplayName("should write payload to HDFS with eventId-based path")
        void shouldWritePayloadToHdfs() throws IOException {
            MqMessage message = messageGenerator.generateMessageWithId("MSG-HDFS-001");

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.getHdfsPath()).isNotNull();
            assertThat(hdfsOperations.exists(result.getHdfsPath())).isTrue();

            String content = hdfsOperations.readFile(result.getHdfsPath());
            assertThat(content).contains("MSG-HDFS-001");
        }

        @Test
        @DisplayName("should publish Kafka envelope with correct data including eventId")
        void shouldPublishKafkaEnvelopeWithCorrectData() {
            MqMessage message = messageGenerator.generateMessageWithId("MSG-KAFKA-001");

            orchestrator.process(message);

            ArgumentCaptor<KafkaEnvelope> captor = ArgumentCaptor.forClass(KafkaEnvelope.class);
            verify(kafkaPublisher).publish(captor.capture());

            KafkaEnvelope envelope = captor.getValue();
            assertThat(envelope.getEventId()).isNotNull();
            assertThat(envelope.getOriginalMqMessageId()).isEqualTo("MSG-KAFKA-001");
            assertThat(envelope.getBridgeMessageId()).isNotNull();
            assertThat(envelope.getTransactionId()).isNotNull();
            assertThat(envelope.getHdfsPath()).isNotNull();
            assertThat(envelope.getChecksum()).isNotNull();
            assertThat(envelope.getMarketingPlanId()).isNotNull();
        }
    }

    @Nested
    @DisplayName("Duplicate-Tolerant Processing")
    class DuplicateTolerantProcessing {

        @Test
        @DisplayName("should allow reprocessing same message - downstream responsible for dedup")
        void shouldAllowReprocessingSameMessage() throws IOException {
            MqMessage message = messageGenerator.generateMessageWithId("MSG-DUP-001");

            ProcessingResult result1 = orchestrator.process(message);

            hdfsOperations.cleanup();
            hdfsOperations = new LocalFileSystemHdfsOperations(Files.createTempDirectory("hdfs-test-2"));
            HdfsSafePayloadWriter hdfsWriter = new HdfsSafePayloadWriter(hdfsOperations, "/data/bridge/payloads", ".tmp");
            orchestrator = new BridgeOrchestrator(
                    new JsonMessageParser(),
                    apiClient,
                    hdfsWriter,
                    new KafkaEnvelopeFactory(),
                    kafkaPublisher,
                    eventIdGenerator,
                    auditPublisher
            );

            ProcessingResult result2 = orchestrator.process(message);

            assertThat(result1.isSuccessful()).isTrue();
            assertThat(result2.isSuccessful()).isTrue();
            assertThat(result1.getEventId()).isEqualTo(result2.getEventId());
        }

        @Test
        @DisplayName("should generate same eventId for duplicate messages")
        void shouldGenerateSameEventIdForDuplicates() {
            String messageId = "MSG-DUP-EVENTID-001";
            String expectedEventId = eventIdGenerator.generateEventId(messageId);

            MqMessage message1 = messageGenerator.generateMessageWithId(messageId);
            ProcessingResult result1 = orchestrator.process(message1);

            assertThat(result1.getEventId()).isEqualTo(expectedEventId);
        }
    }

    @Nested
    @DisplayName("Parse Failure Handling")
    class ParseFailureHandling {

        @Test
        @DisplayName("should handle invalid JSON gracefully")
        void shouldHandleInvalidJson() {
            MqMessage message = messageGenerator.generateInvalidMessage("MSG-INVALID-001");

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
        }

        @Test
        @DisplayName("should publish failure audit event")
        void shouldPublishFailureAuditEvent() {
            MqMessage message = messageGenerator.generateInvalidMessage("MSG-INVALID-002");

            orchestrator.process(message);

            assertThat(auditPublisher.hasEventOfType(AuditEventType.PROCESSING_FAILED)).isTrue();
        }
    }

    @Nested
    @DisplayName("Enrichment Failure Handling")
    class EnrichmentFailureHandling {

        @Test
        @DisplayName("should handle enrichment API failure")
        void shouldHandleEnrichmentFailure() {
            apiClient.setShouldFail(true);
            apiClient.setFailureMessage("API unavailable");
            MqMessage message = messageGenerator.generateMessage();

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("ENRICHMENT_ERROR");
        }

        @Test
        @DisplayName("should publish enrichment failure audit event")
        void shouldPublishEnrichmentFailureAuditEvent() {
            apiClient.setShouldFail(true);
            MqMessage message = messageGenerator.generateMessageWithId("MSG-ENRICH-FAIL-001");

            orchestrator.process(message);

            assertThat(auditPublisher.hasEventOfType(AuditEventType.ENRICHMENT_FAILED)).isTrue();
        }
    }

    @Nested
    @DisplayName("HDFS Failure Handling")
    class HdfsFailureHandling {

        @Test
        @DisplayName("should handle HDFS write failure")
        void shouldHandleHdfsFailure() {
            hdfsOperations.setShouldFailOnCreate(true);
            MqMessage message = messageGenerator.generateMessage();

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("HDFS_ERROR");
        }

        @Test
        @DisplayName("should publish HDFS failure audit event")
        void shouldPublishHdfsFailureAuditEvent() {
            hdfsOperations.setShouldFailOnCreate(true);
            MqMessage message = messageGenerator.generateMessageWithId("MSG-HDFS-FAIL-001");

            orchestrator.process(message);

            assertThat(auditPublisher.hasEventOfType(AuditEventType.HDFS_WRITE_FAILED)).isTrue();
        }
    }

    @Nested
    @DisplayName("Kafka Failure Handling")
    class KafkaFailureHandling {

        @Test
        @DisplayName("should handle Kafka publish failure")
        void shouldHandleKafkaFailure() {
            when(kafkaPublisher.publish(any())).thenThrow(
                    new com.enterprise.bridge.kafka.KafkaPublishException("Kafka unavailable", "MSG-001", "topic", null));

            MqMessage message = messageGenerator.generateMessage();

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("KAFKA_ERROR");
        }

        @Test
        @DisplayName("should preserve HDFS file when Kafka fails")
        void shouldPreserveHdfsFileWhenKafkaFails() throws IOException {
            when(kafkaPublisher.publish(any())).thenThrow(
                    new com.enterprise.bridge.kafka.KafkaPublishException("Kafka unavailable", "MSG-001", "topic", null));

            MqMessage message = messageGenerator.generateMessageWithId("MSG-KAFKA-FAIL-001");
            String expectedEventId = eventIdGenerator.generateEventId("MSG-KAFKA-FAIL-001");

            orchestrator.process(message);

            String expectedPath = "/data/bridge/payloads/order_created/" +
                    java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy/MM/dd")) +
                    "/" + expectedEventId + ".json";

            assertThat(hdfsOperations.exists(expectedPath)).isTrue();
        }
    }
}
