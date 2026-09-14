package com.hcsc.bridge.integration;

import com.hcsc.bridge.api.EnrichmentWrapperFactory;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.kafka.KafkaEnvelopePublisher;
import com.hcsc.bridge.kafka.KafkaNotificationFactory;
import com.hcsc.bridge.mock.FakeMqMessageGenerator;
import com.hcsc.bridge.mock.InMemoryAuditPublisher;
import com.hcsc.bridge.mock.LocalFileSystemHdfsOperations;
import com.hcsc.bridge.mock.MockMarketingPlanApiClient;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.hdfs.HdfsSafePayloadWriter;
import com.hcsc.bridge.orchestrator.BridgeOrchestrator;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import com.hcsc.bridge.parser.JsonMessageParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
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

        when(kafkaPublisher.publish(anyString(), anyString())).thenReturn("12345");

        JsonMessageParser parser = new JsonMessageParser();
        HdfsSafePayloadWriter hdfsWriter = new HdfsSafePayloadWriter(hdfsOperations, "/data/bridge/payloads", "", ".tmp");
        EnrichmentWrapperFactory wrapperFactory = new EnrichmentWrapperFactory();

        orchestrator = new BridgeOrchestrator(
                parser,
                apiClient,
                hdfsWriter,
                wrapperFactory,
                new KafkaNotificationFactory(),
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

            // HDFS content is now the published wrapper (built from the API PlanResponse),
            // not the MQ payload. Assert on the wrapper structure instead.
            String content = hdfsOperations.readFile(result.getHdfsPath());
            assertThat(content).contains("RestAPIResponse");
            assertThat(content).contains("PlanResponse");
            assertThat(content).contains("changeEventTimeStamp");
        }

        @Test
        @DisplayName("should publish claim-check notification with eventId key and hdfsPath")
        void shouldPublishKafkaNotificationWithCorrectData() {
            MqMessage message = messageGenerator.generateMessageWithId("MSG-KAFKA-001");
            String expectedEventId = eventIdGenerator.generateEventId("MSG-KAFKA-001");

            orchestrator.process(message);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaPublisher).publish(keyCaptor.capture(), valueCaptor.capture());

            assertThat(keyCaptor.getValue()).isEqualTo(expectedEventId);
            // Kafka carries only the small notification; the full wrapper lives in HDFS.
            assertThat(valueCaptor.getValue()).doesNotContain("RestAPIResponse");
            assertThat(valueCaptor.getValue()).contains("\"hdfsPath\":");
            assertThat(valueCaptor.getValue()).contains("\"eventId\":\"" + expectedEventId + "\"");
            assertThat(valueCaptor.getValue()).contains("changeEventTypeName");
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
            HdfsSafePayloadWriter hdfsWriter = new HdfsSafePayloadWriter(hdfsOperations, "/data/bridge/payloads", "", ".tmp");
            orchestrator = new BridgeOrchestrator(
                    new JsonMessageParser(),
                    apiClient,
                    hdfsWriter,
                    new EnrichmentWrapperFactory(),
                    new KafkaNotificationFactory(),
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
        @DisplayName("should quarantine and publish MESSAGE_QUARANTINED audit event")
        void shouldPublishQuarantineAuditEvent() {
            MqMessage message = messageGenerator.generateInvalidMessage("MSG-INVALID-002");

            ProcessingResult result = orchestrator.process(message);

            // Parse failures are permanent: with a working HDFS the payload is quarantined
            // (so the listener can ack), not reported as PROCESSING_FAILED
            assertThat(result.isQuarantined()).isTrue();
            assertThat(auditPublisher.hasEventOfType(AuditEventType.MESSAGE_QUARANTINED)).isTrue();
            assertThat(auditPublisher.hasEventOfType(AuditEventType.PROCESSING_FAILED)).isFalse();
        }

        @Test
        @DisplayName("should fall back to PROCESSING_FAILED when the quarantine write fails")
        void shouldFallBackToFailureWhenQuarantineWriteFails() {
            hdfsOperations.setShouldFailOnCreate(true);
            MqMessage message = messageGenerator.generateInvalidMessage("MSG-INVALID-003");

            ProcessingResult result = orchestrator.process(message);

            // Payload could not be durably preserved → message must stay on the queue
            assertThat(result.isQuarantined()).isFalse();
            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
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
            when(kafkaPublisher.publish(anyString(), anyString())).thenThrow(
                    new com.hcsc.bridge.kafka.KafkaPublishException("Kafka unavailable", "MSG-001", "topic", null));

            MqMessage message = messageGenerator.generateMessage();

            ProcessingResult result = orchestrator.process(message);

            assertThat(result.isSuccessful()).isFalse();
            assertThat(result.getErrorCode()).isEqualTo("KAFKA_ERROR");
        }

        @Test
        @DisplayName("should preserve HDFS file when Kafka fails")
        void shouldPreserveHdfsFileWhenKafkaFails() throws IOException {
            when(kafkaPublisher.publish(anyString(), anyString())).thenThrow(
                    new com.hcsc.bridge.kafka.KafkaPublishException("Kafka unavailable", "MSG-001", "topic", null));

            MqMessage message = messageGenerator.generateMessageWithId("MSG-KAFKA-FAIL-001");
            String expectedEventId = eventIdGenerator.generateEventId("MSG-KAFKA-FAIL-001");

            orchestrator.process(message);

            // Flat landing directory: no eventType/date partitioning in the path
            String expectedPath = "/data/bridge/payloads/" + expectedEventId + ".json";

            assertThat(hdfsOperations.exists(expectedPath)).isTrue();
        }
    }
}
