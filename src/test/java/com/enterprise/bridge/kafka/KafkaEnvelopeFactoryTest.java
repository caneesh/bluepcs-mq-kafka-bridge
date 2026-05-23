package com.enterprise.bridge.kafka;

import com.enterprise.bridge.core.ProcessingContext;
import com.enterprise.bridge.model.EnrichedPayload;
import com.enterprise.bridge.model.HdfsWriteResult;
import com.enterprise.bridge.model.KafkaEnvelope;
import com.enterprise.bridge.model.ParsedPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("KafkaEnvelopeFactory")
class KafkaEnvelopeFactoryTest {

    private KafkaEnvelopeFactory factory;

    @BeforeEach
    void setUp() {
        factory = new KafkaEnvelopeFactory();
    }

    @Nested
    @DisplayName("envelope creation")
    class EnvelopeCreation {

        @Test
        @DisplayName("should create envelope with all fields from enriched payload")
        void shouldCreateEnvelopeWithAllFields() {
            EnrichedPayload payload = createEnrichedPayload("MSG-001", "TXN-001", "event-id-001");
            HdfsWriteResult hdfsResult = HdfsWriteResult.success(
                    "/data/bridge/payloads/2024/01/15/event-id-001.json",
                    "abc123checksum",
                    1024
            );

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getEventId()).isEqualTo("event-id-001");
            assertThat(envelope.getMessageId()).isEqualTo("MSG-001");
            assertThat(envelope.getTransactionId()).isEqualTo("TXN-001");
            assertThat(envelope.getEventType()).isEqualTo("ORDER_CREATED");
            assertThat(envelope.getEntityId()).isEqualTo("ENT-001");
            assertThat(envelope.getMarketingPlanId()).isEqualTo("MP-001");
            assertThat(envelope.getCampaignId()).isEqualTo("CAMP-001");
        }

        @Test
        @DisplayName("should propagate HDFS path to envelope")
        void shouldPropagateHdfsPath() {
            EnrichedPayload payload = createEnrichedPayload("MSG-002", "TXN-002", "event-id-002");
            String expectedPath = "/data/bridge/payloads/2024/01/15/event-id-002.json";
            HdfsWriteResult hdfsResult = HdfsWriteResult.success(expectedPath, "checksum", 512);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getHdfsPath()).isEqualTo(expectedPath);
        }

        @Test
        @DisplayName("should propagate checksum to envelope")
        void shouldPropagateChecksum() {
            EnrichedPayload payload = createEnrichedPayload("MSG-003", "TXN-003", "event-id-003");
            String expectedChecksum = "sha256-abcdef123456789";
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", expectedChecksum, 256);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getChecksum()).isEqualTo(expectedChecksum);
        }

        @Test
        @DisplayName("should set processedAt timestamp")
        void shouldSetProcessedAtTimestamp() {
            EnrichedPayload payload = createEnrichedPayload("MSG-004", "TXN-004", "event-id-004");
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 100);

            Instant before = Instant.now();
            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);
            Instant after = Instant.now();

            assertThat(envelope.getProcessedAt()).isNotNull();
            assertThat(envelope.getProcessedAt()).isAfterOrEqualTo(before);
            assertThat(envelope.getProcessedAt()).isBeforeOrEqualTo(after);
        }

        @Test
        @DisplayName("should set schema version")
        void shouldSetSchemaVersion() {
            EnrichedPayload payload = createEnrichedPayload("MSG-005", "TXN-005", "event-id-005");
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 100);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getSchemaVersion()).isEqualTo("1.0");
        }

        @Test
        @DisplayName("should preserve event timestamp from parsed payload")
        void shouldPreserveEventTimestamp() {
            Instant eventTime = Instant.parse("2024-01-15T10:30:00Z");
            ParsedPayload parsed = new ParsedPayload(
                    "MSG-006", "TXN-006", "ORDER_CREATED", "ENT-001",
                    Map.of(), eventTime, "{}"
            );
            ProcessingContext ctx = new ProcessingContext("event-id-006", "MSG-006", Instant.now());
            EnrichedPayload payload = new EnrichedPayload(
                    parsed, ctx, Map.of(), "MP-001", "CAMP-001", Instant.now()
            );
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 100);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getEventTimestamp()).isEqualTo(eventTime);
        }
    }

    @Nested
    @DisplayName("kafka key generation")
    class KafkaKeyGeneration {

        @Test
        @DisplayName("should use eventId as kafka key")
        void shouldUseEventIdAsKafkaKey() {
            EnrichedPayload payload = createEnrichedPayload("MSG-KEY-001", "TXN-KEY-001", "event-id-key-001");
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 100);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getKafkaKey()).isEqualTo("event-id-key-001");
        }

        @Test
        @DisplayName("should include originalMqMessageId and bridgeMessageId")
        void shouldIncludeOriginalMqMessageIdAndBridgeMessageId() {
            ParsedPayload parsed = new ParsedPayload(
                    "MSG-002", "TRANSACTION-ABC", "EVENT", "ENTITY-XYZ",
                    Map.of(), Instant.now(), "{}"
            );
            ProcessingContext ctx = new ProcessingContext("deterministic-event-id", "MSG-002", Instant.now());
            EnrichedPayload payload = new EnrichedPayload(
                    parsed, ctx, Map.of(), null, null, Instant.now()
            );
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 100);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getOriginalMqMessageId()).isEqualTo("MSG-002");
            assertThat(envelope.getBridgeMessageId()).isNotNull();
        }
    }

    @Nested
    @DisplayName("handling already existing files")
    class AlreadyExistingFiles {

        @Test
        @DisplayName("should create envelope from alreadyExists result")
        void shouldCreateEnvelopeFromAlreadyExistsResult() {
            EnrichedPayload payload = createEnrichedPayload("MSG-DUP-001", "TXN-DUP-001", "event-id-dup-001");
            HdfsWriteResult hdfsResult = HdfsWriteResult.alreadyExists(
                    "/data/existing/file.json",
                    "existing-checksum"
            );

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getHdfsPath()).isEqualTo("/data/existing/file.json");
            assertThat(envelope.getChecksum()).isEqualTo("existing-checksum");
            assertThat(envelope.getEventId()).isEqualTo("event-id-dup-001");
        }
    }

    @Nested
    @DisplayName("null handling")
    class NullHandling {

        @Test
        @DisplayName("should handle null marketing plan id")
        void shouldHandleNullMarketingPlanId() {
            ParsedPayload parsed = new ParsedPayload(
                    "MSG-001", "TXN-001", "EVENT", "ENT-001",
                    Map.of(), Instant.now(), "{}"
            );
            ProcessingContext ctx = new ProcessingContext("event-id-001", "MSG-001", Instant.now());
            EnrichedPayload payload = new EnrichedPayload(
                    parsed, ctx, Map.of(), null, "CAMP-001", Instant.now()
            );
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 100);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getMarketingPlanId()).isNull();
            assertThat(envelope.getCampaignId()).isEqualTo("CAMP-001");
        }

        @Test
        @DisplayName("should handle null campaign id")
        void shouldHandleNullCampaignId() {
            ParsedPayload parsed = new ParsedPayload(
                    "MSG-001", "TXN-001", "EVENT", "ENT-001",
                    Map.of(), Instant.now(), "{}"
            );
            ProcessingContext ctx = new ProcessingContext("event-id-001", "MSG-001", Instant.now());
            EnrichedPayload payload = new EnrichedPayload(
                    parsed, ctx, Map.of(), "MP-001", null, Instant.now()
            );
            HdfsWriteResult hdfsResult = HdfsWriteResult.success("/path/file.json", "checksum", 100);

            KafkaEnvelope envelope = factory.createEnvelope(payload, hdfsResult);

            assertThat(envelope.getMarketingPlanId()).isEqualTo("MP-001");
            assertThat(envelope.getCampaignId()).isNull();
        }
    }

    private EnrichedPayload createEnrichedPayload(String messageId, String transactionId, String eventId) {
        ParsedPayload parsed = new ParsedPayload(
                messageId,
                transactionId,
                "ORDER_CREATED",
                "ENT-001",
                Map.of("key", "value"),
                Instant.now(),
                "{\"test\":\"payload\"}"
        );
        ProcessingContext ctx = new ProcessingContext(eventId, messageId, Instant.now());
        return new EnrichedPayload(
                parsed,
                ctx,
                Map.of("enriched", "data"),
                "MP-001",
                "CAMP-001",
                Instant.now()
        );
    }
}
