package com.enterprise.bridge.kafka;

import com.enterprise.bridge.model.KafkaEnvelope;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaEnvelopePublisher")
class KafkaEnvelopePublisherTest {

    private static final String TOPIC = "bridge-events";
    private static final long TIMEOUT_SECONDS = 5;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaEnvelopePublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new KafkaEnvelopePublisher(kafkaTemplate, TOPIC, TIMEOUT_SECONDS);
    }

    @Nested
    @DisplayName("successful publish")
    class SuccessfulPublish {

        @Test
        @DisplayName("should publish envelope and return offset")
        void shouldPublishEnvelopeAndReturnOffset() throws Exception {
            KafkaEnvelope envelope = createEnvelope("event-id-001", "MSG-001", "TXN-001");
            long expectedOffset = 12345L;
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(expectedOffset);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            String result = publisher.publish(envelope);

            assertThat(result).isEqualTo(String.valueOf(expectedOffset));
        }

        @Test
        @DisplayName("should use eventId as kafka key")
        void shouldUseEventIdAsKafkaKey() throws Exception {
            KafkaEnvelope envelope = createEnvelope("deterministic-event-id", "MSG-002", "TXN-002");
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish(envelope);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), anyString());
            assertThat(keyCaptor.getValue()).isEqualTo("deterministic-event-id");
        }

        @Test
        @DisplayName("should serialize envelope to JSON")
        void shouldSerializeEnvelopeToJson() throws Exception {
            KafkaEnvelope envelope = createEnvelope("event-id-003", "MSG-003", "TXN-003");
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish(envelope);

            ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), anyString(), payloadCaptor.capture());

            String payload = payloadCaptor.getValue();
            assertThat(payload).contains("\"eventId\":\"event-id-003\"");
            assertThat(payload).contains("\"messageId\":\"MSG-003\"");
            assertThat(payload).contains("\"transactionId\":\"TXN-003\"");
            assertThat(payload).contains("\"eventType\":\"ORDER_CREATED\"");
            assertThat(payload).contains("\"hdfsPath\":");
            assertThat(payload).contains("\"checksum\":");
            assertThat(payload).contains("\"originalMqMessageId\":");
            assertThat(payload).contains("\"bridgeMessageId\":");
        }

        @Test
        @DisplayName("should send to correct topic")
        void shouldSendToCorrectTopic() throws Exception {
            KafkaEnvelope envelope = createEnvelope("event-id-004", "MSG-004", "TXN-004");
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish(envelope);

            verify(kafkaTemplate).send(eq(TOPIC), anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("interrupted publish")
    class InterruptedPublish {

        @Test
        @DisplayName("should throw KafkaPublishException when interrupted")
        void shouldThrowWhenInterrupted() {
            KafkaEnvelope envelope = createEnvelope("event-id-int-001", "MSG-INT-001", "TXN-INT-001");
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new InterruptedException("Interrupted"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish(envelope))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasMessageContaining("Failed to publish");
        }

        @Test
        @DisplayName("should include messageId in exception for interrupted")
        void shouldIncludeMessageIdInInterruptedException() {
            KafkaEnvelope envelope = createEnvelope("event-id-int-002", "MSG-INT-002", "TXN-INT-002");
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new RuntimeException("Simulated error"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish(envelope))
                    .isInstanceOf(KafkaPublishException.class)
                    .extracting("messageId")
                    .isEqualTo("MSG-INT-002");
        }
    }

    @Nested
    @DisplayName("execution exception")
    class ExecutionExceptionHandling {

        @Test
        @DisplayName("should throw KafkaPublishException on execution error")
        void shouldThrowOnExecutionError() {
            KafkaEnvelope envelope = createEnvelope("event-id-exe-001", "MSG-EXE-001", "TXN-EXE-001");
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new RuntimeException("Broker not available"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish(envelope))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasMessageContaining("Failed to publish");
        }

        @Test
        @DisplayName("should wrap cause in KafkaPublishException")
        void shouldWrapCauseInException() {
            KafkaEnvelope envelope = createEnvelope("event-id-exe-002", "MSG-EXE-002", "TXN-EXE-002");
            RuntimeException cause = new RuntimeException("Original cause");
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(cause);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish(envelope))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasCause(cause);
        }

        @Test
        @DisplayName("should include topic in exception")
        void shouldIncludeTopicInException() {
            KafkaEnvelope envelope = createEnvelope("event-id-exe-003", "MSG-EXE-003", "TXN-EXE-003");
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new RuntimeException("Error"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish(envelope))
                    .isInstanceOf(KafkaPublishException.class)
                    .extracting("topic")
                    .isEqualTo(TOPIC);
        }
    }

    @Nested
    @DisplayName("key correctness")
    class KeyCorrectness {

        @Test
        @DisplayName("should use eventId as key for deduplication")
        void shouldUseEventIdAsKey() throws Exception {
            KafkaEnvelope envelope = createEnvelope("sha256-deterministic-key", "MSG-KEY-001", "TXN-KEY-001");
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish(envelope);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), anyString());
            assertThat(keyCaptor.getValue()).isEqualTo("sha256-deterministic-key");
        }

        @Test
        @DisplayName("should handle special characters in eventId")
        void shouldHandleSpecialCharactersInEventId() throws Exception {
            KafkaEnvelope envelope = createEnvelope("abc123def456789", "MSG-001", "TXN-WITH-DASH");
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish(envelope);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), anyString());
            assertThat(keyCaptor.getValue()).isEqualTo("abc123def456789");
        }
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
                .hdfsPath("/data/bridge/payloads/file.json")
                .checksum("abc123checksum")
                .marketingPlanId("MP-001")
                .campaignId("CAMP-001")
                .eventTimestamp(Instant.now())
                .processedAt(Instant.now())
                .schemaVersion("1.0")
                .build();
    }

    private SettableListenableFuture<SendResult<String, String>> createSuccessFuture(long offset) {
        SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(TOPIC, 0),
                0L,
                (int) offset,
                System.currentTimeMillis(),
                0,
                0
        );
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key", "value");
        SendResult<String, String> result = new SendResult<>(record, metadata);
        future.set(result);
        return future;
    }
}
