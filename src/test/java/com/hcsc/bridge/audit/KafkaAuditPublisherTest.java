package com.hcsc.bridge.audit;

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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaAuditPublisherTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private KafkaAuditPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new KafkaAuditPublisher(kafkaTemplate, "audit-topic", 5);
    }

    @Nested
    @DisplayName("Publish Tests")
    class PublishTests {

        @Test
        @DisplayName("should publish audit event with eventId as key")
        void shouldPublishWithEventIdKey() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.set(null);
            when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

            AuditEvent event = createTestEvent("event-id-001", "bridge-event-001", "MSG-001", AuditEventType.MESSAGE_RECEIVED);

            publisher.publish(event);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq("audit-topic"), keyCaptor.capture(), valueCaptor.capture());

            assertThat(keyCaptor.getValue()).isEqualTo("event-id-001");
            assertThat(valueCaptor.getValue()).contains("auditEventId");
            assertThat(valueCaptor.getValue()).contains("eventId");
            assertThat(valueCaptor.getValue()).contains("bridgeEventId");
        }

        @Test
        @DisplayName("should include audit event ID in payload")
        void shouldIncludeAuditEventId() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.set(null);
            when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

            AuditEvent event = createTestEvent("event-id-002", "bridge-event-002", "MSG-002", AuditEventType.PROCESSING_COMPLETED);

            publisher.publish(event);

            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(anyString(), anyString(), valueCaptor.capture());

            assertThat(valueCaptor.getValue()).contains("\"auditEventId\":\"AUD-event-id-002\"");
        }

        @Test
        @DisplayName("should include bridge event ID in payload")
        void shouldIncludeBridgeEventId() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.set(null);
            when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

            AuditEvent event = createTestEvent("event-id-003", "bridge-event-003", "MSG-003", AuditEventType.KAFKA_PUBLISH_COMPLETED);

            publisher.publish(event);

            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(anyString(), anyString(), valueCaptor.capture());

            assertThat(valueCaptor.getValue()).contains("\"bridgeEventId\":\"bridge-event-003\"");
        }

        @Test
        @DisplayName("should include originalMqMessageId in payload")
        void shouldIncludeOriginalMqMessageId() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.set(null);
            when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

            AuditEvent event = createTestEvent("event-id-004", "bridge-event-004", "JMS-MSG-004", AuditEventType.MESSAGE_RECEIVED);

            publisher.publish(event);

            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(anyString(), anyString(), valueCaptor.capture());

            assertThat(valueCaptor.getValue()).contains("\"originalMqMessageId\":\"JMS-MSG-004\"");
        }
    }

    @Nested
    @DisplayName("Async Publish Tests")
    class AsyncPublishTests {

        @Test
        @DisplayName("should publish async without blocking")
        void shouldPublishAsync() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

            AuditEvent event = createTestEvent("event-id-async-001", "bridge-async-001", "MSG-ASYNC-001", AuditEventType.MESSAGE_RECEIVED);

            publisher.publishAsync(event);

            verify(kafkaTemplate).send(eq("audit-topic"), eq("event-id-async-001"), anyString());
        }
    }

    @Nested
    @DisplayName("Safe Publisher Tests")
    class SafePublisherTests {

        @Test
        @DisplayName("should not throw on publish failure")
        void shouldNotThrowOnFailure() {
            AuditPublisher failingPublisher = new AuditPublisher() {
                @Override
                public void publish(AuditEvent event) {
                    throw new RuntimeException("Simulated failure");
                }

                @Override
                public void publishAsync(AuditEvent event) {
                    throw new RuntimeException("Simulated failure");
                }
            };

            SafeAuditPublisher safePublisher = new SafeAuditPublisher(failingPublisher);
            AuditEvent event = createTestEvent("event-id-safe-001", "bridge-safe-001", "MSG-SAFE-001", AuditEventType.MESSAGE_RECEIVED);

            safePublisher.publish(event);
        }

        @Test
        @DisplayName("should not throw on async publish failure")
        void shouldNotThrowOnAsyncFailure() {
            AuditPublisher failingPublisher = new AuditPublisher() {
                @Override
                public void publish(AuditEvent event) {
                    throw new RuntimeException("Simulated failure");
                }

                @Override
                public void publishAsync(AuditEvent event) {
                    throw new RuntimeException("Simulated failure");
                }
            };

            SafeAuditPublisher safePublisher = new SafeAuditPublisher(failingPublisher);
            AuditEvent event = createTestEvent("event-id-safe-002", "bridge-safe-002", "MSG-SAFE-002", AuditEventType.MESSAGE_RECEIVED);

            safePublisher.publishAsync(event);
        }
    }

    private AuditEvent createTestEvent(String eventId, String bridgeEventId, String originalMqMessageId, AuditEventType eventType) {
        return AuditEvent.builder()
                .auditEventId("AUD-" + eventId)
                .eventId(eventId)
                .bridgeEventId(bridgeEventId)
                .originalMqMessageId(originalMqMessageId)
                .messageId(originalMqMessageId)
                .eventType(eventType)
                .timestamp(Instant.now())
                .build();
    }
}
