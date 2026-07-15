package com.hcsc.bridge.kafka;

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
    private static final String WRAPPER = "{\"changeEventTimeStamp\":\"\",\"RestAPIResponse\":{},\"changeEventTypeName\":\"Unknown\"}";

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
        @DisplayName("should publish value and return offset")
        void shouldPublishValueAndReturnOffset() {
            long expectedOffset = 12345L;
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(expectedOffset);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            String result = publisher.publish("event-id-001", WRAPPER);

            assertThat(result).isEqualTo(String.valueOf(expectedOffset));
        }

        @Test
        @DisplayName("should use supplied key as kafka key")
        void shouldUseSuppliedKeyAsKafkaKey() {
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish("deterministic-event-id", WRAPPER);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), anyString());
            assertThat(keyCaptor.getValue()).isEqualTo("deterministic-event-id");
        }

        @Test
        @DisplayName("should send the wrapper value verbatim")
        void shouldSendWrapperValueVerbatim() {
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish("event-id-003", WRAPPER);

            ArgumentCaptor<String> valueCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), anyString(), valueCaptor.capture());
            assertThat(valueCaptor.getValue()).isEqualTo(WRAPPER);
        }

        @Test
        @DisplayName("should send to correct topic")
        void shouldSendToCorrectTopic() {
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish("event-id-004", WRAPPER);

            verify(kafkaTemplate).send(eq(TOPIC), anyString(), anyString());
        }
    }

    @Nested
    @DisplayName("interrupted publish")
    class InterruptedPublish {

        @Test
        @DisplayName("should throw KafkaPublishException when interrupted")
        void shouldThrowWhenInterrupted() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new InterruptedException("Interrupted"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish("event-id-int-001", WRAPPER))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasMessageContaining("Failed to publish");
        }

        @Test
        @DisplayName("should include key in exception messageId field")
        void shouldIncludeKeyInException() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new RuntimeException("Simulated error"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish("event-id-int-002", WRAPPER))
                    .isInstanceOf(KafkaPublishException.class)
                    .extracting("messageId")
                    .isEqualTo("event-id-int-002");
        }
    }

    @Nested
    @DisplayName("execution exception")
    class ExecutionExceptionHandling {

        @Test
        @DisplayName("should throw KafkaPublishException on execution error")
        void shouldThrowOnExecutionError() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new RuntimeException("Broker not available"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish("event-id-exe-001", WRAPPER))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasMessageContaining("Failed to publish");
        }

        @Test
        @DisplayName("should wrap cause in KafkaPublishException")
        void shouldWrapCauseInException() {
            RuntimeException cause = new RuntimeException("Original cause");
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(cause);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish("event-id-exe-002", WRAPPER))
                    .isInstanceOf(KafkaPublishException.class)
                    .hasCause(cause);
        }

        @Test
        @DisplayName("should include topic in exception")
        void shouldIncludeTopicInException() {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new RuntimeException("Error"));

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            assertThatThrownBy(() -> publisher.publish("event-id-exe-003", WRAPPER))
                    .isInstanceOf(KafkaPublishException.class)
                    .extracting("topic")
                    .isEqualTo(TOPIC);
        }
    }

    @Nested
    @DisplayName("key correctness")
    class KeyCorrectness {

        @Test
        @DisplayName("should use supplied key for deduplication")
        void shouldUseSuppliedKey() {
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish("sha256-deterministic-key", WRAPPER);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), anyString());
            assertThat(keyCaptor.getValue()).isEqualTo("sha256-deterministic-key");
        }

        @Test
        @DisplayName("should handle special characters in key")
        void shouldHandleSpecialCharactersInKey() {
            SettableListenableFuture<SendResult<String, String>> future = createSuccessFuture(100L);

            when(kafkaTemplate.send(eq(TOPIC), anyString(), anyString())).thenReturn(future);

            publisher.publish("abc123def456789", WRAPPER);

            ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
            verify(kafkaTemplate).send(eq(TOPIC), keyCaptor.capture(), anyString());
            assertThat(keyCaptor.getValue()).isEqualTo("abc123def456789");
        }
    }

    private SettableListenableFuture<SendResult<String, String>> createSuccessFuture(long offset) {
        SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition(TOPIC, 0),
                0L,
                (int) offset,
                System.currentTimeMillis(),
                null,  // checksum (deprecated in Kafka 2.8.x)
                0,
                0
        );
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key", "value");
        SendResult<String, String> result = new SendResult<>(record, metadata);
        future.set(result);
        return future;
    }
}
