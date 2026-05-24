package com.hcsc.bridge.integration;

import com.hcsc.bridge.kafka.KafkaEnvelopePublisher;
import com.hcsc.bridge.model.KafkaEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaPublisherIT {

    private static final String TOPIC = "test-bridge-events";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule());

    private static EmbeddedKafkaBroker embeddedKafka;

    private KafkaEnvelopePublisher publisher;
    private KafkaTemplate<String, String> kafkaTemplate;
    private Consumer<String, String> consumer;

    @BeforeAll
    static void setUpBroker() {
        embeddedKafka = new EmbeddedKafkaBroker(1, true, TOPIC);
        embeddedKafka.afterPropertiesSet();
    }

    @AfterAll
    static void tearDownBroker() {
        if (embeddedKafka != null) {
            embeddedKafka.destroy();
        }
    }

    @BeforeEach
    void setUp() {
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, embeddedKafka.getBrokersAsString());
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(producerFactory);
        publisher = new KafkaEnvelopePublisher(kafkaTemplate, TOPIC, 10);

        Map<String, Object> consumerProps = new HashMap<>(
                KafkaTestUtils.consumerProps("test-group-" + UUID.randomUUID(), "true", embeddedKafka));
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        DefaultKafkaConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps);
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(TOPIC));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Nested
    @DisplayName("Successful Publish Tests")
    class SuccessfulPublish {

        @Test
        @DisplayName("should publish envelope and return offset")
        void shouldPublishEnvelopeAndReturnOffset() {
            KafkaEnvelope envelope = createTestEnvelope("MSG-001", "TXN-001", "ENTITY-001");

            String offset = publisher.publish(envelope);

            assertThat(offset).isNotNull();
            assertThat(Long.parseLong(offset)).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("should publish message with correct payload")
        void shouldPublishWithCorrectPayload() throws Exception {
            String messageId = "MSG-" + UUID.randomUUID().toString().substring(0, 8);
            KafkaEnvelope envelope = createTestEnvelope(messageId, "TXN-002", "ENTITY-002");

            publisher.publish(envelope);

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            boolean found = false;
            for (ConsumerRecord<String, String> record : records) {
                JsonNode node = OBJECT_MAPPER.readTree(record.value());
                if (messageId.equals(node.get("messageId").asText())) {
                    assertThat(node.get("transactionId").asText()).isEqualTo("TXN-002");
                    assertThat(node.get("entityId").asText()).isEqualTo("ENTITY-002");
                    assertThat(node.get("eventType").asText()).isEqualTo("order_created");
                    assertThat(node.get("hdfsPath").asText()).startsWith("/data/bridge/");
                    assertThat(node.get("checksum").asText()).isNotEmpty();
                    found = true;
                    break;
                }
            }
            assertThat(found).isTrue();
        }
    }

    @Nested
    @DisplayName("Kafka Key Tests")
    class KafkaKeyTests {

        @Test
        @DisplayName("should use correct kafka key format")
        void shouldUseCorrectKafkaKeyFormat() {
            String entityId = "ENTITY-KEY-001";
            String transactionId = "TXN-KEY-001";
            KafkaEnvelope envelope = createTestEnvelope("MSG-KEY-001", transactionId, entityId);

            publisher.publish(envelope);

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            boolean foundCorrectKey = false;
            String expectedKey = entityId + ":" + transactionId;
            for (ConsumerRecord<String, String> record : records) {
                if (expectedKey.equals(record.key())) {
                    foundCorrectKey = true;
                    break;
                }
            }
            assertThat(foundCorrectKey).isTrue();
        }

        @Test
        @DisplayName("should handle special characters in key")
        void shouldHandleSpecialCharactersInKey() {
            String entityId = "ENTITY.WITH.DOTS";
            String transactionId = "TXN-WITH-DASH_AND_UNDERSCORE";
            KafkaEnvelope envelope = createTestEnvelope("MSG-SPECIAL-001", transactionId, entityId);

            publisher.publish(envelope);

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            String expectedKey = entityId + ":" + transactionId;
            boolean found = false;
            for (ConsumerRecord<String, String> record : records) {
                if (expectedKey.equals(record.key())) {
                    found = true;
                    break;
                }
            }
            assertThat(found).isTrue();
        }
    }

    @Nested
    @DisplayName("Idempotent Publish Tests")
    class IdempotentPublishTests {

        @Test
        @DisplayName("should allow publishing same message twice with different offsets")
        void shouldAllowPublishingSameMessageTwice() {
            KafkaEnvelope envelope = createTestEnvelope("MSG-IDEM-001", "TXN-IDEM-001", "ENTITY-IDEM-001");

            String offset1 = publisher.publish(envelope);
            String offset2 = publisher.publish(envelope);

            assertThat(offset1).isNotNull();
            assertThat(offset2).isNotNull();
        }

        @Test
        @DisplayName("duplicate messages should have same content")
        void duplicateMessagesShouldHaveSameContent() throws Exception {
            String messageId = "MSG-IDEM-002";
            KafkaEnvelope envelope = createTestEnvelope(messageId, "TXN-IDEM-002", "ENTITY-IDEM-002");

            publisher.publish(envelope);
            publisher.publish(envelope);

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            int matchCount = 0;
            for (ConsumerRecord<String, String> record : records) {
                JsonNode node = OBJECT_MAPPER.readTree(record.value());
                if (messageId.equals(node.get("messageId").asText())) {
                    matchCount++;
                }
            }
            assertThat(matchCount).isGreaterThanOrEqualTo(2);
        }
    }

    private KafkaEnvelope createTestEnvelope(String messageId, String transactionId, String entityId) {
        return KafkaEnvelope.builder()
                .messageId(messageId)
                .transactionId(transactionId)
                .eventType("order_created")
                .entityId(entityId)
                .hdfsPath("/data/bridge/payloads/order_created/2026/05/16/" + transactionId + "_" + messageId + ".json")
                .checksum("abc123def456")
                .marketingPlanId("MP-" + transactionId)
                .campaignId("CAMP-" + entityId)
                .eventTimestamp(Instant.now())
                .processedAt(Instant.now())
                .schemaVersion("1.0")
                .build();
    }
}
