package com.hcsc.bridge.integration;

import com.hcsc.bridge.api.EnrichmentWrapperFactory;
import com.hcsc.bridge.kafka.KafkaEnvelopePublisher;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaPublisherIT {

    private static final String TOPIC = "test-bridge-events";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final EnrichmentWrapperFactory WRAPPER_FACTORY = new EnrichmentWrapperFactory();

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
        @DisplayName("should publish wrapper and return offset")
        void shouldPublishWrapperAndReturnOffset() {
            String offset = publisher.publish("event-id-001", buildWrapper("MP-001"));

            assertThat(offset).isNotNull();
            assertThat(Long.parseLong(offset)).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("should publish message with correct wrapper payload")
        void shouldPublishWithCorrectPayload() throws Exception {
            String key = "event-" + UUID.randomUUID().toString().substring(0, 8);
            publisher.publish(key, buildWrapper("MP-CORRECT"));

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            boolean found = false;
            for (ConsumerRecord<String, String> record : records) {
                if (key.equals(record.key())) {
                    JsonNode node = OBJECT_MAPPER.readTree(record.value());
                    assertThat(node.has("changeEventTimeStamp")).isTrue();
                    assertThat(node.has("changeEventTypeName")).isTrue();
                    assertThat(node.path("RestAPIResponse").path("PlanResponse")
                            .path("planIdentification").path("marketingPlanIdentifier").asText())
                            .isEqualTo("MP-CORRECT");
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
        @DisplayName("should use supplied key")
        void shouldUseSuppliedKey() {
            String key = "ENTITY-KEY-001:TXN-KEY-001";
            publisher.publish(key, buildWrapper("MP-KEY"));

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            boolean foundCorrectKey = false;
            for (ConsumerRecord<String, String> record : records) {
                if (key.equals(record.key())) {
                    foundCorrectKey = true;
                    break;
                }
            }
            assertThat(foundCorrectKey).isTrue();
        }

        @Test
        @DisplayName("should handle special characters in key")
        void shouldHandleSpecialCharactersInKey() {
            String key = "ENTITY.WITH.DOTS:TXN-WITH-DASH_AND_UNDERSCORE";
            publisher.publish(key, buildWrapper("MP-SPECIAL"));

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            assertThat(records.count()).isGreaterThanOrEqualTo(1);

            boolean found = false;
            for (ConsumerRecord<String, String> record : records) {
                if (key.equals(record.key())) {
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
            String wrapper = buildWrapper("MP-IDEM");

            String offset1 = publisher.publish("event-idem-001", wrapper);
            String offset2 = publisher.publish("event-idem-001", wrapper);

            assertThat(offset1).isNotNull();
            assertThat(offset2).isNotNull();
        }

        @Test
        @DisplayName("duplicate messages should have same content")
        void duplicateMessagesShouldHaveSameContent() throws Exception {
            String key = "event-idem-002";
            String wrapper = buildWrapper("MP-IDEM-CONTENT");

            publisher.publish(key, wrapper);
            publisher.publish(key, wrapper);

            ConsumerRecords<String, String> records = KafkaTestUtils.getRecords(consumer, 10000);
            int matchCount = 0;
            for (ConsumerRecord<String, String> record : records) {
                if (key.equals(record.key())) {
                    matchCount++;
                }
            }
            assertThat(matchCount).isGreaterThanOrEqualTo(2);
        }
    }

    private String buildWrapper(String marketingPlanId) {
        String raw = "{\"PlanResponse\":{"
                + "\"planIdentification\":{\"marketingPlanIdentifier\":\"" + marketingPlanId + "\"},"
                + "\"changeEvent\":{\"typeName\":\"Update\",\"timestamp\":\"20260710T162108.143 CDT\"}"
                + "}}";
        try {
            return WRAPPER_FACTORY.buildWrapper(OBJECT_MAPPER.readTree(raw));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
