package com.enterprise.bridge.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("EventIdGenerator")
class EventIdGeneratorTest {

    private EventIdGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new EventIdGenerator();
    }

    @Nested
    @DisplayName("event ID generation")
    class EventIdGeneration {

        @Test
        @DisplayName("should generate deterministic event ID from JMS message ID")
        void shouldGenerateDeterministicEventId() {
            String jmsMessageId = "ID:414d5120514d5445535431202020202063a4c756203c1b02";

            String eventId1 = generator.generateEventId(jmsMessageId);
            String eventId2 = generator.generateEventId(jmsMessageId);

            assertThat(eventId1).isEqualTo(eventId2);
        }

        @Test
        @DisplayName("should generate different event IDs for different JMS message IDs")
        void shouldGenerateDifferentEventIdsForDifferentInputs() {
            String eventId1 = generator.generateEventId("MSG-001");
            String eventId2 = generator.generateEventId("MSG-002");

            assertThat(eventId1).isNotEqualTo(eventId2);
        }

        @Test
        @DisplayName("should generate 64 character hex string (SHA-256)")
        void shouldGenerateSha256HexString() {
            String eventId = generator.generateEventId("JMS-MSG-ID-12345");

            assertThat(eventId).hasSize(64);
            assertThat(eventId).matches("[a-f0-9]{64}");
        }

        @Test
        @DisplayName("should handle special characters in JMS message ID")
        void shouldHandleSpecialCharacters() {
            String jmsMessageId = "ID:414d5120:QM1:2024-01-15T10:30:00.000Z";

            String eventId = generator.generateEventId(jmsMessageId);

            assertThat(eventId).isNotNull();
            assertThat(eventId).hasSize(64);
        }

        @Test
        @DisplayName("should handle unicode characters")
        void shouldHandleUnicodeCharacters() {
            String jmsMessageId = "MSG-测试-001";

            String eventId = generator.generateEventId(jmsMessageId);

            assertThat(eventId).isNotNull();
            assertThat(eventId).hasSize(64);
        }
    }

    @Nested
    @DisplayName("input validation")
    class InputValidation {

        @Test
        @DisplayName("should throw exception for null JMS message ID")
        void shouldThrowForNullInput() {
            assertThatThrownBy(() -> generator.generateEventId(null))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JMS Message ID cannot be null or empty");
        }

        @Test
        @DisplayName("should throw exception for empty JMS message ID")
        void shouldThrowForEmptyInput() {
            assertThatThrownBy(() -> generator.generateEventId(""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("JMS Message ID cannot be null or empty");
        }
    }

    @Nested
    @DisplayName("reproducibility")
    class Reproducibility {

        @Test
        @DisplayName("should produce same result across multiple instances")
        void shouldProduceSameResultAcrossInstances() {
            EventIdGenerator generator1 = new EventIdGenerator();
            EventIdGenerator generator2 = new EventIdGenerator();
            String jmsMessageId = "ID:consistent-message-id";

            String eventId1 = generator1.generateEventId(jmsMessageId);
            String eventId2 = generator2.generateEventId(jmsMessageId);

            assertThat(eventId1).isEqualTo(eventId2);
        }

        @Test
        @DisplayName("should produce known hash for known input")
        void shouldProduceKnownHashForKnownInput() {
            String eventId = generator.generateEventId("test");

            assertThat(eventId).isEqualTo("9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08");
        }
    }
}
