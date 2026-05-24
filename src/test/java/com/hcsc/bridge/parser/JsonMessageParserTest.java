package com.hcsc.bridge.parser;

import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.model.ParsedPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JsonMessageParser")
class JsonMessageParserTest {

    private JsonMessageParser parser;

    @BeforeEach
    void setUp() {
        parser = new JsonMessageParser();
    }

    @Nested
    @DisplayName("when parsing valid JSON")
    class ValidJson {

        @Test
        @DisplayName("should parse complete payload with all fields")
        void shouldParseCompletePayload() {
            String payload = "{" +
                    "\"transactionId\": \"TXN-12345\"," +
                    "\"eventType\": \"ORDER_CREATED\"," +
                    "\"entityId\": \"ENT-67890\"," +
                    "\"timestamp\": \"2024-01-15T10:30:00Z\"," +
                    "\"data\": {\"amount\": 100.50, \"currency\": \"USD\"}" +
                    "}";
            MqMessage message = createMessage("MSG-001", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getMessageId()).isEqualTo("MSG-001");
            assertThat(result.getTransactionId()).isEqualTo("TXN-12345");
            assertThat(result.getEventType()).isEqualTo("ORDER_CREATED");
            assertThat(result.getEntityId()).isEqualTo("ENT-67890");
            assertThat(result.getEventTimestamp()).isEqualTo(Instant.parse("2024-01-15T10:30:00Z"));
            assertThat(result.getData()).containsEntry("amount", 100.50);
            assertThat(result.getData()).containsEntry("currency", "USD");
            assertThat(result.getRawPayload()).isEqualTo(payload);
        }

        @Test
        @DisplayName("should parse payload without optional timestamp")
        void shouldParsePayloadWithoutTimestamp() {
            String payload = "{" +
                    "\"transactionId\": \"TXN-12345\"," +
                    "\"eventType\": \"ORDER_CREATED\"," +
                    "\"entityId\": \"ENT-67890\"" +
                    "}";
            MqMessage message = createMessage("MSG-002", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getTransactionId()).isEqualTo("TXN-12345");
            assertThat(result.getEventTimestamp()).isNotNull();
            assertThat(result.getEventTimestamp()).isBeforeOrEqualTo(Instant.now());
        }

        @Test
        @DisplayName("should parse payload without optional data field")
        void shouldParsePayloadWithoutData() {
            String payload = "{" +
                    "\"transactionId\": \"TXN-12345\"," +
                    "\"eventType\": \"ORDER_CREATED\"," +
                    "\"entityId\": \"ENT-67890\"" +
                    "}";
            MqMessage message = createMessage("MSG-003", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getData()).isEmpty();
        }

        @Test
        @DisplayName("should parse timestamp as epoch milliseconds")
        void shouldParseEpochTimestamp() {
            long epochMillis = 1705315800000L;
            String payload = "{" +
                    "\"transactionId\": \"TXN-12345\"," +
                    "\"eventType\": \"ORDER_CREATED\"," +
                    "\"entityId\": \"ENT-67890\"," +
                    "\"timestamp\": " + epochMillis +
                    "}";
            MqMessage message = createMessage("MSG-004", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getEventTimestamp()).isEqualTo(Instant.ofEpochMilli(epochMillis));
        }

        @Test
        @DisplayName("should parse nested data structures")
        void shouldParseNestedData() {
            String payload = "{" +
                    "\"transactionId\": \"TXN-12345\"," +
                    "\"eventType\": \"ORDER_CREATED\"," +
                    "\"entityId\": \"ENT-67890\"," +
                    "\"data\": {\"customer\": {\"id\": \"CUST-001\", \"name\": \"John Doe\"}, \"items\": [\"item1\", \"item2\"]}" +
                    "}";
            MqMessage message = createMessage("MSG-005", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getData()).containsKey("customer");
            assertThat(result.getData()).containsKey("items");
        }
    }

    @Nested
    @DisplayName("when parsing invalid JSON")
    class InvalidJson {

        @Test
        @DisplayName("should throw exception for malformed JSON")
        void shouldThrowForMalformedJson() {
            String payload = "{ invalid json }";
            MqMessage message = createMessage("MSG-ERR-001", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("Invalid JSON format");
        }

        @Test
        @DisplayName("should throw exception for incomplete JSON")
        void shouldThrowForIncompleteJson() {
            String payload = "{\"transactionId\": \"TXN-12345\"";
            MqMessage message = createMessage("MSG-ERR-002", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("Invalid JSON format");
        }

        @Test
        @DisplayName("should throw exception for empty payload")
        void shouldThrowForEmptyPayload() {
            String payload = "";
            MqMessage message = createMessage("MSG-ERR-003", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class);
        }

        @Test
        @DisplayName("should throw exception for whitespace only payload")
        void shouldThrowForWhitespacePayload() {
            String payload = "   \n\t   ";
            MqMessage message = createMessage("MSG-ERR-004", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class);
        }

        @Test
        @DisplayName("should throw exception for array instead of object")
        void shouldThrowForArrayPayload() {
            String payload = "[1, 2, 3]";
            MqMessage message = createMessage("MSG-ERR-005", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("Missing required field");
        }
    }

    @Nested
    @DisplayName("when parsing payload with missing required fields")
    class MissingFields {

        @Test
        @DisplayName("should throw exception when transactionId is missing")
        void shouldThrowWhenTransactionIdMissing() {
            String payload = "{\"eventType\": \"ORDER_CREATED\", \"entityId\": \"ENT-67890\"}";
            MqMessage message = createMessage("MSG-MF-001", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("transactionId");
        }

        @Test
        @DisplayName("should throw exception when eventType is missing")
        void shouldThrowWhenEventTypeMissing() {
            String payload = "{\"transactionId\": \"TXN-12345\", \"entityId\": \"ENT-67890\"}";
            MqMessage message = createMessage("MSG-MF-002", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("eventType");
        }

        @Test
        @DisplayName("should throw exception when entityId is missing")
        void shouldThrowWhenEntityIdMissing() {
            String payload = "{\"transactionId\": \"TXN-12345\", \"eventType\": \"ORDER_CREATED\"}";
            MqMessage message = createMessage("MSG-MF-003", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("entityId");
        }

        @Test
        @DisplayName("should throw exception when transactionId is null")
        void shouldThrowWhenTransactionIdNull() {
            String payload = "{\"transactionId\": null, \"eventType\": \"ORDER_CREATED\", \"entityId\": \"ENT-67890\"}";
            MqMessage message = createMessage("MSG-MF-004", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("transactionId");
        }

        @Test
        @DisplayName("should throw exception when eventType is empty string")
        void shouldThrowWhenEventTypeEmpty() {
            String payload = "{\"transactionId\": \"TXN-12345\", \"eventType\": \"\", \"entityId\": \"ENT-67890\"}";
            MqMessage message = createMessage("MSG-MF-005", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("eventType");
        }
    }

    @Nested
    @DisplayName("when parsing malformed payload")
    class MalformedPayload {

        @Test
        @DisplayName("should include messageId in exception")
        void shouldIncludeMessageIdInException() {
            String payload = "{ bad json";
            MqMessage message = createMessage("MSG-TRACK-001", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .extracting("messageId")
                    .isEqualTo("MSG-TRACK-001");
        }

        @Test
        @DisplayName("should include raw payload in exception")
        void shouldIncludeRawPayloadInException() {
            String payload = "{ bad json";
            MqMessage message = createMessage("MSG-TRACK-002", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .extracting("rawPayload")
                    .isEqualTo(payload);
        }

        @Test
        @DisplayName("should handle invalid timestamp gracefully")
        void shouldHandleInvalidTimestampGracefully() {
            String payload = "{" +
                    "\"transactionId\": \"TXN-12345\"," +
                    "\"eventType\": \"ORDER_CREATED\"," +
                    "\"entityId\": \"ENT-67890\"," +
                    "\"timestamp\": \"not-a-timestamp\"" +
                    "}";
            MqMessage message = createMessage("MSG-TS-001", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getEventTimestamp()).isNotNull();
            assertThat(result.getEventTimestamp()).isBeforeOrEqualTo(Instant.now());
        }
    }

    private MqMessage createMessage(String messageId, String payload) {
        return new MqMessage(
                messageId,
                "CORR-" + messageId,
                payload,
                Instant.now(),
                "TEST.QUEUE"
        );
    }
}
