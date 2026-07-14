package com.hcsc.bridge.parser;

import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.model.ParsedPayload;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.OffsetDateTime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("JsonMessageParser")
class JsonMessageParserTest {

    // Real planNotification sample used as the base fixture for the happy path.
    private static final String SAMPLE_PLAN_NOTIFICATION = "{" +
            "\"planNotification\": {" +
            "  \"channels\": {\"channel\": [{\"channelName\": \"On-Exchange\"}]}," +
            "  \"divisions\": {\"division\": [{\"divisionCode\": \"MT\"}]}," +
            "  \"fundings\": {\"funding\": [{\"funding\": \"PREM\"}]}," +
            "  \"hsaCompatible\": \"Yes\"," +
            "  \"lineOfBusiness\": \"Retail\"," +
            "  \"marketSegments\": {\"marketSegment\": [{\"marketSegmentCode\": \"IndividualQHP\", \"marketSegmentCategoryCode\": \"IndividualQHP\"}]}," +
            "  \"marketingPlanIdentifier\": \"BOSA61BLCIMTP\"," +
            "  \"planVersion\": {" +
            "    \"planVersionIdentifier\": \"4\"," +
            "    \"planEffectivityDates\": {" +
            "      \"effectiveStartDate\": \"2026-01-01T06:00:00.000Z\"," +
            "      \"retiredDate\": \"9999-12-31T00:00:00.000Z\"," +
            "      \"closedDate\": \"9999-12-31T00:00:00.000Z\"" +
            "    }," +
            "    \"planStatus\": \"Open\"" +
            "  }," +
            "  \"productInformation\": {\"productAbbreviation\": \"BLUEFC\", \"productType\": \"HMO\", \"productName\": \"Blue Focus POS\", \"productShortDescription\": \"Blue Focus POS\"}," +
            "  \"productLineOfBusiness\": \"Medical\"," +
            "  \"changeEvent\": {\"typeName\": \"Update\", \"timestamp\": \"2026-03-05T06:21:02.971853484-06:00\", \"eventName\": \"ReadyToSell\"}," +
            "  \"mockIndicator\": \"N\"" +
            "}" +
            "}";

    private JsonMessageParser parser;

    @BeforeEach
    void setUp() {
        parser = new JsonMessageParser();
    }

    @Nested
    @DisplayName("when parsing a valid planNotification message")
    class ValidPlanNotification {

        @Test
        @DisplayName("should parse all fields from the real sample message")
        void shouldParseCompletePayload() {
            MqMessage message = createMessage("MSG-001", SAMPLE_PLAN_NOTIFICATION);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getMessageId()).isEqualTo("MSG-001");
            assertThat(result.getEntityId()).isEqualTo("BOSA61BLCIMTP");
            assertThat(result.getEffectiveDate()).isEqualTo("2026-01-01");
            assertThat(result.getEventType()).isEqualTo("ReadyToSell");
            assertThat(result.getTransactionId()).isEqualTo("BOSA61BLCIMTP-v4");
            assertThat(result.getEventTimestamp())
                    .isEqualTo(OffsetDateTime.parse("2026-03-05T06:21:02.971853484-06:00").toInstant());
            assertThat(result.getRawPayload()).isEqualTo(SAMPLE_PLAN_NOTIFICATION);
        }

        @Test
        @DisplayName("should copy the entire planNotification node into data")
        void shouldExposePlanNotificationAsData() {
            MqMessage message = createMessage("MSG-002", SAMPLE_PLAN_NOTIFICATION);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getData()).containsEntry("marketingPlanIdentifier", "BOSA61BLCIMTP");
            assertThat(result.getData()).containsEntry("lineOfBusiness", "Retail");
            assertThat(result.getData()).containsKey("planVersion");
            assertThat(result.getData()).containsKey("changeEvent");
        }

        @Test
        @DisplayName("should fall back to typeName when eventName is absent")
        void shouldFallBackToTypeName() {
            String payload = "{\"planNotification\": {" +
                    "\"marketingPlanIdentifier\": \"PLAN-1\"," +
                    "\"planVersion\": {\"planVersionIdentifier\": \"2\"," +
                    "\"planEffectivityDates\": {\"effectiveStartDate\": \"2026-01-01T06:00:00.000Z\"}}," +
                    "\"changeEvent\": {\"typeName\": \"Update\", \"timestamp\": \"2026-03-05T06:21:02.971Z\"}" +
                    "}}";
            MqMessage message = createMessage("MSG-003", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getEventType()).isEqualTo("Update");
            assertThat(result.getTransactionId()).isEqualTo("PLAN-1-v2");
        }

        @Test
        @DisplayName("should use 'Unknown' when both eventName and typeName are absent")
        void shouldDefaultEventTypeToUnknown() {
            String payload = "{\"planNotification\": {" +
                    "\"marketingPlanIdentifier\": \"PLAN-1\"," +
                    "\"planVersion\": {\"planEffectivityDates\": {\"effectiveStartDate\": \"2026-01-01T00:00:00Z\"}}," +
                    "\"changeEvent\": {}" +
                    "}}";
            MqMessage message = createMessage("MSG-004", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getEventType()).isEqualTo("Unknown");
        }

        @Test
        @DisplayName("should use marketingPlanIdentifier as transactionId when version is absent")
        void shouldSynthesizeTransactionIdWithoutVersion() {
            String payload = "{\"planNotification\": {" +
                    "\"marketingPlanIdentifier\": \"PLAN-9\"," +
                    "\"planVersion\": {\"planEffectivityDates\": {\"effectiveStartDate\": \"2026-01-01T00:00:00Z\"}}," +
                    "\"changeEvent\": {\"eventName\": \"ReadyToSell\"}" +
                    "}}";
            MqMessage message = createMessage("MSG-005", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getTransactionId()).isEqualTo("PLAN-9");
        }

        @Test
        @DisplayName("should parse a Z-suffixed changeEvent timestamp")
        void shouldParseZSuffixedTimestamp() {
            String payload = "{\"planNotification\": {" +
                    "\"marketingPlanIdentifier\": \"PLAN-1\"," +
                    "\"planVersion\": {\"planEffectivityDates\": {\"effectiveStartDate\": \"2026-01-01T00:00:00Z\"}}," +
                    "\"changeEvent\": {\"eventName\": \"ReadyToSell\", \"timestamp\": \"2026-03-05T12:00:00Z\"}" +
                    "}}";
            MqMessage message = createMessage("MSG-006", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getEventTimestamp()).isEqualTo(Instant.parse("2026-03-05T12:00:00Z"));
        }

        @Test
        @DisplayName("should fall back to current time for an unparseable timestamp")
        void shouldFallBackToCurrentTimeForBadTimestamp() {
            String payload = "{\"planNotification\": {" +
                    "\"marketingPlanIdentifier\": \"PLAN-1\"," +
                    "\"planVersion\": {\"planEffectivityDates\": {\"effectiveStartDate\": \"2026-01-01T00:00:00Z\"}}," +
                    "\"changeEvent\": {\"eventName\": \"ReadyToSell\", \"timestamp\": \"not-a-timestamp\"}" +
                    "}}";
            MqMessage message = createMessage("MSG-007", payload);

            ParsedPayload result = parser.parse(message);

            assertThat(result.getEventTimestamp()).isNotNull();
            assertThat(result.getEventTimestamp()).isBeforeOrEqualTo(Instant.now());
        }
    }

    @Nested
    @DisplayName("when parsing invalid or incomplete messages")
    class InvalidMessages {

        @Test
        @DisplayName("should throw when planNotification is missing")
        void shouldThrowWhenPlanNotificationMissing() {
            String payload = "{\"somethingElse\": {}}";
            MqMessage message = createMessage("MSG-MF-001", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("Missing required field: planNotification");
        }

        @Test
        @DisplayName("should throw when marketingPlanIdentifier is missing")
        void shouldThrowWhenMarketingPlanIdentifierMissing() {
            String payload = "{\"planNotification\": {" +
                    "\"planVersion\": {\"planEffectivityDates\": {\"effectiveStartDate\": \"2026-01-01T00:00:00Z\"}}," +
                    "\"changeEvent\": {\"eventName\": \"ReadyToSell\"}" +
                    "}}";
            MqMessage message = createMessage("MSG-MF-002", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("marketingPlanIdentifier");
        }

        @Test
        @DisplayName("should throw when effectiveStartDate is missing")
        void shouldThrowWhenEffectiveStartDateMissing() {
            String payload = "{\"planNotification\": {" +
                    "\"marketingPlanIdentifier\": \"PLAN-1\"," +
                    "\"planVersion\": {\"planEffectivityDates\": {}}," +
                    "\"changeEvent\": {\"eventName\": \"ReadyToSell\"}" +
                    "}}";
            MqMessage message = createMessage("MSG-MF-003", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("effectiveStartDate");
        }

        @Test
        @DisplayName("should throw when effectiveStartDate is malformed")
        void shouldThrowWhenEffectiveStartDateMalformed() {
            String payload = "{\"planNotification\": {" +
                    "\"marketingPlanIdentifier\": \"PLAN-1\"," +
                    "\"planVersion\": {\"planEffectivityDates\": {\"effectiveStartDate\": \"January 1 2026\"}}," +
                    "\"changeEvent\": {\"eventName\": \"ReadyToSell\"}" +
                    "}}";
            MqMessage message = createMessage("MSG-MF-004", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("effectiveStartDate");
        }

        @Test
        @DisplayName("should throw for malformed JSON")
        void shouldThrowForMalformedJson() {
            String payload = "{ invalid json }";
            MqMessage message = createMessage("MSG-ERR-001", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .hasMessageContaining("Invalid JSON format");
        }

        @Test
        @DisplayName("should include messageId and rawPayload in the exception")
        void shouldIncludeContextInException() {
            String payload = "{\"somethingElse\": {}}";
            MqMessage message = createMessage("MSG-TRACK-001", payload);

            assertThatThrownBy(() -> parser.parse(message))
                    .isInstanceOf(MessageParseException.class)
                    .extracting("messageId", "rawPayload")
                    .containsExactly("MSG-TRACK-001", payload);
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
