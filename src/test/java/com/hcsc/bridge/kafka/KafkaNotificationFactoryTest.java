package com.hcsc.bridge.kafka;

import com.hcsc.bridge.api.EnrichmentWrapperFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("KafkaNotificationFactory")
class KafkaNotificationFactoryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private KafkaNotificationFactory factory;
    private EnrichmentWrapperFactory wrapperFactory;

    @BeforeEach
    void setUp() {
        factory = new KafkaNotificationFactory();
        wrapperFactory = new EnrichmentWrapperFactory();
    }

    private JsonNode read(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private EnrichmentWrapperFactory.WrapperResult wrapperFor(String apiJson) {
        return wrapperFactory.build(read(apiJson), null);
    }

    @Test
    @DisplayName("contains the claim-check fields and none of the API response body")
    void containsClaimCheckFieldsOnly() {
        EnrichmentWrapperFactory.WrapperResult wrapper = wrapperFor(
                "{\"PlanResponse\":{"
                        + "\"planIdentification\":{\"marketingPlanIdentifier\":\"SPSH44PPOIMTO\",\"planName\":\"Some Plan\"},"
                        + "\"changeEvent\":{\"typeName\":\"Update\",\"timestamp\":\"20260710T162108.143 CDT\"}"
                        + "}}");

        String json = factory.buildNotification(wrapper, "SPSH44PPOIMTO",
                "/data/bridge/payloads/readytosell/2026/07/10/abc123.json", "cafe1234", "abc123");
        JsonNode notification = read(json);

        assertThat(notification.get("changeEventTimeStamp").asText()).isEqualTo("20260710T162108.143 CDT");
        assertThat(notification.get("changeEventTypeName").asText()).isEqualTo("Update");
        assertThat(notification.get("marketingPlanIdentifier").asText()).isEqualTo("SPSH44PPOIMTO");
        assertThat(notification.get("hdfsPath").asText())
                .isEqualTo("/data/bridge/payloads/readytosell/2026/07/10/abc123.json");
        assertThat(notification.get("checksum").asText()).isEqualTo("cafe1234");
        assertThat(notification.get("eventId").asText()).isEqualTo("abc123");
        assertThat(notification.get("source").asText()).isEqualTo("mq-kafka-bridge");
        assertThat(notification.get("schemaVersion").asInt()).isEqualTo(1);
        assertThat(notification.size()).isEqualTo(8);
        assertThat(json).doesNotContain("RestAPIResponse").doesNotContain("planName");
    }

    @Test
    @DisplayName("carries the format discriminator distinguishing bridge messages from legacy Talend ones")
    void carriesFormatDiscriminator() {
        EnrichmentWrapperFactory.WrapperResult wrapper = wrapperFor("{\"PlanResponse\":{}}");

        JsonNode notification = read(factory.buildNotification(
                wrapper, "MP", "/p", "c", "e"));

        // Legacy Talend messages have RestAPIResponse inline and no marker fields;
        // consumers detect the claim-check format by schemaVersion presence.
        assertThat(notification.hasNonNull("source")).isTrue();
        assertThat(notification.hasNonNull("schemaVersion")).isTrue();
        assertThat(notification.get("schemaVersion").isInt()).isTrue();
        assertThat(notification.has("RestAPIResponse")).isFalse();
    }

    @Test
    @DisplayName("uses the wrapper defaults when the API response has no changeEvent")
    void usesWrapperDefaults() {
        EnrichmentWrapperFactory.WrapperResult wrapper = wrapperFor("{\"PlanResponse\":{}}");

        JsonNode notification = read(factory.buildNotification(
                wrapper, "SPSH44PPOIMTO", "/path/file.json", "cafe1234", "abc123"));

        assertThat(notification.get("changeEventTimeStamp").asText()).isEmpty();
        assertThat(notification.get("changeEventTypeName").asText()).isEqualTo("Unknown");
    }

    @Test
    @DisplayName("field order is stable: timestamps, type, plan id, path, checksum, eventId")
    void fieldOrder() {
        EnrichmentWrapperFactory.WrapperResult wrapper = wrapperFor("{\"PlanResponse\":{}}");

        String json = factory.buildNotification(wrapper, "MP", "/p", "c", "e");

        assertThat(json.indexOf("changeEventTimeStamp"))
                .isLessThan(json.indexOf("changeEventTypeName"));
        assertThat(json.indexOf("changeEventTypeName"))
                .isLessThan(json.indexOf("marketingPlanIdentifier"));
        assertThat(json.indexOf("marketingPlanIdentifier"))
                .isLessThan(json.indexOf("hdfsPath"));
        assertThat(json.indexOf("hdfsPath")).isLessThan(json.indexOf("checksum"));
        assertThat(json.indexOf("checksum")).isLessThan(json.indexOf("eventId"));
        assertThat(json.indexOf("eventId")).isLessThan(json.indexOf("source"));
        assertThat(json.indexOf("\"source\"")).isLessThan(json.indexOf("schemaVersion"));
    }
}
