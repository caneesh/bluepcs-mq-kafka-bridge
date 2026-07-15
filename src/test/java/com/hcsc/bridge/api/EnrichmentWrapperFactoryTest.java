package com.hcsc.bridge.api;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("EnrichmentWrapperFactory")
class EnrichmentWrapperFactoryTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String FULL_EXAMPLE = "{"
            + "\"PlanResponse\":{"
            + "  \"planIdentification\":{"
            + "    \"corporateProduct\":{\"corporateEntityCode\":\"MT1\"},"
            + "    \"scidVarianceNumber\":\"00\","
            + "    \"marketingPlanIdentifier\":\"SPSH44PPOIMTO\","
            + "    \"rxBinNumber\":\"610455\","
            + "    \"planName\":\"MT Blue Preferred Silver PPO - Balance-00_IND_2027\","
            + "    \"lineOfBusiness\":\"Retail\","
            + "    \"groupSectionXrefs\":{\"groupSectionXref\":[{\"groupNumber\":\"MS2903\",\"sectionNumber\":\"0001\"}]},"
            + "    \"marketedPublicly\":\"Yes\","
            + "    \"retailPointOfSaleIndicator\":\"No\","
            + "    \"productLineOfBusiness\":\"Medical\""
            + "  },"
            + "  \"packageOverrides\":{\"override\":[]},"
            + "  \"planMappings\":{\"planMapping\":[]},"
            + "  \"changeEvent\":{\"eventName\":\"ReadyToSell\",\"typeName\":\"Update\",\"timestamp\":\"20260710T162108.143 CDT\"}"
            + "}}";

    private EnrichmentWrapperFactory factory;

    @BeforeEach
    void setUp() {
        factory = new EnrichmentWrapperFactory();
    }

    private JsonNode read(String json) {
        try {
            return MAPPER.readTree(json);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode buildAndRead(String rawJson) {
        return read(factory.buildWrapper(read(rawJson)));
    }

    @Test
    @DisplayName("case 1: timestamp + typeName New are passed through verbatim")
    void timestampAndTypeNameNew() {
        String raw = "{\"PlanResponse\":{\"changeEvent\":"
                + "{\"eventName\":\"ReadyToSell\",\"typeName\":\"New\",\"timestamp\":\"20260101T010101.000 CDT\"}}}";

        JsonNode wrapper = buildAndRead(raw);

        assertThat(wrapper.get("changeEventTimeStamp").asText()).isEqualTo("20260101T010101.000 CDT");
        assertThat(wrapper.get("changeEventTypeName").asText()).isEqualTo("New");
    }

    @Test
    @DisplayName("case 2: timestamp + typeName Update are passed through verbatim")
    void timestampAndTypeNameUpdate() {
        String raw = "{\"PlanResponse\":{\"changeEvent\":"
                + "{\"typeName\":\"Update\",\"timestamp\":\"20260710T162108.143 CDT\"}}}";

        JsonNode wrapper = buildAndRead(raw);

        assertThat(wrapper.get("changeEventTimeStamp").asText()).isEqualTo("20260710T162108.143 CDT");
        assertThat(wrapper.get("changeEventTypeName").asText()).isEqualTo("Update");
    }

    @Test
    @DisplayName("case 3: changeEvent missing entirely uses defaults without throwing")
    void changeEventMissing() {
        String raw = "{\"PlanResponse\":{\"planIdentification\":{\"marketingPlanIdentifier\":\"X\"}}}";

        JsonNode wrapper = buildAndRead(raw);

        assertThat(wrapper.get("changeEventTimeStamp").asText())
                .isEqualTo(EnrichmentWrapperFactory.DEFAULT_CHANGE_EVENT_TIMESTAMP)
                .isEmpty();
        assertThat(wrapper.get("changeEventTypeName").asText())
                .isEqualTo(EnrichmentWrapperFactory.DEFAULT_CHANGE_EVENT_TYPE_NAME)
                .isEqualTo("Unknown");
    }

    @Test
    @DisplayName("case 4: changeEvent.timestamp missing/null uses empty-string default")
    void timestampMissingOrNull() {
        String missing = "{\"PlanResponse\":{\"changeEvent\":{\"typeName\":\"Update\"}}}";
        assertThat(buildAndRead(missing).get("changeEventTimeStamp").asText())
                .isEqualTo(EnrichmentWrapperFactory.DEFAULT_CHANGE_EVENT_TIMESTAMP);

        String nullTs = "{\"PlanResponse\":{\"changeEvent\":{\"typeName\":\"Update\",\"timestamp\":null}}}";
        assertThat(buildAndRead(nullTs).get("changeEventTimeStamp").asText())
                .isEqualTo(EnrichmentWrapperFactory.DEFAULT_CHANGE_EVENT_TIMESTAMP);
    }

    @Test
    @DisplayName("case 5: typeName missing/null defaults to Unknown, never eventName")
    void typeNameMissingDefaultsToUnknownNotEventName() {
        String missing = "{\"PlanResponse\":{\"changeEvent\":"
                + "{\"eventName\":\"ReadyToSell\",\"timestamp\":\"20260710T162108.143 CDT\"}}}";
        JsonNode wrapperMissing = buildAndRead(missing);
        assertThat(wrapperMissing.get("changeEventTypeName").asText()).isEqualTo("Unknown");
        assertThat(wrapperMissing.get("changeEventTypeName").asText()).isNotEqualTo("ReadyToSell");

        String nullType = "{\"PlanResponse\":{\"changeEvent\":"
                + "{\"eventName\":\"ReadyToSell\",\"typeName\":null,\"timestamp\":\"20260710T162108.143 CDT\"}}}";
        assertThat(buildAndRead(nullType).get("changeEventTypeName").asText()).isEqualTo("Unknown");
    }

    @Test
    @DisplayName("case 6: complete original response is unchanged inside RestAPIResponse")
    void originalResponseUnchangedInsideRestApiResponse() {
        JsonNode original = read(FULL_EXAMPLE);

        JsonNode wrapper = read(factory.buildWrapper(original));

        assertThat(wrapper.get("RestAPIResponse")).isEqualTo(original);
    }

    @Test
    @DisplayName("case 7: no RestAPIResponse.PlanResponse.PlanResponse double-nesting")
    void noDoubleNesting() {
        JsonNode wrapper = read(factory.buildWrapper(read(FULL_EXAMPLE)));

        assertThat(wrapper.path("RestAPIResponse").path("PlanResponse").has("planIdentification")).isTrue();
        assertThat(wrapper.path("RestAPIResponse").path("PlanResponse").path("PlanResponse").isMissingNode()).isTrue();
    }

    @Test
    @DisplayName("top-level field order is changeEventTimeStamp, RestAPIResponse, changeEventTypeName")
    void topLevelFieldOrder() {
        String json = factory.buildWrapper(read(FULL_EXAMPLE));

        int tsIdx = json.indexOf("changeEventTimeStamp");
        int respIdx = json.indexOf("RestAPIResponse");
        int typeIdx = json.indexOf("\"changeEventTypeName\"");

        assertThat(tsIdx).isGreaterThanOrEqualTo(0);
        assertThat(respIdx).isGreaterThan(tsIdx);
        assertThat(typeIdx).isGreaterThan(respIdx);
    }

    @Test
    @DisplayName("null raw response uses defaults and empty RestAPIResponse")
    void nullRawResponse() {
        JsonNode wrapper = read(factory.buildWrapper(null));

        assertThat(wrapper.get("changeEventTimeStamp").asText()).isEmpty();
        assertThat(wrapper.get("changeEventTypeName").asText()).isEqualTo("Unknown");
        assertThat(wrapper.get("RestAPIResponse").isObject()).isTrue();
    }

    @Test
    @DisplayName("MQ typeName fallback is used when API response has no typeName")
    void mqFallbackUsedWhenApiTypeNameMissing() {
        String raw = "{\"PlanResponse\":{\"changeEvent\":"
                + "{\"eventName\":\"ReadyToSell\",\"timestamp\":\"20260710T162108.143 CDT\"}}}";

        JsonNode wrapper = read(factory.buildWrapper(read(raw), "Update"));

        assertThat(wrapper.get("changeEventTypeName").asText()).isEqualTo("Update");
        assertThat(wrapper.get("changeEventTimeStamp").asText()).isEqualTo("20260710T162108.143 CDT");
    }

    @Test
    @DisplayName("API typeName wins over the MQ fallback when both are present")
    void apiTypeNameWinsOverFallback() {
        JsonNode wrapper = read(factory.buildWrapper(read(FULL_EXAMPLE), "New"));

        assertThat(wrapper.get("changeEventTypeName").asText()).isEqualTo("Update");
    }

    @Test
    @DisplayName("default Unknown is used when API typeName and MQ fallback are both absent")
    void defaultUsedWhenApiAndFallbackAbsent() {
        String raw = "{\"PlanResponse\":{\"changeEvent\":"
                + "{\"eventName\":\"ReadyToSell\",\"timestamp\":\"20260710T162108.143 CDT\"}}}";

        assertThat(read(factory.buildWrapper(read(raw), null)).get("changeEventTypeName").asText())
                .isEqualTo("Unknown");
        assertThat(read(factory.buildWrapper(read(raw), "")).get("changeEventTypeName").asText())
                .isEqualTo("Unknown");
    }
}
