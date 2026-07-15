package com.hcsc.bridge.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

/**
 * Builds the JSON wrapper that the bridge publishes to BOTH Kafka (message value)
 * and HDFS (file content).
 *
 * <p>The wrapper has exactly three top-level fields, in this order (insertion order
 * is preserved by {@link ObjectNode}):
 * <pre>
 * {
 *   "changeEventTimeStamp": "&lt;PlanResponse.changeEvent.timestamp verbatim&gt;",
 *   "RestAPIResponse": { ...entire original API response root, unaltered... },
 *   "changeEventTypeName": "&lt;PlanResponse.changeEvent.typeName&gt;"
 * }
 * </pre>
 *
 * <p>The {@code RestAPIResponse} value is the parsed original response root attached
 * directly (the node whose single child is {@code PlanResponse}). It is never
 * reconstructed, filtered or reordered, and it is never double-nested as
 * {@code RestAPIResponse.PlanResponse.PlanResponse}.
 *
 * <p>All extraction is null-safe via {@link JsonNode#path(String)} so a missing or
 * null {@code changeEvent} (or any of its fields) never throws.
 */
@Component
public class EnrichmentWrapperFactory {

    /**
     * Default for {@code changeEventTimeStamp} when
     * {@code PlanResponse.changeEvent.timestamp} is missing or null.
     * The timestamp is otherwise passed through verbatim with NO conversion.
     */
    public static final String DEFAULT_CHANGE_EVENT_TIMESTAMP = "";

    /**
     * Default for {@code changeEventTypeName} when neither the API response's
     * {@code PlanResponse.changeEvent.typeName} nor the fallback (the MQ
     * notification's {@code changeEvent.typeName}) is present.
     * There is no established business rule for deriving this from {@code eventName}.
     */
    public static final String DEFAULT_CHANGE_EVENT_TYPE_NAME = "Unknown";

    private final ObjectMapper objectMapper;

    public EnrichmentWrapperFactory() {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Builds the wrapper JSON string from the raw (parsed, unmodified) API response root.
     *
     * @param rawApiResponse the original response root node whose single child is
     *                       {@code PlanResponse}; may be {@code null} or a missing node,
     *                       in which case an empty object is attached and the documented
     *                       defaults are used.
     * @return the wrapper serialized as a JSON string
     */
    public String buildWrapper(JsonNode rawApiResponse) {
        return buildWrapper(rawApiResponse, null);
    }

    public String buildWrapper(JsonNode rawApiResponse, String fallbackTypeName) {
        return build(rawApiResponse, fallbackTypeName).getWrapperJson();
    }

    /**
     * Builds the wrapper, using {@code fallbackTypeName} (typically the MQ
     * notification's {@code changeEvent.typeName}) when the API response does not carry
     * a {@code typeName}. Precedence: API {@code changeEvent.typeName}, then
     * {@code fallbackTypeName}, then {@link #DEFAULT_CHANGE_EVENT_TYPE_NAME}.
     * The derived fields are exposed alongside the JSON so the Kafka notification
     * message can reuse them without re-deriving.
     */
    public WrapperResult build(JsonNode rawApiResponse, String fallbackTypeName) {
        JsonNode root = (rawApiResponse != null && !rawApiResponse.isMissingNode())
                ? rawApiResponse
                : objectMapper.createObjectNode();

        // Null-safe navigation: PlanResponse -> changeEvent -> {timestamp, typeName}
        JsonNode changeEvent = root.path("PlanResponse").path("changeEvent");

        String changeEventTimeStamp = changeEvent.path("timestamp").isTextual()
                ? changeEvent.path("timestamp").asText()
                : DEFAULT_CHANGE_EVENT_TIMESTAMP;

        String typeName = changeEvent.path("typeName").isTextual()
                ? changeEvent.path("typeName").asText()
                : "";
        String changeEventTypeName;
        if (!typeName.isEmpty()) {
            changeEventTypeName = typeName;
        } else if (fallbackTypeName != null && !fallbackTypeName.isEmpty()) {
            changeEventTypeName = fallbackTypeName;
        } else {
            changeEventTypeName = DEFAULT_CHANGE_EVENT_TYPE_NAME;
        }

        ObjectNode wrapper = objectMapper.createObjectNode();
        wrapper.put("changeEventTimeStamp", changeEventTimeStamp);
        wrapper.set("RestAPIResponse", root);
        wrapper.put("changeEventTypeName", changeEventTypeName);

        try {
            return new WrapperResult(objectMapper.writeValueAsString(wrapper),
                    changeEventTimeStamp, changeEventTypeName);
        } catch (JsonProcessingException e) {
            // ObjectNode serialization does not fail in practice; surface defensively.
            throw new IllegalStateException("Failed to serialize enrichment wrapper", e);
        }
    }

    /** The wrapper JSON plus the two derived fields it embeds. */
    public static final class WrapperResult {
        private final String wrapperJson;
        private final String changeEventTimeStamp;
        private final String changeEventTypeName;

        private WrapperResult(String wrapperJson, String changeEventTimeStamp, String changeEventTypeName) {
            this.wrapperJson = wrapperJson;
            this.changeEventTimeStamp = changeEventTimeStamp;
            this.changeEventTypeName = changeEventTypeName;
        }

        public String getWrapperJson() {
            return wrapperJson;
        }

        public String getChangeEventTimeStamp() {
            return changeEventTimeStamp;
        }

        public String getChangeEventTypeName() {
            return changeEventTypeName;
        }
    }
}
