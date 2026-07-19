package com.hcsc.bridge.parser;

import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.model.ParsedPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.regex.Pattern;

@Component
public class JsonMessageParser implements MessageParser {

    private static final Logger logger = LoggerFactory.getLogger(JsonMessageParser.class);

    private static final String FIELD_PLAN_NOTIFICATION = "planNotification";
    private static final String FIELD_MARKETING_PLAN_IDENTIFIER = "marketingPlanIdentifier";
    private static final String FIELD_PLAN_VERSION = "planVersion";
    private static final String FIELD_PLAN_VERSION_IDENTIFIER = "planVersionIdentifier";
    private static final String FIELD_PLAN_EFFECTIVITY_DATES = "planEffectivityDates";
    private static final String FIELD_EFFECTIVE_START_DATE = "effectiveStartDate";
    private static final String FIELD_CHANGE_EVENT = "changeEvent";
    private static final String FIELD_EVENT_NAME = "eventName";
    private static final String FIELD_TYPE_NAME = "typeName";
    private static final String FIELD_TIMESTAMP = "timestamp";

    private static final String EVENT_TYPE_UNKNOWN = "Unknown";

    // Matches the leading yyyy-MM-dd portion of an ISO-8601 timestamp string
    private static final Pattern DATE_PREFIX_PATTERN = Pattern.compile("^\\d{4}-\\d{2}-\\d{2}");

    private final ObjectMapper objectMapper;

    public JsonMessageParser() {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    @Override
    public ParsedPayload parse(MqMessage message) throws MessageParseException {
        String payload = message.getPayload();
        String messageId = message.getMessageId();

        logger.debug("Parsing message: {}", messageId);

        // A null body is JMS-legal (TextMessage.getText() may return null); treat it as a
        // parse failure so it takes the quarantine path instead of NPE-ing in Jackson
        if (payload == null) {
            throw new MessageParseException("Message payload is null", messageId, null);
        }

        try {
            JsonNode rootNode = objectMapper.readTree(payload);

            JsonNode planNotification = rootNode.get(FIELD_PLAN_NOTIFICATION);
            if (planNotification == null || !planNotification.isObject()) {
                throw new MessageParseException(
                        "Missing required field: " + FIELD_PLAN_NOTIFICATION, messageId, payload);
            }

            String entityId = getRequiredText(planNotification, FIELD_MARKETING_PLAN_IDENTIFIER,
                    messageId, payload);
            String effectiveDate = parseEffectiveDate(planNotification, messageId, payload);
            String eventType = parseEventType(planNotification);
            String transactionId = buildTransactionId(planNotification, entityId);
            Instant eventTimestamp = parseEventTimestamp(planNotification, messageId);
            Map<String, Object> data = objectMapper.convertValue(
                    planNotification, new TypeReference<Map<String, Object>>() {});

            return new ParsedPayload(
                    messageId,
                    transactionId,
                    eventType,
                    entityId,
                    effectiveDate,
                    data,
                    eventTimestamp,
                    payload
            );

        } catch (JsonProcessingException e) {
            logger.error("Invalid JSON in message {}: {}", messageId, e.getMessage());
            throw new MessageParseException("Invalid JSON format", messageId, payload, e);
        }
    }

    /**
     * Reads a required non-empty text field directly under the given node.
     */
    private String getRequiredText(JsonNode node, String fieldName, String messageId, String payload) {
        JsonNode fieldNode = node.get(fieldName);
        if (fieldNode == null || fieldNode.isNull() || fieldNode.asText().isEmpty()) {
            throw new MessageParseException("Missing required field: " + fieldName, messageId, payload);
        }
        return fieldNode.asText();
    }

    /**
     * Extracts the yyyy-MM-dd date portion of planVersion.planEffectivityDates.effectiveStartDate.
     * The value is a full timestamp (e.g. "2026-01-01T06:00:00.000Z"); only the leading date is
     * used, so it is validated with a strict prefix pattern rather than fully parsed.
     */
    private String parseEffectiveDate(JsonNode planNotification, String messageId, String payload) {
        JsonNode planVersion = planNotification.path(FIELD_PLAN_VERSION);
        JsonNode effectivityDates = planVersion.path(FIELD_PLAN_EFFECTIVITY_DATES);
        JsonNode startDateNode = effectivityDates.path(FIELD_EFFECTIVE_START_DATE);

        if (startDateNode.isMissingNode() || startDateNode.isNull() || startDateNode.asText().isEmpty()) {
            throw new MessageParseException(
                    "Missing required field: " + FIELD_EFFECTIVE_START_DATE, messageId, payload);
        }

        String startDate = startDateNode.asText();
        if (!DATE_PREFIX_PATTERN.matcher(startDate).find()) {
            throw new MessageParseException(
                    "Malformed field: " + FIELD_EFFECTIVE_START_DATE, messageId, payload);
        }
        return startDate.substring(0, 10);
    }

    /**
     * Derives the event type from changeEvent.eventName, falling back to changeEvent.typeName,
     * then to "Unknown". Never returns null: the value is lowercased into an HDFS directory name
     * downstream.
     */
    private String parseEventType(JsonNode planNotification) {
        JsonNode changeEvent = planNotification.path(FIELD_CHANGE_EVENT);
        String eventName = changeEvent.path(FIELD_EVENT_NAME).asText("");
        if (!eventName.isEmpty()) {
            return eventName;
        }
        String typeName = changeEvent.path(FIELD_TYPE_NAME).asText("");
        if (!typeName.isEmpty()) {
            return typeName;
        }
        return EVENT_TYPE_UNKNOWN;
    }

    /**
     * Synthesizes a transaction id for tracing/audit. The real message has no transactionId field,
     * so it is derived from the marketing plan identifier plus the plan version when available.
     */
    private String buildTransactionId(JsonNode planNotification, String entityId) {
        String versionId = planNotification.path(FIELD_PLAN_VERSION)
                .path(FIELD_PLAN_VERSION_IDENTIFIER).asText("");
        if (!versionId.isEmpty()) {
            return entityId + "-v" + versionId;
        }
        return entityId;
    }

    /**
     * Parses changeEvent.timestamp. The real value carries a UTC offset (e.g.
     * "2026-03-05T06:21:02.971853484-06:00"), which Instant.parse cannot handle, so
     * OffsetDateTime is tried first and Instant.parse is the fallback for Z-suffixed values.
     * Any failure falls back to the current time, matching the lenient timestamp behavior.
     */
    private Instant parseEventTimestamp(JsonNode planNotification, String messageId) {
        JsonNode changeEvent = planNotification.path(FIELD_CHANGE_EVENT);
        JsonNode timestampNode = changeEvent.path(FIELD_TIMESTAMP);
        if (!timestampNode.isTextual() || timestampNode.asText().isEmpty()) {
            return Instant.now();
        }
        String timestamp = timestampNode.asText();
        try {
            return OffsetDateTime.parse(timestamp).toInstant();
        } catch (Exception offsetFailure) {
            try {
                return Instant.parse(timestamp);
            } catch (Exception instantFailure) {
                logger.warn("Failed to parse timestamp for message {}, using current time", messageId);
                return Instant.now();
            }
        }
    }
}
