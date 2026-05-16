package com.enterprise.bridge.parser;

import com.enterprise.bridge.model.MqMessage;
import com.enterprise.bridge.model.ParsedPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Component
public class JsonMessageParser implements MessageParser {

    private static final Logger logger = LoggerFactory.getLogger(JsonMessageParser.class);

    private static final String FIELD_TRANSACTION_ID = "transactionId";
    private static final String FIELD_EVENT_TYPE = "eventType";
    private static final String FIELD_ENTITY_ID = "entityId";
    private static final String FIELD_TIMESTAMP = "timestamp";
    private static final String FIELD_DATA = "data";

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

        try {
            JsonNode rootNode = objectMapper.readTree(payload);

            validateRequiredFields(rootNode, messageId, payload);

            String transactionId = getRequiredField(rootNode, FIELD_TRANSACTION_ID, messageId, payload);
            String eventType = getRequiredField(rootNode, FIELD_EVENT_TYPE, messageId, payload);
            String entityId = getRequiredField(rootNode, FIELD_ENTITY_ID, messageId, payload);
            Instant timestamp = parseTimestamp(rootNode, messageId, payload);
            Map<String, Object> data = parseData(rootNode);

            return new ParsedPayload(
                    messageId,
                    transactionId,
                    eventType,
                    entityId,
                    data,
                    timestamp,
                    payload
            );

        } catch (JsonProcessingException e) {
            logger.error("Invalid JSON in message {}: {}", messageId, e.getMessage());
            throw new MessageParseException("Invalid JSON format", messageId, payload, e);
        }
    }

    private void validateRequiredFields(JsonNode rootNode, String messageId, String payload) {
        if (!rootNode.has(FIELD_TRANSACTION_ID)) {
            throw new MessageParseException("Missing required field: " + FIELD_TRANSACTION_ID, messageId, payload);
        }
        if (!rootNode.has(FIELD_EVENT_TYPE)) {
            throw new MessageParseException("Missing required field: " + FIELD_EVENT_TYPE, messageId, payload);
        }
        if (!rootNode.has(FIELD_ENTITY_ID)) {
            throw new MessageParseException("Missing required field: " + FIELD_ENTITY_ID, messageId, payload);
        }
    }

    private String getRequiredField(JsonNode rootNode, String fieldName, String messageId, String payload) {
        JsonNode fieldNode = rootNode.get(fieldName);
        if (fieldNode == null || fieldNode.isNull() || fieldNode.asText().isEmpty()) {
            throw new MessageParseException("Field is null or empty: " + fieldName, messageId, payload);
        }
        return fieldNode.asText();
    }

    private Instant parseTimestamp(JsonNode rootNode, String messageId, String payload) {
        if (!rootNode.has(FIELD_TIMESTAMP)) {
            return Instant.now();
        }
        JsonNode timestampNode = rootNode.get(FIELD_TIMESTAMP);
        try {
            if (timestampNode.isTextual()) {
                return Instant.parse(timestampNode.asText());
            } else if (timestampNode.isNumber()) {
                return Instant.ofEpochMilli(timestampNode.asLong());
            }
            return Instant.now();
        } catch (Exception e) {
            logger.warn("Failed to parse timestamp for message {}, using current time", messageId);
            return Instant.now();
        }
    }

    private Map<String, Object> parseData(JsonNode rootNode) {
        if (!rootNode.has(FIELD_DATA)) {
            return Map.of();
        }
        JsonNode dataNode = rootNode.get(FIELD_DATA);
        return objectMapper.convertValue(dataNode, new TypeReference<Map<String, Object>>() {});
    }
}
