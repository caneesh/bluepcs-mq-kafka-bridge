package com.hcsc.bridge.kafka;

import com.hcsc.bridge.api.EnrichmentWrapperFactory.WrapperResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

/**
 * Builds the small notification message published to Kafka (claim-check pattern).
 *
 * <p>The full wrapper document can exceed the broker's message size limit (typically
 * 1 MB), so it is written to HDFS and Kafka carries only a pointer plus enough
 * metadata for consumers to filter without fetching the file:
 * <pre>
 * {
 *   "changeEventTimeStamp": "...",
 *   "changeEventTypeName": "New|Update|Unknown",
 *   "marketingPlanIdentifier": "...",
 *   "hdfsPath": "/.../&lt;eventId&gt;.json",
 *   "checksum": "&lt;sha-256 of the HDFS file content&gt;",
 *   "eventId": "&lt;sha-256 of the JMS message id, also the Kafka message key&gt;"
 * }
 * </pre>
 */
@Component
public class KafkaNotificationFactory {

    private final ObjectMapper objectMapper;

    public KafkaNotificationFactory() {
        this.objectMapper = new ObjectMapper();
    }

    public String buildNotification(WrapperResult wrapper, String marketingPlanIdentifier,
                                    String hdfsPath, String checksum, String eventId) {
        ObjectNode notification = objectMapper.createObjectNode();
        notification.put("changeEventTimeStamp", wrapper.getChangeEventTimeStamp());
        notification.put("changeEventTypeName", wrapper.getChangeEventTypeName());
        notification.put("marketingPlanIdentifier", marketingPlanIdentifier);
        notification.put("hdfsPath", hdfsPath);
        notification.put("checksum", checksum);
        notification.put("eventId", eventId);

        try {
            return objectMapper.writeValueAsString(notification);
        } catch (JsonProcessingException e) {
            // ObjectNode serialization does not fail in practice; surface defensively.
            throw new IllegalStateException("Failed to serialize Kafka notification", e);
        }
    }
}
