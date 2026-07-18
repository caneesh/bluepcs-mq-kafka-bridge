package com.hcsc.bridge.mq;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;
import com.hcsc.bridge.core.SecretMaskingUtil;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.orchestrator.BridgeOrchestrator;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@Profile("!local")
public class MqMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(MqMessageListener.class);
    private static final int MAX_LOGGED_PAYLOAD_CHARS = 500;
    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    private final BridgeOrchestrator orchestrator;
    private final AuditPublisher auditPublisher;

    @Value("${bridge.mq.log-payload:false}")
    private boolean logPayload;

    /**
     * Poison-message guard: if a message's JMSXDeliveryCount exceeds this value, the message is
     * logged in full (masked), audited as MESSAGE_DISCARDED, and acknowledged to unblock the queue.
     * 0 (the default) disables the guard — rely on the queue manager's backout threshold
     * (BOTHRESH/BOQNAME) instead, which preserves the message on a backout queue.
     * Only enable this when broker-side backout is not available: discarded payloads survive
     * only in application logs and the audit topic.
     */
    @Value("${bridge.mq.max-delivery-attempts:0}")
    private int maxDeliveryAttempts;

    public MqMessageListener(BridgeOrchestrator orchestrator, AuditPublisher auditPublisher) {
        this.orchestrator = orchestrator;
        this.auditPublisher = auditPublisher;
    }

    @JmsListener(destination = "${bridge.mq.queue:BRIDGE.INPUT.QUEUE}")
    public void onMessage(Message message) {
        String messageId = null;
        try {
            if (!(message instanceof TextMessage)) {
                logger.error("Received unsupported message type {}, acknowledging to discard",
                        message.getClass().getName());
                // Lenient ack: an ack failure while discarding must not trigger redelivery
                // of a message we cannot process anyway.
                acknowledgeQuietly(message, "unsupported-message-type discard");
                return;
            }

            TextMessage textMessage = (TextMessage) message;
            messageId = textMessage.getJMSMessageID();
            String correlationId = textMessage.getJMSCorrelationID();
            String payload = textMessage.getText();
            int deliveryCount = getDeliveryCount(message);
            String queueName = extractQueueName(message);

            logger.info("=== INCOMING MQ MESSAGE ===");
            logger.info("  JMSMessageID: {}", messageId);
            logger.info("  JMSCorrelationID: {}", correlationId);
            logger.info("  Queue: {}", queueName);
            logger.info("  Payload size: {} bytes", payload != null ? payload.length() : 0);
            if (deliveryCount > 1) {
                logger.warn("  Redelivery: JMSXDeliveryCount={}", deliveryCount);
            }

            if (logPayload && payload != null) {
                String masked = SecretMaskingUtil.maskSecrets(payload);
                if (masked.length() > MAX_LOGGED_PAYLOAD_CHARS) {
                    masked = masked.substring(0, MAX_LOGGED_PAYLOAD_CHARS)
                            + "... (truncated, " + payload.length() + " chars total)";
                }
                logger.info("  Payload:\n{}", masked);
            }
            logger.info("===========================");

            if (maxDeliveryAttempts > 0 && deliveryCount > maxDeliveryAttempts) {
                discardPoisonMessage(message, messageId, correlationId, payload, queueName, deliveryCount);
                return;
            }

            MqMessage mqMessage = new MqMessage(
                    messageId,
                    correlationId,
                    payload,
                    Instant.now(),
                    queueName
            );

            ProcessingResult result = orchestrator.process(mqMessage);

            if (result.isSuccessful()) {
                acknowledgeProcessedMessage(message, messageId, result.getEventId());
            } else {
                logger.error("Processing failed for message {}: {}", messageId, result.getErrorMessage());
                throw new MqProcessingException("Processing failed: " + result.getErrorCode(),
                        messageId, result.getErrorMessage());
            }

        } catch (JMSException e) {
            logger.error("JMS exception processing message: {}", messageId, e);
            throw new MqProcessingException("JMS error", messageId, e.getMessage(), e);
        } catch (RuntimeException e) {
            logger.error("Unexpected error processing message {}, will not acknowledge for redelivery",
                    messageId, e);
            throw e;
        }
    }

    /**
     * Acknowledges a message whose processing (HDFS write + Kafka publish) already succeeded.
     * An acknowledge failure must not be treated as a processing failure: the work is done, and
     * the broker will redeliver the unacknowledged message regardless of what we throw here.
     * The redelivery will re-publish to Kafka with the same eventId — downstream consumers must
     * tolerate duplicates (at-least-once delivery).
     */
    private void acknowledgeProcessedMessage(Message message, String messageId, String eventId) {
        try {
            message.acknowledge();
            logger.info("Successfully processed and acknowledged message: eventId={}", eventId);
        } catch (JMSException e) {
            logger.error("Message processed successfully but acknowledge failed: messageId={}, eventId={}. "
                    + "The broker will redeliver this message; downstream consumers may see a duplicate "
                    + "Kafka event with eventId={}", messageId, eventId, eventId, e);
        }
    }

    /**
     * Discards a message that has exceeded {@code bridge.mq.max-delivery-attempts}: logs the full
     * masked payload (this is the last copy before it is lost from MQ), publishes a
     * MESSAGE_DISCARDED audit event, and acknowledges. Every step here is best-effort — the
     * acknowledge MUST be reached, otherwise the poison message keeps blocking the queue.
     */
    private void discardPoisonMessage(Message message, String messageId, String correlationId,
                                      String payload, String queueName, int deliveryCount) {
        String maskedPayload = payload != null ? SecretMaskingUtil.maskSecrets(payload) : "<null>";
        logger.error("POISON MESSAGE: discarding after {} delivery attempts "
                        + "(bridge.mq.max-delivery-attempts={}): messageId={}, correlationId={}, queue={}. "
                        + "Last copy of masked payload before discard:\n{}",
                deliveryCount, maxDeliveryAttempts, messageId, correlationId, queueName, maskedPayload);

        try {
            auditPublisher.publishAsync(AuditEvent.builder()
                    .auditEventId(UUID.randomUUID().toString())
                    .originalMqMessageId(messageId)
                    .messageId(messageId)
                    .eventType(AuditEventType.MESSAGE_DISCARDED)
                    .description("Poison message discarded after " + deliveryCount
                            + " delivery attempts (max " + maxDeliveryAttempts + ")")
                    .metadata(Map.of(
                            "deliveryCount", deliveryCount,
                            "maxDeliveryAttempts", maxDeliveryAttempts,
                            "sourceQueue", queueName,
                            "correlationId", correlationId != null ? correlationId : ""))
                    .errorMessage("Exceeded max delivery attempts")
                    .build());
        } catch (RuntimeException e) {
            // Never let audit failure prevent the acknowledge below — the payload is already
            // preserved in the log line above.
            logger.error("Failed to publish MESSAGE_DISCARDED audit event for messageId={}", messageId, e);
        }

        acknowledgeQuietly(message, "poison-message discard (messageId=" + messageId + ")");
    }

    /**
     * Reads JMSXDeliveryCount (1 = first delivery). Returns 1 when the property is missing or
     * unreadable so that missing broker metadata can never cause a message to be discarded.
     */
    private int getDeliveryCount(Message message) {
        try {
            if (message.propertyExists(JMSX_DELIVERY_COUNT)) {
                return message.getIntProperty(JMSX_DELIVERY_COUNT);
            }
        } catch (JMSException e) {
            logger.debug("Could not read {} from message", JMSX_DELIVERY_COUNT, e);
        }
        return 1;
    }

    /**
     * Acknowledges a message on a discard path. Failures are logged, never thrown: throwing here
     * would trigger redelivery of a message we have already decided to drop.
     */
    private void acknowledgeQuietly(Message message, String context) {
        try {
            message.acknowledge();
        } catch (JMSException e) {
            logger.error("Acknowledge failed during {}; the broker may redeliver this message", context, e);
        }
    }

    private String extractQueueName(Message message) {
        try {
            if (message.getJMSDestination() != null) {
                return message.getJMSDestination().toString();
            }
        } catch (JMSException e) {
            logger.debug("Could not extract queue name from message", e);
        }
        return "UNKNOWN";
    }
}
