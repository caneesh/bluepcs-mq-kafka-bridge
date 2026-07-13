package com.hcsc.bridge.mq;

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

@Component
@Profile("!local")
public class MqMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(MqMessageListener.class);
    private static final int MAX_LOGGED_PAYLOAD_CHARS = 500;

    private final BridgeOrchestrator orchestrator;

    @Value("${bridge.mq.log-payload:false}")
    private boolean logPayload;

    public MqMessageListener(BridgeOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @JmsListener(destination = "${bridge.mq.queue:BRIDGE.INPUT.QUEUE}")
    public void onMessage(Message message) {
        String messageId = null;
        try {
            if (!(message instanceof TextMessage)) {
                logger.error("Received unsupported message type {}, acknowledging to discard",
                        message.getClass().getName());
                message.acknowledge();
                return;
            }

            TextMessage textMessage = (TextMessage) message;
            messageId = textMessage.getJMSMessageID();
            String correlationId = textMessage.getJMSCorrelationID();
            String payload = textMessage.getText();

            logger.info("=== INCOMING MQ MESSAGE ===");
            logger.info("  JMSMessageID: {}", messageId);
            logger.info("  JMSCorrelationID: {}", correlationId);
            logger.info("  Queue: {}", extractQueueName(message));
            logger.info("  Payload size: {} bytes", payload != null ? payload.length() : 0);

            if (logPayload && payload != null) {
                String masked = SecretMaskingUtil.maskSecrets(payload);
                if (masked.length() > MAX_LOGGED_PAYLOAD_CHARS) {
                    masked = masked.substring(0, MAX_LOGGED_PAYLOAD_CHARS)
                            + "... (truncated, " + payload.length() + " chars total)";
                }
                logger.info("  Payload:\n{}", masked);
            }
            logger.info("===========================");

            MqMessage mqMessage = new MqMessage(
                    messageId,
                    correlationId,
                    payload,
                    Instant.now(),
                    extractQueueName(message)
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
