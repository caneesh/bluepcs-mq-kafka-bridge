package com.hcsc.bridge.mq;

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
                logger.info("  Payload:\n{}", payload);
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
                message.acknowledge();
                logger.info("Successfully processed and acknowledged message: eventId={}", result.getEventId());
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
