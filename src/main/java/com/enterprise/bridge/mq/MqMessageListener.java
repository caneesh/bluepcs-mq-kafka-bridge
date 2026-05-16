package com.enterprise.bridge.mq;

import com.enterprise.bridge.model.MqMessage;
import com.enterprise.bridge.orchestrator.BridgeOrchestrator;
import com.enterprise.bridge.orchestrator.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.time.Instant;

@Component
public class MqMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(MqMessageListener.class);

    private final BridgeOrchestrator orchestrator;

    public MqMessageListener(BridgeOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @JmsListener(destination = "${bridge.mq.queue:BRIDGE.INPUT.QUEUE}")
    public void onMessage(Message message) {
        String messageId = null;
        try {
            if (!(message instanceof TextMessage)) {
                logger.warn("Received non-text message, skipping: {}", message.getClass().getName());
                return;
            }

            TextMessage textMessage = (TextMessage) message;
            messageId = textMessage.getJMSMessageID();
            String correlationId = textMessage.getJMSCorrelationID();
            String payload = textMessage.getText();

            logger.debug("Received MQ message: {}", messageId);

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
                logger.info("Successfully processed and acknowledged message: {}", messageId);
            } else if (result.isDuplicate()) {
                message.acknowledge();
                logger.info("Acknowledged duplicate message: {}", messageId);
            } else {
                logger.error("Processing failed for message {}: {}", messageId, result.getErrorMessage());
                throw new MqProcessingException("Processing failed: " + result.getErrorCode(),
                        messageId, result.getErrorMessage());
            }

        } catch (JMSException e) {
            logger.error("JMS exception processing message: {}", messageId, e);
            throw new MqProcessingException("JMS error", messageId, e.getMessage(), e);
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
