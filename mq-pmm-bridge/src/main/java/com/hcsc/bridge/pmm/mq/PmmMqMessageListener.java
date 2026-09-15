package com.hcsc.bridge.pmm.mq;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.core.SecretMaskingUtil;
import com.hcsc.bridge.hdfs.SafeHdfsWriter;
import com.hcsc.bridge.model.HdfsWriteResult;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.mq.JmsMessageSupport;
import com.hcsc.bridge.mq.MqProcessingException;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import com.hcsc.bridge.pmm.hdfs.WindowedPathResolver;
import com.hcsc.bridge.pmm.orchestrator.PmmOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static com.hcsc.bridge.mq.JmsMessageSupport.acknowledgeQuietly;
import static com.hcsc.bridge.mq.JmsMessageSupport.extractQueueName;
import static com.hcsc.bridge.mq.JmsMessageSupport.readHeaderQuietly;
import static com.hcsc.bridge.mq.JmsMessageSupport.sanitizeForLog;

/**
 * PMM queue listener. Same structure and ack semantics as the PMM+ listener
 * (CLIENT_ACKNOWLEDGE: ack on SUCCESS/QUARANTINED, throw on FAILURE so the broker
 * redelivers; poison guard and redelivery throttle from the {@code bridge.mq.*} keys),
 * plus {@link javax.jms.BytesMessage} support for XML publishers that put MQFMT_NONE.
 *
 * <p>No default destination on purpose: an unset {@code bridge.mq.queue} must fail startup
 * rather than silently consume some default queue.
 */
@Component
@Profile("!local")
public class PmmMqMessageListener {

    private static final Logger logger = LoggerFactory.getLogger(PmmMqMessageListener.class);
    private static final int MAX_LOGGED_PAYLOAD_CHARS = 500;

    private final PmmOrchestrator orchestrator;
    private final AuditPublisher auditPublisher;
    private final SafeHdfsWriter hdfsWriter;
    private final WindowedPathResolver pathResolver;
    private final EventIdGenerator eventIdGenerator;

    @Value("${bridge.mq.log-payload:false}")
    private boolean logPayload;

    /** Poison guard, 0 = disabled (rely on the queue manager's backout threshold). */
    @Value("${bridge.mq.max-delivery-attempts:0}")
    private int maxDeliveryAttempts;

    @Value("${bridge.mq.redelivery-backoff-ms:1000}")
    private long redeliveryBackoffMs;

    @Value("${bridge.mq.redelivery-backoff-max-ms:30000}")
    private long redeliveryBackoffMaxMs;

    /** Refuse BytesMessage bodies above this size before allocating anything. */
    @Value("${bridge.mq.max-message-bytes:67108864}")
    private long maxMessageBytes;

    public PmmMqMessageListener(PmmOrchestrator orchestrator, AuditPublisher auditPublisher,
                                SafeHdfsWriter hdfsWriter, WindowedPathResolver pathResolver,
                                EventIdGenerator eventIdGenerator) {
        this.orchestrator = orchestrator;
        this.auditPublisher = auditPublisher;
        this.hdfsWriter = hdfsWriter;
        this.pathResolver = pathResolver;
        this.eventIdGenerator = eventIdGenerator;
    }

    @PostConstruct
    void warnIfPayloadLoggingEnabled() {
        if (logPayload) {
            logger.warn("bridge.mq.log-payload=true: raw MQ payloads (which may contain PHI/PII) will be written "
                    + "to the application log with secret-pattern masking only, not PHI redaction. Never enable "
                    + "this against a production or PHI-bearing queue.");
        }
    }

    @JmsListener(destination = "${bridge.mq.queue}")
    public void onMessage(Message message) {
        String messageId = null;
        try {
            if (!JmsBodyDecoder.isSupported(message)) {
                logger.error("Received unsupported message type {}, acknowledging to discard",
                        message.getClass().getName());
                auditDiscardedUnsupportedMessage(message);
                acknowledgeQuietly(message, "unsupported-message-type discard");
                return;
            }

            // Headers first, each tolerating failure, so the poison guard is always evaluable
            messageId = readHeaderQuietly(message::getJMSMessageID, "JMSMessageID");
            String correlationId = readHeaderQuietly(message::getJMSCorrelationID, "JMSCorrelationID");
            int deliveryCount = JmsMessageSupport.getDeliveryCount(message);
            String queueName = extractQueueName(message);
            Instant jmsTimestamp = JmsMessageSupport.readJmsTimestamp(message);
            boolean poison = maxDeliveryAttempts > 0 && deliveryCount > maxDeliveryAttempts;

            String payload;
            try {
                payload = JmsBodyDecoder.decode(message, maxMessageBytes);
            } catch (JMSException bodyReadFailure) {
                // A conversion error throws on EVERY delivery; when the guard is armed and
                // tripped this is the only place it can act.
                if (poison) {
                    logger.error("Message body unreadable on delivery {} (max {}): messageId={} — discarding",
                            deliveryCount, maxDeliveryAttempts, messageId, bodyReadFailure);
                    discardPoisonMessage(message, messageId, correlationId, null, queueName, deliveryCount);
                    return;
                }
                throw bodyReadFailure;
            }

            logger.info("=== INCOMING PMM MQ MESSAGE === JMSMessageID={} JMSCorrelationID={} queue={} type={} payloadChars={}",
                    messageId, sanitizeForLog(correlationId), sanitizeForLog(queueName),
                    message.getClass().getSimpleName(), payload != null ? payload.length() : 0);
            if (deliveryCount > 1) {
                logger.warn("Redelivery: JMSXDeliveryCount={} messageId={}", deliveryCount, messageId);
            }
            if (logPayload && payload != null) {
                logger.info("Payload for messageId={}:\n{}", messageId, truncateMasked(payload));
            }

            if (poison) {
                discardPoisonMessage(message, messageId, correlationId, payload, queueName, deliveryCount);
                return;
            }

            throttleRedelivery(deliveryCount, messageId);

            MqMessage mqMessage = new MqMessage(messageId, correlationId, payload, Instant.now(),
                    queueName, jmsTimestamp);

            ProcessingResult result = orchestrator.process(mqMessage);

            if (result.isSuccessful()) {
                acknowledgeProcessedMessage(message, messageId, result.getEventId());
            } else if (result.isQuarantined()) {
                logger.warn("Message {} quarantined ({}): payload preserved at {}; acknowledging",
                        messageId, result.getErrorCode(), result.getHdfsPath());
                acknowledgeQuietly(message, "quarantined-message ack (messageId=" + messageId + ")");
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
     * The work is durable once we get here; an ack failure is logged, never rethrown. The
     * broker redelivers, and the orchestrator's exists() pre-check then skips the API call.
     */
    private void acknowledgeProcessedMessage(Message message, String messageId, String eventId) {
        try {
            message.acknowledge();
            logger.info("Successfully processed and acknowledged message: eventId={}", eventId);
        } catch (JMSException e) {
            logger.error("Message processed successfully but acknowledge failed: messageId={}, eventId={}. "
                    + "The broker will redeliver; the existing target file will be detected and no "
                    + "second API call made", messageId, eventId, e);
        }
    }

    private void discardPoisonMessage(Message message, String messageId, String correlationId,
                                      String payload, String queueName, int deliveryCount) {
        String preservedAt = quarantineDiscardedPayload(messageId, payload);
        if (payload != null && preservedAt == null) {
            // The whole point of the guard is to unblock the queue WITHOUT losing the
            // message. If the quarantine write failed (HDFS outage) there is no durable copy
            // yet: leave the message on the queue — the next delivery retries the
            // quarantine, and the redelivery backoff keeps the loop slow. An unreadable body
            // (payload == null) has nothing to preserve and is discarded below; the queue
            // manager's backout queue is the durable option for those.
            logger.error("POISON MESSAGE: quarantine write failed for messageId={} on delivery {} — NOT "
                    + "acknowledging (no durable copy); will retry the quarantine on redelivery",
                    messageId, deliveryCount);
            throw new MqProcessingException("Poison message could not be quarantined", messageId,
                    "quarantine write failed; message left on the queue");
        }
        logger.error("POISON MESSAGE: discarding after {} delivery attempts (bridge.mq.max-delivery-attempts={}): "
                        + "messageId={}, correlationId={}, queue={}. Payload preserved at: {}",
                deliveryCount, maxDeliveryAttempts, messageId, sanitizeForLog(correlationId),
                sanitizeForLog(queueName), preservedAt);
        try {
            auditPublisher.publishAsync(AuditEvent.builder()
                    .auditEventId(UUID.randomUUID().toString())
                    .originalMqMessageId(messageId)
                    .messageId(messageId)
                    .eventType(AuditEventType.MESSAGE_DISCARDED)
                    .description("Poison message discarded after " + deliveryCount
                            + " delivery attempts (max " + maxDeliveryAttempts + ")")
                    .metadata(Map.of(
                            "pipeline", PmmOrchestrator.PIPELINE,
                            "deliveryCount", deliveryCount,
                            "maxDeliveryAttempts", maxDeliveryAttempts,
                            "sourceQueue", queueName,
                            "correlationId", correlationId != null ? correlationId : ""))
                    .errorMessage("Exceeded max delivery attempts")
                    .build());
        } catch (RuntimeException e) {
            logger.error("Failed to publish MESSAGE_DISCARDED audit event for messageId={}", messageId, e);
        }
        acknowledgeQuietly(message, "poison-message discard (messageId=" + messageId + ")");
    }

    private String quarantineDiscardedPayload(String messageId, String payload) {
        if (payload == null) {
            return "<no payload - body unreadable>";
        }
        try {
            String eventId = eventIdGenerator.generateEventId(
                    messageId != null && !messageId.isEmpty() ? messageId : payload);
            HdfsWriteResult result = hdfsWriter.write(pathResolver.quarantinePath(eventId), payload, messageId);
            return result.getHdfsPath();
        } catch (RuntimeException e) {
            logger.error("Quarantine write failed for poison message {}; falling back to log preservation",
                    messageId, e);
            logger.error("Last (masked, truncated) copy of discarded payload for messageId={}:\n{}",
                    messageId, truncateMasked(payload));
            return null;
        }
    }

    private void throttleRedelivery(int deliveryCount, String messageId) {
        long delay = JmsMessageSupport.redeliveryDelayMs(deliveryCount, redeliveryBackoffMs, redeliveryBackoffMaxMs);
        if (delay <= 0) {
            return;
        }
        logger.info("Redelivery {} of message {}; backing off {} ms before processing", deliveryCount, messageId, delay);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Redelivery backoff interrupted for message {}; continuing", messageId);
        }
    }

    private void auditDiscardedUnsupportedMessage(Message message) {
        String discardedMessageId = readHeaderQuietly(message::getJMSMessageID, "JMSMessageID");
        try {
            auditPublisher.publishAsync(AuditEvent.builder()
                    .auditEventId(UUID.randomUUID().toString())
                    .originalMqMessageId(discardedMessageId)
                    .messageId(discardedMessageId)
                    .eventType(AuditEventType.MESSAGE_DISCARDED)
                    .description("Unsupported message type discarded: " + message.getClass().getName())
                    .metadata(Map.of(
                            "pipeline", PmmOrchestrator.PIPELINE,
                            "messageClass", message.getClass().getName(),
                            "sourceQueue", extractQueueName(message)))
                    .errorMessage("Only TextMessage and BytesMessage are supported")
                    .build());
        } catch (RuntimeException e) {
            logger.error("Failed to publish MESSAGE_DISCARDED audit for unsupported message type", e);
        }
    }

    private static String truncateMasked(String payload) {
        String masked = SecretMaskingUtil.maskSecrets(payload);
        if (masked.length() > MAX_LOGGED_PAYLOAD_CHARS) {
            masked = masked.substring(0, MAX_LOGGED_PAYLOAD_CHARS)
                    + "... (truncated, " + payload.length() + " chars total)";
        }
        return masked;
    }
}
