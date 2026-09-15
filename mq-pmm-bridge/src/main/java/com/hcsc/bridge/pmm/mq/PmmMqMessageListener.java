package com.hcsc.bridge.pmm.mq;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditMetadata;
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
                // No payload to preserve for a type this bridge cannot read; the disposition
                // says so rather than leaving an unexplained acknowledgement.
                JmsMessageSupport.settle(message, ProcessingResult.discardedWithoutPayload(
                        discardIdentity(readHeaderQuietly(message::getJMSMessageID, "JMSMessageID")),
                        "unsupported message type " + message.getClass().getName(),
                        "UNSUPPORTED_TYPE", "Only TextMessage and BytesMessage are supported"), messageId);
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

            // One rule for every outcome, shared with the PMM+ bridge: acknowledge only what
            // is terminal and durable, otherwise leave the message on the queue.
            JmsMessageSupport.settle(message, result, messageId);

        } catch (JMSException e) {
            logger.error("JMS exception processing message: {}", messageId, e);
            throw new MqProcessingException("JMS error", messageId, e.getMessage(), e);
        } catch (RuntimeException e) {
            logger.error("Unexpected error processing message {}, will not acknowledge for redelivery",
                    messageId, e);
            throw e;
        }
    }

    private void discardPoisonMessage(Message message, String messageId, String correlationId,
                                      String payload, String queueName, int deliveryCount) {
        String eventId = discardEventId(messageId, payload);
        String preservedAt = quarantineDiscardedPayload(eventId, messageId, payload);
        String identity = discardIdentity(eventId != null ? eventId : messageId);
        String detail = "Exceeded max delivery attempts (" + deliveryCount + ")";
        ProcessingResult disposition;
        if (payload == null) {
            disposition = ProcessingResult.discardedWithoutPayload(identity,
                    "message body was unreadable on every delivery", "POISON", detail);
        } else if (preservedAt == null) {
            // The guard exists to unblock the queue WITHOUT losing the message: with no durable
            // copy the message stays on the queue and the next delivery retries the quarantine.
            disposition = ProcessingResult.failure(identity, "POISON_QUARANTINE_FAILED",
                    "quarantine write failed; message left on the queue");
        } else {
            disposition = ProcessingResult.discarded(identity, preservedAt, "POISON", detail);
        }

        if (disposition.isAcknowledgeable()) {
            logger.error("POISON MESSAGE: discarding after {} delivery attempts (bridge.mq.max-delivery-attempts={}): "
                            + "messageId={}, correlationId={}, queue={}. Payload preserved at: {}",
                    deliveryCount, maxDeliveryAttempts, messageId, sanitizeForLog(correlationId),
                    sanitizeForLog(queueName),
                    preservedAt != null ? preservedAt : disposition.getNoEvidenceReason());
            auditPoisonDiscard(disposition, eventId, messageId, correlationId, queueName, deliveryCount, preservedAt);
        } else {
            logger.error("POISON MESSAGE: quarantine write failed for messageId={} on delivery {} — NOT "
                    + "acknowledging (no durable copy); will retry the quarantine on redelivery",
                    messageId, deliveryCount);
        }
        JmsMessageSupport.settle(message, disposition, messageId);
    }

    private void auditPoisonDiscard(ProcessingResult disposition, String eventId, String messageId,
                                    String correlationId, String queueName, int deliveryCount,
                                    String preservedAt) {
        try {
            Map<String, Object> metadata = new java.util.HashMap<>();
            metadata.put(AuditMetadata.PIPELINE, PmmOrchestrator.PIPELINE);
            metadata.put(AuditMetadata.ERROR_CODE, "POISON");
            metadata.put(AuditMetadata.HDFS_PATH, preservedAt != null ? preservedAt : "");
            if (disposition.getNoEvidenceReason() != null) {
                metadata.put(AuditMetadata.NO_EVIDENCE_REASON, disposition.getNoEvidenceReason());
            }
            metadata.put(AuditMetadata.DELIVERY_COUNT, deliveryCount);
            metadata.put(AuditMetadata.MAX_DELIVERY_ATTEMPTS, maxDeliveryAttempts);
            metadata.put(AuditMetadata.SOURCE_QUEUE, queueName);
            metadata.put(AuditMetadata.CORRELATION_ID, correlationId != null ? correlationId : "");
            auditPublisher.publishAsync(AuditEvent.builder()
                    .auditEventId(UUID.randomUUID().toString())
                    .eventId(eventId)
                    .originalMqMessageId(messageId)
                    .messageId(messageId)
                    .eventType(AuditEventType.MESSAGE_DISCARDED)
                    .description("Poison message discarded after " + deliveryCount
                            + " delivery attempts (max " + maxDeliveryAttempts + ")")
                    .metadata(metadata)
                    .errorMessage("Exceeded max delivery attempts")
                    .build());
        } catch (RuntimeException e) {
            logger.error("Failed to publish MESSAGE_DISCARDED audit event for messageId={}", messageId, e);
        }
    }

    /** A non-null identity for the disposition of a message we may not be able to identify. */
    private static String discardIdentity(String candidate) {
        return candidate != null && !candidate.isEmpty() ? candidate : "<unidentified-message>";
    }

    /** Same derivation as the orchestrator, so the discard event joins the message's other audit rows. */
    private String discardEventId(String messageId, String payload) {
        try {
            if (messageId != null && !messageId.isEmpty()) {
                return eventIdGenerator.generateEventId(messageId);
            }
            return eventIdGenerator.generateEventId(
                    payload != null && !payload.isEmpty() ? payload : "<empty-mq-message>");
        } catch (RuntimeException e) {
            return null;
        }
    }

    private String quarantineDiscardedPayload(String eventId, String messageId, String payload) {
        if (payload == null) {
            return "<no payload - body unreadable>";
        }
        try {
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
                            AuditMetadata.PIPELINE, PmmOrchestrator.PIPELINE,
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
