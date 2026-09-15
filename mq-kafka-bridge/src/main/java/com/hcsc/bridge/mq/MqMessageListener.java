package com.hcsc.bridge.mq;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditMetadata;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.core.SecretMaskingUtil;
import com.hcsc.bridge.hdfs.HdfsSafePayloadWriter;
import com.hcsc.bridge.model.HdfsWriteResult;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.orchestrator.BridgeOrchestrator;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.stereotype.Component;

import static com.hcsc.bridge.mq.JmsMessageSupport.extractQueueName;
import static com.hcsc.bridge.mq.JmsMessageSupport.readHeaderQuietly;
import static com.hcsc.bridge.mq.JmsMessageSupport.sanitizeForLog;

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

    private final BridgeOrchestrator orchestrator;
    private final AuditPublisher auditPublisher;
    private final HdfsSafePayloadWriter payloadWriter;
    private final EventIdGenerator eventIdGenerator;

    @Value("${bridge.mq.log-payload:false}")
    private boolean logPayload;

    /**
     * Poison-message guard: if a message's JMSXDeliveryCount exceeds this value, the payload is
     * quarantined to HDFS, audited as MESSAGE_DISCARDED, and the message is acknowledged to
     * unblock the queue. 0 (the default) disables the guard — rely on the queue manager's
     * backout threshold (BOTHRESH/BOQNAME) instead, which preserves the message on a backout
     * queue. Only enable this when broker-side backout is not available.
     */
    @Value("${bridge.mq.max-delivery-attempts:0}")
    private int maxDeliveryAttempts;

    /**
     * Redelivery throttle: sleep (deliveryCount-1) * this many ms (capped below) before
     * processing a redelivered message. Without it a persistent failure (HDFS outage, permanent
     * enrichment 4xx with the guard disabled) spins the same message at tens of attempts per
     * second, flooding logs and the audit topic. 0 disables.
     */
    @Value("${bridge.mq.redelivery-backoff-ms:1000}")
    private long redeliveryBackoffMs;

    @Value("${bridge.mq.redelivery-backoff-max-ms:30000}")
    private long redeliveryBackoffMaxMs;

    public MqMessageListener(BridgeOrchestrator orchestrator, AuditPublisher auditPublisher,
                             HdfsSafePayloadWriter payloadWriter, EventIdGenerator eventIdGenerator) {
        this.orchestrator = orchestrator;
        this.auditPublisher = auditPublisher;
        this.payloadWriter = payloadWriter;
        this.eventIdGenerator = eventIdGenerator;
    }

    @JmsListener(destination = "${bridge.mq.queue:BRIDGE.INPUT.QUEUE}")
    public void onMessage(Message message) {
        String messageId = null;
        try {
            if (!(message instanceof TextMessage)) {
                logger.error("Received unsupported message type {}, acknowledging to discard",
                        message.getClass().getName());
                auditDiscardedNonTextMessage(message);
                // There is no payload to preserve for a type this bridge cannot read, which the
                // disposition states explicitly rather than leaving as an unexplained ack.
                JmsMessageSupport.settle(message, ProcessingResult.discardedWithoutPayload(
                        discardIdentity(readHeaderQuietly(message::getJMSMessageID, "JMSMessageID")),
                        "unsupported message type " + message.getClass().getName(),
                        "UNSUPPORTED_TYPE", "Only TextMessage is supported"), messageId);
                return;
            }

            TextMessage textMessage = (TextMessage) message;
            // Headers/properties first, each tolerating failure: the poison guard must be
            // evaluable even when individual header reads (or the body read below) throw
            // on every delivery — a header JMSException before the guard would loop the
            // message forever with the guard never consulted.
            messageId = readHeaderQuietly(textMessage::getJMSMessageID, "JMSMessageID");
            String correlationId = readHeaderQuietly(textMessage::getJMSCorrelationID, "JMSCorrelationID");
            int deliveryCount = JmsMessageSupport.getDeliveryCount(message);
            String queueName = extractQueueName(message);
            boolean poison = maxDeliveryAttempts > 0 && deliveryCount > maxDeliveryAttempts;

            String payload;
            try {
                payload = textMessage.getText();
            } catch (JMSException bodyReadFailure) {
                // Typically a CCSID/format conversion error — it throws on EVERY delivery,
                // so if the guard is armed and tripped this is the only place it can act;
                // rethrowing here forever would bypass the guard entirely.
                if (poison) {
                    logger.error("Message body unreadable on delivery {} (max {}): messageId={} — discarding",
                            deliveryCount, maxDeliveryAttempts, messageId, bodyReadFailure);
                    discardPoisonMessage(message, messageId, correlationId, null, queueName, deliveryCount);
                    return;
                }
                throw bodyReadFailure;
            }

            // One structured line, not a banner: at production rates a multi-line block
            // per message dominates the log. Size is in CHARS (Java string length) —
            // the HDFS write logs UTF-8 bytes, which differ for non-ASCII payloads.
            logger.info("=== INCOMING MQ MESSAGE === JMSMessageID={} JMSCorrelationID={} queue={} payloadChars={}",
                    messageId, sanitizeForLog(correlationId), sanitizeForLog(queueName),
                    payload != null ? payload.length() : 0);
            if (deliveryCount > 1) {
                logger.warn("Redelivery: JMSXDeliveryCount={} messageId={}", deliveryCount, messageId);
            }

            if (logPayload && payload != null) {
                String masked = SecretMaskingUtil.maskSecrets(payload);
                if (masked.length() > MAX_LOGGED_PAYLOAD_CHARS) {
                    masked = masked.substring(0, MAX_LOGGED_PAYLOAD_CHARS)
                            + "... (truncated, " + payload.length() + " chars total)";
                }
                logger.info("Payload for messageId={}:\n{}", messageId, masked);
            }

            if (poison) {
                discardPoisonMessage(message, messageId, correlationId, payload, queueName, deliveryCount);
                return;
            }

            throttleRedelivery(deliveryCount, messageId);

            MqMessage mqMessage = new MqMessage(
                    messageId,
                    correlationId,
                    payload,
                    Instant.now(),
                    queueName
            );

            ProcessingResult result = orchestrator.process(mqMessage);

            // One rule for every outcome, shared with the PMM bridge: acknowledge only what
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
     * Discards a message that has exceeded {@code bridge.mq.max-delivery-attempts}: preserves the
     * payload in the HDFS quarantine directory (durable, access-controlled — payloads may contain
     * PHI and must not land in application logs), publishes a MESSAGE_DISCARDED audit event, and
     * acknowledges. Every step here is best-effort — the acknowledge MUST be reached, otherwise
     * the poison message keeps blocking the queue.
     */
    private void discardPoisonMessage(Message message, String messageId, String correlationId,
                                      String payload, String queueName, int deliveryCount) {
        String eventId = discardEventId(messageId, payload);
        String preservedAt = quarantineDiscardedPayload(eventId, messageId, payload);
        ProcessingResult disposition = poisonDisposition(eventId, messageId, payload, preservedAt, deliveryCount);

        if (disposition.isAcknowledgeable()) {
            logger.error("POISON MESSAGE: discarding after {} delivery attempts "
                            + "(bridge.mq.max-delivery-attempts={}): messageId={}, correlationId={}, queue={}. "
                            + "Payload preserved at: {}",
                    deliveryCount, maxDeliveryAttempts, messageId, sanitizeForLog(correlationId),
                    sanitizeForLog(queueName), preservedAt != null ? preservedAt : disposition.getNoEvidenceReason());
            auditPoisonDiscard(disposition, eventId, messageId, correlationId, queueName, deliveryCount);
        } else {
            logger.error("POISON MESSAGE: quarantine write failed for messageId={} on delivery {} — NOT "
                    + "acknowledging (no durable copy); will retry the quarantine on redelivery",
                    messageId, deliveryCount);
        }
        JmsMessageSupport.settle(message, disposition, messageId);
    }

    /**
     * The disposition of a poison message. Acknowledging is only safe once the payload is
     * durably preserved, or when there was no readable payload to preserve at all - the guard
     * exists to unblock the queue without losing the message, so a failed quarantine write
     * leaves it on the queue for the next delivery to retry.
     */
    private ProcessingResult poisonDisposition(String eventId, String messageId, String payload,
                                               String preservedAt, int deliveryCount) {
        String identity = discardIdentity(eventId != null ? eventId : messageId);
        String detail = "Exceeded max delivery attempts (" + deliveryCount + ")";
        if (payload == null) {
            return ProcessingResult.discardedWithoutPayload(identity,
                    "message body was unreadable on every delivery", "POISON", detail);
        }
        if (preservedAt == null) {
            return ProcessingResult.failure(identity, "POISON_QUARANTINE_FAILED",
                    "quarantine write failed; message left on the queue");
        }
        return ProcessingResult.discarded(identity, preservedAt, "POISON", detail);
    }

    private void auditPoisonDiscard(ProcessingResult disposition, String eventId, String messageId,
                                    String correlationId, String queueName, int deliveryCount) {
        try {
            Map<String, Object> metadata = new java.util.HashMap<>();
            // eventId + errorCode make this the message's TERMINAL event for the gap and balance
            // checks (its earlier MESSAGE_RECEIVED would otherwise read as stuck forever);
            // hdfsPath says where the payload was preserved, or why nothing could be.
            metadata.put(AuditMetadata.ERROR_CODE, "POISON");
            metadata.put(AuditMetadata.HDFS_PATH,
                    disposition.getHdfsPath() != null ? disposition.getHdfsPath() : "");
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
            // Never let audit failure prevent the acknowledge that unblocks the queue.
            logger.error("Failed to publish MESSAGE_DISCARDED audit event for messageId={}", messageId, e);
        }
    }

    /** A non-null identity for the disposition of a message we may not be able to identify. */
    private static String discardIdentity(String candidate) {
        return candidate != null && !candidate.isEmpty() ? candidate : "<unidentified-message>";
    }

    /**
     * Preserves a to-be-discarded payload in the HDFS quarantine directory and returns its path,
     * or null when the write failed (a masked, truncated log copy is emitted for forensics, but
     * it is NOT a durable copy — the caller must not acknowledge on null).
     */
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
            HdfsWriteResult result = payloadWriter.writeQuarantine(eventId, payload, messageId);
            return result.getHdfsPath();
        } catch (RuntimeException e) {
            logger.error("Quarantine write failed for poison message {}; falling back to log preservation",
                    messageId, e);
            String masked = SecretMaskingUtil.maskSecrets(payload);
            if (masked.length() > MAX_LOGGED_PAYLOAD_CHARS) {
                masked = masked.substring(0, MAX_LOGGED_PAYLOAD_CHARS)
                        + "... (truncated, " + payload.length() + " chars total)";
            }
            logger.error("Last (masked, truncated) copy of discarded payload for messageId={}:\n{}",
                    messageId, masked);
            return null;
        }
    }

    /**
     * Backoff for redelivered messages — see {@code redeliveryBackoffMs}. Sleeping on the listener
     * thread is deliberate: with concurrency=1 the point is to slow the redelivery spin of a
     * persistently failing message, trading queue latency for not melting logs/audit.
     */
    private void throttleRedelivery(int deliveryCount, String messageId) {
        long delay = JmsMessageSupport.redeliveryDelayMs(deliveryCount, redeliveryBackoffMs, redeliveryBackoffMaxMs);
        if (delay <= 0) {
            return;
        }
        logger.info("Redelivery {} of message {}; backing off {} ms before processing",
                deliveryCount, messageId, delay);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Redelivery backoff interrupted for message {}; continuing", messageId);
        }
    }

    /**
     * Publishes a MESSAGE_DISCARDED audit for an unsupported (non-text) message so the discard
     * leaves a trail beyond the log line. Best-effort on every step: the message ID may be
     * unreadable and audit publishing may fail — neither must prevent the discard ack.
     */
    private void auditDiscardedNonTextMessage(Message message) {
        String discardedMessageId = null;
        try {
            discardedMessageId = message.getJMSMessageID();
        } catch (JMSException e) {
            logger.debug("Could not read JMSMessageID from unsupported message", e);
        }
        try {
            auditPublisher.publishAsync(AuditEvent.builder()
                    .auditEventId(UUID.randomUUID().toString())
                    .originalMqMessageId(discardedMessageId)
                    .messageId(discardedMessageId)
                    .eventType(AuditEventType.MESSAGE_DISCARDED)
                    .description("Unsupported message type discarded: " + message.getClass().getName())
                    .metadata(Map.of(
                            "messageClass", message.getClass().getName(),
                            "sourceQueue", extractQueueName(message)))
                    .errorMessage("Only TextMessage is supported")
                    .build());
        } catch (RuntimeException e) {
            logger.error("Failed to publish MESSAGE_DISCARDED audit for unsupported message type", e);
        }
    }

}
