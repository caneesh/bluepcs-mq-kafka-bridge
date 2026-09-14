package com.hcsc.bridge.mq;

import com.hcsc.bridge.audit.AuditEvent;
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
                // Lenient ack: an ack failure while discarding must not trigger redelivery
                // of a message we cannot process anyway.
                acknowledgeQuietly(message, "unsupported-message-type discard");
                return;
            }

            TextMessage textMessage = (TextMessage) message;
            // Headers/properties first, each tolerating failure: the poison guard must be
            // evaluable even when individual header reads (or the body read below) throw
            // on every delivery — a header JMSException before the guard would loop the
            // message forever with the guard never consulted.
            messageId = readHeaderQuietly(textMessage::getJMSMessageID, "JMSMessageID");
            String correlationId = readHeaderQuietly(textMessage::getJMSCorrelationID, "JMSCorrelationID");
            int deliveryCount = getDeliveryCount(message);
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

            if (result.isSuccessful()) {
                acknowledgeProcessedMessage(message, messageId, result.getEventId());
            } else if (result.isQuarantined()) {
                // Permanent failure, payload preserved in the HDFS quarantine directory —
                // acknowledge so the bad message cannot block the queue. Ack failure is
                // tolerable: redelivery re-quarantines idempotently to the same file.
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
        String preservedAt = quarantineDiscardedPayload(messageId, payload);
        logger.error("POISON MESSAGE: discarding after {} delivery attempts "
                        + "(bridge.mq.max-delivery-attempts={}): messageId={}, correlationId={}, queue={}. "
                        + "Payload preserved at: {}",
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
                            "deliveryCount", deliveryCount,
                            "maxDeliveryAttempts", maxDeliveryAttempts,
                            "sourceQueue", queueName,
                            "correlationId", correlationId != null ? correlationId : ""))
                    .errorMessage("Exceeded max delivery attempts")
                    .build());
        } catch (RuntimeException e) {
            // Never let audit failure prevent the acknowledge below.
            logger.error("Failed to publish MESSAGE_DISCARDED audit event for messageId={}", messageId, e);
        }

        acknowledgeQuietly(message, "poison-message discard (messageId=" + messageId + ")");
    }

    /**
     * Preserves a to-be-discarded payload in the HDFS quarantine directory and returns its path.
     * Falls back to a masked, truncated log dump only when the quarantine write fails — losing
     * the payload entirely would be worse than a bounded, masked log line.
     */
    private String quarantineDiscardedPayload(String messageId, String payload) {
        if (payload == null) {
            return "<no payload - body unreadable>";
        }
        try {
            String eventId = eventIdGenerator.generateEventId(
                    messageId != null && !messageId.isEmpty() ? messageId : payload);
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
            return "<quarantine write failed - see log>";
        }
    }

    /**
     * Backoff for redelivered messages — see {@code redeliveryBackoffMs}. Sleeping on the listener
     * thread is deliberate: with concurrency=1 the point is to slow the redelivery spin of a
     * persistently failing message, trading queue latency for not melting logs/audit.
     */
    private void throttleRedelivery(int deliveryCount, String messageId) {
        if (redeliveryBackoffMs <= 0 || deliveryCount <= 1) {
            return;
        }
        long delay = Math.min((deliveryCount - 1) * redeliveryBackoffMs, redeliveryBackoffMaxMs);
        logger.info("Redelivery {} of message {}; backing off {} ms before processing",
                deliveryCount, messageId, delay);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Redelivery backoff interrupted for message {}; continuing", messageId);
        }
    }

    /** Strips CR/LF from message-derived values so a crafted header cannot forge log lines. */
    private static String sanitizeForLog(String value) {
        return value == null ? null : value.replaceAll("[\\r\\n]", "_");
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

    @FunctionalInterface
    private interface HeaderReader {
        String read() throws JMSException;
    }

    /**
     * Header reads must never bypass the poison guard: a header that throws on every
     * delivery (like a converted body can) would otherwise redeliver the message forever
     * with the guard never consulted. WARN, not DEBUG — a missing JMSMessageID also
     * changes eventId derivation (the payload-hash fallback takes over).
     */
    private String readHeaderQuietly(HeaderReader reader, String headerName) {
        try {
            return reader.read();
        } catch (JMSException e) {
            logger.warn("Could not read {} header; continuing without it: {}", headerName, e.getMessage());
            return null;
        }
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
            // WARN, not DEBUG: if this fails persistently the poison guard is silently
            // inoperative, and the on-call engineer needs to see why a known-poison
            // message is not being discarded.
            logger.warn("Could not read {} from message; poison guard sees delivery count 1",
                    JMSX_DELIVERY_COUNT, e);
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
