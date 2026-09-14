package com.hcsc.bridge.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Instant;

/**
 * Header/ack helpers shared by the bridge listeners. Every method is deliberately
 * lenient: a header that throws on every delivery must never bypass a listener's
 * poison-message guard or block a discard acknowledge.
 */
public final class JmsMessageSupport {

    private static final Logger logger = LoggerFactory.getLogger(JmsMessageSupport.class);
    private static final String JMSX_DELIVERY_COUNT = "JMSXDeliveryCount";

    private JmsMessageSupport() {
    }

    @FunctionalInterface
    public interface HeaderReader {
        String read() throws JMSException;
    }

    /**
     * Header reads must never bypass the poison guard: a header that throws on every
     * delivery (like a converted body can) would otherwise redeliver the message forever
     * with the guard never consulted. WARN, not DEBUG — a missing JMSMessageID also
     * changes eventId derivation (the payload-hash fallback takes over).
     */
    public static String readHeaderQuietly(HeaderReader reader, String headerName) {
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
    public static int getDeliveryCount(Message message) {
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
     * The broker's put time (JMSTimestamp), or null when the header is absent (0) or
     * unreadable. Stable across redeliveries, unlike a receive-time stamp.
     */
    public static Instant readJmsTimestamp(Message message) {
        try {
            long millis = message.getJMSTimestamp();
            return millis > 0 ? Instant.ofEpochMilli(millis) : null;
        } catch (JMSException e) {
            logger.warn("Could not read JMSTimestamp header; continuing without it: {}", e.getMessage());
            return null;
        }
    }

    public static String extractQueueName(Message message) {
        try {
            if (message.getJMSDestination() != null) {
                return message.getJMSDestination().toString();
            }
        } catch (JMSException e) {
            logger.debug("Could not extract queue name from message", e);
        }
        return "UNKNOWN";
    }

    /**
     * Acknowledges a message on a discard path. Failures are logged, never thrown: throwing here
     * would trigger redelivery of a message we have already decided to drop.
     */
    public static void acknowledgeQuietly(Message message, String context) {
        try {
            message.acknowledge();
        } catch (JMSException e) {
            logger.error("Acknowledge failed during {}; the broker may redeliver this message", context, e);
        }
    }

    /**
     * Linear redelivery backoff: {@code (deliveryCount - 1) * backoffMs}, capped at
     * {@code maxMs}; 0 for a first delivery or when the backoff is disabled.
     */
    public static long redeliveryDelayMs(int deliveryCount, long backoffMs, long maxMs) {
        if (backoffMs <= 0 || deliveryCount <= 1) {
            return 0;
        }
        return Math.min((deliveryCount - 1) * backoffMs, maxMs);
    }

    /** Strips CR/LF from message-derived values so a crafted header cannot forge log lines. */
    public static String sanitizeForLog(String value) {
        return value == null ? null : value.replaceAll("[\\r\\n]", "_");
    }
}
