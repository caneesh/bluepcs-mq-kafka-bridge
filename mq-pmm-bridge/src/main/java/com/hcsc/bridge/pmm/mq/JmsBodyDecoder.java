package com.hcsc.bridge.pmm.mq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Reads an XML body from either a {@link TextMessage} (MQFMT_STRING puts) or a
 * {@link BytesMessage} (MQFMT_NONE puts — common for XML publishers). Bytes are decoded
 * with the charset the queue manager reports in {@code JMS_IBM_Character_Set}, falling
 * back to UTF-8; a leading BOM is stripped so the XML parser sees the declaration first.
 */
public final class JmsBodyDecoder {

    private static final Logger logger = LoggerFactory.getLogger(JmsBodyDecoder.class);
    static final String JMS_IBM_CHARACTER_SET = "JMS_IBM_Character_Set";

    private JmsBodyDecoder() {
    }

    public static boolean isSupported(Message message) {
        return message instanceof TextMessage || message instanceof BytesMessage;
    }

    /** Default body cap: PMM messages are small; anything near this is a broken publisher. */
    public static final long DEFAULT_MAX_BODY_BYTES = 64L * 1024 * 1024;

    public static String decode(Message message) throws JMSException {
        return decode(message, DEFAULT_MAX_BODY_BYTES);
    }

    /**
     * @param maxBodyBytes upper bound for a BytesMessage body; larger bodies are refused
     *                     before any allocation (a JMSException, so the listener's poison
     *                     guard can still discard the message)
     */
    public static String decode(Message message, long maxBodyBytes) throws JMSException {
        if (message instanceof TextMessage) {
            // Same producer habit as a BOM-prefixed BytesMessage: an MQFMT_STRING put from a
            // UTF-8-with-BOM file. The parser rejects U+FEFF before the declaration.
            return stripBom(((TextMessage) message).getText());
        }
        if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            long length = bytesMessage.getBodyLength();
            if (length <= 0) {
                // Treated like an empty TextMessage: the extractor quarantines it
                return "";
            }
            if (length > maxBodyBytes || length > Integer.MAX_VALUE - 8) {
                throw new JMSException("BytesMessage body too large to decode: " + length
                        + " bytes (max " + maxBodyBytes + ")");
            }
            byte[] buffer = new byte[(int) length];
            bytesMessage.reset();
            int read = bytesMessage.readBytes(buffer);
            if (read != length) {
                throw new JMSException("Short read of BytesMessage body: expected " + length + " bytes, got " + read);
            }
            return stripBom(new String(buffer, charsetOf(message)));
        }
        throw new JMSException("Unsupported message type: " + message.getClass().getName());
    }

    static Charset charsetOf(Message message) {
        String name = null;
        try {
            name = message.getStringProperty(JMS_IBM_CHARACTER_SET);
        } catch (JMSException e) {
            logger.debug("Could not read {}; decoding body as UTF-8", JMS_IBM_CHARACTER_SET);
        }
        if (name == null || name.trim().isEmpty()) {
            return StandardCharsets.UTF_8;
        }
        try {
            return Charset.forName(name.trim());
        } catch (IllegalArgumentException e) {
            logger.warn("Unsupported {} '{}' on message; decoding body as UTF-8", JMS_IBM_CHARACTER_SET, name);
            return StandardCharsets.UTF_8;
        }
    }

    static String stripBom(String text) {
        return text != null && !text.isEmpty() && text.charAt(0) == '\uFEFF' ? text.substring(1) : text;
    }
}
