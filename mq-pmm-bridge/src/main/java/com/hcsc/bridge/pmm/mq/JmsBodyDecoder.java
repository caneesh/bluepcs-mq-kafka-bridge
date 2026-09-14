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

    public static String decode(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            return ((TextMessage) message).getText();
        }
        if (message instanceof BytesMessage) {
            BytesMessage bytesMessage = (BytesMessage) message;
            long length = bytesMessage.getBodyLength();
            if (length > Integer.MAX_VALUE - 8) {
                throw new JMSException("BytesMessage body too large to decode: " + length + " bytes");
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
        return text != null && !text.isEmpty() && text.charAt(0) == '﻿' ? text.substring(1) : text;
    }
}
