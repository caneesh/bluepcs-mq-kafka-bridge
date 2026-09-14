package com.hcsc.bridge.pmm.mq;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("JmsBodyDecoder")
class JmsBodyDecoderTest {

    private static BytesMessage bytesMessage(byte[] bytes, String charsetProperty) throws JMSException {
        BytesMessage message = mock(BytesMessage.class);
        when(message.getBodyLength()).thenReturn((long) bytes.length);
        when(message.readBytes(any(byte[].class))).thenAnswer(inv -> {
            byte[] target = inv.getArgument(0);
            System.arraycopy(bytes, 0, target, 0, bytes.length);
            return bytes.length;
        });
        when(message.getStringProperty(JmsBodyDecoder.JMS_IBM_CHARACTER_SET)).thenReturn(charsetProperty);
        return message;
    }

    @Test
    @DisplayName("passes a TextMessage body through unchanged")
    void textMessage() throws JMSException {
        TextMessage message = mock(TextMessage.class);
        when(message.getText()).thenReturn("<a/>");

        assertThat(JmsBodyDecoder.decode(message)).isEqualTo("<a/>");
        assertThat(JmsBodyDecoder.isSupported(message)).isTrue();
    }

    @Test
    @DisplayName("decodes a BytesMessage with the charset the queue manager reports")
    void bytesMessageWithCharset() throws JMSException {
        String xml = "<a>ü</a>";
        BytesMessage message = bytesMessage(xml.getBytes(Charset.forName("ISO-8859-1")), "ISO-8859-1");

        assertThat(JmsBodyDecoder.decode(message)).isEqualTo(xml);
    }

    @Test
    @DisplayName("falls back to UTF-8 for a missing or unsupported charset property")
    void charsetFallback() throws JMSException {
        String xml = "<a>ü</a>";
        assertThat(JmsBodyDecoder.decode(bytesMessage(xml.getBytes(StandardCharsets.UTF_8), null))).isEqualTo(xml);
        assertThat(JmsBodyDecoder.decode(bytesMessage(xml.getBytes(StandardCharsets.UTF_8), "NOPE-1"))).isEqualTo(xml);
    }

    @Test
    @DisplayName("strips a leading byte-order mark")
    void stripsBom() throws JMSException {
        byte[] withBom = ("\uFEFF<a/>").getBytes(StandardCharsets.UTF_8);

        assertThat(JmsBodyDecoder.decode(bytesMessage(withBom, "UTF-8"))).isEqualTo("<a/>");
    }

    @Test
    @DisplayName("strips a leading byte-order mark from a TextMessage too")
    void stripsBomFromText() throws JMSException {
        TextMessage message = mock(TextMessage.class);
        when(message.getText()).thenReturn("\uFEFF<?xml version=\"1.0\"?><a/>");

        assertThat(JmsBodyDecoder.decode(message)).isEqualTo("<?xml version=\"1.0\"?><a/>");
    }

    @Test
    @DisplayName("decodes an empty BytesMessage as an empty string instead of failing the read")
    void emptyBytesMessage() throws JMSException {
        BytesMessage message = mock(BytesMessage.class);
        when(message.getBodyLength()).thenReturn(0L);

        assertThat(JmsBodyDecoder.decode(message)).isEmpty();
    }

    @Test
    @DisplayName("refuses a body above the configured cap before allocating it")
    void refusesOversizedBody() throws JMSException {
        BytesMessage message = mock(BytesMessage.class);
        when(message.getBodyLength()).thenReturn(1024L);

        assertThatThrownBy(() -> JmsBodyDecoder.decode(message, 512))
                .isInstanceOf(JMSException.class).hasMessageContaining("too large");
    }

    @Test
    @DisplayName("rejects other message types")
    void unsupported() {
        Message message = mock(Message.class);

        assertThat(JmsBodyDecoder.isSupported(message)).isFalse();
        assertThatThrownBy(() -> JmsBodyDecoder.decode(message)).isInstanceOf(JMSException.class);
    }
}
