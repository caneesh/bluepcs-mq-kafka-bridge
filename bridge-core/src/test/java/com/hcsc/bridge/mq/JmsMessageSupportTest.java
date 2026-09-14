package com.hcsc.bridge.mq;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.jms.JMSException;
import javax.jms.Message;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("JmsMessageSupport")
class JmsMessageSupportTest {

    @Test
    @DisplayName("readJmsTimestamp returns the put time, null for an absent (0) or unreadable header")
    void readJmsTimestamp() throws JMSException {
        Message present = mock(Message.class);
        when(present.getJMSTimestamp()).thenReturn(1_757_739_600_000L);
        Message absent = mock(Message.class);
        when(absent.getJMSTimestamp()).thenReturn(0L);
        Message broken = mock(Message.class);
        when(broken.getJMSTimestamp()).thenThrow(new JMSException("no header"));

        assertThat(JmsMessageSupport.readJmsTimestamp(present)).isEqualTo(Instant.ofEpochMilli(1_757_739_600_000L));
        assertThat(JmsMessageSupport.readJmsTimestamp(absent)).isNull();
        assertThat(JmsMessageSupport.readJmsTimestamp(broken)).isNull();
    }

    @Test
    @DisplayName("getDeliveryCount defaults to 1 when the property is missing or unreadable")
    void deliveryCount() throws JMSException {
        Message missing = mock(Message.class);
        when(missing.propertyExists("JMSXDeliveryCount")).thenReturn(false);
        Message third = mock(Message.class);
        when(third.propertyExists("JMSXDeliveryCount")).thenReturn(true);
        when(third.getIntProperty("JMSXDeliveryCount")).thenReturn(3);
        Message broken = mock(Message.class);
        when(broken.propertyExists("JMSXDeliveryCount")).thenThrow(new JMSException("x"));

        assertThat(JmsMessageSupport.getDeliveryCount(missing)).isEqualTo(1);
        assertThat(JmsMessageSupport.getDeliveryCount(third)).isEqualTo(3);
        assertThat(JmsMessageSupport.getDeliveryCount(broken)).isEqualTo(1);
    }

    @Test
    @DisplayName("redeliveryDelayMs is linear, capped, and zero for a first delivery or a disabled backoff")
    void redeliveryDelay() {
        assertThat(JmsMessageSupport.redeliveryDelayMs(1, 1000, 30000)).isZero();
        assertThat(JmsMessageSupport.redeliveryDelayMs(3, 1000, 30000)).isEqualTo(2000);
        assertThat(JmsMessageSupport.redeliveryDelayMs(100, 1000, 30000)).isEqualTo(30000);
        assertThat(JmsMessageSupport.redeliveryDelayMs(5, 0, 30000)).isZero();
    }

    @Test
    @DisplayName("sanitizeForLog strips line breaks so a header cannot forge log lines")
    void sanitize() {
        assertThat(JmsMessageSupport.sanitizeForLog("a\r\nb")).isEqualTo("a__b");
        assertThat(JmsMessageSupport.sanitizeForLog(null)).isNull();
    }
}
