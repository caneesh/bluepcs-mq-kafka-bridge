package com.hcsc.bridge.pmm.mq;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.hdfs.SafeHdfsWriter;
import com.hcsc.bridge.model.HdfsWriteResult;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.mq.MqProcessingException;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import com.hcsc.bridge.pmm.hdfs.WindowedPathResolver;
import com.hcsc.bridge.pmm.orchestrator.PmmOrchestrator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("PmmMqMessageListener")
class PmmMqMessageListenerTest {

    private static final String XML = "<PmmMessage/>";
    private static final long PUT_MILLIS = Instant.parse("2026-09-13T05:00:00Z").toEpochMilli();

    @Mock private PmmOrchestrator orchestrator;
    @Mock private AuditPublisher auditPublisher;
    @Mock private SafeHdfsWriter hdfsWriter;
    @Mock private WindowedPathResolver pathResolver;
    @Mock private EventIdGenerator eventIdGenerator;
    @Mock private TextMessage textMessage;

    private PmmMqMessageListener listener;

    @BeforeEach
    void setUp() throws JMSException {
        listener = new PmmMqMessageListener(orchestrator, auditPublisher, hdfsWriter, pathResolver, eventIdGenerator);
        ReflectionTestUtils.setField(listener, "redeliveryBackoffMs", 0L);
        ReflectionTestUtils.setField(listener, "maxMessageBytes", JmsBodyDecoder.DEFAULT_MAX_BODY_BYTES);
        // lenient: the message-type tests drive a BytesMessage / plain Message instead
        lenient().when(textMessage.getJMSMessageID()).thenReturn("MSG-1");
        lenient().when(textMessage.getText()).thenReturn(XML);
        lenient().when(textMessage.getJMSTimestamp()).thenReturn(PUT_MILLIS);
    }

    @Nested
    @DisplayName("dispositions")
    class Dispositions {

        @Test
        @DisplayName("acknowledges on success and passes the JMS put time through as the anchor")
        void success() throws JMSException {
            when(orchestrator.process(any())).thenReturn(ProcessingResult.success("evt", "/p"));

            listener.onMessage(textMessage);

            ArgumentCaptor<MqMessage> captor = ArgumentCaptor.forClass(MqMessage.class);
            verify(orchestrator).process(captor.capture());
            assertThat(captor.getValue().getMessageId()).isEqualTo("MSG-1");
            assertThat(captor.getValue().getPayload()).isEqualTo(XML);
            assertThat(captor.getValue().getJmsTimestamp()).isEqualTo(Instant.ofEpochMilli(PUT_MILLIS));
            verify(textMessage).acknowledge();
        }

        @Test
        @DisplayName("acknowledges a quarantined message without throwing")
        void quarantined() throws JMSException {
            when(orchestrator.process(any())).thenReturn(ProcessingResult.quarantined("evt", "/q", "PARSE_ERROR", "bad"));

            listener.onMessage(textMessage);

            verify(textMessage).acknowledge();
        }

        @Test
        @DisplayName("throws MqProcessingException and does not acknowledge on failure")
        void failure() throws JMSException {
            when(orchestrator.process(any())).thenReturn(ProcessingResult.failure("evt", "API_ERROR", "503"));

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class).hasMessageContaining("API_ERROR");
            verify(textMessage, never()).acknowledge();
        }

        @Test
        @DisplayName("does not throw when the acknowledge fails after a successful process")
        void ackFailureAfterSuccess() throws JMSException {
            when(orchestrator.process(any())).thenReturn(ProcessingResult.success("evt", "/p"));
            doThrow(new JMSException("ack failed")).when(textMessage).acknowledge();

            listener.onMessage(textMessage);
        }

        @Test
        @DisplayName("throws MqProcessingException when the body cannot be read and the guard is not armed")
        void bodyUnreadable() throws JMSException {
            when(textMessage.getText()).thenThrow(new JMSException("CCSID conversion"));

            assertThatThrownBy(() -> listener.onMessage(textMessage)).isInstanceOf(MqProcessingException.class);
            verify(orchestrator, never()).process(any());
        }
    }

    @Nested
    @DisplayName("message types")
    class MessageTypes {

        @Test
        @DisplayName("decodes a BytesMessage and processes it like text")
        void bytesMessage() throws JMSException {
            BytesMessage bytes = mock(BytesMessage.class);
            byte[] body = XML.getBytes(StandardCharsets.UTF_8);
            when(bytes.getJMSMessageID()).thenReturn("MSG-2");
            when(bytes.getBodyLength()).thenReturn((long) body.length);
            when(bytes.readBytes(any(byte[].class))).thenAnswer(inv -> {
                System.arraycopy(body, 0, inv.getArgument(0), 0, body.length);
                return body.length;
            });
            when(bytes.getStringProperty(JmsBodyDecoder.JMS_IBM_CHARACTER_SET)).thenReturn("UTF-8");
            when(orchestrator.process(any())).thenReturn(ProcessingResult.success("evt", "/p"));

            listener.onMessage(bytes);

            ArgumentCaptor<MqMessage> captor = ArgumentCaptor.forClass(MqMessage.class);
            verify(orchestrator).process(captor.capture());
            assertThat(captor.getValue().getPayload()).isEqualTo(XML);
            verify(bytes).acknowledge();
        }

        @Test
        @DisplayName("discards and acknowledges an unsupported message type with a MESSAGE_DISCARDED audit")
        void unsupported() throws JMSException {
            Message other = mock(Message.class);

            listener.onMessage(other);

            verify(orchestrator, never()).process(any());
            verify(other).acknowledge();
            ArgumentCaptor<AuditEvent> audit = ArgumentCaptor.forClass(AuditEvent.class);
            verify(auditPublisher).publishAsync(audit.capture());
            assertThat(audit.getValue().getEventType()).isEqualTo(AuditEventType.MESSAGE_DISCARDED);
            assertThat(audit.getValue().getMetadata()).containsEntry("pipeline", "pmm");
        }
    }

    @Nested
    @DisplayName("poison guard")
    class PoisonGuard {

        @Test
        @DisplayName("quarantines to errors/<eventId>.xml, audits MESSAGE_DISCARDED and acknowledges past the threshold")
        void discardsPoison() throws JMSException {
            ReflectionTestUtils.setField(listener, "maxDeliveryAttempts", 2);
            when(textMessage.propertyExists("JMSXDeliveryCount")).thenReturn(true);
            when(textMessage.getIntProperty("JMSXDeliveryCount")).thenReturn(3);
            when(eventIdGenerator.generateEventId("MSG-1")).thenReturn("evt");
            when(pathResolver.quarantinePath("evt")).thenReturn("/data/pmm/errors/evt.xml");
            when(hdfsWriter.write("/data/pmm/errors/evt.xml", XML, "MSG-1"))
                    .thenReturn(HdfsWriteResult.success("/data/pmm/errors/evt.xml", "s", 1));

            listener.onMessage(textMessage);

            verify(orchestrator, never()).process(any());
            verify(textMessage).acknowledge();
            ArgumentCaptor<AuditEvent> audit = ArgumentCaptor.forClass(AuditEvent.class);
            verify(auditPublisher).publishAsync(audit.capture());
            assertThat(audit.getValue().getEventType()).isEqualTo(AuditEventType.MESSAGE_DISCARDED);
            assertThat(audit.getValue().getMetadata()).containsEntry("deliveryCount", 3).containsEntry("pipeline", "pmm");
        }

        @Test
        @DisplayName("does not acknowledge a poison message whose quarantine write failed")
        void quarantineFailureLeavesMessageOnQueue() throws JMSException {
            ReflectionTestUtils.setField(listener, "maxDeliveryAttempts", 2);
            when(textMessage.propertyExists("JMSXDeliveryCount")).thenReturn(true);
            when(textMessage.getIntProperty("JMSXDeliveryCount")).thenReturn(3);
            when(eventIdGenerator.generateEventId("MSG-1")).thenReturn("evt");
            when(pathResolver.quarantinePath("evt")).thenReturn("/data/pmm/errors/evt.xml");
            when(hdfsWriter.write("/data/pmm/errors/evt.xml", XML, "MSG-1"))
                    .thenThrow(new com.hcsc.bridge.hdfs.HdfsWriteException("hdfs down", "/data/pmm/errors/evt.xml", "MSG-1"));

            assertThatThrownBy(() -> listener.onMessage(textMessage)).isInstanceOf(MqProcessingException.class);

            verify(textMessage, never()).acknowledge();
            verify(orchestrator, never()).process(any());
        }

        @Test
        @DisplayName("processes normally below the threshold")
        void belowThreshold() throws JMSException {
            ReflectionTestUtils.setField(listener, "maxDeliveryAttempts", 5);
            when(textMessage.propertyExists("JMSXDeliveryCount")).thenReturn(true);
            when(textMessage.getIntProperty("JMSXDeliveryCount")).thenReturn(2);
            when(orchestrator.process(any())).thenReturn(ProcessingResult.success("evt", "/p"));

            listener.onMessage(textMessage);

            verify(orchestrator).process(any());
            verify(hdfsWriter, never()).write(anyString(), anyString(), any());
        }
    }
}
