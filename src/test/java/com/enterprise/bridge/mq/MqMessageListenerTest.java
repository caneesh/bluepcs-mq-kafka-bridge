package com.enterprise.bridge.mq;

import com.enterprise.bridge.orchestrator.BridgeOrchestrator;
import com.enterprise.bridge.orchestrator.ProcessingResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("MqMessageListener")
class MqMessageListenerTest {

    @Mock
    private BridgeOrchestrator orchestrator;

    @Mock
    private TextMessage textMessage;

    @Mock
    private Destination destination;

    private MqMessageListener listener;

    @BeforeEach
    void setUp() {
        listener = new MqMessageListener(orchestrator);
    }

    @Nested
    @DisplayName("successful processing")
    class SuccessfulProcessing {

        @Test
        @DisplayName("should process message and acknowledge on success")
        void shouldProcessAndAcknowledgeOnSuccess() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-001");
            when(textMessage.getJMSCorrelationID()).thenReturn("CORR-001");
            when(textMessage.getText()).thenReturn("{\"valid\":\"json\"}");
            when(textMessage.getJMSDestination()).thenReturn(destination);
            when(destination.toString()).thenReturn("TEST.QUEUE");
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.success("MSG-001", "/path/file.json", "12345")
            );
            doNothing().when(textMessage).acknowledge();

            listener.onMessage(textMessage);

            verify(orchestrator).process(any());
            verify(textMessage).acknowledge();
        }

        @Test
        @DisplayName("should pass correct message data to orchestrator")
        void shouldPassCorrectMessageDataToOrchestrator() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-002");
            when(textMessage.getJMSCorrelationID()).thenReturn("CORR-002");
            when(textMessage.getText()).thenReturn("{\"test\":\"data\"}");
            when(textMessage.getJMSDestination()).thenReturn(destination);
            when(destination.toString()).thenReturn("INPUT.QUEUE");
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.success("MSG-002", "/path", "123")
            );
            doNothing().when(textMessage).acknowledge();

            ArgumentCaptor<com.enterprise.bridge.model.MqMessage> captor =
                    ArgumentCaptor.forClass(com.enterprise.bridge.model.MqMessage.class);

            listener.onMessage(textMessage);

            verify(orchestrator).process(captor.capture());
            com.enterprise.bridge.model.MqMessage mqMessage = captor.getValue();
            assertThat(mqMessage.getMessageId()).isEqualTo("MSG-002");
            assertThat(mqMessage.getCorrelationId()).isEqualTo("CORR-002");
            assertThat(mqMessage.getPayload()).isEqualTo("{\"test\":\"data\"}");
        }
    }

    @Nested
    @DisplayName("acknowledge behavior")
    class AcknowledgeBehavior {

        @Test
        @DisplayName("should acknowledge on successful processing")
        void shouldAcknowledgeOnSuccess() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-ACK-001");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.success("MSG-ACK-001", "/path", "123")
            );
            doNothing().when(textMessage).acknowledge();

            listener.onMessage(textMessage);

            verify(textMessage).acknowledge();
        }

        @Test
        @DisplayName("should acknowledge duplicate messages")
        void shouldAcknowledgeDuplicateMessages() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-ACK-002");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.duplicate("MSG-ACK-002")
            );
            doNothing().when(textMessage).acknowledge();

            listener.onMessage(textMessage);

            verify(textMessage).acknowledge();
        }

        @Test
        @DisplayName("should not acknowledge on processing failure")
        void shouldNotAcknowledgeOnFailure() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-ACK-003");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.failure("MSG-ACK-003", "ERROR", "Processing failed")
            );

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class);

            verify(textMessage, never()).acknowledge();
        }
    }

    @Nested
    @DisplayName("processing exception handling")
    class ProcessingExceptionHandling {

        @Test
        @DisplayName("should throw MqProcessingException on processing failure")
        void shouldThrowOnProcessingFailure() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-EXC-001");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.failure("MSG-EXC-001", "PARSE_ERROR", "Invalid JSON")
            );

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class)
                    .hasMessageContaining("PARSE_ERROR");
        }

        @Test
        @DisplayName("should include message id in exception")
        void shouldIncludeMessageIdInException() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-EXC-002");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.failure("MSG-EXC-002", "ERROR", "Failed")
            );

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class)
                    .extracting("messageId")
                    .isEqualTo("MSG-EXC-002");
        }

        @Test
        @DisplayName("should propagate orchestrator exception")
        void shouldPropagateOrchestratorException() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-EXC-003");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenThrow(new RuntimeException("Unexpected error"));

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("Unexpected error");
        }
    }

    @Nested
    @DisplayName("JMS exception handling")
    class JmsExceptionHandling {

        @Test
        @DisplayName("should throw MqProcessingException on JMS error getting message ID")
        void shouldThrowOnJmsErrorGettingMessageId() throws JMSException {
            when(textMessage.getJMSMessageID()).thenThrow(new JMSException("Connection lost"));

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class)
                    .hasMessageContaining("JMS error");
        }

        @Test
        @DisplayName("should throw MqProcessingException on JMS error getting text")
        void shouldThrowOnJmsErrorGettingText() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-JMS-002");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenThrow(new JMSException("Message body unavailable"));

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class);
        }

        @Test
        @DisplayName("should throw MqProcessingException on acknowledge failure")
        void shouldThrowOnAcknowledgeFailure() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-JMS-003");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.success("MSG-JMS-003", "/path", "123")
            );
            doThrow(new JMSException("Acknowledge failed")).when(textMessage).acknowledge();

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class)
                    .hasMessageContaining("JMS error");
        }

        @Test
        @DisplayName("should include JMS exception as cause")
        void shouldIncludeJmsExceptionAsCause() throws JMSException {
            JMSException jmsException = new JMSException("Original error");
            when(textMessage.getJMSMessageID()).thenThrow(jmsException);

            assertThatThrownBy(() -> listener.onMessage(textMessage))
                    .isInstanceOf(MqProcessingException.class)
                    .hasCause(jmsException);
        }
    }

    @Nested
    @DisplayName("non-text message handling")
    class NonTextMessageHandling {

        @Test
        @DisplayName("should skip non-text messages")
        void shouldSkipNonTextMessages() {
            Message nonTextMessage = mock(Message.class);

            listener.onMessage(nonTextMessage);

            verify(orchestrator, never()).process(any());
        }

        @Test
        @DisplayName("should not throw for non-text messages")
        void shouldNotThrowForNonTextMessages() {
            Message nonTextMessage = mock(Message.class);

            listener.onMessage(nonTextMessage);
        }
    }

    @Nested
    @DisplayName("queue name extraction")
    class QueueNameExtraction {

        @Test
        @DisplayName("should extract queue name from destination")
        void shouldExtractQueueNameFromDestination() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-Q-001");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(destination);
            when(destination.toString()).thenReturn("queue://BRIDGE.INPUT.QUEUE");
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.success("MSG-Q-001", "/path", "123")
            );
            doNothing().when(textMessage).acknowledge();

            ArgumentCaptor<com.enterprise.bridge.model.MqMessage> captor =
                    ArgumentCaptor.forClass(com.enterprise.bridge.model.MqMessage.class);

            listener.onMessage(textMessage);

            verify(orchestrator).process(captor.capture());
            assertThat(captor.getValue().getSourceQueue()).isEqualTo("queue://BRIDGE.INPUT.QUEUE");
        }

        @Test
        @DisplayName("should use UNKNOWN when destination is null")
        void shouldUseUnknownWhenDestinationNull() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-Q-002");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenReturn(null);
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.success("MSG-Q-002", "/path", "123")
            );
            doNothing().when(textMessage).acknowledge();

            ArgumentCaptor<com.enterprise.bridge.model.MqMessage> captor =
                    ArgumentCaptor.forClass(com.enterprise.bridge.model.MqMessage.class);

            listener.onMessage(textMessage);

            verify(orchestrator).process(captor.capture());
            assertThat(captor.getValue().getSourceQueue()).isEqualTo("UNKNOWN");
        }

        @Test
        @DisplayName("should handle JMS exception when getting destination")
        void shouldHandleJmsExceptionWhenGettingDestination() throws JMSException {
            when(textMessage.getJMSMessageID()).thenReturn("MSG-Q-003");
            when(textMessage.getJMSCorrelationID()).thenReturn(null);
            when(textMessage.getText()).thenReturn("{}");
            when(textMessage.getJMSDestination()).thenThrow(new JMSException("Destination unavailable"));
            when(orchestrator.process(any())).thenReturn(
                    ProcessingResult.success("MSG-Q-003", "/path", "123")
            );
            doNothing().when(textMessage).acknowledge();

            ArgumentCaptor<com.enterprise.bridge.model.MqMessage> captor =
                    ArgumentCaptor.forClass(com.enterprise.bridge.model.MqMessage.class);

            listener.onMessage(textMessage);

            verify(orchestrator).process(captor.capture());
            assertThat(captor.getValue().getSourceQueue()).isEqualTo("UNKNOWN");
        }
    }
}
