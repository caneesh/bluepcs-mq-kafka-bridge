package com.hcsc.bridge.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.test.util.ReflectionTestUtils;

import javax.jms.Session;

import static org.assertj.core.api.Assertions.assertThat;

class MqConfigurationTest {

    @Nested
    @DisplayName("Connection Factory Configuration")
    class ConnectionFactoryTests {

        @Test
        @DisplayName("should expose configured host")
        void shouldExposeHost() {
            MqConfiguration config = createConfiguration();
            assertThat(config.getHost()).isEqualTo("mq.enterprise.com");
        }

        @Test
        @DisplayName("should expose configured port")
        void shouldExposePort() {
            MqConfiguration config = createConfiguration();
            assertThat(config.getPort()).isEqualTo(1414);
        }

        @Test
        @DisplayName("should expose queue manager")
        void shouldExposeQueueManager() {
            MqConfiguration config = createConfiguration();
            assertThat(config.getQueueManager()).isEqualTo("PROD_QM");
        }

        @Test
        @DisplayName("should expose channel")
        void shouldExposeChannel() {
            MqConfiguration config = createConfiguration();
            assertThat(config.getChannel()).isEqualTo("APP.CHANNEL");
        }

        @Test
        @DisplayName("should expose queue name")
        void shouldExposeQueueName() {
            MqConfiguration config = createConfiguration();
            assertThat(config.getQueue()).isEqualTo("BRIDGE.INPUT");
        }
    }

    @Nested
    @DisplayName("JMS Listener Factory Configuration")
    class JmsListenerFactoryTests {

        @Test
        @DisplayName("should configure CLIENT_ACKNOWLEDGE mode")
        void shouldConfigureClientAcknowledge() throws Exception {
            MqConfiguration config = createConfiguration();

            MockConnectionFactory mockFactory = new MockConnectionFactory();
            JmsListenerContainerFactory<?> factory = config.jmsListenerContainerFactory(mockFactory);

            assertThat(factory).isInstanceOf(DefaultJmsListenerContainerFactory.class);
            DefaultJmsListenerContainerFactory defaultFactory = (DefaultJmsListenerContainerFactory) factory;

            Integer ackMode = (Integer) ReflectionTestUtils.getField(defaultFactory, "sessionAcknowledgeMode");
            assertThat(ackMode).isEqualTo(Session.CLIENT_ACKNOWLEDGE);
        }

        @Test
        @DisplayName("should configure concurrency")
        void shouldConfigureConcurrency() throws Exception {
            MqConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "concurrency", 3);

            MockConnectionFactory mockFactory = new MockConnectionFactory();
            JmsListenerContainerFactory<?> factory = config.jmsListenerContainerFactory(mockFactory);

            DefaultJmsListenerContainerFactory defaultFactory = (DefaultJmsListenerContainerFactory) factory;
            String concurrency = (String) ReflectionTestUtils.getField(defaultFactory, "concurrency");
            assertThat(concurrency).isEqualTo("3");
        }

        @Test
        @DisplayName("should configure receive timeout")
        void shouldConfigureReceiveTimeout() throws Exception {
            MqConfiguration config = createConfiguration();
            ReflectionTestUtils.setField(config, "receiveTimeout", 10000L);

            MockConnectionFactory mockFactory = new MockConnectionFactory();
            JmsListenerContainerFactory<?> factory = config.jmsListenerContainerFactory(mockFactory);

            DefaultJmsListenerContainerFactory defaultFactory = (DefaultJmsListenerContainerFactory) factory;
            Long timeout = (Long) ReflectionTestUtils.getField(defaultFactory, "receiveTimeout");
            assertThat(timeout).isEqualTo(10000L);
        }

        @Test
        @DisplayName("should have error handler configured")
        void shouldHaveErrorHandler() throws Exception {
            MqConfiguration config = createConfiguration();

            MockConnectionFactory mockFactory = new MockConnectionFactory();
            JmsListenerContainerFactory<?> factory = config.jmsListenerContainerFactory(mockFactory);

            DefaultJmsListenerContainerFactory defaultFactory = (DefaultJmsListenerContainerFactory) factory;
            Object errorHandler = ReflectionTestUtils.getField(defaultFactory, "errorHandler");
            assertThat(errorHandler).isNotNull();
        }

        @Test
        @DisplayName("concurrency should default to 1")
        void concurrencyShouldDefaultToOne() {
            MqConfiguration config = createConfiguration();
            assertThat(config.getConcurrency()).isEqualTo(1);
        }
    }

    private MqConfiguration createConfiguration() {
        MqConfiguration config = new MqConfiguration();
        ReflectionTestUtils.setField(config, "host", "mq.enterprise.com");
        ReflectionTestUtils.setField(config, "port", 1414);
        ReflectionTestUtils.setField(config, "queueManager", "PROD_QM");
        ReflectionTestUtils.setField(config, "channel", "APP.CHANNEL");
        ReflectionTestUtils.setField(config, "queue", "BRIDGE.INPUT");
        ReflectionTestUtils.setField(config, "username", "appuser");
        ReflectionTestUtils.setField(config, "password", "secret");
        ReflectionTestUtils.setField(config, "sslCipherSuite", "");
        ReflectionTestUtils.setField(config, "truststoreLocation", "");
        ReflectionTestUtils.setField(config, "truststorePassword", "");
        ReflectionTestUtils.setField(config, "concurrency", 1);
        ReflectionTestUtils.setField(config, "receiveTimeout", 5000L);
        return config;
    }

    private static class MockConnectionFactory implements javax.jms.ConnectionFactory {
        @Override
        public javax.jms.Connection createConnection() {
            return null;
        }

        @Override
        public javax.jms.Connection createConnection(String userName, String password) {
            return null;
        }

        @Override
        public javax.jms.JMSContext createContext() {
            return null;
        }

        @Override
        public javax.jms.JMSContext createContext(String userName, String password) {
            return null;
        }

        @Override
        public javax.jms.JMSContext createContext(String userName, String password, int sessionMode) {
            return null;
        }

        @Override
        public javax.jms.JMSContext createContext(int sessionMode) {
            return null;
        }
    }
}
