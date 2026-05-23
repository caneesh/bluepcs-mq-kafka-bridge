package com.enterprise.bridge.config;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.config.JmsListenerContainerFactory;
import org.springframework.jms.connection.CachingConnectionFactory;

import javax.jms.ConnectionFactory;
import javax.jms.Session;

@Configuration
@EnableJms
@Profile("!local")
public class MqConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(MqConfiguration.class);

    @Value("${bridge.mq.host:localhost}")
    private String host;

    @Value("${bridge.mq.port:1414}")
    private int port;

    @Value("${bridge.mq.queue-manager:QM1}")
    private String queueManager;

    @Value("${bridge.mq.channel:DEV.APP.SVRCONN}")
    private String channel;

    @Value("${bridge.mq.queue:BRIDGE.INPUT.QUEUE}")
    private String queue;

    @Value("${bridge.mq.username:}")
    private String username;

    @Value("${bridge.mq.password:}")
    private String password;

    @Value("${bridge.mq.ssl.cipher-suite:}")
    private String sslCipherSuite;

    @Value("${bridge.mq.ssl.truststore-location:}")
    private String truststoreLocation;

    @Value("${bridge.mq.ssl.truststore-password:}")
    private String truststorePassword;

    @Value("${bridge.mq.concurrency:1}")
    private int concurrency;

    @Value("${bridge.mq.receive-timeout:5000}")
    private long receiveTimeout;

    @Value("${bridge.mq.listener-enabled:false}")
    private boolean listenerEnabled;

    @Value("${bridge.validate-only:false}")
    private boolean validateOnly;

    @Bean
    public ConnectionFactory mqConnectionFactory() throws Exception {
        MQConnectionFactory factory = new MQConnectionFactory();

        factory.setHostName(host);
        factory.setPort(port);
        factory.setQueueManager(queueManager);
        factory.setChannel(channel);
        factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);

        if (username != null && !username.isEmpty()) {
            factory.setStringProperty(WMQConstants.USERID, username);
            factory.setStringProperty(WMQConstants.PASSWORD, password);
            factory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        }

        if (sslCipherSuite != null && !sslCipherSuite.isEmpty()) {
            factory.setSSLCipherSuite(sslCipherSuite);

            if (truststoreLocation != null && !truststoreLocation.isEmpty()) {
                System.setProperty("javax.net.ssl.trustStore", truststoreLocation);
                if (truststorePassword != null && !truststorePassword.isEmpty()) {
                    System.setProperty("javax.net.ssl.trustStorePassword", truststorePassword);
                }
            }
        }

        logger.info("Configured IBM MQ connection factory: {}:{}/{}", host, port, queueManager);
        return new CachingConnectionFactory(factory);
    }

    @Bean
    public JmsListenerContainerFactory<?> jmsListenerContainerFactory(ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        factory.setConcurrency(String.valueOf(concurrency));
        factory.setReceiveTimeout(receiveTimeout);
        factory.setErrorHandler(t -> logger.error("JMS listener error", t));

        // Disable auto-start if listener is disabled or in validate-only mode
        boolean shouldStart = listenerEnabled && !validateOnly;
        factory.setAutoStartup(shouldStart);

        if (validateOnly) {
            logger.info("JMS listener auto-start DISABLED (validate-only mode)");
        } else if (!listenerEnabled) {
            logger.info("JMS listener auto-start DISABLED (bridge.mq.listener-enabled=false)");
        } else {
            logger.info("JMS listener auto-start ENABLED");
        }

        logger.info("Configured JMS listener factory with CLIENT_ACKNOWLEDGE and concurrency: {}", concurrency);
        return factory;
    }

    public String getQueue() {
        return queue;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getQueueManager() {
        return queueManager;
    }

    public String getChannel() {
        return channel;
    }

    public int getConcurrency() {
        return concurrency;
    }
}
