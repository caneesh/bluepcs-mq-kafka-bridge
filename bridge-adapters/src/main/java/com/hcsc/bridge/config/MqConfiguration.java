package com.hcsc.bridge.config;

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

    @Value("${bridge.mq.ssl.enabled:false}")
    private boolean sslEnabled;

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
            // MQCSP authentication requires a password; a passwordless connection
            // sends only the user id (queue manager authenticates via channel auth)
            if (password != null && !password.isEmpty()) {
                factory.setStringProperty(WMQConstants.PASSWORD, password);
                factory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
            }
        }

        // SSL is driven by the explicit enabled flag OR (backwards-compat) a configured
        // cipher suite. A dedicated SSLSocketFactory keeps the MQ truststore scoped to
        // this connection — the previous javax.net.ssl.* SYSTEM properties silently
        // repointed the STS/API HTTPS clients at the MQ truststore, breaking their TLS
        // unless the CAs happened to be merged.
        boolean sslConfigured = sslEnabled || (sslCipherSuite != null && !sslCipherSuite.isEmpty());
        if (sslConfigured) {
            if (sslCipherSuite == null || sslCipherSuite.isEmpty()) {
                throw new IllegalStateException("bridge.mq.ssl.enabled=true requires "
                        + "bridge.mq.ssl.cipher-suite (must match the SVRCONN channel's SSLCIPH)");
            }
            factory.setSSLCipherSuite(sslCipherSuite);
            if (truststoreLocation != null && !truststoreLocation.isEmpty()) {
                factory.setSSLSocketFactory(buildSslSocketFactory());
            }
        }

        logger.info("Configured IBM MQ connection factory: {}:{}/{}", host, port, queueManager);
        CachingConnectionFactory cachingFactory = new CachingConnectionFactory(factory);
        // DefaultMessageListenerContainer manages its own consumers; cached consumers hold
        // prefetched messages that would appear "stuck" on the queue. Keep connection/session
        // caching, but let the listener container own the consumer lifecycle (Spring recommendation).
        cachingFactory.setCacheConsumers(false);
        return cachingFactory;
    }

    /**
     * Trust manager from the configured MQ truststore, wrapped in a TLS 1.2 context.
     * Scoped to the MQ connection factory only — never installed JVM-wide.
     */
    private javax.net.ssl.SSLSocketFactory buildSslSocketFactory() throws Exception {
        java.security.KeyStore trustStore =
                java.security.KeyStore.getInstance(java.security.KeyStore.getDefaultType());
        try (java.io.FileInputStream in = new java.io.FileInputStream(truststoreLocation)) {
            trustStore.load(in, truststorePassword != null && !truststorePassword.isEmpty()
                    ? truststorePassword.toCharArray() : null);
        }
        javax.net.ssl.TrustManagerFactory tmf = javax.net.ssl.TrustManagerFactory
                .getInstance(javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLSv1.2");
        sslContext.init(null, tmf.getTrustManagers(), null);
        return sslContext.getSocketFactory();
    }

    @Bean
    public JmsListenerContainerFactory<?> jmsListenerContainerFactory(ConnectionFactory connectionFactory) {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(connectionFactory);
        factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
        factory.setConcurrency(String.valueOf(concurrency));
        factory.setReceiveTimeout(receiveTimeout);
        // The listener already logs each MqProcessingException with full context before
        // rethrowing — repeating it here with a stack pointing at the listener (not the
        // root cause) just doubles the noise per failure. Reserve ERROR+stack for
        // exception types that did NOT come through that path.
        factory.setErrorHandler(t -> {
            if (t instanceof com.hcsc.bridge.mq.MqProcessingException) {
                logger.warn("JMS listener error (already logged with context): {}", t.getMessage());
            } else {
                logger.error("JMS listener error", t);
            }
        });

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
