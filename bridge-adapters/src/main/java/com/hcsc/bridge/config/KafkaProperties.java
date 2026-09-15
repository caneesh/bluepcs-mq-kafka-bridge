package com.hcsc.bridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * The Kafka client settings, bound and defaulted in exactly one place.
 *
 * <p>This replaces a 700-line BridgeProperties that declared groups for MQ, HDFS, the API,
 * security, audit and logging as well - none of which anything read, because those
 * components bind their keys with {@code @Value}. Two declarations of the same key with
 * their own defaults is how a default drifts from the value the runtime actually uses, so
 * the unused groups are gone and this one is the single binding site for
 * {@code bridge.kafka.*}.
 *
 * <p>The keys are deliberately FLAT (bridge.kafka.security-protocol, not
 * bridge.kafka.security.protocol): nested spellings bound to nothing and sat in the YAML
 * as dead configuration for a while, so the flat shape is the contract the profiles,
 * templates and the startup validator all use.
 */
@Component
@ConfigurationProperties(prefix = "bridge.kafka")
public class KafkaProperties {

    
    private String bootstrapServers;

    
    private String topic;

    private String auditTopic;

    private String securityProtocol = "SASL_SSL";

    private String saslMechanism = "GSSAPI";

    private String saslJaasConfig;

    private String jaasConfigPath;

    private String kerberosServiceName = "kafka";

    private String truststoreLocation;

    private String truststorePassword;

    private String truststoreType = "JKS";

    private String keystoreLocation;

    private String keystorePassword;

    private String keyPassword;

    
    private int requestSize = 4194400;

    
    private long deliveryTimeoutMs = 120000;

    
    private long requestTimeoutMs = 30000;

    private String acks = "all";

    // Upper bound on how long send() itself may block (metadata fetch / full buffer)
    private long maxBlockMs = 60000;

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getAuditTopic() {
        return auditTopic;
    }

    public void setAuditTopic(String auditTopic) {
        this.auditTopic = auditTopic;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslJaasConfig() {
        return saslJaasConfig;
    }

    public void setSaslJaasConfig(String saslJaasConfig) {
        this.saslJaasConfig = saslJaasConfig;
    }

    public String getJaasConfigPath() {
        return jaasConfigPath;
    }

    public void setJaasConfigPath(String jaasConfigPath) {
        this.jaasConfigPath = jaasConfigPath;
    }

    public String getKerberosServiceName() {
        return kerberosServiceName;
    }

    public void setKerberosServiceName(String kerberosServiceName) {
        this.kerberosServiceName = kerberosServiceName;
    }

    public String getTruststoreLocation() {
        return truststoreLocation;
    }

    public void setTruststoreLocation(String truststoreLocation) {
        this.truststoreLocation = truststoreLocation;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public void setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType;
    }

    public String getKeystoreLocation() {
        return keystoreLocation;
    }

    public void setKeystoreLocation(String keystoreLocation) {
        this.keystoreLocation = keystoreLocation;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public int getRequestSize() {
        return requestSize;
    }

    public void setRequestSize(int requestSize) {
        this.requestSize = requestSize;
    }

    public long getDeliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    public void setDeliveryTimeoutMs(long deliveryTimeoutMs) {
        this.deliveryTimeoutMs = deliveryTimeoutMs;
    }

    public long getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public void setRequestTimeoutMs(long requestTimeoutMs) {
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public String getAcks() {
        return acks;
    }

    public void setAcks(String acks) {
        this.acks = acks;
    }

    public long getMaxBlockMs() {
        return maxBlockMs;
    }

    public void setMaxBlockMs(long maxBlockMs) {
        this.maxBlockMs = maxBlockMs;
    }
}
