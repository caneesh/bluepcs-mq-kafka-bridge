package com.hcsc.bridge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bridge")
public class BridgeProperties {

    @NestedConfigurationProperty
    private MqProperties mq = new MqProperties();

    @NestedConfigurationProperty
    private KafkaProperties kafka = new KafkaProperties();

    @NestedConfigurationProperty
    private HdfsProperties hdfs = new HdfsProperties();

    @NestedConfigurationProperty
    private HBaseProperties hbase = new HBaseProperties();

    @NestedConfigurationProperty
    private ApiProperties api = new ApiProperties();

    @NestedConfigurationProperty
    private SecurityProperties security = new SecurityProperties();

    @NestedConfigurationProperty
    private AuditProperties audit = new AuditProperties();

    @NestedConfigurationProperty
    private LoggingProperties logging = new LoggingProperties();

    @NestedConfigurationProperty
    private RecoveryProperties recovery = new RecoveryProperties();

    // Getters and setters

    public MqProperties getMq() {
        return mq;
    }

    public void setMq(MqProperties mq) {
        this.mq = mq;
    }

    public KafkaProperties getKafka() {
        return kafka;
    }

    public void setKafka(KafkaProperties kafka) {
        this.kafka = kafka;
    }

    public HdfsProperties getHdfs() {
        return hdfs;
    }

    public void setHdfs(HdfsProperties hdfs) {
        this.hdfs = hdfs;
    }

    public HBaseProperties getHbase() {
        return hbase;
    }

    public void setHbase(HBaseProperties hbase) {
        this.hbase = hbase;
    }

    public ApiProperties getApi() {
        return api;
    }

    public void setApi(ApiProperties api) {
        this.api = api;
    }

    public SecurityProperties getSecurity() {
        return security;
    }

    public void setSecurity(SecurityProperties security) {
        this.security = security;
    }

    public AuditProperties getAudit() {
        return audit;
    }

    public void setAudit(AuditProperties audit) {
        this.audit = audit;
    }

    public LoggingProperties getLogging() {
        return logging;
    }

    public void setLogging(LoggingProperties logging) {
        this.logging = logging;
    }

    public RecoveryProperties getRecovery() {
        return recovery;
    }

    public void setRecovery(RecoveryProperties recovery) {
        this.recovery = recovery;
    }

    // ============================================================================
    // MQ Properties
    // ============================================================================
    public static class MqProperties {

        
        private String host = "localhost";

        
        private int port = 1414;

        
        private String queueManager = "QM1";

        
        private String channel = "DEV.APP.SVRCONN";

        
        private String queue = "BRIDGE.INPUT.QUEUE";

        private String username;
        private String password;

        
        private int concurrency = 1;

        
        private long receiveTimeout = 5000;

        @NestedConfigurationProperty
        private SslProperties ssl = new SslProperties();

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }

        public String getQueueManager() {
            return queueManager;
        }

        public void setQueueManager(String queueManager) {
            this.queueManager = queueManager;
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }

        public String getQueue() {
            return queue;
        }

        public void setQueue(String queue) {
            this.queue = queue;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }

        public int getConcurrency() {
            return concurrency;
        }

        public void setConcurrency(int concurrency) {
            this.concurrency = concurrency;
        }

        public long getReceiveTimeout() {
            return receiveTimeout;
        }

        public void setReceiveTimeout(long receiveTimeout) {
            this.receiveTimeout = receiveTimeout;
        }

        public SslProperties getSsl() {
            return ssl;
        }

        public void setSsl(SslProperties ssl) {
            this.ssl = ssl;
        }
    }

    // ============================================================================
    // Kafka Properties
    // ============================================================================
    public static class KafkaProperties {

        
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

        
        private int retries = 5;

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

        public int getRetries() {
            return retries;
        }

        public void setRetries(int retries) {
            this.retries = retries;
        }
    }

    // ============================================================================
    // HDFS Properties
    // ============================================================================
    public static class HdfsProperties {

        
        private String namenode;

        
        private String basePath;

        private String archivePath;

        
        private int replication = 3;

        private String tempSuffix = ".tmp";

        @NestedConfigurationProperty
        private KerberosProperties kerberos = new KerberosProperties();

        public String getNamenode() {
            return namenode;
        }

        public void setNamenode(String namenode) {
            this.namenode = namenode;
        }

        public String getBasePath() {
            return basePath;
        }

        public void setBasePath(String basePath) {
            this.basePath = basePath;
        }

        public String getArchivePath() {
            return archivePath;
        }

        public void setArchivePath(String archivePath) {
            this.archivePath = archivePath;
        }

        public int getReplication() {
            return replication;
        }

        public void setReplication(int replication) {
            this.replication = replication;
        }

        public String getTempSuffix() {
            return tempSuffix;
        }

        public void setTempSuffix(String tempSuffix) {
            this.tempSuffix = tempSuffix;
        }

        public KerberosProperties getKerberos() {
            return kerberos;
        }

        public void setKerberos(KerberosProperties kerberos) {
            this.kerberos = kerberos;
        }
    }

    // ============================================================================
    // Kerberos Properties
    // ============================================================================
    public static class KerberosProperties {

        private boolean enabled = false;

        private String principal;

        private String keytab;

        private String namenodePrincipal;

        private String resourcemanagerPrincipal;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getPrincipal() {
            return principal;
        }

        public void setPrincipal(String principal) {
            this.principal = principal;
        }

        public String getKeytab() {
            return keytab;
        }

        public void setKeytab(String keytab) {
            this.keytab = keytab;
        }

        public String getNamenodePrincipal() {
            return namenodePrincipal;
        }

        public void setNamenodePrincipal(String namenodePrincipal) {
            this.namenodePrincipal = namenodePrincipal;
        }

        public String getResourcemanagerPrincipal() {
            return resourcemanagerPrincipal;
        }

        public void setResourcemanagerPrincipal(String resourcemanagerPrincipal) {
            this.resourcemanagerPrincipal = resourcemanagerPrincipal;
        }
    }

    // ============================================================================
    // HBase Properties
    // ============================================================================
    public static class HBaseProperties {

        private String zookeeperQuorum;

        
        private int zookeeperPort = 2181;

        private String znodeParent = "/hbase";

        private String ledgerTable = "bridge_ledger";

        public String getZookeeperQuorum() {
            return zookeeperQuorum;
        }

        public void setZookeeperQuorum(String zookeeperQuorum) {
            this.zookeeperQuorum = zookeeperQuorum;
        }

        public int getZookeeperPort() {
            return zookeeperPort;
        }

        public void setZookeeperPort(int zookeeperPort) {
            this.zookeeperPort = zookeeperPort;
        }

        public String getZnodeParent() {
            return znodeParent;
        }

        public void setZnodeParent(String znodeParent) {
            this.znodeParent = znodeParent;
        }

        public String getLedgerTable() {
            return ledgerTable;
        }

        public void setLedgerTable(String ledgerTable) {
            this.ledgerTable = ledgerTable;
        }
    }

    // ============================================================================
    // API Properties
    // ============================================================================
    public static class ApiProperties {

        
        private String baseUrl;

        
        private int timeoutSeconds = 30;

        
        private int retryAttempts = 3;

        
        private long retryDelayMs = 1000;

        public String getBaseUrl() {
            return baseUrl;
        }

        public void setBaseUrl(String baseUrl) {
            this.baseUrl = baseUrl;
        }

        public int getTimeoutSeconds() {
            return timeoutSeconds;
        }

        public void setTimeoutSeconds(int timeoutSeconds) {
            this.timeoutSeconds = timeoutSeconds;
        }

        public int getRetryAttempts() {
            return retryAttempts;
        }

        public void setRetryAttempts(int retryAttempts) {
            this.retryAttempts = retryAttempts;
        }

        public long getRetryDelayMs() {
            return retryDelayMs;
        }

        public void setRetryDelayMs(long retryDelayMs) {
            this.retryDelayMs = retryDelayMs;
        }
    }

    // ============================================================================
    // Security Properties
    // ============================================================================
    public static class SecurityProperties {

        private String tokenUrl;

        private String clientId;

        private String clientSecret;

        private String scope;

        private String username;

        private String password;

        public String getTokenUrl() {
            return tokenUrl;
        }

        public void setTokenUrl(String tokenUrl) {
            this.tokenUrl = tokenUrl;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public String getClientSecret() {
            return clientSecret;
        }

        public void setClientSecret(String clientSecret) {
            this.clientSecret = clientSecret;
        }

        public String getScope() {
            return scope;
        }

        public void setScope(String scope) {
            this.scope = scope;
        }

        public String getUsername() {
            return username;
        }

        public void setUsername(String username) {
            this.username = username;
        }

        public String getPassword() {
            return password;
        }

        public void setPassword(String password) {
            this.password = password;
        }
    }

    // ============================================================================
    // Audit Properties
    // ============================================================================
    public static class AuditProperties {

        private String hiveTable;

        private boolean enabled = true;

        public String getHiveTable() {
            return hiveTable;
        }

        public void setHiveTable(String hiveTable) {
            this.hiveTable = hiveTable;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    // ============================================================================
    // Logging Properties
    // ============================================================================
    public static class LoggingProperties {

        private String directory;

        public String getDirectory() {
            return directory;
        }

        public void setDirectory(String directory) {
            this.directory = directory;
        }
    }

    // ============================================================================
    // Recovery Properties
    // ============================================================================
    public static class RecoveryProperties {

        private boolean enabled = true;

        
        private int maxRetries = 5;

        
        private long intervalMs = 120000;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public long getIntervalMs() {
            return intervalMs;
        }

        public void setIntervalMs(long intervalMs) {
            this.intervalMs = intervalMs;
        }
    }

    // ============================================================================
    // SSL Properties (shared)
    // ============================================================================
    public static class SslProperties {

        private boolean enabled = false;

        private String cipherSuite;

        private String truststoreLocation;

        private String truststorePassword;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public String getCipherSuite() {
            return cipherSuite;
        }

        public void setCipherSuite(String cipherSuite) {
            this.cipherSuite = cipherSuite;
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
    }
}
