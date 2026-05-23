# MQ-Kafka Bridge Deployment Checklist

## Pre-Deployment Validation

### 1. Environment Variables

Set all required environment variables before starting the application:

```bash
# MQ Configuration (Required)
export MQ_HOST=your-mq-host.example.com
export MQ_PORT=1414
export MQ_QUEUE_MANAGER=QMGR1
export MQ_CHANNEL=APP.SVRCONN
export MQ_QUEUE=BRIDGE.INPUT.QUEUE
export MQ_USERNAME=mquser
export MQ_PASSWORD=<secret>

# Kafka Configuration (Required)
export KAFKA_BOOTSTRAP_SERVERS=kafka1:9093,kafka2:9093,kafka3:9093
export KAFKA_TOPIC=bridge-events
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=GSSAPI
export KAFKA_KERBEROS_SERVICE_NAME=kafka
export KAFKA_TRUSTSTORE_LOCATION=/path/to/kafka.truststore.jks
export KAFKA_TRUSTSTORE_PASSWORD=<secret>

# HDFS Configuration (Required)
export HDFS_NAMENODE=hdfs://namenode:8020
export HDFS_BASE_PATH=/data/bridge/payloads
export HDFS_KERBEROS_ENABLED=true
export HDFS_KERBEROS_PRINCIPAL=bridgeuser@REALM.COM
export HDFS_KERBEROS_KEYTAB=/etc/security/keytabs/bridgeuser.keytab

# API Configuration (Required)
export API_BASE_URL=https://api.example.com/v1
export OAUTH_TOKEN_URL=https://auth.example.com/oauth/token
export OAUTH_CLIENT_ID=bridge-client
export OAUTH_CLIENT_SECRET=<secret>
```

### 2. File System Prerequisites

- [ ] Kafka truststore file exists and is readable
- [ ] HDFS keytab file exists and is readable (if Kerberos enabled)
- [ ] JAAS config file exists (if using file-based Kerberos)
- [ ] Log directory exists and is writable
- [ ] Hadoop conf directory exists (if using HADOOP_CONF_DIR)

### 3. Network Prerequisites

- [ ] MQ host:port reachable
- [ ] Kafka bootstrap servers reachable
- [ ] HDFS namenode reachable
- [ ] OAuth token endpoint reachable
- [ ] API base URL reachable

---

## Deployment Steps

### Step 1: Validate Configuration (Recommended)

Run the application in validate-only mode to verify all connectivity:

```bash
java -jar mq-kafka-bridge.jar \
  --spring.profiles.active=prod \
  --bridge.validate-only=true
```

**Expected output:**
```
=== RUNNING READINESS CHECKS ===
[PASS] MQ_CONNECTION: MQ connection successful
[PASS] KAFKA_CONNECTION: Kafka cluster connected
[PASS] HDFS_CONNECTION: HDFS base path accessible
[PASS] OAUTH_TOKEN: OAuth token acquired successfully
=== VALIDATION RESULT: PASSED ===
```

**Exit codes:**
- 0: All checks passed
- 1: One or more checks failed
- 2: Validation exception

### Step 2: Start with Listener Disabled (Optional)

Start the application without consuming messages to verify startup:

```bash
java -jar mq-kafka-bridge.jar \
  --spring.profiles.active=prod \
  --bridge.mq.listener-enabled=false
```

Verify:
- [ ] Application starts without errors
- [ ] Health endpoint returns UP: `curl http://localhost:8080/actuator/health`

### Step 3: Enable Message Consumption

Start with listener enabled:

```bash
java -jar mq-kafka-bridge.jar \
  --spring.profiles.active=prod \
  --bridge.mq.listener-enabled=true
```

---

## Runtime Monitoring

### Health Endpoints

```bash
# Overall health
curl http://localhost:8080/actuator/health

# Detailed health (if authorized)
curl http://localhost:8080/actuator/health -u admin:password

# Application info
curl http://localhost:8080/actuator/info

# Metrics
curl http://localhost:8080/actuator/metrics
```

### Log Monitoring

Key log patterns to monitor:

```
# Successful processing
"Successfully processed message: eventId=..."

# Processing failures (will not ack MQ message)
"Parse failure for eventId..."
"Enrichment failure for eventId..."
"HDFS write failure for eventId..."
"Kafka publish failure for eventId..."

# Configuration issues
"Configuration validation failed..."
"VALIDATION RESULT: FAILED"
```

---

## Troubleshooting

### Configuration Validation Failed

1. Check error messages in startup logs
2. Verify all required environment variables are set
3. Verify file paths exist and are readable
4. Run validate-only mode to identify specific failures

### MQ Connection Issues

1. Verify MQ host:port is reachable: `telnet $MQ_HOST $MQ_PORT`
2. Verify queue manager name is correct
3. Verify channel name is correct
4. Verify credentials are correct
5. Check MQ error logs

### Kafka Connection Issues

1. Verify bootstrap servers are reachable
2. Verify truststore file exists and contains correct certificates
3. Verify SASL/Kerberos configuration
4. Check Kafka broker logs

### HDFS Connection Issues

1. Verify namenode is reachable: `hdfs dfs -ls /`
2. Verify Kerberos ticket: `klist`
3. Verify keytab is valid: `kinit -kt $HDFS_KERBEROS_KEYTAB $HDFS_KERBEROS_PRINCIPAL`
4. Verify base path exists and is writable

### OAuth Token Issues

1. Verify token endpoint is reachable
2. Verify client credentials are correct
3. Check OAuth server logs
4. Test token acquisition: `curl -X POST $OAUTH_TOKEN_URL -d "grant_type=client_credentials&client_id=$OAUTH_CLIENT_ID&client_secret=$OAUTH_CLIENT_SECRET"`

---

## Rollback Procedure

1. Stop the application: `kill -TERM <pid>` or stop the service
2. Messages not yet acknowledged will be redelivered by MQ
3. Duplicate Kafka messages are expected and handled by downstream consumers
4. No data loss expected due to at-least-once delivery semantics

---

## Key Behaviors

### Message Processing Order

1. Receive message from MQ (not acknowledged yet)
2. Parse message payload
3. Call enrichment API
4. Write payload to HDFS (temp file → rename)
5. Publish envelope to Kafka
6. Acknowledge MQ message (only on complete success)

### Failure Handling

- Any step failure → MQ message NOT acknowledged → MQ will redeliver
- Duplicate Kafka publishes are acceptable (downstream deduplicates by event_id)
- HDFS writes are idempotent (file already exists = skip)
- Event ID is deterministic (SHA-256 of JMS Message ID)

### Important Properties

| Property | Default | Description |
|----------|---------|-------------|
| `bridge.validate-only` | false | Run validation and exit |
| `bridge.mq.listener-enabled` | false | Enable MQ message consumption |
| `bridge.mq.concurrency` | 1 | Number of concurrent listeners |
| `bridge.api.retry-attempts` | 3 | API call retry attempts |
| `bridge.api.timeout-seconds` | 30 | API call timeout |
