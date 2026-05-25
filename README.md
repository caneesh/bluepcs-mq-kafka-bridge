# MQ-Kafka Bridge

Enterprise message bridge that consumes messages from IBM MQ, enriches them via REST API, writes payloads to HDFS, and publishes envelopes to Kafka.

## Architecture

```
IBM MQ → Parse → Enrich (REST API) → Write HDFS → Publish Kafka → Acknowledge MQ
```

**Key behaviors:**
- MQ acknowledgment only after successful HDFS write + Kafka publish
- Deterministic event ID (SHA-256 of JMS Message ID)
- At-least-once delivery (duplicates handled by downstream consumers)
- Fail-fast configuration validation

## Prerequisites

- Java 11
- Maven 3.6+
- Access to IBM MQ, Kafka, HDFS, and OAuth endpoints (for non-local profiles)

## Building

```bash
mvn clean package -DskipTests
```

## Running

### Local Development (no external dependencies)

```bash
java -jar target/mq-kafka-bridge-*.jar --spring.profiles.active=local
```

Or use the script:
```bash
./scripts/run-local.sh
```

### Test Environment

```bash
java -jar target/mq-kafka-bridge-*.jar --spring.profiles.active=test-env
```

### Production

```bash
# Set required secrets
export MQ_PASSWORD=<secret>
export KAFKA_TRUSTSTORE_PASSWORD=<secret>
export OAUTH_CLIENT_SECRET=<secret>

# Run with listener disabled (safe start)
java -jar target/mq-kafka-bridge-*.jar --spring.profiles.active=prod

# Run with message consumption enabled
java -jar target/mq-kafka-bridge-*.jar \
  --spring.profiles.active=prod \
  --bridge.mq.listener-enabled=true
```

## Profiles

| Profile | Description |
|---------|-------------|
| `local` | Local development with mocks, no external dependencies |
| `test-env` | Lower/UAT environment with test defaults |
| `prod` | Production environment |
| `hbase` | Enables HBase ledger (optional, for recovery features) |

## Startup Modes

### Validate-Only Mode

Validates configuration and connectivity without consuming messages:

```bash
java -jar target/mq-kafka-bridge-*.jar \
  --spring.profiles.active=prod \
  --bridge.validate-only=true
```

Exit codes:
- `0` - All checks passed
- `1` - One or more checks failed
- `2` - Validation exception

### Listener Control

The MQ listener is **disabled by default** for safety. Enable explicitly:

```bash
--bridge.mq.listener-enabled=true
```

## Configuration

### Required Environment Variables (Production)

| Variable | Description |
|----------|-------------|
| `MQ_PASSWORD` | IBM MQ password |
| `KAFKA_TRUSTSTORE_PASSWORD` | Kafka SSL truststore password |
| `OAUTH_CLIENT_SECRET` | OAuth2 client secret |

### Key Properties

| Property | Default | Description |
|----------|---------|-------------|
| `bridge.validate-only` | `false` | Run validation and exit |
| `bridge.mq.listener-enabled` | `false` | Enable MQ message consumption |
| `bridge.reconciliation.enabled` | `false` | Enable reconciliation (requires HBase) |
| `bridge.recovery.enabled` | `false` | Enable recovery processing |

See `config/` directory for full configuration templates.

## Health & Monitoring

```bash
# Health check
curl http://localhost:8080/actuator/health

# Metrics
curl http://localhost:8080/actuator/metrics

# Application info
curl http://localhost:8080/actuator/info
```

## Testing

```bash
# Run all tests
mvn test

# Run with coverage
mvn test jacoco:report
```

## Scripts

| Script | Description |
|--------|-------------|
| `scripts/run-local.sh` | Run in local development mode |
| `scripts/validate-only.sh` | Run validation mode |
| `scripts/smoke-test.sh` | Start app, verify health, exit |

## Project Structure

```
src/main/java/com/hcsc/bridge/
├── api/           # REST API client for enrichment
├── audit/         # Audit event publishing
├── config/        # Spring configuration
├── core/          # Core utilities (event ID, secrets)
├── hdfs/          # HDFS file operations
├── kafka/         # Kafka envelope publishing
├── ledger/        # Ledger repository (HBase)
├── local/         # Local development implementations
├── model/         # Domain models
├── mq/            # IBM MQ listener
├── orchestrator/  # Message processing orchestration
├── parser/        # Message parsing
├── reconciliation/# Reconciliation (optional)
├── recovery/      # Recovery processing (optional)
└── security/      # OAuth2/JWT token provider
```

## Deployment Checklist

See [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md) for detailed deployment steps.

**Quick checklist:**
1. Set all required environment variables
2. Verify file paths (truststore, keytab) exist
3. Run `--bridge.validate-only=true` to verify connectivity
4. Start with `--bridge.mq.listener-enabled=false` to verify health
5. Enable listener: `--bridge.mq.listener-enabled=true`

## License

Proprietary - HCSC Internal Use Only
