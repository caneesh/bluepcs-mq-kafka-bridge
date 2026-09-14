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

The repository is a Maven reactor. `bridge-core` is a shared library; every other
module is a bootable Spring Boot application that builds into
`<module>/target/<module>-*.jar` and runs as its own JVM.

```bash
mvn clean package -DskipTests                       # all modules
mvn -pl mq-kafka-bridge -am package -DskipTests     # one application + its dependencies
```

Use `-DskipTests`, not `-Dmaven.test.skip=true`: the latter also skips test *compilation*,
and the applications' tests depend on the `bridge-core` test-jar (shared fakes), so the
build fails resolving it.

## Running

### Local Development (no external dependencies)

```bash
java -jar mq-kafka-bridge/target/mq-kafka-bridge-*.jar --spring.profiles.active=local
```

Or use the script:
```bash
./scripts/run-local.sh
```

### Test Environment

The `test-env` profile requires `KAFKA_TRUSTSTORE_PASSWORD` and
`OAUTH_CLIENT_SECRET` — the application fails at startup if they are
missing. (`MQ_PASSWORD` is not needed: the test queue manager authenticates
by user id / channel auth without a password.) Put the secrets in a `.env`
file in the project root (gitignored, sourced automatically by the scripts):

```bash
cp .env.template .env
# edit .env and fill in the secrets, then:
./scripts/run-test-env.sh
```

Or export them and run the jar directly:

```bash
export KAFKA_TRUSTSTORE_PASSWORD=<secret>
export OAUTH_CLIENT_SECRET=<secret>
java -jar mq-kafka-bridge/target/mq-kafka-bridge-*.jar --spring.profiles.active=test-env
```

### Production

```bash
# Set required secrets
export KAFKA_TRUSTSTORE_PASSWORD=<secret>
export OAUTH_CLIENT_SECRET=<secret>
# Only if the queue manager requires MQCSP password auth (it may authenticate
# by user id / channel auth alone, like test):
# export MQ_PASSWORD=<secret>

# Run with listener disabled (safe start)
java -jar mq-kafka-bridge/target/mq-kafka-bridge-*.jar --spring.profiles.active=prod

# Run with message consumption enabled
java -jar mq-kafka-bridge/target/mq-kafka-bridge-*.jar \
  --spring.profiles.active=prod \
  --bridge.mq.listener-enabled=true
```

## Profiles

| Profile | Description |
|---------|-------------|
| `local` | Local development with mocks, no external dependencies |
| `test-env` | Lower/UAT environment with test defaults |
| `prod` | Production environment |

## Startup Modes

### Validate-Only Mode

Validates configuration and connectivity without consuming messages:

```bash
java -jar mq-kafka-bridge/target/mq-kafka-bridge-*.jar \
  --spring.profiles.active=prod \
  --bridge.validate-only=true
```

Exit codes:
- `0` - All checks passed
- `1` - One or more checks failed
- `2` - Validation exception

### Component-Test Mode

Exercises exactly ONE part of the bridge (mq, api, kafka, or hdfs) against real
infrastructure and exits `0`=PASS / `1`=FAIL / `2`=bad mode/args. Intended for
environment bring-up: `./scripts/component-test.sh <mq|api|kafka|hdfs> [args] [profile]`.
See DEPLOYMENT_CHECKLIST.md ("Component-by-Component Testing") for the recommended order.

### Listener Control

The MQ listener is **disabled by default** for safety. Enable explicitly:

```bash
--bridge.mq.listener-enabled=true
```

## Configuration

### Required Environment Variables (test-env and prod)

| Variable | Description |
|----------|-------------|
| `KAFKA_TRUSTSTORE_PASSWORD` | Kafka SSL truststore password |
| `OAUTH_CLIENT_SECRET` | OAuth2 client secret |
| `MQ_PASSWORD` | IBM MQ password — optional in all environments; set only if the queue manager requires MQCSP auth (both test and prod may authenticate by user id / channel auth) |

### Key Properties

| Property | Default | Description |
|----------|---------|-------------|
| `bridge.validate-only` | `false` | Run validation and exit |
| `bridge.mq.listener-enabled` | `false` | Enable MQ message consumption |

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
pom.xml                          # reactor parent: versions, plugins, shared dependency list
bridge-core/                     # shared library (no application class, no application*.yml)
  src/main/java/com/hcsc/bridge/
  ├── audit/         # Audit event publishing
  ├── config/        # MQ/HDFS/Kafka/Kerberos configuration, readiness + monitor + validate-only runners
  ├── core/          # Core utilities (event ID, digest, secrets)
  ├── hdfs/          # HDFS file operations
  ├── health/        # Actuator health indicators
  ├── local/         # Local-profile implementations (token, HDFS)
  ├── model/         # Generic value objects (MqMessage, HdfsWriteResult)
  ├── mq/            # MqProcessingException
  ├── orchestrator/  # ProcessingResult
  └── security/      # STS/JWT token provider
  src/test/java/com/hcsc/bridge/mock/   # reusable test fakes (published as a test-jar)
mq-kafka-bridge/                 # PMM+ JSON bridge application (this README)
  src/main/java/com/hcsc/bridge/
  ├── MqKafkaBridgeApplication.java
  ├── api/           # REST API client for enrichment
  ├── config/        # Startup validator, component-test and quarantine-replay runners
  ├── hdfs/          # HdfsSafePayloadWriter (flat landing directory)
  ├── kafka/         # Kafka envelope publishing
  ├── local/         # Local-profile API client
  ├── model/         # ParsedPayload, EnrichedPayload
  ├── mq/            # IBM MQ listener
  ├── orchestrator/  # Message processing orchestration
  └── parser/        # Message parsing
  src/main/resources/application*.yml
mq-pmm-bridge/                   # PMM canonical-XML bridge application (see below)
  src/main/java/com/hcsc/bridge/
  ├── PmmBridgeApplication.java
  └── pmm/
      ├── xml/           # hardened XML parsing + XPath extraction
      ├── template/      # request template loading/rendering
      ├── api/           # POST client with token refresh + retry
      ├── hdfs/          # 4-hourly window path resolver, backlog scanner
      ├── orchestrator/  # PmmOrchestrator
      ├── mq/            # PMM queue listener (Text + Bytes messages)
      ├── config/        # startup validator, readiness checks
      └── local/         # local-profile API stub + sample runner
audit-hive-consumer/             # standalone Spark/Hive consumer for the audit topic
```

## PMM bridge (`mq-pmm-bridge`)

A second bootable application in the same reactor for the BluePCS **PMM** canonical XML
feed: MQ (XML) → two XPath values → XML request template → `POST` with the STS token →
raw XML response landed as `<base>/<yyyy-MM-dd>/<HH>/<eventId>.xml` (new folder every
4 hours) → audit only. It runs as its own JVM (port 8081, own `.env`, own systemd unit)
and reuses everything in `bridge-core`. See `CONFIGURATION_GUIDE.md` §11 and
`DEPLOYMENT_CHECKLIST.md` "Second application".

```bash
# local, no infrastructure: push one sample message through the pipeline
PMM_LOCAL_SAMPLE_MESSAGE=docs/sample-pmm-message.xml BRIDGE_APP=mq-pmm-bridge scripts/run-local.sh
```

## Deployment Checklist

See [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) for a step-by-step walkthrough
of every property (what it means, where to get the value, how to verify each
component), and [DEPLOYMENT_CHECKLIST.md](DEPLOYMENT_CHECKLIST.md) for detailed deployment steps.

**Quick checklist:**
1. Set all required environment variables
2. Verify file paths (truststore, keytab) exist
3. Run `--bridge.validate-only=true` to verify connectivity
4. Start with `--bridge.mq.listener-enabled=false` to verify health
5. Enable listener: `--bridge.mq.listener-enabled=true`

## License

Proprietary - HCSC Internal Use Only
