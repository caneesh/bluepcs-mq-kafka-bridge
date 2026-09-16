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

## Removed on purpose

Several components were deleted in September 2026 after a review found nothing on
either bridge's message path used them. Do not reintroduce them without a new reason:

| Removed | What it was | Why it went |
|---|---|---|
| HBase (`hbase-client`, `HBaseLedgerRepository`, `hbase` profile) | Optional store for the message ledger | Ledger was never written by the live path; dependency added ~27 MB per jar |
| Ledger, recovery, reconciliation packages (`file-ledger` profile, `bridge.ledger/recovery/reconciliation.*`, `RECOVERY_*` / `RECONCILIATION_*` audit types) | Scaffolding for a state-store based retry loop | Idempotent HDFS writes keyed by the deterministic event id, plus the audit stream and the audit-based gap/balance checks, are the actual bookkeeping |
| JAXB-generated classes (never added; the WebSphere predecessor used them) | Schema-bound object model of the PMM XML | The PMM bridge reads two XPath values and forwards the response verbatim; no XSD is available and a bound model would only add drift |

## Prerequisites

- Java 11
- Maven 3.6+
- Access to IBM MQ, Kafka, HDFS, and OAuth endpoints (for non-local profiles)

## Building

The repository is a Maven reactor: three library modules and two bootable
applications. Each application builds into `<module>/target/<module>-*.jar` and runs as
its own JVM.

```bash
mvn clean package -DskipTests                       # all modules
mvn -pl mq-kafka-bridge -am package -DskipTests     # one application + its dependencies
```

Use `-DskipTests`, not `-Dmaven.test.skip=true`: the latter also skips test *compilation*,
and the applications' tests depend on the `bridge-contract` test-jar (shared fakes), so
the build fails resolving it.

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

## Who owns what

Dependencies run one way, and the module a class lives in states what it is allowed to
know about:

| Module | Owns | Must not |
|---|---|---|
| `bridge-contract` | Message identity (`MqMessage`, `ProcessingContext`, the deterministic event id), the outcome of processing and the rule for when acknowledging is safe (`ProcessingResult`), the audit event and metadata contract, and the interfaces an adapter implements | Depend on MQ, Kafka, Hadoop, an HTTP client, or anything that can end a process. Its only dependency is a Spring annotation |
| `bridge-adapters` | The implementations that talk to infrastructure: MQ connection and listener factory, HDFS operations and the safe writer, the STS token provider, audit publishers, Kerberos renewal, health indicators, local-profile stand-ins | Decide what a message means or what should happen to one |
| `bridge-diagnostics` | Operational behaviour an application opts into: validate-only and monitor modes, the readiness checks, the shared startup rules, and the diagnostic JVM exit that hands an exit code to a scheduler | Be pulled in by accident. It is a separate module precisely because it can terminate the JVM |
| `mq-kafka-bridge` | The PMM+ workflow: JSON parsing, REST enrichment, response validation, the claim-check notification | Reimplement a shared rule (acknowledgement, quarantine durability, startup validation) |
| `mq-pmm-bridge` | The PMM workflow: XML extraction, request rendering, the 4-hourly landing layout | The same |

Two things are deliberately shared, because each is one decision: **when acknowledging an
MQ message is safe** (`ProcessingResult` plus `JmsMessageSupport.settle`) and **what a valid
configuration is** (`bridge-diagnostics/startup`). Two things are deliberately not shared,
because forcing them together would be worse than the duplication: JSON versus XML
processing, and GET-enrichment versus POST-submission semantics. Each application has its
own orchestrator and neither subclasses the other.

## Project Structure

```
pom.xml                          # reactor: versions and plugins only; modules declare their own dependencies
bridge-contract/                 # what the bridges agree on (no infrastructure, no JVM exit)
  src/main/java/com/hcsc/bridge/
  ├── audit/         # AuditEvent, AuditEventType, AuditMetadata, AuditPublisher
  ├── core/          # event id, digest, processing context, secret masking
  ├── hdfs/          # HdfsFileOperations (interface), HdfsWriteException
  ├── model/         # MqMessage, HdfsWriteResult
  ├── mq/            # MqProcessingException
  ├── orchestrator/  # ProcessingResult: the acknowledgement rule and its evidence invariants
  └── security/      # JwtTokenProvider (interface)
  src/test/java/com/hcsc/bridge/mock/   # reusable fakes, published as a test-jar
bridge-adapters/                 # the implementations
  src/main/java/com/hcsc/bridge/
  ├── audit/         # Kafka and file audit publishers
  ├── config/        # MQ, HDFS, Kafka and Kerberos configuration; KafkaProperties
  ├── hdfs/          # Hadoop operations, SafeHdfsWriter
  ├── health/        # actuator indicators, including mqConsumer
  ├── local/         # local-profile stand-ins
  ├── mq/            # JmsMessageSupport: header helpers and settle()
  └── security/      # OAuth2JwtTokenProvider
bridge-diagnostics/              # opt-in operational behaviour
  src/main/java/com/hcsc/bridge/diagnostics/
  ├── startup/       # the startup rules both applications share
  └── ...            # readiness checks, validate-only, monitor, DiagnosticJvmExit
mq-kafka-bridge/                 # PMM+ JSON bridge application (this README)
  src/main/java/com/hcsc/bridge/
  ├── MqKafkaBridgeApplication.java
  ├── api/           # REST enrichment client and response validation
  ├── config/        # startup validator, component-test and quarantine-replay runners
  ├── hdfs/          # HdfsSafePayloadWriter (flat landing directory, landed-payload lookup)
  ├── kafka/         # claim-check notification and publisher
  ├── model/         # ParsedPayload, EnrichedPayload
  ├── mq/            # MQ listener
  ├── orchestrator/  # BridgeOrchestrator
  └── parser/        # JSON message parsing
  src/main/resources/application*.yml
mq-pmm-bridge/                   # PMM canonical-XML bridge application (see below)
audit-hive-consumer/             # standalone Spark/Hive consumer for the audit topic
scripts/test/                    # Hive-to-SQLite harness for the reconciliation scripts
```

## PMM bridge (`mq-pmm-bridge`)

A second bootable application in the same reactor for the BluePCS **PMM** canonical XML
feed: MQ (XML) → two XPath values → XML request template → `POST` with the STS token →
raw XML response landed as `<base>/<yyyy-MM-dd>_<HH>/<eventId>.xml` (new folder every
4 hours) → audit only. It runs as its own JVM (port 8081, own `.env`, own systemd unit)
and reuses the shared modules. See `CONFIGURATION_GUIDE.md` §11 and
`DEPLOYMENT_CHECKLIST.md` "Second application".

```bash
# local, no infrastructure: push one sample message through the pipeline
PMM_LOCAL_SAMPLE_MESSAGE=docs/sample-pmm-message.xml BRIDGE_APP=mq-pmm-bridge scripts/run-local.sh
```

## Configuration

[docs/CONFIGURATION_REFERENCE.md](docs/CONFIGURATION_REFERENCE.md) lists every property both
bridges read: what the PMM+ bridge needs added to its existing `.env` (three optional
variables, all defaulting to the behaviour you want), and the eleven values the PMM bridge
cannot start without. [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) is the step-by-step
walkthrough behind it.

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
