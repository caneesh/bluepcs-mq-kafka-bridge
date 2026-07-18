# MQ-Kafka Bridge Deployment Checklist

## Transferring Code to the Office Network

The office environment is reached via: push to git → download → OneDrive →
copy to the office machine. Two cautions before uploading:

- [ ] Do not zip the raw working directory — a local `.env` with real
      credentials would ride along. Transfer via `git archive`, a fresh
      clone, or delete `.env` from the copy first.
- [ ] Be aware the `.git` history travels with a clone and contains
      everything ever committed.

### Dependency Strategy (decide BEFORE transferring)

Building requires Maven to resolve Spring Boot, Hadoop, HBase and Kafka
artifacts. Pick one:

1. **Internal mirror**: the office network has a Nexus/Artifactory mirror —
   configure it in `~/.m2/settings.xml` on the office machine.
2. **Offline repository**: at home run `mvn dependency:go-offline`, zip
   `~/.m2/repository`, carry it alongside the code, and build with
   `mvn -o clean package` (offline mode).
3. **Carry the jar**: build at home (`mvn clean package`) and transfer
   `target/mq-kafka-bridge-*.jar` itself. The Spring Boot fat jar is
   self-contained; the office machine then only needs a JDK, no Maven.

## Building and First Run on the Office Machine

### Step 1: Verify tooling

```bash
java -version    # JDK 11 required
mvn -version     # unless carrying a pre-built jar
```

### Step 2: Build

```bash
mvn clean package               # runs unit tests (no external infra needed)
mvn clean package -DskipTests   # faster, jar only
```

Integration tests (`*IT.java`) only run under `mvn verify`, so a plain
`package` never touches real infrastructure.

### Step 3: Set up the edge node

The application must RUN on a cluster edge node — the keytab
(`/etc/security/keytabs/...`), the Kafka truststore
(`/prod/gold/integration/conf/...`), the log directory, and network access to
MQ/Kafka/HDFS only exist there. Building can happen anywhere (office
workstation with JDK 11 + Maven, or the edge node itself if it has them); only
the fat jar needs to reach the edge node.

Recreate this layout on the edge node — the scripts locate the jar and `.env`
relative to themselves (`PROJECT_DIR` = the parent of `scripts/`):

```
~/bluepcs-bridge/                  <- project root
|-- .env                           <- created HERE by hand, never transferred
|-- scripts/                       <- copied from the repo
`-- target/
    `-- mq-kafka-bridge-*.jar      <- the built jar
```

- [ ] Jar copied to `~/bluepcs-bridge/target/`
- [ ] `scripts/` copied and executable: `chmod +x scripts/*.sh`
- [ ] Running as the service account (not a personal login) — it must be able
      to read the keytab: `ls -l /etc/security/keytabs/e4193139.keytab`
- [ ] `.env` created on the edge node itself (next step) — never carried
      through OneDrive with real values
- [ ] `.env` locked down: `chmod 600 .env`

On a shared edge node where others can become the same service account,
`export` the two secrets in your session instead of keeping them in `.env` —
the scripts support both.

### Step 4: Configure secrets and properties

Follow [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) — it walks through
every component's properties step by step. The short version:

```bash
cp .env.template .env
# test-env requires exactly four values (guide §2):
#   KAFKA_TRUSTSTORE_PASSWORD  — from the Kafka team / Talend cv_kfk_* context
#   OAUTH_CLIENT_ID            — STS client id (ClientID header)
#   OAUTH_CLIENT_SECRET        — STS client secret (ClientSecret header)
#   API_PASSWORD               — STS user password (token request JSON body)
# MQ_PASSWORD is NOT needed for test-env (prod only).
```

Everything else has working test-env defaults carried over from the Talend
`.prm`; override a variable in `.env` only when a component test (below)
fails and the guide's per-component section says which value to fix:

- [ ] Guide §2 — the two required secrets set in `.env`
- [ ] Guide §3 — MQ values reviewed (defaults usually correct)
- [ ] Guide §4 — API base URL + OAuth values reviewed
- [ ] Guide §5 — Kafka truststore location exists; JAAS/keytab decided
- [ ] Guide §6 — HDFS namenode/principal/keytab reviewed

`.env` is gitignored and sourced automatically by the scripts under
`scripts/`. The test-env and prod profiles fail fast at startup if the
required secrets are missing.

### Step 5: Verify supporting files

- [ ] Kafka truststore exists at the configured `KAFKA_TRUSTSTORE_LOCATION`
- [ ] Kerberos keytab exists; verify with `klist -kt <keytab>`
- [ ] Log directory exists and is writable

### Step 6: Verify the HDFS layout matches what downstream expects

The bridge writes every wrapper flat into one landing directory:
`{base-path}/{eventId}.json` — no eventType/date subdirectories. The
consumer owns the file lifecycle: after processing it moves each file to
its own archive or error location (the bridge never moves or deletes
landed files).

- [ ] Consumer team confirms they read from the landing directory and
      handle the archive/error moves
- [ ] Note for consumers: if a file is moved away and MQ ever redelivers
      the same message (e.g. recovery), the bridge will re-land and
      re-notify it — consumers should dedupe on `eventId`
- [ ] Prod base path verified against the prod `.prm`:
      `application-prod.yml` currently defaults `HDFS_BASE_PATH` to
      `/test/oort/product/bluepcs/hive/csv` — a *test*-looking path in the
      prod profile. Confirm the real prod value and override via
      `HDFS_BASE_PATH` if it differs.

### Step 7: Validate connectivity (no messages consumed)

```bash
./scripts/validate-only.sh test-env    # exit 0 = ready
```

This checks MQ/Kafka/HDFS reachability and acquires a real OAuth token.
An SSL handshake error mentioning certificate/hostname means a broker
certificate does not match its hostname — fix the certificate; do not
disable hostname verification.

### Component-by-Component Testing (optional)

Before starting the full app, each part of the bridge can be exercised in
isolation against real infrastructure. Each command exits `0` on PASS, `1` on
FAIL, `2` on bad mode/args. Run in this order — cheapest/safest first:

```bash
./scripts/component-test.sh hdfs                          # write + checksum-verify + cleanup a scratch file
./scripts/component-test.sh api SPSH44PPOIMTO,2026-01-01  # one real enrichment call for a known plan
./scripts/component-test.sh kafka my-scratch-topic        # publish ONE marked message (pass a scratch topic!)
./scripts/component-test.sh mq                            # non-destructively browse the input queue
```

- `hdfs` PASS proves the app can create/rename/checksum/delete under the base path (write path works, and it cleans up after itself).
- `api` PASS proves OAuth + the REST enrichment call succeed and the wrapper's derived fields parse.
- `kafka` PASS proves the producer can connect and get an ack (writes a real, clearly-marked message — prefer a scratch topic).
- `mq` PASS proves the MQ connection + queue browse work; browsing consumes nothing (0 messages is still a PASS).

On any FAIL, look up the error in [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md):
each component section (§3 MQ, §4 API, §5 Kafka, §6 HDFS) explains what the
common failures mean and which property to fix; §10 is a quick index keyed by
error text.

### Step 8: Start safely, then enable consumption

```bash
./scripts/run-test-env.sh                      # listener OFF by default
curl localhost:8080/actuator/health            # confirm healthy
./scripts/run-test-env.sh --listener-enabled   # start consuming
```

### Step 9: Verify the first message end-to-end

- [ ] Log shows the `=== INCOMING MQ MESSAGE ===` block
- [ ] Wrapper JSON file appears in HDFS under the configured base path
- [ ] Claim-check notification arrives on the Kafka topic (small JSON with
      `hdfsPath`, `checksum`, `eventId`, `changeEventTypeName`)
- [ ] The `hdfsPath` in the notification points at the written file and the
      file's SHA-256 matches `checksum`
- [ ] MQ queue depth decreases

---

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

- **Parse failure (permanent):** raw payload is quarantined to the HDFS error directory
  (`bridge.hdfs.error-path`, default `<base-path>/errors`), a `MESSAGE_QUARANTINED` audit
  event is published, and the MQ message IS acknowledged — a bad message cannot block the
  queue. The message is only acked if the quarantine write succeeded; if HDFS is down the
  message stays on the queue and redelivery retries the quarantine.
- **Transient failure (enrichment/HDFS/Kafka):** MQ message NOT acknowledged → MQ will
  redeliver until the dependency recovers
- Duplicate Kafka publishes are acceptable (downstream deduplicates by event_id)
- HDFS writes are idempotent (file already exists = skip) — including quarantine writes
- Event ID is deterministic (SHA-256 of JMS Message ID)

### Poison Messages

Unparseable messages are handled by the quarantine flow above and cannot block the queue.
Backstops for OTHER repeating failures (e.g., a message that permanently fails enrichment):

1. **Queue-manager backout (preferred — preserves the message):** ask the MQ team to set a
   backout threshold and backout requeue queue on the input queue:
   `ALTER QLOCAL(<INPUT.QUEUE>) BOTHRESH(5) BOQNAME(<INPUT.QUEUE>.BACKOUT)`
   and grant the bridge user put authority on the backout queue.
2. **Application guard (fallback — discards the message):** set
   `bridge.mq.max-delivery-attempts` > 0. When `JMSXDeliveryCount` exceeds it, the bridge logs
   the full masked payload, publishes a `MESSAGE_DISCARDED` audit event, and acknowledges the
   message. The payload then survives only in logs and the audit topic — use this only when
   broker-side backout cannot be configured.

Note: redelivery also occurs during transient outages (HDFS/Kafka down). Backout thresholds
should be sized so an outage does not exhaust them within seconds; the app guard has the same
caveat — do not set `max-delivery-attempts` low if outages are expected.

### Important Properties

| Property | Default | Description |
|----------|---------|-------------|
| `bridge.validate-only` | false | Run validation and exit |
| `bridge.mq.listener-enabled` | false | Enable MQ message consumption |
| `bridge.mq.concurrency` | 1 | Number of concurrent listeners |
| `bridge.mq.max-delivery-attempts` | 0 | Poison-message guard; 0 = disabled (use queue BOTHRESH instead) |
| `bridge.hdfs.error-path` | `<base-path>/errors` | Quarantine dir for unparseable messages (`HDFS_ERROR_PATH`) |
| `bridge.audit.publisher` | kafka | `kafka` = audit topic; `log` = app log fallback until the audit topic exists (`AUDIT_PUBLISHER`) |
| `bridge.api.retry-attempts` | 3 | API call retry attempts |
| `bridge.api.timeout-seconds` | 30 | API call timeout |
