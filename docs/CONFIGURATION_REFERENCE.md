# Configuration reference: both bridges

Every property either bridge reads, what it defaults to, and which ones a deployment must
supply. `CONFIGURATION_GUIDE.md` is the walkthrough for a first deployment; this is the
complete list to check an existing one against.

## How a value reaches the application

```
environment variable  ->  ${VAR:default} in the active profile's YAML  ->  the property
```

Each bridge is its own JVM with its **own deploy directory and its own `.env`**. Nothing is
shared at runtime between them: same variable name in two `.env` files means two
independent values.

```
/prod/gold/integration/product/bluepcs-bridge/        <- PMM+ (deployed today)
    .env                                              mode 600, never in git
    mq-kafka-bridge/target/mq-kafka-bridge-*.jar
/prod/gold/integration/product/bluepcs-pmm-bridge/    <- PMM (new)
    .env                                              its own file, PMM_* values
    pmm-request-template.xml                          the large XML request
    mq-pmm-bridge/target/mq-pmm-bridge-*.jar
```

A missing value with no default fails at startup with
`Could not resolve placeholder '<NAME>'`. A present-but-wrong value is caught by the
startup validator, which lists every problem at once and refuses to start.

---

## 1. PMM+ bridge (`mq-kafka-bridge`) — already in production

**Nothing in your current `.env` has to change.** The three new variables all default to
the behaviour you want, and the nine obsolete ones are ignored if left in place.

### 1.1 New since the deployed version

| Variable | Property | Default | What it does |
|---|---|---|---|
| `API_PLAN_ID_MISMATCH` | `bridge.api.plan-id-mismatch` | `reject` | `reject`: an enrichment response whose `marketingPlanIdentifier` differs from the one the MQ notification asked for is quarantined instead of landed and published. `warn` restores the previous log-only behaviour. Set it to `warn` only if your gateway is known to reformat identifiers |
| `AUDIT_FILE_FALLBACK` | `bridge.audit.file-fallback` | `true` | Writes any audit event Kafka refuses (send failure, or the cooldown after one) to `<LOG_DIRECTORY>/bridge-application.log-audit.jsonl`, so an audit outage leaves evidence. Leave on |
| `AUDIT_ENABLED` | `bridge.audit.enabled` | `true` | Master audit switch, now overridable by environment. Turning it off blinds the gap and balance checks |

### 1.2 Already in your `.env`, but now load-bearing

| Variable | Property | Why it matters now |
|---|---|---|
| `HDFS_ARCHIVE_PATH` | `bridge.hdfs.archive-path` | The bridge now **reads** this path, not just the cleanup script. On redelivery it looks for the message's file in the landing directory and then here; a file found in either is republished from rather than re-fetched from the API. It must point at wherever the consumer moves processed files. Default `<HDFS_BASE_PATH>/archive` |

### 1.3 Obsolete — safe to leave, better to delete

`HBASE_ZOOKEEPER_QUORUM`, `HBASE_ZOOKEEPER_PORT`, `HBASE_ZNODE_PARENT`,
`HBASE_LEDGER_TABLE`, `LEDGER_PATH`, `RECOVERY_ENABLED`, `RECOVERY_MAX_RETRIES`,
`RECOVERY_INTERVAL_MS`, `RECONCILIATION_ENABLED`.

The HBase and ledger/recovery/reconciliation subsystems were removed; unknown variables are
ignored, so leaving them breaks nothing.

### 1.4 The full set it reads

Mandatory (no default — startup fails without them):

| Variable | Property |
|---|---|
| `KAFKA_TRUSTSTORE_PASSWORD` | `bridge.kafka.truststore-password` |
| `OAUTH_CLIENT_ID` | `bridge.security.client-id` |
| `OAUTH_CLIENT_SECRET` | `bridge.security.client-secret` |
| `API_PASSWORD` | `bridge.security.password` |

Everything else has a production default in `mq-kafka-bridge/src/main/resources/application-prod.yml`
and only needs an entry when the environment differs from it: `MQ_HOST`, `MQ_PORT`,
`MQ_QUEUE_MANAGER`, `MQ_CHANNEL`, `MQ_QUEUE`, `MQ_USERNAME`, `MQ_PASSWORD`,
`MQ_SSL_ENABLED`, `MQ_SSL_CIPHER_SUITE`, `MQ_SSL_TRUSTSTORE_LOCATION`,
`MQ_SSL_TRUSTSTORE_PASSWORD`, `MQ_CONCURRENCY`, `MQ_RECEIVE_TIMEOUT`, `MQ_LOG_PAYLOAD`,
`MQ_MAX_DELIVERY_ATTEMPTS`, `MQ_REDELIVERY_BACKOFF_MS`, `MQ_REDELIVERY_BACKOFF_MAX_MS`,
`REQUIRE_LISTENER_ENABLED`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`, `KAFKA_AUDIT_TOPIC`,
`KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_JAAS_CONFIG`,
`KAFKA_JAAS_CONFIG_PATH`, `KAFKA_KERBEROS_SERVICE_NAME`, `KAFKA_TRUSTSTORE_LOCATION`,
`KAFKA_TRUSTSTORE_TYPE`, `KAFKA_KEYSTORE_LOCATION`, `KAFKA_KEYSTORE_PASSWORD`,
`KAFKA_KEY_PASSWORD`, `KAFKA_REQUEST_SIZE`, `KAFKA_TIMEOUT_SECONDS`,
`KAFKA_AUDIT_TIMEOUT_SECONDS`, `KAFKA_DELIVERY_TIMEOUT_MS`, `KAFKA_REQUEST_TIMEOUT_MS`,
`KAFKA_MAX_BLOCK_MS`, `HDFS_NAMENODE`, `HDFS_BASE_PATH`, `HDFS_ERROR_PATH`,
`HDFS_ARCHIVE_PATH`, `HDFS_REPLICATION`, `HDFS_KERBEROS_ENABLED`,
`HDFS_KERBEROS_PRINCIPAL`, `HDFS_KERBEROS_KEYTAB`, `HDFS_NAMENODE_PRINCIPAL`,
`HDFS_RESOURCEMANAGER_PRINCIPAL`, `HADOOP_CONF_DIR`, `API_BASE_URL`, `API_TIMEOUT_SECONDS`,
`API_RETRY_ATTEMPTS`, `API_RETRY_DELAY_MS`, `API_PLAN_ID_MISMATCH`, `OAUTH_TOKEN_URL`,
`OAUTH_SCOPE`, `API_USERNAME`, `AUDIT_PUBLISHER`, `AUDIT_ENABLED`, `AUDIT_FILE_FALLBACK`,
`AUDIT_HIVE_TABLE`, `LOG_DIRECTORY`, `LOG_LEVEL`, `SERVER_ADDRESS`, `MONITOR_ENABLED`,
`MONITOR_HEALTH_URL`, `MONITOR_BACKLOG_AGE_MINUTES`, `MONITOR_BACKLOG_MAX_FILES`.

---

## 2. PMM bridge (`mq-pmm-bridge`) — new deployment

### 2.1 Must be set — no defaults anywhere

These identify the pipeline. They deliberately have no fallback, so a `.env` copied from
the PMM+ bridge cannot make this one consume the PMM+ queue or write into its landing
directory.

| Variable | Property | What to supply |
|---|---|---|
| `PMM_MQ_QUEUE` | `bridge.mq.queue` | The PMM queue name |
| `PMM_HDFS_BASE_PATH` | `bridge.hdfs.base-path` | Root of the landing tree. Files land at `<root>/<yyyy-MM-dd>/<HH>/<eventId>.xml` |
| `PMM_API_URL` | `bridge.pmm.api.url` | Full URL the request is POSTed to |
| `PMM_TEMPLATE_LOCATION` | `bridge.pmm.template.location` | `file:/path/to/pmm-request-template.xml`. The large XML request, containing `${value1}` and `${value2}` in element text or quoted attribute values only |
| `PMM_XPATH_VALUE1` | `bridge.pmm.xpath.value1` | XPath 1.0 selecting the first value. Must match **exactly one** node |
| `PMM_XPATH_VALUE2` | `bridge.pmm.xpath.value2` | XPath for the second value. Same rule |
| `PMM_OAUTH_TOKEN_URL` | `bridge.security.token-url` | STS endpoint for this service. May be the same URL as the PMM+ bridge's |
| `OAUTH_CLIENT_ID` | `bridge.security.client-id` | Same STS credentials as PMM+ |
| `OAUTH_CLIENT_SECRET` | `bridge.security.client-secret` | |
| `API_PASSWORD` | `bridge.security.password` | STS user password |
| `KAFKA_TRUSTSTORE_PASSWORD` | `bridge.kafka.truststore-password` | Only when `AUDIT_PUBLISHER=kafka` (the default) |

Quoting note: XPath expressions contain `[`, `/`, `@` and quotes — single-quote them in
`.env`, e.g. `PMM_XPATH_VALUE1='/PmmMessage/Product/ProductIdentifier'`.

### 2.2 Infrastructure — set only if it differs from the PMM+ defaults

The PMM profile inherits the PMM+ production defaults for the shared estate. Set these in
the PMM `.env` **only** where the PMM feed differs:

| Variable | Default | Set it when |
|---|---|---|
| `MQ_HOST`, `MQ_PORT`, `MQ_QUEUE_MANAGER`, `MQ_CHANNEL` | the PMM+ queue manager | The PMM queue is on a different queue manager (still unconfirmed) |
| `MQ_USERNAME`, `MQ_PASSWORD` | `hdpapp`, empty | Different credentials, or the QM requires MQCSP auth |
| `HDFS_NAMENODE`, `HDFS_KERBEROS_*`, `HADOOP_CONF_DIR` | the PMM+ cluster and principal | A different cluster or service account |
| `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_AUDIT_TOPIC`, `KAFKA_*` | the PMM+ brokers and `DAS_PRODUCT_BRIDGE_AUDIT` | Both bridges publish audit to the **same topic on purpose**; the balance checks separate them by `metadata.pipeline` |
| `OAUTH_SCOPE`, `API_USERNAME` | as PMM+ | Different STS account |
| `PMM_HDFS_ERROR_PATH` | `<PMM_HDFS_BASE_PATH>/errors` | Quarantine should live elsewhere |

### 2.3 Tuning — defaults are production-ready

| Variable | Default | Meaning |
|---|---|---|
| `PMM_SERVER_PORT` | `8081` | Actuator port. Must not collide with the PMM+ bridge's 8080 |
| `PMM_WINDOW_HOURS` | `4` | Folder rotation. Must divide 24 |
| `PMM_WINDOW_ZONE` | `UTC` | Zone the window boundaries are computed in |
| `PMM_DATE_PATTERN` | `yyyy-MM-dd` | Date directory format |
| `PMM_API_TIMEOUT_SECONDS` | `60` | Connect/read/write timeout for the POST |
| `PMM_API_RETRY_ATTEMPTS` / `PMM_API_RETRY_DELAY_MS` | `3` / `1000` | Retry budget with linear backoff |
| `PMM_API_CONTENT_TYPE` / `PMM_API_ACCEPT` | `application/xml` | Sent verbatim; use `text/xml` if the gateway insists |
| `PMM_XPATH_NAMESPACE_AWARE` | `false` | Leave off and use `//*[local-name()='X']` for namespaced XML |
| `PMM_XPATH_ALLOW_EMPTY` | `false` | Whether an empty extracted value is acceptable |
| `MQ_MAX_MESSAGE_BYTES` | `67108864` | A larger `BytesMessage` is refused before allocation |
| `MQ_MAX_DELIVERY_ATTEMPTS` | `0` (off) | Poison guard. Prefer the queue manager's `BOTHRESH`/`BOQNAME` |
| `MQ_CONCURRENCY` | `1` | Must stay 1: the HDFS check/write/rename sequence is not atomic |
| `AUDIT_PUBLISHER` | `kafka` | `log` writes the audit stream to the log file instead |
| `AUDIT_FILE_FALLBACK` | `true` | Events Kafka refuses go to the audit file |
| `PMM_MONITOR_BACKLOG_WINDOWS` | `2` | Windows the monitor scans for orphaned `*.xml.tmp` |
| `MONITOR_HEALTH_URL` | `http://localhost:8081/actuator/health` | Must match `PMM_SERVER_PORT` |
| `LOG_DIRECTORY`, `LOG_LEVEL`, `SERVER_ADDRESS` | as PMM+ | |

### 2.4 Starting point for the PMM `.env`

```bash
# --- identity (no defaults: startup fails without these)
PMM_MQ_QUEUE=
PMM_HDFS_BASE_PATH=
PMM_API_URL=
PMM_TEMPLATE_LOCATION=file:/prod/gold/integration/product/bluepcs-pmm-bridge/pmm-request-template.xml
PMM_XPATH_VALUE1='/PmmMessage/Product/ProductIdentifier'
PMM_XPATH_VALUE2='/PmmMessage/Product/EffectiveDate'
PMM_OAUTH_TOKEN_URL=

# --- secrets (same STS account as the PMM+ bridge)
OAUTH_CLIENT_ID=
OAUTH_CLIENT_SECRET=
API_PASSWORD=
KAFKA_TRUSTSTORE_PASSWORD=

# --- this JVM
BRIDGE_PROFILE=prod
PMM_SERVER_PORT=8081

# --- supervision and Control-M (shell only, not Spring properties)
BRIDGE_APP=mq-pmm-bridge
HEALTH_URL=http://localhost:8081/actuator/health/liveness
MONITOR_HEALTH_URL=http://localhost:8081/actuator/health
ABC_PIPELINE=pmm
AUDIT_GAP_PIPELINE=pmm

# --- only if the PMM feed differs from the PMM+ estate
# MQ_HOST=
# MQ_PORT=
# MQ_QUEUE_MANAGER=
# MQ_CHANNEL=
```

---

## 3. Variables the scripts read (not Spring properties)

These are shell-level and belong in the same `.env`.

| Variable | Used by | Default | Notes |
|---|---|---|---|
| `BRIDGE_PROFILE` | every run/supervision script | `test-env` | Set to `prod` on a production node |
| `BRIDGE_APP` | every script | `mq-kafka-bridge` | Set to `mq-pmm-bridge` in the PMM `.env`; selects the jar and namespaces the keepalive's state files |
| `HEALTH_URL` | `bridge-keepalive.sh`, watchdog | `…:8080/actuator/health/liveness` | Point at the right port per bridge |
| `HIVE_CMD` | audit scripts | `hive -S -e` | Beeline example in the script header |
| `AUDIT_GAP_TABLE` | audit scripts | `bluepcs.bridge_audit_event` | |
| `AUDIT_GAP_PIPELINE` | `audit-gap-check.sh` | `bridge` | `pmm` for the PMM bridge; the Hive-load check is skipped for it, since that pipeline has no consumer stage |
| `AUDIT_GAP_SILENCE_MINUTES` | `audit-gap-check.sh` | `0` (off) | **New.** Exit 5 when no message has been received for this long. Turn it on for a queue with steady traffic: otherwise an audit outage looks exactly like an idle bridge |
| `AUDIT_GAP_THRESHOLD_MINUTES`, `AUDIT_GAP_GRACE_MINUTES`, `AUDIT_GAP_LOOKBACK_DAYS`, `AUDIT_GAP_RESULT_LIMIT` | `audit-gap-check.sh` | `120`, `30`, `2`, `50` | |
| `ABC_PIPELINE` | `abc-balance-check.sh` | `bridge` | `pmm` runs the PMM equations instead |
| `ABC_EMPTY_WINDOW` | `abc-balance-check.sh` | `warn` | **New.** A window that received nothing is a warning, not a silent pass. `pass` for legitimately idle queues |
| `ABC_CONTROL_TABLE`, `ABC_WINDOW_HOURS`, `ABC_WINDOW_LAG_MINUTES`, `ABC_TOLERANCE_PCT_HIVE_LOAD` | `abc-balance-check.sh` | `bluepcs.bridge_control_run`, `1`, `30`, `2` | |
| `HDFS_ARCHIVE_PATH`, `LANDING_RETENTION_DAYS`, `ARCHIVE_RETENTION_DAYS` | `hdfs-landing-cleanup.sh` | `<base>/archive`, `7`, `30` | PMM+ only |
| `PMM_HDFS_ARCHIVE_PATH`, `PMM_LANDING_RETENTION_DAYS`, `PMM_ARCHIVE_RETENTION_DAYS` | `pmm-hdfs-cleanup.sh` | `<base>/archive`, `7`, `30` | PMM only. Never point `hdfs-landing-cleanup.sh` at the PMM tree: it is flat and `.json`-only |

---

## 4. Checking a deployment

```bash
# PMM+ (from its own directory)
scripts/validate-only.sh prod

# PMM (BRIDGE_APP comes from its .env; the prefix works too)
BRIDGE_APP=mq-pmm-bridge scripts/validate-only.sh prod
```

Validate-only runs the readiness checks and exits 0 only if all pass. The PMM+ bridge runs
four (MQ, Kafka, HDFS, STS); the PMM bridge runs six — the same four plus
`PMM_API_REACHABLE` and `PMM_TEMPLATE`, which proves the template loads and renders with
no placeholder left behind.

A configuration that is wrong rather than missing is reported by the startup validator,
which lists every problem before refusing to start.
