# Configuration Guide — Step by Step

This guide walks through every component's configuration: what each property
means, where its value comes from, and how to verify it works before moving
to the next component.

---

## 1. How configuration works (read this first)

**One rule explains everything:** every setting in the YAML files looks like

```yaml
host: ${MQ_HOST:teenuslika02.app.test.hscint.net}
```

which means: *use the environment variable `MQ_HOST` if it is set, otherwise
use the default after the colon*. A placeholder with **no default** —
`${KAFKA_TRUSTSTORE_PASSWORD}` — means the variable is **required** and the
app refuses to start without it.

**Profiles** pick which YAML file supplies the defaults:

| You run with | Defaults come from | Intended for |
|---|---|---|
| `--spring.profiles.active=local` | `application-local.yml` | your laptop, all mocks, no real infra |
| `--spring.profiles.active=test-env` | `application-test-env.yml` | the TEST environment (values from the old Talend `.prm`) |
| `--spring.profiles.active=prod` | `application-prod.yml` | production |

**Where to put your values:** the `.env` file in the project root. Copy the
template once, then edit:

```bash
cp .env.template .env
nano .env
```

Every script under `scripts/` sources `.env` automatically. One caveat: a
value in `.env` **overrides** anything exported in your shell — delete a line
from `.env` if you want a shell `export` to win.

**The good news:** for `test-env`, almost everything already has a correct
default (hosts, ports, queue names, topics — all carried over from the Talend
`.prm` file). You only *must* supply the secrets in step 2.

---

## 2. The four values you MUST set (test-env)

| Variable | What it is | Where to get it |
|---|---|---|
| `KAFKA_TRUSTSTORE_PASSWORD` | password of the JKS truststore at `/prod/gold/integration/conf/product/common/kafka_truststore_test.jks` | Kafka platform team, or the old Talend job's context (`cv_kfk_*`) |
| `OAUTH_CLIENT_ID` | STS client id (32-char hex) — sent as the `ClientID` header to both the token endpoint and the enrichment API | your working Insomnia token request / security team |
| `OAUTH_CLIENT_SECRET` | STS client secret — sent as the `ClientSecret` header | same source |
| `API_PASSWORD` | password for the STS user (`a6193139`) — JSON body of the token request | same source |

Put them in `.env`:

```bash
KAFKA_TRUSTSTORE_PASSWORD=<value>
OAUTH_CLIENT_ID=<value>
OAUTH_CLIENT_SECRET=<value>
API_PASSWORD=<value>
```

`MQ_PASSWORD` is **not** needed for test-env — the test queue manager
authenticates by user id / channel auth. The prod queue manager may work the
same way: set `MQ_PASSWORD` only if it requires MQCSP password auth
(`MQRC_NOT_AUTHORIZED (2035)` on connect is the tell).

If the app starts without a placeholder-resolution error, this step is done.

---

## 3. Component: IBM MQ (message consumption)

**What it does:** connects to the queue manager and consumes planNotification
messages from the input queue.

| Variable | test-env default | Meaning / where it comes from |
|---|---|---|
| `MQ_HOST` | `teenuslika02.app.test.hscint.net` | queue manager host (Talend `cv_MQ_host`) |
| `MQ_PORT` | `1412` | listener port |
| `MQ_QUEUE_MANAGER` | `MQGPT1` | queue manager name |
| `MQ_CHANNEL` | `QDA.APP.SVRCONN` | server-connection channel |
| `MQ_QUEUE` | `QDP.BPCS.MALESODA.UAT.BLOB` | the input queue the bridge reads |
| `MQ_USERNAME` | `bluepcsapp` | service account user id |
| `MQ_PASSWORD` | *(empty — not needed in test)* | only set if the QM starts requiring MQCSP auth |
| `MQ_LOG_PAYLOAD` | `false` | set `true` only for debugging; payloads may contain PHI/PII |
| `MQ_CONCURRENCY` | `1` | listener threads — **must stay 1** (HDFS write race) |

You normally set **none of these** for test-env; the defaults are the real
test values.

**Verify:**

```bash
./scripts/component-test.sh mq
```

PASS = connected and browsed the queue (nothing consumed). It also tells you
how many messages are waiting.

**If it fails:** `MQRC_NOT_AUTHORIZED (2035)` → the QM requires a password
after all — get it and set `MQ_PASSWORD`. `MQRC_HOST_NOT_AVAILABLE (2538)` →
wrong host/port or firewall. `MQRC_UNKNOWN_CHANNEL_NAME` → wrong `MQ_CHANNEL`.

---

## 4. Component: REST enrichment API + OAuth

**What it does:** for each MQ message, gets an OAuth token, then calls
`GET {API_BASE_URL}/{marketingPlanIdentifier}/{effectiveDate}` with
`Authorization: Bearer`, `ClientID`, and `ClientSecret` headers.

| Variable | test-env default | Meaning / where it comes from |
|---|---|---|
| `API_BASE_URL` | `https://api-gateway-ssl-svc.test.hcscint.net/eps_product_catalog/v2/plans` | the plans resource (from your working Insomnia request) |
| `OAUTH_TOKEN_URL` | `https://t-sso-sg-uat-svc.test.hcscint.net/sts/v5/jwt_token_internal` | STS token endpoint (verified via working curl) |
| `OAUTH_CLIENT_ID` | **required, no default** | sent as the `ClientID` HEADER on token + API requests |
| `OAUTH_CLIENT_SECRET` | **required, no default** | sent as the `ClientSecret` HEADER |
| `OAUTH_SCOPE` | `oob openid profile roles permissions` | sent as the `scope` HEADER on the token request |
| `API_USERNAME` | `a6193139` | `username` in the token request's JSON body |
| `API_PASSWORD` | **required, no default** | `password` in the token request's JSON body |
| `API_TIMEOUT_SECONDS` | `30` | HTTP timeout |

**The STS token request shape** (not standard OAuth2 — form-encoding gets 415):

```
POST {OAUTH_TOKEN_URL}
ClientID: <OAUTH_CLIENT_ID>          ← headers, not body fields
ClientSecret: <OAUTH_CLIENT_SECRET>
scope: oob openid profile roles permissions
Content-Type: application/json

{"username": "a6193139", "password": "<API_PASSWORD>"}
```

The token is read from `access_token`/`accessToken`/`token`/`jwt`/`id_token`
in the response, and expiry from `expires_in`, the JWT's own `exp` claim, or a
one-hour default — in that order.

**Verify** (use any plan id + effective date you know exists — take one from
a real MQ message or from Insomnia):

```bash
./scripts/component-test.sh api SPSH44PPOIMTO,2026-01-01
```

PASS = token acquired and the API returned 2xx; the output shows the derived
`changeEventTimeStamp` / `changeEventTypeName`.

**If it fails:** `Token refresh failed with status 401` → wrong
`OAUTH_CLIENT_SECRET` (or `API_USERNAME`/`API_PASSWORD` needed). `API client
error: 401/403` → token OK but gateway rejected `ClientID`/`ClientSecret`
headers. `404` → plan id or date doesn't exist (try a different one) or wrong
`API_BASE_URL`. SSL error → the JVM doesn't trust the gateway certificate —
ask which CA signs it.

---

## 5. Component: Kafka (publish)

**What it does:** publishes the small claim-check notification (with the
HDFS path) to the topic, over SASL_SSL with Kerberos (GSSAPI).

| Variable | test-env default | Meaning / where it comes from |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `teenuslika02..04.app.test.hscint.net:9093` | broker list (Talend `cv_kfk_brokers`) |
| `KAFKA_TOPIC` | `MOCK01_PMM_PRODUCT_COLA_TEST` | destination topic |
| `KAFKA_AUDIT_TOPIC` | `MOCK01_BRIDGE_AUDIT_TEST` | audit events topic |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_SSL` | don't change unless the brokers change |
| `KAFKA_SASL_MECHANISM` | `GSSAPI` | Kerberos |
| `KAFKA_KERBEROS_SERVICE_NAME` | `kafka` | broker service principal name |
| `KAFKA_TRUSTSTORE_LOCATION` | `/prod/gold/integration/conf/product/common/kafka_truststore_test.jks` | must EXIST on the edge node |
| `KAFKA_TRUSTSTORE_PASSWORD` | **required, no default** | see step 2 |
| `KAFKA_JAAS_CONFIG_PATH` | *(unset)* | path to a JAAS file for Kerberos login, if the cluster requires one |

Kerberos for Kafka GSSAPI typically needs a JAAS config naming the keytab.
If publishing fails with a Kerberos login error, create a `kafka-jaas.conf`:

```
KafkaClient {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=true
  keyTab="/etc/security/keytabs/e4193139.keytab"
  principal="e4193139@HSCTEST.NET"
  storeKey=true;
};
```

and set `KAFKA_JAAS_CONFIG_PATH=/path/to/kafka-jaas.conf` in `.env`.

**Verify** (writes ONE clearly-marked message — prefer a scratch topic):

```bash
./scripts/component-test.sh kafka            # uses the configured topic
./scripts/component-test.sh kafka MY_TOPIC   # or a scratch topic
```

PASS = broker acknowledged; the output shows partition/offset.

**If it fails:** SSL handshake / certificate error → truststore wrong, or the
broker cert doesn't match its hostname (fix the cert — do not disable
hostname verification). `SaslAuthenticationException` / GSS errors → JAAS
config or keytab problem (check `klist -kt`). Timeout with no error →
firewall or wrong `KAFKA_BOOTSTRAP_SERVERS`.

---

## 6. Component: HDFS (write)

**What it does:** writes the full wrapper JSON flat into one landing
directory — `{HDFS_BASE_PATH}/{eventId}.json` — authenticated via Kerberos
keytab. The consumer moves processed files to its own archive/error
locations; the bridge never moves or deletes landed files.

| Variable | test-env default | Meaning / where it comes from |
|---|---|---|
| `HDFS_NAMENODE` | `hdfs://TSTODPHA` | the test HDFS load balancer (prod: `hdfs://PRDODPHA`); a plain DNS name, NameNode default port 8020 |
| `HDFS_BASE_PATH` | `/test/oort/product/bluepcs/hive/csv` | the flat landing directory — every wrapper lands as `{eventId}.json` directly under it (see checklist Step 6) |
| `HDFS_KERBEROS_ENABLED` | `true` | leave on for the cluster |
| `HDFS_KERBEROS_PRINCIPAL` | `e4193139@HSCTEST.NET` | the service account principal |
| `HDFS_KERBEROS_KEYTAB` | `/etc/security/keytabs/e4193139.keytab` | must EXIST and be readable; check `klist -kt <keytab>` |
| `HDFS_NAMENODE_PRINCIPAL` | `nn/_HOST@HSCTEST.NET` | cluster-side principal pattern |
| `HADOOP_CONF_DIR` | *(unset — not normally needed)* | only if cluster-specific settings ever need to be loaded from `core-site.xml`/`hdfs-site.xml` |

**Verify** (writes, checksums, and deletes a scratch file — leaves nothing):

```bash
./scripts/component-test.sh hdfs
```

**If it fails:** `GSSException` / `Login failure` → keytab path, keytab
contents (`klist -kt`), or principal spelling. `Permission denied` → the
service account can't write under `HDFS_BASE_PATH`. `UnknownHostException:
TSTODPHA` → the load balancer name isn't resolvable from this machine
(DNS/network issue) — check with `nslookup TSTODPHA`. Connection refused →
the LB isn't forwarding port 8020.

---

## 7. Optional components (defaults are fine to start)

| Variable | Default | Only change if... |
|---|---|---|
| `RECOVERY_ENABLED` | `false` | you want the scheduled recovery loop (needs a ledger) |
| `RECONCILIATION_ENABLED` | `false` | you want reconciliation (needs the `file-ledger` profile) |
| `AUDIT_HIVE_TABLE` | `bluepcs_mq_listener` | audit table name changes |
| `LOG_DIRECTORY` | `/datalakedm/prod/gold/integration/logs/product/bluepcs/talend` | logs should go elsewhere (dir must be writable) |

---

## 8. Complete .env example for the test environment

```bash
# --- Required secrets ---
KAFKA_TRUSTSTORE_PASSWORD=<from Kafka team / Talend context>
OAUTH_CLIENT_ID=<STS client id, from the working token request>
OAUTH_CLIENT_SECRET=<STS client secret>
API_PASSWORD=<STS user password for a6193139>

# --- Only if the defaults don't match your environment ---
# MQ_HOST=teenuslika02.app.test.hscint.net
# MQ_QUEUE=QDP.BPCS.MALESODA.UAT.BLOB
# API_BASE_URL=https://api-gateway-ssl-svc.test.hcscint.net/eps_product_catalog/v2/plans
# KAFKA_TRUSTSTORE_LOCATION=/prod/gold/integration/conf/product/common/kafka_truststore_test.jks
# KAFKA_JAAS_CONFIG_PATH=/home/<you>/kafka-jaas.conf
# HDFS_KERBEROS_KEYTAB=/etc/security/keytabs/e4193139.keytab
# HDFS_NAMENODE=hdfs://TSTODPHA

# --- Debug switches (leave off normally) ---
# MQ_LOG_PAYLOAD=true
```

---

## 9. The full sequence, start to finish

```bash
cp .env.template .env && nano .env        # step 2: the two secrets
./scripts/validate-only.sh test-env       # everything reachable? token OK?
./scripts/component-test.sh hdfs          # section 6
./scripts/component-test.sh api <planId>,<date>   # section 4
./scripts/component-test.sh kafka <scratch-topic> # section 5
./scripts/component-test.sh mq            # section 3
./scripts/run-test-env.sh                 # app up, listener off, check health
./scripts/run-test-env.sh --listener-enabled      # go live
```

Each script tells you PASS or FAIL and each section above tells you what a
failure means. Work top to bottom; when all four component tests pass, the
full pipeline has no untested integration left.

---

## 10. Quick troubleshooting index

| Startup/run error contains... | Look at |
|---|---|
| `Could not resolve placeholder 'KAFKA_TRUSTSTORE_PASSWORD'` | step 2 — secret missing from `.env` |
| `Could not resolve placeholder 'OAUTH_CLIENT_ID'` / `'OAUTH_CLIENT_SECRET'` / `'API_PASSWORD'` | step 2 |
| `Token refresh failed with status: 415` | section 4 — token endpoint URL is wrong (an old form-encoded endpoint); use the v5 STS URL |
| `MQRC_NOT_AUTHORIZED` / `2035` | section 3 — MQ needs a real password |
| `Token refresh failed with status 401` | section 4 — OAuth credentials |
| `API client error: 401` (token was OK) | section 4 — ClientID/ClientSecret headers |
| `SSL handshake` / `certificate` (Kafka) | section 5 — truststore or broker cert hostname |
| `SaslAuthenticationException` / `GSS` (Kafka) | section 5 — JAAS/keytab |
| `Login failure for e4193139` (HDFS) | section 6 — keytab/principal |
| `Permission denied` on a path | section 6 — HDFS write permission |
| SSL truststore file does not exist | section 5 — `KAFKA_TRUSTSTORE_LOCATION` path on this machine |

## 11. The PMM bridge (`mq-pmm-bridge`) — second application

The PMM (Product Message Model, canonical XML) feed is a **separate bootable
application** built from the same repository: `mq-pmm-bridge/target/mq-pmm-bridge-*.jar`.
It runs as its own JVM with its own deploy directory, `.env` and systemd unit
(`deploy/mq-pmm-bridge.service`), and listens on port **8081** by default so it can
share an edge node with the PMM+ bridge.

Pipeline: MQ (XML) → two XPath values → XML request template → `POST` with the STS
token in `Authorization` → raw XML response written to
`<PMM_HDFS_BASE_PATH>/<yyyy-MM-dd>/<HH>/<eventId>.xml` (a new folder every 4 hours,
`HH` ∈ 00,04,08,12,16,20 in UTC) → audit events (no Kafka notification).

### 11.1 Values you MUST set (no defaults in `prod` / `test-env`)

| Variable | Property | What it is |
|---|---|---|
| `PMM_MQ_QUEUE` | `bridge.mq.queue` | The PMM queue. Deliberately not `MQ_QUEUE`: a copied PMM+ `.env` can never make this bridge consume the PMM+ queue |
| `PMM_HDFS_BASE_PATH` | `bridge.hdfs.base-path` | Root of the windowed landing tree |
| `PMM_API_URL` | `bridge.pmm.api.url` | Full POST URL of the web service |
| `PMM_TEMPLATE_LOCATION` | `bridge.pmm.template.location` | `file:/path/to/template.xml` — the large XML request with `${value1}` / `${value2}` placeholders (see `mq-pmm-bridge/src/main/resources/pmm/request-template.sample.xml`). Placeholders may sit only in element text or a quoted attribute value; one inside a CDATA section, comment or processing instruction, or any other `${…}` token, fails startup |
| `PMM_XPATH_VALUE1`, `PMM_XPATH_VALUE2` | `bridge.pmm.xpath.value1/2` | XPath 1.0 expressions selecting the two values from the MQ message; each must match **exactly one** node (an expression matching several is a parse failure — disambiguate with `(//Element)[1]`). Single-quote them in `.env`. For namespaced XML use `//*[local-name()='Element']` |
| `PMM_OAUTH_TOKEN_URL` | `bridge.security.token-url` | STS endpoint for the PMM service (same protocol and `OAUTH_CLIENT_ID/SECRET`, `API_USERNAME/PASSWORD` as the PMM+ bridge; may be the same URL) |

Shared infrastructure keeps the same variable names as sections 3–6 (`MQ_HOST`,
`MQ_QUEUE_MANAGER`, `MQ_CHANNEL`, `KAFKA_*` for the audit topic, `HDFS_NAMENODE`,
`HDFS_KERBEROS_*`, `OAUTH_CLIENT_ID`, …) and is set in the PMM bridge's own `.env`
— point them at a different queue manager if the PMM queue lives elsewhere.

### 11.2 Optional tuning

| Variable | Default | Meaning |
|---|---|---|
| `PMM_SERVER_PORT` | `8081` | Actuator/health port of this JVM |
| `PMM_WINDOW_HOURS` / `PMM_WINDOW_ZONE` | `4` / `UTC` | Folder rotation; hours must divide 24 |
| `PMM_HDFS_ERROR_PATH` | `<base>/errors` | Flat quarantine directory (`<eventId>.xml`) |
| `PMM_API_TIMEOUT_SECONDS`, `PMM_API_RETRY_ATTEMPTS`, `PMM_API_RETRY_DELAY_MS` | `60`, `3`, `1000` | Same retry/backoff semantics as section 4 |
| `PMM_API_CONTENT_TYPE` / `PMM_API_ACCEPT` | `application/xml` | Sent verbatim (use `text/xml` if the gateway insists) |
| `PMM_MONITOR_BACKLOG_WINDOWS` | `2` | Windows scanned by the monitor's backlog check (current + previous). The backlog is orphaned in-flight `*.xml.tmp` files older than `MONITOR_BACKLOG_AGE_MINUTES`; landed `.xml` files are read in place and never count |
| `MQ_MAX_MESSAGE_BYTES` | `67108864` | `BytesMessage` bodies above this are refused before allocation |
| `AUDIT_PUBLISHER` | `kafka` | `log` keeps the audit trail in `<LOG_DIRECTORY>/pmm-bridge-application.log-audit.jsonl` |

### 11.3 Running it

Every script accepts `BRIDGE_APP=mq-pmm-bridge` (put it in the PMM `.env` together
with `HEALTH_URL=http://localhost:8081/actuator/health/liveness` and
`MONITOR_HEALTH_URL=http://localhost:8081/actuator/health`):

```bash
BRIDGE_APP=mq-pmm-bridge scripts/validate-only.sh test-env     # adds PMM_API_REACHABLE + PMM_TEMPLATE checks
BRIDGE_APP=mq-pmm-bridge scripts/run-test-env.sh --listener-enabled --port 8081
BRIDGE_APP=mq-pmm-bridge scripts/monitor.sh                    # backlog check walks the windowed tree
scripts/pmm-hdfs-cleanup.sh --dry-run                          # retention on whole date directories
```

Local, no infrastructure: `PMM_LOCAL_SAMPLE_MESSAGE=docs/sample-pmm-message.xml
BRIDGE_APP=mq-pmm-bridge scripts/run-local.sh` pushes one sample through the pipeline
and writes `./data/hdfs/pmm/<date>/<HH>/<eventId>.xml`.

Startup fails fast (like section 1) when a `PMM_*` value is missing, an XPath does not
compile, the template is missing or contains an unknown or unsafely placed placeholder,
or the window size does not divide 24.

Downstream contract: readers of the window directories must select `*.xml` only. A
write in progress is visible as `<eventId>.<uuid>.xml.tmp` next to the landed files
until the atomic rename, and a crashed write leaves that temp file until
`pmm-hdfs-cleanup.sh` removes it. Do not change `PMM_WINDOW_HOURS` / `PMM_WINDOW_ZONE`
while messages are being redelivered: the window (and so the file path) of an
unacknowledged message would change and it would land a second copy.

