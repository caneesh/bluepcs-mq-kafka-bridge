# Production Readiness Review — BluePCS MQ→Kafka Bridge

Verified at commit `78e6d94`. Every claim below carries `file:line` evidence or is
explicitly marked as undeterminable from code.

---

## 1. Executive summary

The bridge is a well-built **at-least-once** integration with correct side-effect
ordering and no path that acknowledges an MQ message before the work is durable. Three
targeted reviews at current HEAD found no defect in the message-processing logic itself;
every genuine finding was in the **configuration and deployment surface around it**, and
the significant ones are now fixed (§4).

**Verdict: Ready with accepted risks — conditional on three go-live gates (§4) that
cannot be closed from this repository.**

## 2. Current architecture

MQ → parse → STS token (cached) → REST `GET` enrich → HDFS claim-check write → Kafka
notification → MQ ack. One JVM, one pipeline thread (`bridge.mq.concurrency=1`, pinned
because the HDFS exists/write/rename sequence is not atomic across writers). See
`docs/architecture.html`; the synchronous design was evaluated against a Kafka-buffered
alternative and deliberately retained (`docs/DECOUPLING_EVALUATION.md`).

## 3. Actual end-to-end delivery guarantee

**At-least-once. Not exactly-once, and exactly-once is nowhere claimed in the code.**

| Property | Determination | Evidence |
|---|---|---|
| Loss | None on any traced path — every failure either quarantines durably before acking or leaves the message unacked | `BridgeOrchestrator.java:224-247` |
| Duplication | Structural: the ack is a second, non-atomic commit after Kafka confirmation | `MqMessageListener.java:182-191` |
| REST side effect | **None to duplicate** — the enrichment call is a `GET` | `RestMarketingPlanApiClient.java:150-152` |
| HDFS | Idempotent by deterministic `eventId` + checksum verification | `HdfsSafePayloadWriter.java:73-74,142-155` |
| Kafka | **Duplicates are possible.** `enable.idempotence` dedupes producer-internal retries within one session only; it cannot suppress a re-send after MQ redelivery | `KafkaConfiguration.java:97` |
| Dedupe | Downstream consumers **must** dedupe on `eventId` (Kafka key + notification field). Nothing in this repo verifies they do | `KafkaNotificationFactory.java:57` |

**Failure matrix** (condensed; REST repeat is uniformly safe because it is a GET):

| Failure point | Persisted | Kafka dup? | MQ redelivers | Manual reconciliation |
|---|---|---|---|---|
| Before/during token retrieval | audits only | No | Yes | No |
| Before REST send / mid-flight | audits only | No | Yes | No |
| After REST ok, before HDFS | audits only | No | Yes | No |
| After HDFS rename, before Kafka send | wrapper file (checksum-verified) | No | Yes | Only if wedged (§6, risk 2) |
| **After Kafka send, before confirm** | file; record may or may not be committed | **Yes** | Yes | No *if* consumers dedupe |
| **After confirm, before ack** | everything | **Yes, guaranteed** | Yes | No |
| During ack | everything; ack lost | **Yes** | Yes | No |
| During shutdown | whatever stage completed | Possible | Yes | Audit gap possible |

## 4. Go-live gates (must be closed before production)

These cannot be resolved from this repository and require action by other teams.

| # | Gate | Why it blocks | Owner |
|---|---|---|---|
| **G1** | **Confirm `BOTHRESH`/`BOQNAME` are set on the input queue** — or set `MQ_MAX_DELIVERY_ATTEMPTS` as an in-app backstop | With the poison guard off by default (`application.yml:106`) there is **no terminal path** for a repeating non-quarantinable failure (HDFS outage, Kafka outage, failed quarantine write). With `concurrency=1`, one wedged message stalls the **entire** queue indefinitely | MQ admins + a config decision |
| **G2** | **Verify `JMSXDeliveryCount` actually increments** for this CLIENT_ACKNOWLEDGE + `session.recover()` combination | If it does not, both the in-app poison guard (`MqMessageListener.java:96`) **and** broker BOTHRESH are silently inoperative. This is the single highest-value UAT check | UAT against the real QM |
| **G3** | **Run a dependency scan** against the shipped jar | The stack is EOL across the board: Spring Boot 2.7.18, Spring 5.3.31, logback 1.2.12, snakeyaml 1.30, jackson-databind 2.13.5, **jackson-mapper-asl 1.9.13** (abandoned 2013, transitive via Hadoop), avro 1.7.7, jetty 9.4.53; plus Spark 2.4.8 / Scala 2.11.12 in `audit-hive-consumer`. No scanner was available here | Security team |

Two further items are strongly recommended rather than blocking: confirm downstream
consumers dedupe on `eventId`, and confirm the enrichment API returns byte-identical
responses for the same plan/date (see §6, risk 2).

## 5. Fixed during this review

| Severity | Finding | Fix |
|---|---|---|
| BLOCKER | Prod systemd unit ran the `test-env` profile with prod paths — test MQ/Kafka/HDFS defaults for anything not overridden, DEBUG logging, and app+audit logs to `${java.io.tmpdir}` | `deploy/mq-kafka-bridge.service` → `prod`; `application-test-env.yml` now sets `logging.file.name` |
| BLOCKER | `ENRICHMENT_FAILED` stopped being emitted for permanent failures, hiding them from the event type used to measure enrichment health | `BridgeOrchestrator` emits the failure fact *and* the quarantine disposition |
| MAJOR | `.gitignore` missed `prod.env`/`test-env.env`/`*.jks`/`*.keytab` — the exact names the templates tell operators to create | Coverage added with a `!*.env.template` negation |
| MAJOR | `bridge.mq.ssl.enabled` bound in one profile only → `MQ_SSL_ENABLED=true` was inert, channel came up plaintext, validator could not detect it | Moved to `application.yml` |
| MAJOR | Actuator published full health details (MQ/Kafka/HDFS endpoints) on `0.0.0.0` while a comment claimed loopback-only | `server.address: 127.0.0.1` |
| MAJOR | Unmasked STS response body logged by the readiness check | Truncated + `SecretMaskingUtil` |
| MAJOR | Three misleading doc claims — two made false by this session's own code changes | Corrected in `AUDIT.md`, `DEPLOYMENT_CHECKLIST.md` |

**Rejected after verification (recorded so it is not re-litigated):** a reported BLOCKER
that `local result="$(...)"` masks `$?` in `audit-gap-check.sh`, making its failure branch
dead. The script declares `local result` on line 137 and assigns on line 140 — the split
form, which propagates status correctly. Confirmed empirically: only the *combined* form
masks the status, and neither script uses it.

## 6. Accepted risks

Accepted by explicit decision, each with its compensating control.

| Risk | Compensating control |
|---|---|
| No circuit breaker | `concurrency=1` plus no-ack/redelivery-with-backoff is the breaker: a failing downstream stops the pipeline by construction |
| No rate limiter | `concurrency=1` **is** the rate limit — at most one in-flight request |
| `Retry-After` not honoured on 429 | 429 is retryable with bounded in-process attempts, then MQ redelivery with capped backoff |
| No Micrometer/business metrics | Control-M exit codes (monitor, gap check, balance check), the ABC control table, Dynatrace OneAgent. **Largest observability gap** — no sub-hour rate/latency signal |
| MQ-side control totals not automated | Documented manual `MSGDEQD` corroboration (`RECONCILIATION_PLAN.md`) |
| Product-table verification ("check 4") not implemented | Config names reserved; stage balance covers the bridge pipeline |
| **HDFS checksum-mismatch wedge** | If the API is not byte-stable, a post-HDFS failure wedges that message permanently (by design — it refuses to overwrite). The bridge adds no volatility of its own; risk is entirely upstream. Manual review per `HdfsSafePayloadWriter.java:147-150` |
| **Health stays UP through an MQ outage** | `isRunning()` is a container flag; a DMLC failing its 5s reconnect loop still reports UP. Caught only by MQ depth alarms, the hourly gap check, or the 30-min backlog check |
| **Shutdown budget mismatch** | Worst-case in-flight ≈ 300s vs `TimeoutStopSec=60` → SIGKILL is realistic. No loss or corruption (ordering guarantees hold), but a duplicate and an audit gap are possible |
| Duplicates are invisible to the ABC balance check | A redelivery re-emits every stage event, so the equations still tie out. Duplicate detection is a downstream responsibility |
| JMS metadata dropped | `JMSCorrelationID`, `sourceQueue`, delivery count and `JMSTimestamp` do not reach the audit trail, so a Kafka/HDFS record cannot be traced to the upstream producer's correlation id. Cheap to fix later: the `MESSAGE_RECEIVED` metadata map already reaches Hive with no DDL change |

## 7. Verification evidence

- `mvn -o verify` → **BUILD SUCCESS**: 366 unit + 56 integration tests, 0 failures,
  4 skips (pre-existing HBase ledger ITs).
- All 12 shell scripts pass `bash -n`; `abc-balance-check.sh --dry-run` renders correctly.
- No defaultless `${MQ_PASSWORD}` placeholders; no substring profile guards; the colliding
  deployable `test` profile is gone.
- **Tooling caveat:** `mvn -o clean verify` cannot run here (`maven-clean-plugin` is not in
  the offline repo) and a failed clean leaves **stale reports that read as a passing run**.
  Use `mvn -o verify` after `rm -rf target/*-reports`. `mvn test` alone does **not** run
  `*IT.java` — that gap hid two real defects for six days during this session.
- Sandbox runs JDK 21 while `pom.xml` targets 11 (enforcer has `fail=false`). Tests were
  **not** exercised on the production JDK.

## 8. Test and observability posture

25 canonical scenarios: 13 covered, 8 partial, 4 missing (token-endpoint timeout; Kafka
timeout with uncertain publication; crash after REST success; graceful shutdown), 3 N/A by
design (idempotency key — the call is a GET; circuit breaker; bounded queues).

Highest-value uncovered failure mode: **`KafkaEnvelopePublisher` `TimeoutException`** — a
premature timeout reports failure for a record that is usually delivered, and MQ redelivery
then guarantees a downstream duplicate. Also untested: the Scala audit consumer (no test
tree at all) and all shell scripts.

Operational readiness: **6/10** — strong health indicators and a well-reasoned
liveness/aggregate split, four independent alerting surfaces with disjoint exit codes, a
durable audit trail with a pinned wire contract and an ABC control store; held back by zero
business metrics, manual/lossy correlation (no MDC, eventId not in Kafka headers), and a
liveness signal that cannot see a wedged consumer thread.

## 9. Required before and after go-live

**Runbooks that exist:** config validation failure; MQ/Kafka/HDFS/OAuth connection
failures; quarantine review and replay; poison-message handling; HDFS lifecycle; ABC
balance FAIL per equation; gap-check exit routing; rollback; keepalive markers.

**Runbooks still missing:** Kafka broker outage (audit cooldown makes the resulting Hive
gap *expected* — the balance-check operator is not told this); `Existing file checksum
mismatch` wedge; plan-id mismatch warning; STS outage tolerance; planned-maintenance
shutdown; heap-dump triage.

**Alerts required:** balance check exit 1; gap check exit 1/2; monitor exit 1/2/3;
keepalive `START-FAIL`; MQ queue depth (owned by MQ admins — the definitive
not-consuming signal, since app health cannot see it).

**Rollback:** stop the unit, restore the previous jar, restart. State is external
(HDFS files, Kafka offsets, MQ queue) and the deterministic `eventId` makes replay
idempotent, so rollback needs no data repair.

## 10. Final statement

The bridge provides **at-least-once delivery with idempotent HDFS persistence and
deterministic event identity**. Duplicate Kafka records are possible by design at three
points and must be absorbed by downstream dedupe on `eventId`. Exactly-once is not
provided and is not claimed. Subject to gates G1–G3, this is fit for production.
