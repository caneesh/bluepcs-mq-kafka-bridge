# Audit Process

How the bridge records what happened to every message it touches: which events exist, who
emits them, where they go, what guarantees they carry, and how to consume them.

## Purpose and design principles

The audit stream is the bridge's per-message paper trail. Every MQ message produces a
sequence of audit events as it moves through the pipeline, so that for any eventId you can
answer: *was it received, did it parse, was it enriched, where did the payload land in HDFS,
was the notification published, and if not — why not, and where is the payload now?*

Three principles govern the design:

1. **Audit must never break processing.** Every emit path is wrapped so that an audit
   failure (Kafka down, serialization error, topic missing) is logged and swallowed —
   the message pipeline continues. Audit is strictly best-effort.
2. **Audit is asynchronous.** Emitters call `publishAsync(...)`; the pipeline never waits
   on an audit write.
3. **Everything correlates through `eventId`.** The same deterministic id
   (SHA-256 of the JMS message id) is the audit correlation key, the Kafka message key on
   both the notification and audit topics, and the HDFS filename — one id joins the audit
   trail, the published notification, and the payload file.

## Event flow

```
IBM MQ message
   │
   ▼
MqMessageListener ──(non-text type)──────────────► MESSAGE_DISCARDED, ack
   │              ──(delivery count > max)───────► MESSAGE_DISCARDED, ack   (poison guard)
   ▼
BridgeOrchestrator.process()
   │ MESSAGE_RECEIVED
   ├─ parse ──────────── ok ─► MESSAGE_PARSED
   │      └─ fail ─► quarantine write to HDFS errors/
   │                    ├ ok ──► MESSAGE_QUARANTINED, ack
   │                    └ fail ► PROCESSING_FAILED, no ack → redelivery
   ├─ enrich ─────────── ok ─► ENRICHMENT_COMPLETED
   │      └─ fail ─────────► ENRICHMENT_FAILED, no ack → redelivery
   ├─ HDFS write ─────── ok ─► HDFS_WRITE_COMPLETED   (or HDFS_WRITE_SKIPPED if the
   │      │                     file already exists — a redelivered message)
   │      └─ fail ─────────► HDFS_WRITE_FAILED, no ack → redelivery
   ├─ Kafka publish ──── ok ─► KAFKA_PUBLISH_COMPLETED
   │      └─ fail ─────────► KAFKA_PUBLISH_FAILED, no ack → redelivery
   ├─ (any other RuntimeException) ─► PROCESSING_FAILED, no ack → redelivery
   ▼
PROCESSING_COMPLETED, ack
```

A fully successful message therefore emits **six** bridge events: `MESSAGE_RECEIVED`,
`MESSAGE_PARSED`, `ENRICHMENT_COMPLETED`, `HDFS_WRITE_COMPLETED`,
`KAFKA_PUBLISH_COMPLETED`, `PROCESSING_COMPLETED`.

### Consumer stages (end-to-end trail)

The bridge's `PROCESSING_COMPLETED` only means "handed to Kafka" — it says nothing about
the downstream DStream job actually loading the data into the Hive product tables. The
consumer job closes the loop by emitting its own events to the **same audit topic**, keyed
by the same `eventId` (see `docs/consumer/ConsumerAuditEmitter.scala`):

```
BluepcsPMMPLusConsumer (per micro-batch, claim-check messages only)
   │
   ├─ resolve hdfsPath ── ok ──► CLAIM_CHECK_RESOLVED
   │        └─ file missing ──► CLAIM_CHECK_SKIPPED   (already-processed duplicate)
   ├─ BluepcsPMMPLusProcessor loads batch to Hive product tables
   │        ├─ ok (before offset commit) ──► HIVE_LOAD_COMPLETED  per eventId
   │        └─ fail ────────────────────► HIVE_LOAD_FAILED     per eventId, batch retries
```

A fully successful end-to-end message therefore shows **eight** events for one `eventId`:
the six bridge events plus `CLAIM_CHECK_RESOLVED` and `HIVE_LOAD_COMPLETED`.
`HIVE_LOAD_COMPLETED` — not `PROCESSING_COMPLETED` — is the true terminal state of the
pipeline. Legacy Talend inline messages carry no `eventId` and emit no consumer events.

Consumer-emitted events differ from bridge events in these fields: `bridgeEventId` is null,
`transactionId` is null, and `metadata` carries `kafkaPartition`/`kafkaOffset`/`batchTime`.
The gap between `PROCESSING_COMPLETED` and `HIVE_LOAD_COMPLETED` is monitored by
`scripts/audit-gap-check.sh` (Control-M, see DEPLOYMENT_CHECKLIST.md).

## Event catalog

| Event type | Emitted by | When |
|---|---|---|
| `MESSAGE_RECEIVED` | `BridgeOrchestrator` | Processing starts (before parse) |
| `MESSAGE_PARSED` | `BridgeOrchestrator` | JSON payload parsed successfully |
| `ENRICHMENT_COMPLETED` | `BridgeOrchestrator` | REST enrichment returned |
| `ENRICHMENT_FAILED` | `BridgeOrchestrator` | Enrichment failed after client-side retries (incl. token-acquisition failures) |
| `HDFS_WRITE_COMPLETED` | `BridgeOrchestrator` | Wrapper written to the landing dir (temp → checksum-verify → rename) |
| `HDFS_WRITE_SKIPPED` | `BridgeOrchestrator` | Target file already existed — idempotent redelivery |
| `HDFS_WRITE_FAILED` | `BridgeOrchestrator` | HDFS write/verify/rename failed |
| `KAFKA_PUBLISH_COMPLETED` | `BridgeOrchestrator` | Claim-check notification published (description carries the offset) |
| `KAFKA_PUBLISH_FAILED` | `BridgeOrchestrator` | Publish failed/timed out |
| `PROCESSING_COMPLETED` | `BridgeOrchestrator` | Whole pipeline succeeded; the MQ message will be acked |
| `PROCESSING_FAILED` | `BridgeOrchestrator` | Quarantine write failed after a parse failure, **or** an unexpected `RuntimeException` escaped the typed handlers (`UNEXPECTED_ERROR`) |
| `MESSAGE_QUARANTINED` | `BridgeOrchestrator` | Unparseable payload durably preserved in the HDFS error dir; message acked |
| `MESSAGE_DISCARDED` | `MqMessageListener` | Poison guard exceeded (`bridge.mq.max-delivery-attempts`) **or** unsupported (non-text) message type; message acked |
| `RECOVERY_STARTED` / `RECOVERY_FAILED` | `RecoveryService` | Only when `bridge.recovery.enabled=true` (off by default; ledger-based) |
| `CLAIM_CHECK_RESOLVED` | DStream consumer | HDFS payload fetched and checksum-verified |
| `CLAIM_CHECK_SKIPPED` | DStream consumer | HDFS file missing → treated as already-processed duplicate |
| `HIVE_LOAD_COMPLETED` | DStream consumer | Batch containing this eventId loaded into the Hive product tables (emitted before offset commit) |
| `HIVE_LOAD_FAILED` | DStream consumer | Batch load failed for this eventId's batch; batch will retry |
| `DUPLICATE_DETECTED`, `RECOVERY_COMPLETED`, `RECONCILIATION_*` | — | **Reserved, never emitted today.** Consumers must tolerate them but should not expect them. |

## Event schema

`AuditEvent` serializes to JSON with these fields:

| Field | Meaning | Nullability |
|---|---|---|
| `auditEventId` | Random UUID, unique **per audit event** | never null |
| `eventId` | SHA-256 of the JMS message id (payload hash if the id was null) — the cross-system correlation key; also the Kafka key | null on listener-emitted discard events |
| `bridgeEventId` | Random UUID per **processing attempt** — distinguishes redeliveries of the same eventId | null on listener-emitted events |
| `originalMqMessageId` / `messageId` | The JMS message id (both fields carry the same value) | null (JMS permits a null id) |
| `transactionId` | From the parsed payload | null before/without a successful parse |
| `eventType` | One of the catalog above | never null |
| `description` | Human-readable stage summary (e.g. HDFS path, Kafka offset) | usually set |
| `errorMessage` | Failure detail on `*_FAILED` / discard / quarantine events | null on success events |
| `metadata` | Map of extras — used by discard events (`deliveryCount`, `maxDeliveryAttempts`, `sourceQueue`, `correlationId`, `messageClass`) | empty map when unused |
| `timestamp` | Event creation time (UTC instant) | never null |

## Publishers

Emitters depend on the `AuditPublisher` interface; the active implementation is chosen at
startup:

- **`SafeAuditPublisher`** (`@Primary`, all non-local profiles) — a decorator that wraps
  every call in a catch-all so audit can never break processing, and selects the delegate
  from `bridge.audit.publisher`:
  - `kafka` (default) → `KafkaAuditPublisher`
  - `log` → `LoggingAuditPublisher` — the **bring-up fallback** for when the Kafka audit
    topic does not exist yet. On a secured cluster a missing topic surfaces as
    `TopicAuthorizationException` and every event would be silently dropped; `log` mode
    keeps the trail in the application log instead. A WARN is logged at startup when this
    mode is active.
- **`KafkaAuditPublisher`** — serializes the event to JSON and sends to
  `bridge.kafka.audit-topic`, keyed by `eventId` (falling back to `auditEventId`), so all
  events for one message land in one partition, in order. `publishAsync` is fire-and-forget
  with a logging callback; both paths swallow their own failures too (defense in depth
  under the Safe wrapper).
- **`LoggingAuditPublisher`** — writes each event as a JSON line to the SLF4J logger named
  `audit` at INFO. There is no dedicated appender configuration in the repo, so these lines
  go wherever the default logging goes; route the `audit` logger to its own file in the
  deployment's logging config if separation is needed. In the **local profile** this is the
  only publisher (the Safe/Kafka publishers are `@Profile("!local")`).

## Delivery guarantees — what the audit stream is and is not

- **Best-effort, not guaranteed.** Events are fired asynchronously and all failures are
  swallowed by design. A JVM crash, a Kafka outage, or a full producer buffer can lose
  audit events while message processing continues (or vice versa). The audit stream is an
  operational trail, **not** a transactional ledger — do not build financial/compliance
  reconciliation on it alone.
- **Duplicates are normal.** The pipeline is at-least-once: a redelivered MQ message
  re-emits its whole event sequence with the **same `eventId`** but a **new
  `bridgeEventId`** (and new `auditEventId`s). `HDFS_WRITE_SKIPPED` in a sequence is the
  telltale of a redelivery that found the payload already written.
- **Correlated failure caveat.** The audit publisher shares the Kafka cluster (and
  producer settings) with the main notification topic. When Kafka is down, the
  `KAFKA_PUBLISH_FAILED` audit for that outage will usually fail too — during a Kafka
  outage, the application log is the only reliable trail.
- **Ordering** is per-partition on the audit topic; since the key is `eventId`, one
  message's events arrive in order. Events for different messages have no cross-ordering
  guarantee.

## Configuration reference

| Property | Default | Meaning |
|---|---|---|
| `bridge.audit.publisher` (`AUDIT_PUBLISHER`) | `kafka` | `kafka` or `log` (bring-up fallback) |
| `bridge.kafka.audit-topic` (`KAFKA_AUDIT_TOPIC`) | `bridge-audit` (`MOCK01_BRIDGE_AUDIT_TEST` in test-env) | Audit topic name |
| `bridge.kafka.audit-timeout-seconds` (`KAFKA_AUDIT_TIMEOUT_SECONDS`) | `5` | Sync-publish wait (the async path does not wait) |
| `bridge.audit.enabled` | `true` | Present in config; **not currently enforced in code** — emitters always fire |
| `bridge.audit.hive-table` (`AUDIT_HIVE_TABLE`) | env-specific | Downstream Hive table name (consumer-side concern; the bridge only carries it in config) |

## Consuming the audit stream

The Kafka→Hive audit consumer is a buildable subproject at
[`audit-hive-consumer/`](../audit-hive-consumer/) (Spark DStream job, Hive DDL, README).
The DStream job's consumer-stage instrumentation is the drop-in reference
[consumer/ConsumerAuditEmitter.scala](consumer/ConsumerAuditEmitter.scala).

- **Join key:** `eventId` → the Kafka notification (`eventId` field / message key) and the
  HDFS payload (`<landing>/<eventId>.json`, quarantines at `<landing>/errors/<eventId>.json`).
- **Dedup:** treat (`eventId`, `eventType`) as repeatable; use `bridgeEventId` to group one
  processing attempt, `auditEventId` as the unique row key.
- **Terminal states per message:** `HIVE_LOAD_COMPLETED` (end-to-end success),
  `PROCESSING_COMPLETED` (bridge success, consumer stage pending or not yet instrumented),
  `MESSAGE_QUARANTINED` (permanent parse failure, payload preserved), `MESSAGE_DISCARDED`
  (poison/unsupported — payload preserved **only** in the application log line for poison
  discards), or no terminal event yet (still failing/redelivering — look for the latest
  `*_FAILED`).
- **ACLs:** three principals touch the audit topic — the bridge (produce), the DStream
  consumer job (produce, for its stage events), and the audit Hive consumer group (consume).
- **Monitoring signals:** silence on the audit topic during expected-traffic hours means
  consumption stopped (see the "Running 24/7" monitoring table in
  `DEPLOYMENT_CHECKLIST.md`); a growing rate of `*_FAILED` events for a single `eventId`
  indicates a redelivery loop worth intervening on.

## Known limitations (as of this writing)

- `bridge.audit.enabled` is config-only; there is no code path that disables emission.
- The reserved event types (`DUPLICATE_DETECTED`, `RECONCILIATION_*`, `RECOVERY_COMPLETED`)
  are declared but unused; the ledger-based recovery/reconciliation subsystems that would
  emit them are disabled by default and not wired to the main pipeline.
- Poison-discard events preserve the payload only in an application-log ERROR line (masked),
  not in HDFS — see the MQ listener review notes about masking and discard durability
  before relying on this path in production.
