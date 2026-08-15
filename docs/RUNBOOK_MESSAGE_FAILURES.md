# Runbook: message failures, poison messages and redelivery loops

What the bridge does with a message that cannot be processed, and how to diagnose one that
is stuck. Every value here is quoted from the code it comes from — if you change a default,
update the reference.

## Symptom index

| What you see | Go to |
|---|---|
| API called on a repeating cycle (minutes) but little or no throughput | [§5 Redelivery loops](#5-diagnosing-a-redelivery-loop) |
| Queue depth stuck at N and not draining | [§5](#5-diagnosing-a-redelivery-loop) — one wedged message blocks everything (`concurrency=1`) |
| A message never reaches Hive | [§5](#5-diagnosing-a-redelivery-loop), then `scripts/audit-gap-check.sh` |
| `Existing file checksum mismatch` in the log | [§6 The checksum wedge](#6-the-checksum-mismatch-wedge-never-self-heals) — will **never** clear on its own |
| Messages disappearing / `MESSAGE_DISCARDED` events | [§3 Poison messages](#3-poison-message-handling) |
| Downstream got a record with a null plan id | [§4 API errors and "no data"](#4-api-errors-and-no-data) — a 200 with no plan id is a **success** |

## 1. The one rule that explains every outcome

Two dispositions, chosen by one flag:

- **Retryable** → the message is **not acknowledged** → MQ redelivers it. Nothing is lost.
- **Permanent** → the raw payload is written to the HDFS quarantine directory, **then** the
  message is acknowledged. It leaves the queue.

Because quarantine is followed by an ack, only a failure that can **never** succeed for
*this* message is allowed to be permanent (`RestMarketingPlanApiClient.java:174-180`,
`BridgeOrchestrator.java:250-280`). Environmental failures — auth outage, throttling,
timeouts, 5xx — stay on the queue deliberately.

The safety invariant: the ack happens **only if** the quarantine write succeeded. If HDFS
is also down, the result falls back to a plain failure and the message stays on the queue
(`BridgeOrchestrator.java:224-247`).

## 2. Where a message can end up

| Terminal state | Meaning | Audit event |
|---|---|---|
| Success | HDFS written + Kafka confirmed + acked | `PROCESSING_COMPLETED` |
| Quarantined | Permanent failure; payload preserved in HDFS `errors/`; acked | `MESSAGE_QUARANTINED` |
| Discarded | Poison guard tripped; payload preserved; acked | `MESSAGE_DISCARDED` |
| *(none)* | Retryable failure — still on the queue, will come back | `*_FAILED` |

## 3. Poison-message handling

**The guard is OFF by default.** `bridge.mq.max-delivery-attempts` defaults to `0`, and `0`
disables it (`MqMessageListener.java:50`, `application.yml:106`). No profile overrides it,
so as shipped **nothing is ever discarded** — the fallback is the queue manager's
`BOTHRESH`/`BOQNAME` backout, which is documented but not configured or verified by this
repo. See `docs/PRODUCTION_READINESS_REVIEW.md` gate G1.

When enabled (`MQ_MAX_DELIVERY_ATTEMPTS=N`), each delivery does this:

1. **Headers and the guard are evaluated before the body is read**
   (`MqMessageListener.java:90-96`). This ordering is deliberate: a message whose body
   cannot be converted (CCSID/format error) throws on *every* delivery, so a guard placed
   after the body read could never fire and that message would loop forever. The
   unreadable-body case is handled explicitly at `:100-110`.
2. **The payload is quarantined to HDFS first** (`:202`) — payloads may contain PHI and
   must not land in application logs. A masked, truncated log copy is used **only** if the
   quarantine write itself fails.
3. **A `MESSAGE_DISCARDED` audit event** is published with `deliveryCount`,
   `maxDeliveryAttempts`, `sourceQueue`, `correlationId` (`:209-224`).
4. **The message is acknowledged** (`:229`). Every step above is best-effort precisely so
   this is always reached — otherwise the poison message keeps blocking the queue.

Two things to know:

- If `JMSXDeliveryCount` cannot be read, the count **fails open to 1** and the guard
  silently never fires (logged at WARN). Confirming that the count actually increments on
  your queue manager is gate G2 in the readiness review.
- Discarded messages never emitted `MESSAGE_RECEIVED` (both discard paths run before the
  orchestrator), so they sit **outside** the audited funnel and carry a null `event_id`.
  The ABC balance check reports them as a separate INFO line rather than a drain.

## 4. API errors and "no data"

Retry policy: `bridge.api.retry-attempts` = 3, `bridge.api.timeout-seconds` = 30,
`bridge.api.retry-delay-ms` = 1000 applied linearly (`application-prod.yml:114-115`,
`application.yml:81`).

| Response | Retryable | Net effect |
|---|---|---|
| 5xx | Yes | 3 attempts, then redeliver |
| Connection timeout / IO error | Yes | 3 attempts, then redeliver |
| **401 / 403** | Yes | Forces **one** token refresh and retries (`RestMarketingPlanApiClient.java:96-111`); redelivers if it persists |
| **408 / 429** | Yes | Redeliver — throttling is environmental, not this message's fault |
| **400 / 404 / 422 …** | **No** | **Quarantine + ack** |

`ENRICHMENT_FAILED` is emitted for **every** enrichment failure, retryable or not;
`MESSAGE_QUARANTINED` is added on top for the permanent case
(`BridgeOrchestrator.java:258-280`).

### "No data" has three different outcomes

| Case | Outcome |
|---|---|
| Empty body on HTTP 200 | Non-retryable → **quarantined + acked** (`RestMarketingPlanApiClient.java:252-254`) |
| Malformed JSON on 2xx | Non-retryable → quarantined (retrying gets the same bytes) |
| **Valid JSON, no `marketingPlanIdentifier`** | **Treated as SUCCESS.** `marketingPlanId` is extracted null-safely and becomes `null`; processing continues to HDFS and Kafka and the record is published (`:265-271`) |

That last row is the one that surprises people: a 200 with an empty or unmatched
`PlanResponse` is **not** an error. The only signal is the plan-id cross-check WARN at
`BridgeOrchestrator.java:91-97`, which has no audit event, no metric and no alert behind
it. If a missing plan id should fail instead of publishing, that is a deliberate decision
to make — it is not current behaviour.

Note also that the empty-2xx-body case is arguably mis-classified: a truncated response is
a network artefact, not a property of the message, so this can quarantine a healthy
message on a gateway hiccup.

## 5. Diagnosing a redelivery loop

**Symptom:** the enrichment API is called on a repeating cycle while little or nothing is
published downstream — e.g. Dynatrace shows a call every ~5 minutes with one upstream
message.

**Why:** the bridge acks only after HDFS *and* Kafka succeed. A failure at any stage after
enrichment means no ack, MQ re-presents the message, and processing restarts from the top —
**including a fresh enrichment call**. One API call per cycle, forever. With
`concurrency=1`, that message also head-of-line-blocks the entire queue.

The cycle length identifies the failing stage:

| Failing stage | Cycle length | Budget |
|---|---|---|
| Kafka publish wait + redelivery backoff | ~3.5–4 min | `bridge.kafka.timeout-seconds`=190s (`application.yml:30`) + backoff capped at 30s (`:110-111`) |
| Slow/timing-out enrichment, then Kafka | ~5 min | 3 × 30s + linear backoff, then the above |
| Kafka metadata unavailable | ~1.5 min | `max-block-ms`=60000 (`:34`) |
| Broker reachable but not acking | ~2.5 min | `delivery-timeout-ms`=120000 (`:32`) |
| HDFS RPC retries | minutes, variable | Hadoop client defaults |

### Confirm it

A loop re-emits `MESSAGE_RECEIVED` with the **same deterministic `event_id`** every cycle,
so a count greater than 1 is conclusive:

```sql
-- 1. Is one message looping?
SELECT event_id, count(*) AS deliveries, max(event_timestamp) AS last_seen
FROM bluepcs.bridge_audit_event
WHERE event_dt >= date_sub(current_date, 1) AND event_type = 'MESSAGE_RECEIVED'
GROUP BY event_id ORDER BY deliveries DESC LIMIT 5;

-- 2. Which stage is it dying at?
SELECT event_timestamp, event_type, error_message
FROM bluepcs.bridge_audit_event
WHERE event_dt >= date_sub(current_date, 1) AND event_id = '<id from query 1>'
ORDER BY event_timestamp DESC LIMIT 20;
```

`scripts/audit-gap-check.sh` already detects this as its **stuck** check (exit code 2):
`MESSAGE_RECEIVED` with no terminal state past the grace period.

In the application log:

```bash
grep -E 'Redelivery: JMSXDeliveryCount=|backing off|_FAILED' <log>
```

A climbing delivery count confirms the loop; the `*_FAILED` line names the stage.

## 6. The checksum-mismatch wedge (never self-heals)

```
Existing file checksum mismatch for message <id>: expected <a> but found <b>;
refusing to accept or overwrite — manual review required
```

(`HdfsSafePayloadWriter.java:147-149`)

**What happened:** the message wrote its wrapper file successfully on an earlier attempt,
failed later (Kafka, or a crash), and on redelivery the enrichment API returned *different
bytes* for the same plan and effective date. The writer refuses to overwrite, because
silently replacing a file that downstream may already have consumed is worse than stopping.

**This will loop forever until a human acts.** Steps:

1. Read the existing file at the path in the exception and compare it with a fresh
   `GET {baseUrl}/{planId}/{effectiveDate}` — identify which field differs.
2. If the difference is immaterial (a volatile timestamp or ordering in the API response),
   the upstream API is not byte-stable for the same input. That is the root cause; record
   it, because it will recur for every message that fails after the HDFS write.
3. To clear the specific message: move the existing file aside (do not delete until the
   difference is understood), then let the redelivery re-write it.
4. Confirm recovery: the next delivery should produce `HDFS_WRITE_COMPLETED` (or
   `HDFS_WRITE_SKIPPED`) followed by `PROCESSING_COMPLETED`.

## 7. Clearing a stuck message — options and trade-offs

| Option | Effect | Cost |
|---|---|---|
| **Fix the failing stage** (broker, HDFS, gateway) | The message completes normally on the next delivery | Nothing lost — always prefer this |
| **Enable the poison guard** (`MQ_MAX_DELIVERY_ATTEMPTS=N`) | After N attempts the payload is quarantined to HDFS and acked; the queue unblocks | If the root cause is an outage rather than a bad message, this **discards a healthy message** (recoverable from quarantine, but it leaves the pipeline) |
| **Queue-manager backout** (`BOTHRESH`/`BOQNAME`) | The QM moves the message to a backout queue after N backouts | Preserves the message off-queue; needs MQ admin action and is not verified by this repo |
| **Stop the listener** (`bridge.mq.listener-enabled=false`) | Halts consumption entirely | Buys time to investigate; the backlog grows on the queue |

Identify the failing stage (§5) **before** choosing — the right action differs, and enabling
the guard during a Kafka outage would quarantine perfectly good messages.

## Related

- `docs/sample-mq-message.json` — a valid input message, with the fields the parser requires
- `docs/AUDIT.md` — the event catalogue these queries read
- `docs/AUDIT_BALANCE_CONTROL.md` — stage balance equations and their runbook
- `docs/PRODUCTION_READINESS_REVIEW.md` — gates G1/G2 (poison path, delivery count) and the
  accepted-risk register
- `DEPLOYMENT_CHECKLIST.md` — quarantine review/replay and poison-message configuration
