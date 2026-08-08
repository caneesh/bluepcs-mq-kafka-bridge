# Audit, Balance and Control (ABC)

How the bridge proves that what MQ handed it is what landed downstream — and
records that proof.

| Pillar | Question it answers | Implementation |
|---|---|---|
| **Audit** | *What happened to this message?* | 13 bridge + 4 consumer event types → Kafka audit topic → `bluepcs.bridge_audit_event` (see [AUDIT.md](AUDIT.md)) |
| **Balance** | *Do the stage totals tie out for this window?* | `scripts/abc-balance-check.sh` — 6 control-total equations |
| **Control** | *Was the check run, what did it say, and who was told?* | `bluepcs.bridge_control_run` + Control-M exit codes + Dynatrace |

Two checks, deliberately different questions — run both:

- **Gap check** (`audit-gap-check.sh`) — *existence*, per `event_id`: **which** message
  is missing. Finds the needle.
- **Balance check** (`abc-balance-check.sh`) — *arithmetic*, per window: **whether the
  totals tie out**. Finds that a needle exists at all, including losses the gap check
  cannot see (e.g. a message that never emitted a terminal event *and* aged past the
  gap check's lookback).

## The balance equations

Each equation subtracts the *legitimate drains* at the stage where they occur, rather
than expecting naive equality. Counts are `COUNT(DISTINCT event_id)` over
`bridge_audit_event_deduped` — redeliveries re-emit the same deterministic `event_id`,
so distinct-counting correctly collapses them to the one message they represent.

| # | from → to | expected | tolerance |
|---|---|---|---|
| 1 | `MESSAGE_RECEIVED` → `MESSAGE_PARSED` | received − quarantined(`PARSE_ERROR`) | exact |
| 2 | `MESSAGE_PARSED` → `ENRICHMENT_COMPLETED` | parsed − quarantined(`ENRICHMENT_ERROR`) | exact |
| 3 | `ENRICHMENT_COMPLETED` → `HDFS_WRITE_COMPLETED`+`_SKIPPED` | enriched | exact |
| 4 | HDFS written → `KAFKA_PUBLISH_COMPLETED` | hdfs_written | exact |
| 5 | `KAFKA_PUBLISH_COMPLETED` → `PROCESSING_COMPLETED` | kafka_published | exact |
| 6 | `PROCESSING_COMPLETED` → `HIVE_LOAD_COMPLETED` | completed − `CLAIM_CHECK_SKIPPED` | `ABC_TOLERANCE_PCT_HIVE_LOAD` (2%) |

Why each drain term sits where it does:

- **Quarantines** leave the funnel at a specific stage. The stage is identified by the
  `errorCode` metadata key (`PARSE_ERROR` / `ENRICHMENT_ERROR`) written by
  `BridgeOrchestrator`; rows predating that key fall back to description matching.
- **`CLAIM_CHECK_SKIPPED`** is the consumer reporting "the HDFS file was already gone —
  this is a redelivery of something I already loaded". Expected behaviour, not loss.
- **`MESSAGE_DISCARDED` is *not* a drain from `MESSAGE_RECEIVED`.** Both discard paths
  (non-`TextMessage` and poison-threshold) run in `MqMessageListener` *before* the
  orchestrator emits `MESSAGE_RECEIVED`, so those messages never enter the audited
  funnel and carry a null `event_id`. They are reported as a separate INFO row,
  counted as rows rather than distinct ids.
- **Equation 6 is the only tolerant one**, because the consumer batches (default 300s)
  and can legitimately lag the window edge. `ABC_WINDOW_LAG_MINUTES` (default 30) must
  exceed that batch interval plus its Hive write time.

## Reading the verdict: the sign of the variance matters

`variance = expected − actual`.

| Sign | Meaning | Status |
|---|---|---|
| `0` | Ties out | **PASS** |
| `> 0` | Fewer messages downstream than upstream — messages may have been lost | **FAIL** (`POSSIBLE_LOSS`) beyond tolerance |
| `< 0` | *More* messages downstream than upstream | **WARN** (`AUDIT_LOSS`) — never FAIL |

A negative variance is arithmetically impossible for real message flow: the bridge
cannot publish more messages than it received. It therefore proves that the **audit
stream** lost events — the publisher drops events during its 60-second failure cooldown
(`KafkaAuditPublisher`) — and not that data was lost. Treating that as a data-loss
alarm would train operators to ignore the check.

**Diagnostic signature:** a mid-funnel audit gap appears as an *adjacent FAIL+WARN
pair* — a positive variance at equation *N* and a negative one at *N+1*. A genuine loss
appears as a positive variance at one equation with the downstream equations balancing.

## Control store

`bluepcs.bridge_control_run` (DDL: `audit-hive-consumer/hive/bridge_control_run.ddl`),
partitioned by `run_dt`. One row per equation per execution, written on **every** run —
including clean ones — so "did last Tuesday 14:00 balance?" is answerable months later
instead of requiring a Control-M job-history archaeology dig.

Trend query — failures by equation over the last week:

```sql
SELECT equation_no, stage_from, stage_to, status, count(*) AS runs
FROM bluepcs.bridge_control_run
WHERE run_dt >= date_sub(current_date, 7)
GROUP BY equation_no, stage_from, stage_to, status
ORDER BY equation_no, status;
```

Windows where anything failed:

```sql
SELECT window_start, equation_no, expected_count, actual_count, variance, reason_code, detail
FROM bluepcs.bridge_control_run
WHERE run_dt >= date_sub(current_date, 7) AND status = 'FAIL'
ORDER BY window_start;
```

## Exit codes and runbook

| Exit | Meaning | Control-M | Action |
|---|---|---|---|
| 0 | All equations PASS | green | none |
| 1 | At least one FAIL — possible loss | **red, page** | runbook below |
| 2 | WARN only — audit-stream loss or within-tolerance drift | notify | check broker health; data is probably fine |
| 3 | Could not evaluate (config/query error, or the control write failed) | red | the *check* is broken, not necessarily the data |

**FAIL on equations 1-5** — a message left one bridge stage and never reached the next.
The bridge stages are in-process and synchronous, so this is either real loss or audit
loss. Confirm which: if the adjacent downstream equation shows a negative variance,
it's audit loss (see the diagnostic signature above). If not, take the `event_id`s from
the gap check for the same window and trace them; check `errors/` in HDFS and the
application log for the window.

**FAIL on equation 6** — the bridge finished but the consumer did not report loading.
Usual causes, in order: consumer job down or lagging (check the Spark job and its
`@@@ AUDIT->HIVE batch written` markers); `ABC_WINDOW_LAG_MINUTES` set below the
consumer's batch interval; consumer audit emission failing while loads succeed (check
for `@@@ AUDIT OFFSET COMMIT FAILED`).

**Exit 3** — read the Hive error in the job sysout. Most common: the control table or
the deduped view does not exist yet (run the DDLs), or `HIVE_CMD` is wrong for the
environment.

## Honest limitations

- **This balances over a best-effort audit stream.** Audit is explicitly not a
  transactional ledger ([AUDIT.md](AUDIT.md)); both bridge and consumer emitters drop
  events during failure cooldowns. The framework **detects** loss; it cannot by itself
  **prove the absence** of loss.
- The independent corroboration is MQ's own statistics (`MSGDEQD` vs distinct
  `MESSAGE_RECEIVED`) — the one source that cannot lie. Not automated; see
  [RECONCILIATION_PLAN.md](RECONCILIATION_PLAN.md).
- Downstream **product-table** verification (does each loaded message produce rows in
  raw/curated/gold?) is out of scope here; config names are reserved in
  RECONCILIATION_PLAN.md ("check 4").
- Amount balancing beyond counts is partially in place: `payloadBytes`, `bytesWritten`
  and `checksum` are now recorded in `metadata_json`, but no equation compares them
  yet.

## Setup

```bash
# once per environment, in order
hive -f audit-hive-consumer/hive/bridge_audit_event.ddl     # if not already present
hive -f audit-hive-consumer/hive/bridge_balance_views.ddl
hive -f audit-hive-consumer/hive/bridge_control_run.ddl

./scripts/abc-balance-check.sh --dry-run   # prints window + queries, writes nothing
./scripts/abc-balance-check.sh             # first real run
```

Configuration lives in `.env` (`ABC_*`, plus the shared `HIVE_CMD` / `AUDIT_GAP_TABLE`) —
see `.env.template`.
