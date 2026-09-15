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

The window selects a **cohort**: the messages whose `MESSAGE_RECEIVED` falls inside it.
Every later stage of those messages is then counted wherever in time it happened (up
to a day after the window), so a message received at 09:59 and completed at 10:01 is
one message in the 09:00 window, never a loss in two. Stages are **per-event flags**
over `bridge_audit_event_deduped`: a message "reached" a stage if it has at least one
such event. Redeliveries re-emit stages for the same deterministic `event_id` and
collapse to the one message they represent, and sets that overlap (a message both
loaded and later skipped) can no longer cancel against a different message's loss.

PMM+ funnel (`ABC_PIPELINE=bridge`, the default):

| # | from → to | expected | tolerance |
|---|---|---|---|
| 1 | `MESSAGE_RECEIVED` → `MESSAGE_PARSED` | received − quarantined(`PARSE_ERROR`) − poison-discarded | exact |
| 2 | `MESSAGE_PARSED` → landed (`HDFS_WRITE_COMPLETED` or `_SKIPPED`) | parsed − quarantined(`ENRICHMENT_ERROR`) | exact |
| 3 | landed → `KAFKA_PUBLISH_COMPLETED` | landed | exact |
| 4 | `KAFKA_PUBLISH_COMPLETED` → `PROCESSING_COMPLETED` | published | exact |
| 5 | `PROCESSING_COMPLETED` → `HIVE_LOAD_COMPLETED` | completed | `ABC_TOLERANCE_PCT_HIVE_LOAD` (2%) |
| 6 | `CLAIM_CHECK_SKIPPED` without any `HIVE_LOAD_COMPLETED` | 0 | exact, **FAIL** when > 0 |

`ENRICHMENT_COMPLETED` and `CLAIM_CHECK_SKIPPED` counts are printed as INFO rows.
Equation 2 lands on "landed" rather than on `ENRICHMENT_COMPLETED` because a
redelivery that resumes from an already-landed file never calls the API again (see
RUNBOOK §6a); the landed flag is what every parsed, non-quarantined message must reach.

PMM funnel (`ABC_PIPELINE=pmm`):

| # | from → to | expected | tolerance |
|---|---|---|---|
| P1 | `MESSAGE_RECEIVED` → `MESSAGE_PARSED` | received − quarantined(`PARSE_ERROR`) − poison-discarded | exact |
| P2 | `MESSAGE_PARSED` → landed | parsed − quarantined(`API_ERROR`) | exact |
| P3 | landed → `PROCESSING_COMPLETED` | landed | exact |

`API_CALL_COMPLETED` is an INFO row: a redelivery resolved by the pre-check lands
without a web-service call. There is no consumer stage for PMM.

Why each drain term sits where it does:

- **Quarantines** leave the funnel at a specific stage, identified by the `errorCode`
  metadata key (`PARSE_ERROR` / `ENRICHMENT_ERROR` / `API_ERROR`); rows predating that
  key fall back to description matching.
- **Poison discards** (`MESSAGE_DISCARDED` with an `event_id` and `errorCode=POISON`)
  are the terminal state of a message the listener gave up on after N deliveries; they
  are subtracted in equation 1 because such a message never parsed. Discards **without**
  an `event_id` (unsupported message type) never entered the funnel and are an INFO row.
- **`CLAIM_CHECK_SKIPPED` is never a substitute for a load.** It means the consumer
  found no file. After a successful load it is a benign duplicate redelivery; without a
  load it is exactly the loss this check exists for (equation 6), and the gap check lists
  the same message as `SKIPPED-WITHOUT-LOAD`.
- **Equation 5 is the only tolerant one**, because the consumer batches (default 300s)
  and can legitimately lag the window edge. `ABC_WINDOW_LAG_MINUTES` (default 30) must
  exceed that batch interval plus its Hive write time.
- **A window that received nothing** is reported as `NO_DATA` with exit 2 (WARN) unless
  `ABC_EMPTY_WINDOW=pass`: an empty audit table during an audit outage looks identical
  to an idle bridge, and a silent PASS would hide the outage. The gap check has the same
  guard as `AUDIT_GAP_SILENCE_MINUTES`, off by default.

## Two pipelines on one topic

The PMM bridge (`mq-pmm-bridge`) publishes to the same audit topic and table. Its
events carry `metadata.pipeline = 'pmm'`; PMM+ events carry no key. Both scripts
therefore filter on `COALESCE(get_json_object(metadata_json, '$.pipeline'), 'bridge')`:
`abc-balance-check.sh` with `ABC_PIPELINE` (default `bridge`), `audit-gap-check.sh`
with `AUDIT_GAP_PIPELINE` (default `bridge`). Without the filter PMM traffic would
fail equations 2–5 as `POSSIBLE_LOSS`, because that funnel has no enrichment or
Kafka-publish stage.

### PMM bridge funnel

Run a second balance job with `ABC_PIPELINE=pmm`; its equations (P1–P3) are listed
above and implemented in `abc-balance-check.sh`. `HDFS_WRITE_SKIPPED` with
`metadata.reason = target-exists-before-api-call` is a redelivery resolved without a
web-service call (see AUDIT.md, PMM flow) and counts as landed.

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
