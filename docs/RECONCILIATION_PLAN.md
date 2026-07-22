# Reconciliation Strategy: confirming consumed → landed → loaded

Status: **checks 1–3 implemented** (`scripts/audit-gap-check.sh`); check 4 (target-table)
is a planned, configuration-activated extension.

## Goal

Confirm that every message the bridge consumes from MQ (a) reaches a proper terminal
outcome, (b) lands in HDFS, and (c) is actually loaded by the downstream consumer — with an
optional check that rows appear in the target product tables.

## Evidence model (three layers, strongest first)

1. **Design guarantee (prevention).** The bridge acknowledges an MQ message only after one
   of three terminal outcomes: full success (`PROCESSING_COMPLETED`), durable quarantine
   (`MESSAGE_QUARANTINED`), or audited discard (`MESSAGE_DISCARDED`). Any failure → no ack →
   MQ redelivers. "Consumed" therefore *implies* "processed or preserved" by construction.
2. **End-to-end audit trail (detection).** The bridge and the DStream consumer emit to the
   same audit topic keyed by the same `eventId` (see `docs/AUDIT.md`, "Consumer stages");
   the `audit-hive-consumer/` job lands everything in `bluepcs.bridge_audit_event`.
   `HIVE_LOAD_COMPLETED` — not `PROCESSING_COMPLETED` — is the true terminal state.
3. **Independent cross-checks (corroboration).** Audit is best-effort, so counts should be
   corroborated against sources that cannot lie: MQ dequeue counts, HDFS file lifecycle,
   and (eventually) target-table rows.

## The checks

### Implemented — `scripts/audit-gap-check.sh` (hourly Control-M)

| # | Check | Question answered | Exit code |
|---|---|---|---|
| 1 | **Load gaps** — `PROCESSING_COMPLETED` with no `HIVE_LOAD_COMPLETED` within `AUDIT_GAP_THRESHOLD_MINUTES` | Did everything the bridge landed get loaded by the consumer? | 1 |
| — | WARN bucket: same pattern but with `CLAIM_CHECK_SKIPPED` | Redelivery after the consumer archived the file — expected, review-only | (warn) |
| 2 | **Stuck** — `MESSAGE_RECEIVED` but no terminal state past `AUDIT_GAP_GRACE_MINUTES` | Is anything looping through `*_FAILED` redeliveries? | 2 |
| 3 | **Quarantined** — `MESSAGE_QUARANTINED` in the lookback | What awaits manual review in HDFS `errors/`? | 3 (info) |

The event-pair mechanism (check 1) is the primary loaded-confirmation: it is direct
testimony from the consumer and sidesteps the landing-directory age-archival ambiguity
(`hdfs-landing-cleanup.sh` moves files to archive by age, so physical location alone cannot
prove consumer processing beyond `LANDING_RETENTION_DAYS`).

### Planned — check 4: target-table verification (configuration-activated)

Confirms rows actually exist in the product tables — the one thing `HIVE_LOAD_COMPLETED`
(batch-granularity testimony) cannot prove. Two modes, activated by config only:

- **Count mode** (`RECON_TARGET_TABLE` + `RECON_TARGET_DT_COLUMN`): rows in the window vs
  loaded-message count; mismatch beyond a tolerance fails.
- **Row mode** (+ `RECON_TARGET_KEY_COLUMN`): anti-join audit-completed `transaction_id`s
  against the table's key column; lists specific missing rows. Requires the target table to
  carry a lineage key — adding `transaction_id` to the curated table is the recommended
  durable fix.

### Demoted — HDFS file-lifecycle classification (optional cross-check)

An earlier design classified each completed `<eventId>.json` by directory location
(landing/archive/consumer-error). Superseded as the primary signal by the event-pair check,
which is unambiguous; the file-lifecycle check remains useful as an audit-independent
cross-check (it catches audit-stream loss) and for uninstrumented legacy traffic. Implement
only if audit-loss becomes a real concern; run it inside `LANDING_RETENTION_DAYS`.

## Known limits (accepted, documented)

- **Audit is best-effort**: a lost `HIVE_LOAD_COMPLETED` event false-alarms check 1; a lost
  `PROCESSING_COMPLETED` false-passes it. After incidents, corroborate with MQ dequeue
  counts (`MSGDEQD`) vs distinct `MESSAGE_RECEIVED` event_ids.
- **Gap amnesia**: a real gap stops alerting once its rows age past
  `AUDIT_GAP_LOOKBACK_DAYS`; treat every exit-1 seriously while it fires.
- Legacy Talend inline messages carry no `eventId` and are invisible to all audit-based
  checks; they phase out with Talend.

## Useful ad-hoc queries

Dedupe view (create once; all queries below should target it):

```sql
CREATE VIEW IF NOT EXISTS bluepcs.bridge_audit_event_deduped AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
      PARTITION BY COALESCE(audit_event_id, CONCAT(kafka_partition, '-', kafka_offset))
      ORDER BY event_timestamp) rn
  FROM bluepcs.bridge_audit_event) t
WHERE rn = 1;
-- COALESCE matters: raw-fallback rows have NULL audit_event_id and would otherwise
-- collapse into a single row.
```

Daily funnel:

```sql
SELECT event_dt,
       count(DISTINCT CASE WHEN event_type='MESSAGE_RECEIVED'     THEN event_id END) AS received,
       count(DISTINCT CASE WHEN event_type='PROCESSING_COMPLETED' THEN event_id END) AS bridge_done,
       count(DISTINCT CASE WHEN event_type='HIVE_LOAD_COMPLETED'  THEN event_id END) AS loaded,
       count(DISTINCT CASE WHEN event_type='MESSAGE_QUARANTINED'  THEN event_id END) AS quarantined
FROM bluepcs.bridge_audit_event_deduped
WHERE event_dt >= date_sub(current_date, 7)
GROUP BY event_dt ORDER BY event_dt;
```

Full lifecycle of one message: filter the deduped view by `event_id` (or `transaction_id`)
and order by `event_timestamp` — eight events end to end on the happy path.
