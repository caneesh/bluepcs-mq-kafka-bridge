# Implementation Plan: End-to-End Reconciliation (consumed → landed → loaded)

Status: **planned, not yet implemented.** This document is the agreed design for the
reconciliation job; implementation will follow it.

## Context

Goal: confirm that every message the bridge consumed from MQ (a) reached a proper terminal
outcome, (b) landed in HDFS, and (c) was picked up by the downstream consumer — with an
optional check that rows appear in the target tables.

Evidence sources: the audit Hive table (`bluepcs.bridge_audit_event`, fed by
`docs/consumer/AuditHiveConsumer.scala`) and the HDFS landing-directory file lifecycle.

Key design constraint: the retention sweep (`scripts/hdfs-landing-cleanup.sh`) archives
landing files **by age** (`LANDING_RETENTION_DAYS`, default 7), so "file in archive" is
ambiguous evidence — it may mean consumer-processed *or* merely aged out. The reliable
"loaded by consumer" signal is *absent from the landing dir while younger than the retention
window* (only the consumer moves young files). The reconciliation must therefore run daily,
inside that window.

## Deliverable 1 — `scripts/audit-reconciliation.sh`

Read-only Control-M job following the repo's script conventions (`set -u`, `.env` sourcing,
optional kinit via `HDFS_KERBEROS_PRINCIPAL`/`HDFS_KERBEROS_KEYTAB`, `--dry-run` flag,
summary block for sysout capture). It never remediates.

### Configuration (env / `.env`; defaults in brackets)

| Variable | Default | Meaning |
|---|---|---|
| `BEELINE_CMD` | (required) | e.g. `beeline -u '<jdbc-url>' --silent=true --outputformat=tsv2` |
| `AUDIT_HIVE_TABLE_FQN` | `bluepcs.bridge_audit_event` | Audit table |
| `HDFS_BASE_PATH` | (required) | Landing dir — same variable the run scripts use |
| `RECON_WINDOW_DAYS` | 1 | Reconcile events with `event_dt` in the last N days |
| `RECON_GRACE_MINUTES` | 30 | Ignore messages received more recently than this |
| `RECON_CONSUMER_LAG_MINUTES` | 60 | Landing file older than this ⇒ consumer stalled |
| `LANDING_RETENTION_DAYS` | 7 | MUST match `hdfs-landing-cleanup.sh` |
| `CONSUMER_ERROR_PATH` | (optional) | Consumer's error dir, when known |
| `RECON_TARGET_TABLE` | (optional) | Enables the table-level check (Check D) |
| `RECON_TARGET_DT_COLUMN` | (with table) | The table's date/partition column |
| `RECON_TARGET_KEY_COLUMN` | (optional) | Enables row-level anti-join on `transaction_id` |
| `RECON_TABLE_TOLERANCE` | 0 | Allowed count difference in count mode |

### Checks (all always run; the report shows every outcome)

**A. Terminal state (audit only).** Beeline query: event_ids in the window with
`MESSAGE_RECEIVED` but none of `PROCESSING_COMPLETED | MESSAGE_QUARANTINED |
MESSAGE_DISCARDED`, and last activity older than the grace period. Non-empty ⇒ stuck or
looping messages (report event_id, last event type, last timestamp).

**B. Landed → loaded (audit + HDFS).** For each completed event_id in the window, classify
`${HDFS_BASE_PATH}/<eventId>.json` (one `hdfs dfs -ls` per directory, not per file):

| Observation | Classification | Outcome |
|---|---|---|
| In landing, mtime older than lag threshold | NOT_LOADED (consumer stalled) | failure |
| In landing, younger | PENDING | informational |
| In `CONSUMER_ERROR_PATH` (when configured) | CONSUMER_FAILED — listed for replay | failure |
| Absent, event age < `LANDING_RETENTION_DAYS` | LOADED (only the consumer moves young files) | success |
| Absent, older than retention | AMBIGUOUS (consumer or age-sweep) | labeled, never a failure |

**C. Quarantine visibility.** List `MESSAGE_QUARANTINED` event_ids in the window —
bridge-side permanent failures awaiting manual review. Informational; never fails the run.

**D. Table-level (optional; prints SKIPPED until `RECON_TARGET_TABLE` is set).**
- Count mode (table only): rows in the window's `RECON_TARGET_DT_COLUMN` vs LOADED count
  from Check B; mismatch beyond `RECON_TABLE_TOLERANCE` ⇒ failure.
- Row mode (key column also set): anti-join audit-completed `transaction_id`s against the
  table's key column; lists the specific missing rows.

### Exit codes (Control-M On-Do routing; highest severity wins: 5 > 1 > 3 > 2 > 4)

| Code | Meaning |
|---|---|
| 0 | Fully reconciled |
| 1 | Unterminated messages (stuck/looping) |
| 2 | Landing backlog (consumer not picking up) |
| 3 | Consumer failures (files in error dir) |
| 4 | Table check mismatch |
| 5 | Could not evaluate (beeline/HDFS access failed) |

## Deliverable 2 — `docs/RECONCILIATION.md`

- The three-layer evidence model and what each layer catches: ack-ordering design guarantee
  (a message cannot leave MQ without a terminal outcome) → audit terminal states → physical
  file lifecycle → optional table rows.
- Dedupe view DDL (`ROW_NUMBER() OVER (PARTITION BY audit_event_id ...) = 1`) and
  copy-pasteable versions of every query the script runs (incl. daily funnel counts and the
  stuck-loop query).
- The archive ambiguity: why reconciliation must run daily / within `LANDING_RETENTION_DAYS`.
- Prerequisites: audit topic provisioned, `AuditHiveConsumer` running (the script's data
  source), beeline/JDBC access from the edge node, read ACL on the target table for Check D.
- Control-M setup: daily job after the audit consumer's last batch; exit-code table;
  no-remediation rule (consistent with `monitor.sh`).
- Limitations: audit is best-effort (cross-check MQ dequeue counts after incidents); Check D
  is count-level unless the target table carries a lineage key — recommend adding
  `transaction_id` to the curated table as the durable row-level fix.

## Deliverable 3 — `DEPLOYMENT_CHECKLIST.md` addition

Extend the Control-M/monitoring section: daily reconciliation job, exit-code routing table,
pointer to `docs/RECONCILIATION.md`.

## Verification

- `bash -n` + shellcheck on the script.
- `--dry-run` locally: prints queries/commands, exercises env parsing and the summary block
  without cluster access.
- Live validation on the edge node against test-env once the audit consumer is running.
- No Java changes — the Maven test suite is unaffected.

## Dependencies / rollout order

1. Audit topic provisioned + `AuditHiveConsumer` deployed (data source for Checks A–C).
2. Script + docs land; Control-M daily job created — Checks A–C active immediately.
3. Target-table details (`RECON_TARGET_TABLE` etc.) filled in later — Check D activates via
   configuration only, no code change.
