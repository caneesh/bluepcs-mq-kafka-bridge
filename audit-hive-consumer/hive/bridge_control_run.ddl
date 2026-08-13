-- ============================================================================
-- Control store for the Audit-Balance-Control (ABC) framework.
-- Written by scripts/abc-balance-check.sh — one row per balance equation per
-- execution, on EVERY run (PASS included), so "did this hour tie out?" is an
-- answerable question months later instead of a Control-M job-history lookup.
--
-- Framework, equations and runbook: docs/AUDIT_BALANCE_CONTROL.md.
-- Companion table: bluepcs.bridge_audit_event (the audited facts this
-- balances over) — see bridge_audit_event.ddl.
--
-- Run once per environment before the first abc-balance-check.sh execution;
-- adjust database/location to the environment's standards.
-- ============================================================================

CREATE TABLE IF NOT EXISTS bluepcs.bridge_control_run (
  run_id          STRING,   -- one uuid per script execution; groups the rows below
  check_name      STRING,   -- BALANCE_STAGE | DISCARDED_OUTSIDE_FUNNEL
  equation_no     INT,      -- 1..6 for stage balances; NULL for informational rows
  stage_from      STRING,   -- audit event type the flow leaves
  stage_to        STRING,   -- audit event type the flow should reach
  window_start    STRING,   -- ISO-8601 UTC, inclusive
  window_end      STRING,   -- ISO-8601 UTC, exclusive
  expected_count  BIGINT,   -- upstream count minus legitimate drains
  actual_count    BIGINT,   -- downstream count
  variance        BIGINT,   -- expected - actual; SIGN IS DIAGNOSTIC, see below
  variance_pct    DOUBLE,   -- variance / expected * 100 (0.0 when expected = 0)
  tolerance_pct   DOUBLE,   -- allowed |variance_pct| before the row fails
  status          STRING,   -- PASS | WARN | FAIL | INFO
  reason_code     STRING,   -- OK | POSSIBLE_LOSS | AUDIT_LOSS | WITHIN_TOLERANCE | INFO
  detail          STRING,   -- human-readable one-liner for the runbook
  host            STRING,   -- edge node that ran the check
  started_at      STRING,   -- ISO-8601 UTC, script start
  ended_at        STRING    -- ISO-8601 UTC, row write time
)
PARTITIONED BY (run_dt STRING)   -- yyyy-MM-dd (UTC) of the run, not of the window
STORED AS ORC;

-- ----------------------------------------------------------------------------
-- Reading `variance` (expected - actual):
--
--   variance > 0  fewer messages reached the downstream stage than left the
--                 upstream one  ->  POSSIBLE_LOSS (FAIL beyond tolerance)
--   variance = 0  ties out exactly                              ->  PASS
--   variance < 0  MORE messages arrived downstream than were seen upstream.
--                 That is arithmetically impossible for real message flow, so
--                 it proves the AUDIT STREAM lost events (the publisher drops
--                 events during its 60s failure cooldown), not that data was
--                 lost  ->  AUDIT_LOSS (WARN, never FAIL).
--
-- This table balances over a best-effort audit stream. It detects loss; it
-- cannot by itself prove the absence of loss. Corroborate incidents against MQ
-- queue statistics (MSGDEQD) — see docs/RECONCILIATION_PLAN.md.
-- ----------------------------------------------------------------------------
