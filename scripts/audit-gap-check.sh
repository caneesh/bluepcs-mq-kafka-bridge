#!/bin/bash
# =============================================================================
# End-to-end audit gap check (for Control-M / external schedulers)
# =============================================================================
# Three read-only checks against the Hive audit table (see docs/AUDIT.md):
#
#   1. LOAD GAPS   — bridge finished (PROCESSING_COMPLETED) but the data never
#                    reached the Hive product tables (no HIVE_LOAD_COMPLETED)
#                    within the threshold. Rows that instead show
#                    CLAIM_CHECK_SKIPPED are reported as a WARN bucket, not a
#                    gap: a redelivery after the consumer archived the file
#                    legitimately produces exactly that pattern.
#   2. STUCK       — consumed (MESSAGE_RECEIVED) but no terminal state at all
#                    (no PROCESSING_COMPLETED / MESSAGE_QUARANTINED /
#                    MESSAGE_DISCARDED) beyond a grace period: a message
#                    looping through *_FAILED redeliveries.
#   3. QUARANTINED — MESSAGE_QUARANTINED in the lookback: permanently
#                    unparseable payloads preserved in HDFS errors/, awaiting
#                    manual review. Informational severity.
#
# Never remediates. Schedule cyclic (e.g. hourly) on the edge-node agent,
# Run As the service account. Route exit codes (highest severity wins,
# 4 > 2 > 1 > 3):
#
#   0 - all checks passed
#   1 - load gaps found            -> investigate the DStream consumer job
#   2 - stuck messages found       -> investigate the bridge (redelivery loop)
#   3 - quarantined messages exist -> review queue, not a page
#   4 - could not evaluate (Hive query failed) -> investigate this job/edge node
#
# Tuning (via .env or environment):
#   AUDIT_GAP_TABLE              default bluepcs.bridge_audit_event
#   AUDIT_GAP_THRESHOLD_MINUTES  default 120 — must comfortably exceed the
#                                audit consumer's batch interval (300s) PLUS
#                                the DStream job's batch cadence; too low
#                                gives false alarms for in-flight messages
#   AUDIT_GAP_GRACE_MINUTES      default 30 — how long a message may stay
#                                non-terminal before it counts as stuck
#   AUDIT_GAP_LOOKBACK_DAYS      default 2 — window scanned (partition
#                                pruning). NOTE: a real gap stops alerting
#                                once its rows age past the lookback — treat
#                                every exit-1 seriously while it fires.
#   AUDIT_GAP_RESULT_LIMIT       default 50 — max rows printed per check
#   HIVE_CMD                     default "hive -S -e"; for beeline use e.g.
#                                HIVE_CMD="beeline -u jdbc:hive2://<host>:10000/default;principal=<hs2-principal> --silent=true --outputformat=tsv2 --showHeader=false -e"
#
# Timestamp contract: the bridge serializes event_timestamp as ISO-8601 UTC
# strings (pinned by KafkaAuditPublisherTest), so lexicographic comparison in
# HQL is chronologically correct.
#
# --dry-run prints the rendered queries without executing (no Hive needed).
# =============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env first (same convention as the other scripts)
if [ -f "${PROJECT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

AUDIT_GAP_TABLE="${AUDIT_GAP_TABLE:-bluepcs.bridge_audit_event}"
AUDIT_GAP_THRESHOLD_MINUTES="${AUDIT_GAP_THRESHOLD_MINUTES:-120}"
AUDIT_GAP_GRACE_MINUTES="${AUDIT_GAP_GRACE_MINUTES:-30}"
AUDIT_GAP_LOOKBACK_DAYS="${AUDIT_GAP_LOOKBACK_DAYS:-2}"
AUDIT_GAP_RESULT_LIMIT="${AUDIT_GAP_RESULT_LIMIT:-50}"
HIVE_CMD="${HIVE_CMD:-hive -S -e}"

ERR_FILE="/tmp/audit-gap-check.$$.err"
trap 'rm -f "$ERR_FILE"' EXIT

# ISO-8601 UTC cutoffs (see timestamp-contract note in the header)
GAP_CUTOFF="$(date -u -d "-${AUDIT_GAP_THRESHOLD_MINUTES} minutes" '+%Y-%m-%dT%H:%M:%S')"
GRACE_CUTOFF="$(date -u -d "-${AUDIT_GAP_GRACE_MINUTES} minutes" '+%Y-%m-%dT%H:%M:%S')"

# Check 1: load gaps. Last column separates hard gaps (0) from the
# redelivery-after-archive WARN bucket (1 = CLAIM_CHECK_SKIPPED present).
QUERY_GAPS="
SELECT event_id,
       max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN event_timestamp END) AS bridge_completed_at,
       max(CASE WHEN event_type = 'CLAIM_CHECK_SKIPPED'  THEN 1 ELSE 0 END)        AS was_skipped
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
GROUP BY event_id
HAVING max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN 1 ELSE 0 END) = 1
   AND max(CASE WHEN event_type = 'HIVE_LOAD_COMPLETED'  THEN 1 ELSE 0 END) = 0
   AND max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN event_timestamp END) < '${GAP_CUTOFF}'
ORDER BY was_skipped ASC, bridge_completed_at ASC
LIMIT ${AUDIT_GAP_RESULT_LIMIT}
"
# ORDER BY was_skipped ASC: hard gaps (0) sort before WARN-bucket skips (1), so a
# mass-redelivery skip storm can never push real gaps past the LIMIT and false-PASS.

# Check 2: stuck — consumed but never terminal, and quiet past the grace period
QUERY_STUCK="
SELECT event_id,
       max(event_timestamp) AS last_seen,
       collect_set(event_type) AS events
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
  AND event_id IS NOT NULL
GROUP BY event_id
HAVING max(CASE WHEN event_type = 'MESSAGE_RECEIVED' THEN 1 ELSE 0 END) = 1
   AND max(CASE WHEN event_type IN ('PROCESSING_COMPLETED','MESSAGE_QUARANTINED','MESSAGE_DISCARDED')
                THEN 1 ELSE 0 END) = 0
   AND max(event_timestamp) < '${GRACE_CUTOFF}'
LIMIT ${AUDIT_GAP_RESULT_LIMIT}
"

# Check 3: quarantined payloads awaiting manual review (informational)
QUERY_QUARANTINED="
SELECT DISTINCT event_id
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
  AND event_type = 'MESSAGE_QUARANTINED'
LIMIT ${AUDIT_GAP_RESULT_LIMIT}
"

if [ "${1:-}" = "--dry-run" ]; then
    echo "Would run via: ${HIVE_CMD} <query>"
    echo "--- Check 1 (load gaps):";   echo "${QUERY_GAPS}"
    echo "--- Check 2 (stuck):";       echo "${QUERY_STUCK}"
    echo "--- Check 3 (quarantined):"; echo "${QUERY_QUARANTINED}"
    exit 0
fi

EVAL_FAILED=false

# Runs a query; prints rows to stdout (may be empty). Sets EVAL_FAILED on error.
run_query() {
    local query="$1"
    local result
    # HIVE_CMD is intentionally word-split: it carries the command plus its flags.
    # shellcheck disable=SC2086
    result="$(${HIVE_CMD} "${query}" 2>"$ERR_FILE")"
    if [ $? -ne 0 ]; then
        echo "__QUERY_FAILED__"
        sed 's/^/  hive: /' "$ERR_FILE" | tail -10 >&2
        return 1
    fi
    echo "${result}" | sed '/^[[:space:]]*$/d'
}

count_rows() { [ -z "$1" ] && echo 0 || echo "$1" | wc -l; }

note_truncation() {
    # $1 = count, $2 = label
    if [ "$1" -ge "${AUDIT_GAP_RESULT_LIMIT}" ]; then
        echo "  (${2} list truncated at ${AUDIT_GAP_RESULT_LIMIT} rows — the real count may be higher)"
    fi
}

echo "============================================"
echo "Audit gap check: ${AUDIT_GAP_TABLE}"
echo "Gap threshold: ${AUDIT_GAP_THRESHOLD_MINUTES}m (cutoff ${GAP_CUTOFF}Z)  Grace: ${AUDIT_GAP_GRACE_MINUTES}m  Lookback: ${AUDIT_GAP_LOOKBACK_DAYS}d"
echo "============================================"

# --- Check 1: load gaps ------------------------------------------------------
GAP_ROWS="$(run_query "${QUERY_GAPS}")" || true
HARD_GAPS=""; SKIP_WARNS=""
if [ "${GAP_ROWS}" = "__QUERY_FAILED__" ]; then
    EVAL_FAILED=true
    echo "CHECK 1 (load gaps): ERROR - query failed"
else
    HARD_GAPS="$(echo "${GAP_ROWS}" | awk -F'\t' '$3 == 0 {print $1"\t"$2}' | sed '/^[[:space:]]*$/d')"
    SKIP_WARNS="$(echo "${GAP_ROWS}" | awk -F'\t' '$3 == 1 {print $1"\t"$2}' | sed '/^[[:space:]]*$/d')"
    HARD_COUNT="$(count_rows "${HARD_GAPS}")"
    SKIP_COUNT="$(count_rows "${SKIP_WARNS}")"
    if [ "${HARD_COUNT}" -eq 0 ]; then
        echo "CHECK 1 (load gaps): PASSED - every bridge-completed message reached Hive"
    else
        echo "CHECK 1 (load gaps): FAILED - ${HARD_COUNT} message(s) completed the bridge but never loaded to Hive:"
        echo "${HARD_GAPS}" | sed 's/^/  GAP: /'
        note_truncation "$(count_rows "${GAP_ROWS}")" "gap"
    fi
    if [ "${SKIP_COUNT}" -gt 0 ]; then
        echo "  WARN: ${SKIP_COUNT} redelivered message(s) skipped as already-processed duplicates"
        echo "  (CLAIM_CHECK_SKIPPED, no new Hive load — expected after archive; review only if unexpected):"
        echo "${SKIP_WARNS}" | sed 's/^/    SKIPPED: /'
    fi
fi

# --- Check 2: stuck messages -------------------------------------------------
STUCK_ROWS="$(run_query "${QUERY_STUCK}")" || true
STUCK_COUNT=0
if [ "${STUCK_ROWS}" = "__QUERY_FAILED__" ]; then
    EVAL_FAILED=true
    echo "CHECK 2 (stuck): ERROR - query failed"
else
    STUCK_COUNT="$(count_rows "${STUCK_ROWS}")"
    if [ "${STUCK_COUNT}" -eq 0 ]; then
        echo "CHECK 2 (stuck): PASSED - no messages without a terminal state"
    else
        echo "CHECK 2 (stuck): FAILED - ${STUCK_COUNT} message(s) consumed but never terminal (redelivery loop?):"
        echo "${STUCK_ROWS}" | sed 's/^/  STUCK: /'
        note_truncation "${STUCK_COUNT}" "stuck"
    fi
fi

# --- Check 3: quarantined ----------------------------------------------------
Q_ROWS="$(run_query "${QUERY_QUARANTINED}")" || true
Q_COUNT=0
if [ "${Q_ROWS}" = "__QUERY_FAILED__" ]; then
    EVAL_FAILED=true
    echo "CHECK 3 (quarantined): ERROR - query failed"
else
    Q_COUNT="$(count_rows "${Q_ROWS}")"
    if [ "${Q_COUNT}" -eq 0 ]; then
        echo "CHECK 3 (quarantined): none in the window"
    else
        echo "CHECK 3 (quarantined): ${Q_COUNT} unparseable payload(s) in HDFS errors/ awaiting review:"
        echo "${Q_ROWS}" | sed 's/^/  QUARANTINED: /'
        note_truncation "${Q_COUNT}" "quarantined"
    fi
fi

# --- Verdict (severity: 4 > 2 > 1 > 3) ---------------------------------------
HARD_COUNT="${HARD_COUNT:-0}"
EXIT_CODE=0
[ "${Q_COUNT}" -gt 0 ] && EXIT_CODE=3
[ "${HARD_COUNT}" -gt 0 ] && EXIT_CODE=1
[ "${STUCK_COUNT}" -gt 0 ] && EXIT_CODE=2
[ "${EVAL_FAILED}" = true ] && EXIT_CODE=4

echo "============================================"
case ${EXIT_CODE} in
    0) echo "GAP CHECK: PASSED" ;;
    1) echo "GAP CHECK: FAILED - load gaps (investigate the DStream consumer job; the bridge finished its part)" ;;
    2) echo "GAP CHECK: FAILED - stuck messages (investigate the bridge: look for *_FAILED events per eventId)" ;;
    3) echo "GAP CHECK: REVIEW - quarantined payloads pending manual review" ;;
    4) echo "GAP CHECK: ERROR - could not evaluate (Hive query failed)" ;;
esac
exit ${EXIT_CODE}
