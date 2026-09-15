#!/bin/bash
# =============================================================================
# End-to-end audit gap check (for Control-M / external schedulers)
# =============================================================================
# Three read-only checks against the Hive audit table (see docs/AUDIT.md):
#
#   0. EVIDENCE    — optional liveness of the audit stream itself: with
#                    AUDIT_GAP_SILENCE_MINUTES > 0, no bridge event newer than that
#                    is exit 5. Off by default (idle queues are legitimate) — turn
#                    it on for a queue with steady traffic, otherwise an audit
#                    outage (Kafka down, ACL lost) looks like a healthy idle system.
#   1. LOAD GAPS   — bridge finished (PROCESSING_COMPLETED) but the data never
#                    reached the Hive product tables (no HIVE_LOAD_COMPLETED)
#                    within the threshold. A CLAIM_CHECK_SKIPPED without a load is
#                    STILL a gap: the consumer found no file, so nothing was loaded.
#                    (A skip AFTER a load is a benign redelivery and is not listed.)
#   2. STUCK       — consumed (MESSAGE_RECEIVED) but no terminal state at all
#                    (no PROCESSING_COMPLETED / MESSAGE_QUARANTINED /
#                    MESSAGE_DISCARDED) for longer than the grace period since the
#                    FIRST receipt: a message looping through *_FAILED redeliveries
#                    keeps producing fresh events, so "last seen" can never age.
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
#   5 - no audit evidence for AUDIT_GAP_SILENCE_MINUTES -> audit outage or bridge idle
#
# Tuning (via .env or environment):
#   AUDIT_GAP_TABLE              default bluepcs.bridge_audit_event
#   AUDIT_GAP_THRESHOLD_MINUTES  default 120 — must comfortably exceed the
#                                audit consumer's batch interval (300s) PLUS
#                                the DStream job's batch cadence; too low
#                                gives false alarms for in-flight messages
#   AUDIT_GAP_GRACE_MINUTES      default 30 — how long a message may stay
#                                non-terminal (measured from its FIRST receipt)
#                                before it counts as stuck
#   AUDIT_GAP_SILENCE_MINUTES    default 0 (off) — see check 0
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
AUDIT_GAP_SILENCE_MINUTES="${AUDIT_GAP_SILENCE_MINUTES:-0}"
HIVE_CMD="${HIVE_CMD:-hive -S -e}"

ERR_FILE="/tmp/audit-gap-check.$$.err"
trap 'rm -f "$ERR_FILE"' EXIT

# ISO-8601 UTC cutoffs (see timestamp-contract note in the header)
# Shared audit topic: PMM-bridge rows carry metadata.pipeline='pmm', PMM+ rows no
# key. Check 1 (Hive load gaps) exists only for the PMM+ funnel and is skipped for
# any other pipeline; run with AUDIT_GAP_PIPELINE=pmm for the PMM bridge's
# stuck/quarantined checks.
AUDIT_GAP_PIPELINE="${AUDIT_GAP_PIPELINE:-bridge}"

GAP_CUTOFF="$(date -u -d "-${AUDIT_GAP_THRESHOLD_MINUTES} minutes" '+%Y-%m-%dT%H:%M:%S')"
GRACE_CUTOFF="$(date -u -d "-${AUDIT_GAP_GRACE_MINUTES} minutes" '+%Y-%m-%dT%H:%M:%S')"

# Check 1: load gaps. A CLAIM_CHECK_SKIPPED for an event that was never loaded is
# reported as a gap WITH the skip flagged (last column = 1): the consumer looked for
# the file, found nothing, and loaded nothing — that is exactly the loss this check
# exists for. Only an event that has BOTH a load and a skip is a benign redelivery.
QUERY_GAPS="
SELECT event_id,
       max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN event_timestamp END) AS bridge_completed_at,
       max(CASE WHEN event_type = 'CLAIM_CHECK_SKIPPED'  THEN 1 ELSE 0 END)        AS was_skipped
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
  AND COALESCE(get_json_object(metadata_json, '\$.pipeline'), 'bridge') = '${AUDIT_GAP_PIPELINE}'
GROUP BY event_id
HAVING max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN 1 ELSE 0 END) = 1
   AND max(CASE WHEN event_type = 'HIVE_LOAD_COMPLETED'  THEN 1 ELSE 0 END) = 0
   AND max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN event_timestamp END) < '${GAP_CUTOFF}'
ORDER BY bridge_completed_at ASC
LIMIT ${AUDIT_GAP_RESULT_LIMIT}
"

# Check 2: stuck — consumed but never terminal, for longer than the grace period
# since the FIRST receipt. Measuring from the latest event would let a message that
# fails every minute reset the clock forever and never count as stuck.
QUERY_STUCK="
SELECT event_id,
       min(CASE WHEN event_type = 'MESSAGE_RECEIVED' THEN event_timestamp END) AS first_seen,
       max(event_timestamp) AS last_seen,
       collect_set(event_type) AS events
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
  AND COALESCE(get_json_object(metadata_json, '\$.pipeline'), 'bridge') = '${AUDIT_GAP_PIPELINE}'
  AND event_id IS NOT NULL
GROUP BY event_id
HAVING max(CASE WHEN event_type = 'MESSAGE_RECEIVED' THEN 1 ELSE 0 END) = 1
   AND max(CASE WHEN event_type IN ('PROCESSING_COMPLETED','MESSAGE_QUARANTINED','MESSAGE_DISCARDED')
                THEN 1 ELSE 0 END) = 0
   AND min(CASE WHEN event_type = 'MESSAGE_RECEIVED' THEN event_timestamp END) < '${GRACE_CUTOFF}'
LIMIT ${AUDIT_GAP_RESULT_LIMIT}
"

# Check 0: is audit evidence arriving at all? Newest bridge event for this pipeline.
QUERY_NEWEST="
SELECT max(event_timestamp)
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
  AND COALESCE(get_json_object(metadata_json, '\$.pipeline'), 'bridge') = '${AUDIT_GAP_PIPELINE}'
  AND event_type = 'MESSAGE_RECEIVED'
"

# Check 3: quarantined payloads awaiting manual review (informational)
QUERY_QUARANTINED="
SELECT DISTINCT event_id
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
  AND COALESCE(get_json_object(metadata_json, '\$.pipeline'), 'bridge') = '${AUDIT_GAP_PIPELINE}'
  AND event_type = 'MESSAGE_QUARANTINED'
LIMIT ${AUDIT_GAP_RESULT_LIMIT}
"

if [ "${1:-}" = "--dry-run" ]; then
    echo "Would run via: ${HIVE_CMD} <query>"
    echo "--- Check 0 (evidence):";    echo "${QUERY_NEWEST}"
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

# --- Check 0: evidence -------------------------------------------------------
SILENT=false
if [ "${AUDIT_GAP_SILENCE_MINUTES}" -gt 0 ]; then
    SILENCE_CUTOFF="$(date -u -d "-${AUDIT_GAP_SILENCE_MINUTES} minutes" '+%Y-%m-%dT%H:%M:%S')"
    NEWEST="$(run_query "${QUERY_NEWEST}")" || true
    if [ "${NEWEST}" = "__QUERY_FAILED__" ]; then
        EVAL_FAILED=true
        echo "CHECK 0 (evidence): ERROR - query failed"
    else
        NEWEST="$(echo "${NEWEST}" | tail -1)"
        case "${NEWEST}" in
            ''|NULL|null) NEWEST="" ;;
        esac
        if [ -z "${NEWEST}" ] || [ "${NEWEST}" \< "${SILENCE_CUTOFF}" ]; then
            SILENT=true
            echo "CHECK 0 (evidence): FAILED - no MESSAGE_RECEIVED newer than ${SILENCE_CUTOFF}Z (newest: ${NEWEST:-none}) —"
            echo "  either the bridge is idle or the audit stream is down (Kafka outage / topic ACL); check the bridge's"
            echo "  <log>-audit.jsonl fallback file and the monitor before trusting the checks below"
        else
            echo "CHECK 0 (evidence): PASSED - newest MESSAGE_RECEIVED at ${NEWEST}"
        fi
    fi
else
    echo "CHECK 0 (evidence): SKIPPED - AUDIT_GAP_SILENCE_MINUTES=0; an empty audit table passes checks 1-3 by construction"
fi

# --- Check 1: load gaps ------------------------------------------------------
# Only the PMM+ funnel has a Hive-load stage; for any other pipeline every completed
# message would (correctly, and forever) lack HIVE_LOAD_COMPLETED and page as a gap.
HARD_GAPS=""; SKIPPED_GAPS=""; HARD_COUNT=0
if [ "${AUDIT_GAP_PIPELINE}" != "bridge" ]; then
    GAP_ROWS=""
    echo "CHECK 1 (load gaps): SKIPPED - pipeline '${AUDIT_GAP_PIPELINE}' has no Hive load stage"
else
GAP_ROWS="$(run_query "${QUERY_GAPS}")" || true
fi
if [ "${AUDIT_GAP_PIPELINE}" != "bridge" ]; then
    :
elif [ "${GAP_ROWS}" = "__QUERY_FAILED__" ]; then
    EVAL_FAILED=true
    echo "CHECK 1 (load gaps): ERROR - query failed"
else
    HARD_GAPS="$(echo "${GAP_ROWS}" | awk -F'\t' '$1 != "" {print $1"\t"$2}' | sed '/^[[:space:]]*$/d')"
    SKIPPED_GAPS="$(echo "${GAP_ROWS}" | awk -F'\t' '$3 == 1 {print $1"\t"$2}' | sed '/^[[:space:]]*$/d')"
    HARD_COUNT="$(count_rows "${HARD_GAPS}")"
    SKIP_COUNT="$(count_rows "${SKIPPED_GAPS}")"
    if [ "${HARD_COUNT}" -eq 0 ]; then
        echo "CHECK 1 (load gaps): PASSED - every bridge-completed message reached Hive"
    else
        echo "CHECK 1 (load gaps): FAILED - ${HARD_COUNT} message(s) completed the bridge but never loaded to Hive:"
        echo "${HARD_GAPS}" | sed 's/^/  GAP: /'
        note_truncation "$(count_rows "${GAP_ROWS}")" "gap"
        if [ "${SKIP_COUNT}" -gt 0 ]; then
            echo "  ${SKIP_COUNT} of them were CLAIM_CHECK_SKIPPED by the consumer: it found NO file to load —"
            echo "  the payload is missing (archived/deleted before the load), not a duplicate:"
            echo "${SKIPPED_GAPS}" | sed 's/^/    SKIPPED-WITHOUT-LOAD: /'
        fi
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
        echo "CHECK 2 (stuck): FAILED - ${STUCK_COUNT} message(s) consumed but never terminal for longer than ${AUDIT_GAP_GRACE_MINUTES}m since first receipt (redelivery loop?):"
        echo "  (columns: event_id, first_seen, last_seen, events)"
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

# --- Verdict (severity: 4 > 5 > 2 > 1 > 3) -----------------------------------
HARD_COUNT="${HARD_COUNT:-0}"
EXIT_CODE=0
[ "${Q_COUNT}" -gt 0 ] && EXIT_CODE=3
[ "${HARD_COUNT}" -gt 0 ] && EXIT_CODE=1
[ "${STUCK_COUNT}" -gt 0 ] && EXIT_CODE=2
[ "${SILENT}" = true ] && EXIT_CODE=5
[ "${EVAL_FAILED}" = true ] && EXIT_CODE=4

echo "============================================"
case ${EXIT_CODE} in
    0) echo "GAP CHECK: PASSED" ;;
    1) echo "GAP CHECK: FAILED - load gaps (investigate the DStream consumer job; the bridge finished its part)" ;;
    2) echo "GAP CHECK: FAILED - stuck messages (investigate the bridge: look for *_FAILED events per eventId)" ;;
    3) echo "GAP CHECK: REVIEW - quarantined payloads pending manual review" ;;
    4) echo "GAP CHECK: ERROR - could not evaluate (Hive query failed)" ;;
    5) echo "GAP CHECK: NO EVIDENCE - no audit events for ${AUDIT_GAP_SILENCE_MINUTES}m (audit outage or idle bridge); checks 1-3 are not trustworthy" ;;
esac
exit ${EXIT_CODE}
