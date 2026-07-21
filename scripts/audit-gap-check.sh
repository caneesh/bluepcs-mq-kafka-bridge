#!/bin/bash
# =============================================================================
# End-to-end audit gap check (for Control-M / external schedulers)
# =============================================================================
# Queries the Hive audit table for messages the BRIDGE finished
# (PROCESSING_COMPLETED) that never reached the Hive product tables
# (no HIVE_LOAD_COMPLETED) within a threshold — the definitive
# "did every message make it all the way?" alarm. See docs/AUDIT.md.
#
# Read-only; never remediates. Schedule cyclic (e.g. hourly) on the edge-node
# agent, Run As the service account. Route exit codes:
#
#   0 - no gaps
#   1 - GAPS FOUND (eventIds printed) -> alert; investigate the DStream
#       consumer job first (bridge already finished its part)
#   2 - could not evaluate (Hive query failed) -> investigate this job/edge node
#
# Tuning (via .env or environment):
#   AUDIT_GAP_TABLE              default bluepcs.bridge_audit_event
#   AUDIT_GAP_THRESHOLD_MINUTES  default 120 — must comfortably exceed the
#                                audit consumer's batch interval (300s) PLUS
#                                the DStream job's batch cadence; too low
#                                gives false alarms for in-flight messages
#   AUDIT_GAP_LOOKBACK_DAYS      default 2 — window scanned (partition pruning)
#   HIVE_CMD                     default "hive -S -e"; for beeline use e.g.
#                                HIVE_CMD="beeline -u jdbc:hive2://<host>:10000/default;principal=<hs2-principal> --silent=true --outputformat=tsv2 --showHeader=false -e"
#
# --dry-run prints the rendered query without executing (no Hive needed).
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
AUDIT_GAP_LOOKBACK_DAYS="${AUDIT_GAP_LOOKBACK_DAYS:-2}"
HIVE_CMD="${HIVE_CMD:-hive -S -e}"

# ISO-8601 UTC cutoff; audit event_timestamp is ISO-8601 UTC, so lexicographic
# comparison in HQL is chronologically correct.
CUTOFF="$(date -u -d "-${AUDIT_GAP_THRESHOLD_MINUTES} minutes" '+%Y-%m-%dT%H:%M:%S')"

QUERY="
SELECT event_id,
       max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN event_timestamp END) AS bridge_completed_at
FROM ${AUDIT_GAP_TABLE}
WHERE event_dt >= date_sub(current_date, ${AUDIT_GAP_LOOKBACK_DAYS})
GROUP BY event_id
HAVING max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN 1 ELSE 0 END) = 1
   AND max(CASE WHEN event_type = 'HIVE_LOAD_COMPLETED'  THEN 1 ELSE 0 END) = 0
   AND max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN event_timestamp END) < '${CUTOFF}'
"

if [ "${1:-}" = "--dry-run" ]; then
    echo "Would run: ${HIVE_CMD} <query>"
    echo "${QUERY}"
    exit 0
fi

echo "============================================"
echo "Audit gap check: ${AUDIT_GAP_TABLE}"
echo "Threshold: ${AUDIT_GAP_THRESHOLD_MINUTES}m (cutoff ${CUTOFF}Z), lookback ${AUDIT_GAP_LOOKBACK_DAYS}d"
echo "============================================"

# HIVE_CMD is intentionally word-split: it carries the command plus its flags.
# shellcheck disable=SC2086
RESULT="$(${HIVE_CMD} "${QUERY}" 2>/tmp/audit-gap-check.$$.err)"
STATUS=$?

if [ $STATUS -ne 0 ]; then
    echo "GAP CHECK: ERROR - Hive query failed (exit ${STATUS})"
    sed 's/^/  /' "/tmp/audit-gap-check.$$.err" | tail -20
    rm -f "/tmp/audit-gap-check.$$.err"
    exit 2
fi
rm -f "/tmp/audit-gap-check.$$.err"

GAPS="$(echo "${RESULT}" | sed '/^[[:space:]]*$/d')"
GAP_COUNT=0
[ -n "${GAPS}" ] && GAP_COUNT="$(echo "${GAPS}" | wc -l)"

if [ "${GAP_COUNT}" -eq 0 ]; then
    echo "GAP CHECK: PASSED - every bridge-completed message reached Hive"
    exit 0
fi

echo "GAP CHECK: FAILED - ${GAP_COUNT} message(s) completed the bridge but never loaded to Hive:"
echo "${GAPS}" | sed 's/^/  /'
echo ""
echo "Investigate the DStream consumer job (the bridge finished its part):"
echo "  - Is the job running / behind? Check its YARN app and batch lag."
echo "  - Per eventId, look for HIVE_LOAD_FAILED or CLAIM_CHECK_SKIPPED rows in ${AUDIT_GAP_TABLE}."
echo "  - Audit-consumer lag can also cause this if it exceeds the threshold:"
echo "    check the audit consumer's '@@@ AUDIT->HIVE batch written' cadence."
exit 1
