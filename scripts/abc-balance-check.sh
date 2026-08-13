#!/bin/bash
# =============================================================================
# Audit-Balance-Control: stage balance check
# =============================================================================
# Answers the control-total question the gap check cannot: "of the N messages
# received in this window, do the pipeline stage counts tie out?" — and records
# the verdict in bluepcs.bridge_control_run so the answer survives the run.
#
# Complementary to scripts/audit-gap-check.sh, not a replacement:
#   gap check      -> WHICH message is missing (existence, per event_id)
#   balance check  -> WHETHER THE TOTALS TIE OUT (arithmetic, per window)
#
# Read-only against the audit data; the ONLY thing it writes is one control row
# per equation. It never remediates and never touches the bridge.
#
# Framework, equations, tolerances and runbook: docs/AUDIT_BALANCE_CONTROL.md
#
# Usage:
#   ./abc-balance-check.sh [--dry-run]
#
# Configuration (from project .env if present, else environment):
#   HIVE_CMD                      default "hive -S -e"; beeline example in
#                                 audit-gap-check.sh's header
#   AUDIT_GAP_TABLE               audit table (reused name; the deduped VIEW
#                                 over it is what is actually read)
#   ABC_CONTROL_TABLE             default bluepcs.bridge_control_run
#   ABC_WINDOW_HOURS              window width, default 1
#   ABC_WINDOW_LAG_MINUTES        how far behind "now" the window ends, default
#                                 30. MUST exceed the audit consumer's batch
#                                 interval (spark.bluepcs.audit.batch.seconds,
#                                 default 300s) plus its Hive write time, or
#                                 still-in-flight events read as loss.
#   ABC_TOLERANCE_PCT_HIVE_LOAD   allowed % variance on the consumer stage,
#                                 default 2
#
# Exit codes (deliberately distinct from audit-gap-check.sh's):
#   0 - all equations PASS (or INFO only)
#   1 - at least one FAIL (possible message loss) -> page
#   2 - WARN only (audit-stream loss or within-tolerance drift) -> notify
#   3 - could not evaluate (config or query error) -> investigate the check
#
# --dry-run prints the rendered window and queries without executing (no Hive
# needed) and writes nothing.
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
DEDUPED_VIEW="${AUDIT_GAP_TABLE}_deduped"
ABC_CONTROL_TABLE="${ABC_CONTROL_TABLE:-bluepcs.bridge_control_run}"
ABC_WINDOW_HOURS="${ABC_WINDOW_HOURS:-1}"
ABC_WINDOW_LAG_MINUTES="${ABC_WINDOW_LAG_MINUTES:-30}"
ABC_TOLERANCE_PCT_HIVE_LOAD="${ABC_TOLERANCE_PCT_HIVE_LOAD:-2}"
HIVE_CMD="${HIVE_CMD:-hive -S -e}"

DRY_RUN=false
[ "${1:-}" = "--dry-run" ] && DRY_RUN=true

ERR_FILE="/tmp/abc-balance-check.$$.err"
trap 'rm -f "$ERR_FILE"' EXIT

# --- Window -----------------------------------------------------------------
# Ends on an hour boundary at least ABC_WINDOW_LAG_MINUTES behind now, so the
# window is closed and the downstream consumer has had time to report.
WINDOW_END="$(date -u -d "-${ABC_WINDOW_LAG_MINUTES} minutes" '+%Y-%m-%dT%H:00:00')"
WINDOW_START="$(date -u -d "${WINDOW_END}Z -${ABC_WINDOW_HOURS} hours" '+%Y-%m-%dT%H:00:00')"
# Partition-pruning bounds (event_dt is a date string); one day either side of
# the window is enough because the window is at most hours wide.
PART_FROM="$(date -u -d "${WINDOW_START}Z" '+%Y-%m-%d')"
PART_TO="$(date -u -d "${WINDOW_END}Z" '+%Y-%m-%d')"

RUN_ID="$(cat /proc/sys/kernel/random/uuid 2>/dev/null || date -u '+%Y%m%d%H%M%S')-$$"
RUN_DT="$(date -u '+%Y-%m-%d')"
STARTED_AT="$(date -u '+%Y-%m-%dT%H:%M:%S')"
HOSTNAME_SAFE="$(hostname 2>/dev/null || echo unknown)"

# --- Counts -----------------------------------------------------------------
# One round trip. Reads the DEDUPED view: audit rows arrive at-least-once, and
# a replayed Spark batch would otherwise inflate counts into a false variance.
#
# COUNT(DISTINCT event_id) per stage: a redelivered message re-emits its stage
# events with the SAME deterministic event_id, so distinct-counting collapses
# redeliveries to the single message they represent.
#
# MESSAGE_DISCARDED is counted as ROWS: both discard paths (non-TextMessage and
# poison-threshold) run in the listener BEFORE the orchestrator emits
# MESSAGE_RECEIVED, so those messages carry a null event_id and never enter the
# audited funnel. They are reported as INFO, never as a drain from received.
QUERY_COUNTS="
SELECT
  COUNT(DISTINCT CASE WHEN event_type = 'MESSAGE_RECEIVED'        THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'MESSAGE_PARSED'          THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'ENRICHMENT_COMPLETED'    THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type IN ('HDFS_WRITE_COMPLETED',
                                          'HDFS_WRITE_SKIPPED')   THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'KAFKA_PUBLISH_COMPLETED' THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'PROCESSING_COMPLETED'    THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'HIVE_LOAD_COMPLETED'     THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'CLAIM_CHECK_SKIPPED'     THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'MESSAGE_QUARANTINED'
                       AND COALESCE(get_json_object(metadata_json, '\$.errorCode'),
                             CASE WHEN description LIKE 'Unparseable%' THEN 'PARSE_ERROR' END)
                           = 'PARSE_ERROR'                        THEN event_id END),
  COUNT(DISTINCT CASE WHEN event_type = 'MESSAGE_QUARANTINED'
                       AND COALESCE(get_json_object(metadata_json, '\$.errorCode'),
                             CASE WHEN description LIKE 'Non-retryable enrichment%'
                                  THEN 'ENRICHMENT_ERROR' END)
                           = 'ENRICHMENT_ERROR'                   THEN event_id END),
  SUM(CASE WHEN event_type = 'MESSAGE_DISCARDED' THEN 1 ELSE 0 END)
FROM ${DEDUPED_VIEW}
WHERE event_dt >= '${PART_FROM}' AND event_dt <= '${PART_TO}'
  AND event_timestamp >= '${WINDOW_START}'
  AND event_timestamp <  '${WINDOW_END}'
"

echo "============================================"
echo "ABC Balance Check"
echo "============================================"
echo "Run id:      ${RUN_ID}"
echo "Window:      ${WINDOW_START} (incl) .. ${WINDOW_END} (excl) UTC"
echo "Source view: ${DEDUPED_VIEW}"
echo "Control:     ${ABC_CONTROL_TABLE}"
echo ""

if [ "$DRY_RUN" = true ]; then
    echo "DRY RUN - nothing executed, nothing written."
    echo "Would run via: ${HIVE_CMD} <query>"
    echo "--- counts query:"
    echo "${QUERY_COUNTS}"
    echo "--- control insert: INSERT INTO TABLE ${ABC_CONTROL_TABLE} PARTITION (run_dt='${RUN_DT}') VALUES (...)"
    exit 0
fi

# shellcheck disable=SC2086
COUNTS_RAW="$(${HIVE_CMD} "${QUERY_COUNTS}" 2>"$ERR_FILE")"
if [ $? -ne 0 ] || [ -z "${COUNTS_RAW}" ]; then
    echo "ERROR: could not read audit counts" >&2
    sed 's/^/  hive: /' "$ERR_FILE" | tail -10 >&2
    echo "RESULT: COULD NOT EVALUATE (exit 3)"
    exit 3
fi

# Last non-empty line, tab separated
COUNTS_LINE="$(echo "${COUNTS_RAW}" | sed '/^[[:space:]]*$/d' | tail -1)"
IFS=$'\t' read -r RECEIVED PARSED ENRICHED HDFS_WRITTEN KAFKA_PUBLISHED COMPLETED \
    HIVE_LOADED CLAIM_SKIPPED QUAR_PARSE QUAR_ENRICH DISCARDED <<< "${COUNTS_LINE}"

# Hive prints NULL for SUM over no rows; normalise everything to an integer.
for v in RECEIVED PARSED ENRICHED HDFS_WRITTEN KAFKA_PUBLISHED COMPLETED \
         HIVE_LOADED CLAIM_SKIPPED QUAR_PARSE QUAR_ENRICH DISCARDED; do
    val="${!v:-0}"
    case "$val" in
        ''|NULL|null) val=0 ;;
        *[!0-9]*) echo "ERROR: non-numeric count for ${v}: '${val}'" >&2
                  echo "RESULT: COULD NOT EVALUATE (exit 3)"; exit 3 ;;
    esac
    printf -v "$v" '%s' "$val"
done

echo "Counts: received=${RECEIVED} parsed=${PARSED} enriched=${ENRICHED} hdfs=${HDFS_WRITTEN}"
echo "        kafka=${KAFKA_PUBLISHED} completed=${COMPLETED} hive_loaded=${HIVE_LOADED}"
echo "        quarantined(parse)=${QUAR_PARSE} quarantined(enrich)=${QUAR_ENRICH}"
echo "        claim_check_skipped=${CLAIM_SKIPPED} discarded_outside_funnel=${DISCARDED}"
echo ""

# --- Equation evaluation ----------------------------------------------------
VALUES=""
ANY_FAIL=false
ANY_WARN=false

printf "%-3s %-28s %-28s %9s %9s %9s %-6s %s\n" \
    "#" "from" "to" "expected" "actual" "variance" "status" "reason"
printf -- "----------------------------------------------------------------------------------------------------------\n"

# expected, actual, tolerance% -> emits a control row and prints a line
evaluate() {
    local no="$1" from="$2" to="$3" expected="$4" actual="$5" tol="$6" detail="$7"
    local variance=$(( expected - actual ))
    local variance_pct status reason

    if [ "$expected" -eq 0 ]; then
        variance_pct="0.0"
    else
        variance_pct="$(awk -v v="$variance" -v e="$expected" 'BEGIN{printf "%.4f", (v/e)*100}')"
    fi

    if [ "$variance" -eq 0 ]; then
        status="PASS"; reason="OK"
    elif [ "$variance" -lt 0 ]; then
        # Impossible for real flow: more messages downstream than upstream.
        # Proves audit-stream loss (publisher cooldown drops), not data loss.
        status="WARN"; reason="AUDIT_LOSS"; ANY_WARN=true
    else
        local within
        within="$(awk -v v="$variance_pct" -v t="$tol" 'BEGIN{vv=(v<0?-v:v); print (vv<=t)?1:0}')"
        if [ "$within" -eq 1 ] && [ "$(awk -v t="$tol" 'BEGIN{print (t>0)?1:0}')" -eq 1 ]; then
            status="WARN"; reason="WITHIN_TOLERANCE"; ANY_WARN=true
        else
            status="FAIL"; reason="POSSIBLE_LOSS"; ANY_FAIL=true
        fi
    fi

    printf "%-3s %-28s %-28s %9s %9s %9s %-6s %s\n" \
        "$no" "$from" "$to" "$expected" "$actual" "$variance" "$status" "$reason"

    local ended_at; ended_at="$(date -u '+%Y-%m-%dT%H:%M:%S')"
    [ -n "$VALUES" ] && VALUES="${VALUES},"
    VALUES="${VALUES}('${RUN_ID}','BALANCE_STAGE',${no},'${from}','${to}','${WINDOW_START}','${WINDOW_END}',${expected},${actual},${variance},${variance_pct},${tol},'${status}','${reason}','${detail}','${HOSTNAME_SAFE}','${STARTED_AT}','${ended_at}')"
}

# Drains are subtracted where they actually occur; see the DDL comments and
# docs/AUDIT_BALANCE_CONTROL.md for why each term is where it is.
evaluate 1 "MESSAGE_RECEIVED" "MESSAGE_PARSED" \
    $(( RECEIVED - QUAR_PARSE )) "${PARSED}" 0 \
    "received minus parse-quarantined should equal parsed"

evaluate 2 "MESSAGE_PARSED" "ENRICHMENT_COMPLETED" \
    $(( PARSED - QUAR_ENRICH )) "${ENRICHED}" 0 \
    "parsed minus enrichment-quarantined should equal enriched"

evaluate 3 "ENRICHMENT_COMPLETED" "HDFS_WRITE_COMPLETED+SKIPPED" \
    "${ENRICHED}" "${HDFS_WRITTEN}" 0 \
    "every enriched message must land in HDFS"

evaluate 4 "HDFS_WRITE_COMPLETED+SKIPPED" "KAFKA_PUBLISH_COMPLETED" \
    "${HDFS_WRITTEN}" "${KAFKA_PUBLISHED}" 0 \
    "every landed payload must be announced on Kafka"

evaluate 5 "KAFKA_PUBLISH_COMPLETED" "PROCESSING_COMPLETED" \
    "${KAFKA_PUBLISHED}" "${COMPLETED}" 0 \
    "every published message must reach the terminal bridge state"

evaluate 6 "PROCESSING_COMPLETED" "HIVE_LOAD_COMPLETED" \
    $(( COMPLETED - CLAIM_SKIPPED )) "${HIVE_LOADED}" "${ABC_TOLERANCE_PCT_HIVE_LOAD}" \
    "bridge-completed minus consumer-skipped should be loaded into Hive"

# Informational: messages discarded before entering the audited funnel.
ENDED_AT="$(date -u '+%Y-%m-%dT%H:%M:%S')"
VALUES="${VALUES},('${RUN_ID}','DISCARDED_OUTSIDE_FUNNEL',NULL,'MQ','MESSAGE_DISCARDED','${WINDOW_START}','${WINDOW_END}',0,${DISCARDED},0,0.0,0,'INFO','INFO','discarded before MESSAGE_RECEIVED - never entered the funnel','${HOSTNAME_SAFE}','${STARTED_AT}','${ENDED_AT}')"
printf "%-3s %-28s %-28s %9s %9s %9s %-6s %s\n" \
    "-" "MQ" "MESSAGE_DISCARDED" "-" "${DISCARDED}" "-" "INFO" "outside funnel"
echo ""

# --- Persist ----------------------------------------------------------------
INSERT_SQL="INSERT INTO TABLE ${ABC_CONTROL_TABLE} PARTITION (run_dt='${RUN_DT}') VALUES ${VALUES}"
# shellcheck disable=SC2086
if ! ${HIVE_CMD} "${INSERT_SQL}" >/dev/null 2>"$ERR_FILE"; then
    echo "ERROR: balance evaluated but the control row write FAILED - the verdict below is not recorded" >&2
    sed 's/^/  hive: /' "$ERR_FILE" | tail -10 >&2
    echo "RESULT: COULD NOT EVALUATE (exit 3)"
    exit 3
fi
echo "Control rows written to ${ABC_CONTROL_TABLE} (run_dt=${RUN_DT}, run_id=${RUN_ID})"
echo ""

# --- Verdict ----------------------------------------------------------------
echo "============================================"
if [ "$ANY_FAIL" = true ]; then
    echo "RESULT: FAIL - possible message loss (exit 1)"
    echo "Investigate: docs/AUDIT_BALANCE_CONTROL.md, runbook section for the failing equation."
    echo "============================================"
    exit 1
elif [ "$ANY_WARN" = true ]; then
    echo "RESULT: WARN - audit-stream loss or within-tolerance drift (exit 2)"
    echo "A negative variance means audit events were dropped, not messages."
    echo "============================================"
    exit 2
fi
echo "RESULT: PASS - all stage balances tie out (exit 0)"
echo "============================================"
exit 0
