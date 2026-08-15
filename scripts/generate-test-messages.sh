#!/bin/bash
# =============================================================================
# Generate one MQ message file per marketingPlanIdentifier
# =============================================================================
# Produces the MINIMAL valid planNotification the bridge's parser accepts — one
# file per identifier, ready to feed to an MQ publisher that takes a single file
# at a time.
#
# Minimal is deliberate: the whole planNotification node is copied verbatim into
# the HDFS wrapper and flows to the downstream consumer, so no invented business
# attributes (product type, channels, divisions, ...) are emitted here.
#
# Fields written, and why each is present (see JsonMessageParser):
#   planNotification                                     required, must be an object
#   .marketingPlanIdentifier                             required, non-empty  -> entityId
#   .planVersion.planEffectivityDates.effectiveStartDate required, must start yyyy-MM-dd
#                                                        -> effectiveDate (first 10 chars)
#   .planVersion.planVersionIdentifier                   optional -> transactionId "<id>-v<n>"
#   .changeEvent.eventName                               optional -> eventType
#                                                        (falls back to typeName, then "Unknown")
#   .changeEvent.timestamp                               optional -> eventTimestamp
#                                                        (unparseable/absent -> receipt time)
#
# Each message drives one enrichment call: GET {baseUrl}/{identifier}/{date}
#
# Usage:
#   ./generate-test-messages.sh <identifier-list-file> [options]
#
#   The list is one identifier per line. Blank lines, lines starting with '#',
#   surrounding whitespace and CR (CRLF files) are ignored; if a line contains
#   commas the FIRST field is used, so a one-column CSV export works as-is.
#
# Options:
#   -o, --out-dir DIR     output directory   (default: target/test-messages)
#   -d, --date DATE       effective start date, yyyy-MM-dd (default: 2027-01-01)
#   -v, --version VER     planVersionIdentifier            (default: 1)
#   -e, --event NAME      changeEvent.eventName            (default: ReadyToSell)
#   -t, --type NAME       changeEvent.typeName             (default: Update)
#       --force           overwrite a non-empty output directory
#
# Examples:
#   ./generate-test-messages.sh plans.txt
#   ./generate-test-messages.sh plans.csv -d 2027-01-01 -o /tmp/msgs
#
# Exit codes:
#   0 - all files written
#   1 - nothing written (empty list, or every line rejected)
#   2 - bad arguments / unreadable list / unsafe identifier found
# =============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

OUT_DIR="${PROJECT_DIR}/target/test-messages"
EFFECTIVE_DATE="2027-01-01"
PLAN_VERSION="1"
EVENT_NAME="ReadyToSell"
TYPE_NAME="Update"
FORCE=false
LIST_FILE=""

while [ $# -gt 0 ]; do
    case "$1" in
        -o|--out-dir) OUT_DIR="$2"; shift 2 ;;
        -d|--date)    EFFECTIVE_DATE="$2"; shift 2 ;;
        -v|--version) PLAN_VERSION="$2"; shift 2 ;;
        -e|--event)   EVENT_NAME="$2"; shift 2 ;;
        -t|--type)    TYPE_NAME="$2"; shift 2 ;;
        --force)      FORCE=true; shift ;;
        -h|--help)    sed -n '2,50p' "$0"; exit 0 ;;
        -*)           echo "ERROR: unknown option '$1'" >&2; exit 2 ;;
        *)            if [ -n "$LIST_FILE" ]; then echo "ERROR: more than one list file given" >&2; exit 2; fi
                      LIST_FILE="$1"; shift ;;
    esac
done

if [ -z "$LIST_FILE" ]; then
    echo "ERROR: no identifier list file given" >&2
    echo "Usage: ./generate-test-messages.sh <identifier-list-file> [options]" >&2
    exit 2
fi
if [ ! -r "$LIST_FILE" ]; then
    echo "ERROR: cannot read identifier list: ${LIST_FILE}" >&2
    exit 2
fi

# The parser only needs the yyyy-MM-dd prefix, but a malformed date here would
# quarantine all 500 messages — reject it now rather than at 3am.
if ! echo "$EFFECTIVE_DATE" | grep -qE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
    echo "ERROR: --date must be yyyy-MM-dd (got '${EFFECTIVE_DATE}')" >&2
    exit 2
fi

if [ -d "$OUT_DIR" ] && [ -n "$(ls -A "$OUT_DIR" 2>/dev/null)" ] && [ "$FORCE" != true ]; then
    echo "ERROR: output directory is not empty: ${OUT_DIR}" >&2
    echo "       re-run with --force to overwrite, or choose another -o directory" >&2
    exit 2
fi
mkdir -p "$OUT_DIR" || { echo "ERROR: cannot create ${OUT_DIR}" >&2; exit 2; }

# 06:00:00Z matches the real feed's convention; only the date prefix is parsed.
START_DATE_TS="${EFFECTIVE_DATE}T06:00:00.000Z"
EVENT_TS="$(date -u '+%Y-%m-%dT%H:%M:%S.000Z')"

written=0
skipped=0
declare -A seen

while IFS= read -r raw || [ -n "$raw" ]; do
    line="${raw%$'\r'}"                 # tolerate CRLF
    line="${line%%,*}"                  # first CSV field if comma-separated
    line="$(echo "$line" | tr -d '[:space:]')"
    [ -z "$line" ] && continue
    case "$line" in \#*) continue ;; esac

    # Identifiers go into JSON unescaped and into a filename — accept only a
    # conservative charset instead of hand-rolling JSON/shell escaping.
    if ! echo "$line" | grep -qE '^[A-Za-z0-9._-]+$'; then
        echo "SKIP: unsafe identifier (allowed: A-Z a-z 0-9 . _ -): '${line}'" >&2
        skipped=$((skipped + 1))
        continue
    fi
    if [ -n "${seen[$line]:-}" ]; then
        echo "SKIP: duplicate identifier: ${line}" >&2
        skipped=$((skipped + 1))
        continue
    fi
    seen[$line]=1

    cat > "${OUT_DIR}/${line}.json" <<EOF
{
  "planNotification": {
    "marketingPlanIdentifier": "${line}",
    "planVersion": {
      "planVersionIdentifier": "${PLAN_VERSION}",
      "planEffectivityDates": {
        "effectiveStartDate": "${START_DATE_TS}"
      }
    },
    "changeEvent": {
      "eventName": "${EVENT_NAME}",
      "typeName": "${TYPE_NAME}",
      "timestamp": "${EVENT_TS}"
    }
  }
}
EOF
    written=$((written + 1))
done < "$LIST_FILE"

echo "============================================"
echo "Generated ${written} message file(s)"
[ "$skipped" -gt 0 ] && echo "Skipped   ${skipped} line(s) — see SKIP lines above"
echo "Output:    ${OUT_DIR}"
echo "Date:      ${EFFECTIVE_DATE}  (effectiveStartDate ${START_DATE_TS})"
echo "Version:   ${PLAN_VERSION}   Event: ${EVENT_NAME}/${TYPE_NAME}"
echo "Enrichment calls will be: GET {baseUrl}/{identifier}/${EFFECTIVE_DATE}"
echo "============================================"

[ "$written" -eq 0 ] && { echo "ERROR: no files written" >&2; exit 1; }
exit 0
