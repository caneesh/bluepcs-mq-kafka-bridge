#!/bin/bash
# =============================================================================
# Quarantine Replay Script
# =============================================================================
# Re-puts quarantined payloads from the HDFS error directory back onto the MQ
# input queue. Quarantine is otherwise a one-way door: a quarantined message
# was ACKed away from the queue and exists only as <error-path>/<eventId>.json.
#
# Usage:
#   ./quarantine-replay.sh list                        # show quarantined files
#   ./quarantine-replay.sh <hdfsPath> [hdfsPath...]    # replay these files
#
# Profile comes from BRIDGE_PROFILE in .env (default test-env); override by
# exporting BRIDGE_PROFILE before running.
#
# Replay semantics (see QuarantineReplayRunner):
#   - the payload is re-put verbatim as a NEW message (new JMSMessageID, new
#     eventId); the running bridge picks it up and processes it fresh
#   - quarantined messages never reached Kafka, so replay cannot create
#     downstream duplicates by itself
#   - each successfully replayed file is moved to <error-path>/replayed/ so a
#     re-run cannot double-replay it
#
# ONLY replay a file after fixing what quarantined it (e.g. a parser fix for
# an unparseable message) — otherwise it will simply quarantine again.
#
# Exit codes (propagated from the JVM):
#   0 - all files replayed (or list printed)
#   1 - at least one file failed (check the log; a replayed-but-not-moved file
#       MUST be moved manually before re-running)
#   2 - bad arguments
#   3 - missing prerequisites (secrets / JAR) — set by this script, not the JVM
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

if [ $# -lt 1 ]; then
    echo "Usage: ./quarantine-replay.sh list | <hdfsPath> [hdfsPath...]"
    exit 2
fi

# Load .env if present. Note: .env values override anything already exported
# in the shell, including blank assignments — remove a line from .env to use
# an exported value instead.
if [ -f "${PROJECT_DIR}/.env" ]; then
    echo "Loading environment from ${PROJECT_DIR}/.env"
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

PROFILE="${BRIDGE_PROFILE:-test-env}"

# Join all arguments into the comma-separated --bridge.replay value
REPLAY_ARG="$1"
shift
for path in "$@"; do
    REPLAY_ARG="${REPLAY_ARG},${path}"
done

# The profiles declare secrets with no defaults; require them here for a clear
# message instead of a Spring placeholder-resolution stack trace.
MISSING_VARS=0
for var in KAFKA_TRUSTSTORE_PASSWORD OAUTH_CLIENT_ID OAUTH_CLIENT_SECRET API_PASSWORD; do
    if [ -z "${!var}" ]; then
        echo "MISSING: $var"
        MISSING_VARS=$((MISSING_VARS + 1))
    fi
done

if [ $MISSING_VARS -gt 0 ]; then
    echo ""
    echo "ERROR: $MISSING_VARS required secret(s) not set."
    echo "Copy .env.template to .env in the project root and fill in the values,"
    echo "or export the variables before running this script."
    exit 3
fi

JAR_FILE="${PROJECT_DIR}/target/mq-kafka-bridge-*.jar"
if ! ls ${JAR_FILE} 1> /dev/null 2>&1; then
    echo "ERROR: JAR not found under ${PROJECT_DIR}/target. Build or copy it first."
    exit 3
fi
JAR_PATH=$(ls ${JAR_FILE} | head -1)

echo "============================================"
echo "MQ-Kafka Bridge - Quarantine Replay"
echo "============================================"
echo "Replay: ${REPLAY_ARG}"
echo "Profile: ${PROFILE}"
echo "JAR: ${JAR_PATH}"
echo "============================================"
echo ""

set +e
# - web-application-type=none: must not fight the running bridge for port 8080
# - listener-enabled=false: the replay JVM must never consume; the RUNNING
#   bridge is the intended consumer of the replayed message
java -jar "${JAR_PATH}" \
    --spring.profiles.active="${PROFILE}" \
    --spring.main.web-application-type=none \
    --bridge.replay="${REPLAY_ARG}" \
    --bridge.mq.listener-enabled=false

EXIT_CODE=$?
set -e

echo ""
echo "============================================"
case $EXIT_CODE in
    0) echo "RESULT: OK" ;;
    1) echo "RESULT: FAILURE - at least one file failed; check the log above." ;;
    2) echo "RESULT: BAD ARGS" ;;
    *) echo "RESULT: FAIL (exit code: $EXIT_CODE)" ;;
esac
echo "============================================"

exit $EXIT_CODE
