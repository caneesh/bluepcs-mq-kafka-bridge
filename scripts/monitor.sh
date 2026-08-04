#!/bin/bash
# =============================================================================
# Monitor Mode Script (for Control-M / external schedulers)
# =============================================================================
# Read-only checks against the RUNNING bridge instance:
#   - actuator health (incl. mqListener listener-enabled state)
#   - HDFS landing-directory backlog
#
# The monitor never remediates — it exits with a status code the scheduler
# routes to alerts. Restarts belong to systemd / the health watchdog.
#
# Usage: ./monitor.sh [prod|test-env]
#
# Exit codes:
#   0 - All checks passed
#   1 - Bridge unreachable or health DOWN
#   2 - Bridge up but MQ listener disabled (NOT consuming)
#   3 - HDFS landing-directory backlog exceeded
#   4 - Monitor could not evaluate a check
# =============================================================================

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env FIRST (same convention as the other scripts): it supplies
# BRIDGE_PROFILE so the same Control-M job definition works unchanged in
# test and prod.
if [ -f "${PROJECT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

# Profile: explicit argument > BRIDGE_PROFILE from .env > test-env (safe default)
PROFILE="${1:-${BRIDGE_PROFILE:-test-env}}"
JAR_FILE="${PROJECT_DIR}/target/mq-kafka-bridge-*.jar"

if ! ls ${JAR_FILE} 1> /dev/null 2>&1; then
    echo "ERROR: JAR file not found. Run 'mvn package' first."
    exit 4
fi

JAR_PATH=$(ls ${JAR_FILE} | head -1)

echo "============================================"
echo "MQ-Kafka Bridge - Monitor Mode"
echo "Profile: ${PROFILE}"
echo "JAR: ${JAR_PATH}"
echo "============================================"

mkdir -p "${PROJECT_DIR}/logs"
MONITOR_OUT="$(mktemp)"
trap 'rm -f "$MONITOR_OUT"' EXIT

# - web-application-type=none: the monitor JVM must not fight the running
#   bridge for port 8080
# - listener-enabled=false: the monitor must never consume a message
# - logging.file.name: the monitor JVM must NOT share the running bridge's
#   rolling log file — two logback instances rolling the same file rename it
#   out from under each other and lose lines
java -jar "${JAR_PATH}" \
    --spring.profiles.active="${PROFILE}" \
    --spring.main.web-application-type=none \
    --bridge.monitor.enabled=true \
    --bridge.mq.listener-enabled=false \
    --logging.file.name="${PROJECT_DIR}/logs/bridge-monitor.log" \
    2>&1 | tee "$MONITOR_OUT"

EXIT_CODE=${PIPESTATUS[0]}

# A Spring context that fails to START also exits 1 — but that is a monitor-side
# problem (exit 4: investigate this JVM/host), not "bridge unreachable" (exit 1:
# page someone). The runner always prints a MONITOR RESULT marker once its checks
# actually ran; a non-zero exit with no marker means they never did.
if [ "$EXIT_CODE" -ne 0 ] && ! grep -q "MONITOR RESULT:" "$MONITOR_OUT"; then
    echo "MONITOR: JVM exited ${EXIT_CODE} before running any check — reclassifying as exit 4"
    EXIT_CODE=4
fi

echo "============================================"
case $EXIT_CODE in
    0) echo "MONITOR: PASSED" ;;
    1) echo "MONITOR: FAILED - bridge unreachable or health DOWN" ;;
    2) echo "MONITOR: FAILED - bridge up but NOT consuming (listener disabled)" ;;
    3) echo "MONITOR: FAILED - HDFS landing-directory backlog" ;;
    4) echo "MONITOR: FAILED - monitor could not evaluate" ;;
    *) echo "MONITOR: FAILED - unexpected exit code $EXIT_CODE" ;;
esac
echo "============================================"

exit $EXIT_CODE
