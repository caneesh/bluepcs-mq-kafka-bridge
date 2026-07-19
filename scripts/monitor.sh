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

PROFILE="${1:-test-env}"
JAR_FILE="${PROJECT_DIR}/target/mq-kafka-bridge-*.jar"

# Load .env if present (same convention as the other scripts)
if [ -f "${PROJECT_DIR}/.env" ]; then
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

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

# - web-application-type=none: the monitor JVM must not fight the running
#   bridge for port 8080
# - listener-enabled=false: the monitor must never consume a message
java -jar "${JAR_PATH}" \
    --spring.profiles.active="${PROFILE}" \
    --spring.main.web-application-type=none \
    --bridge.monitor.enabled=true \
    --bridge.mq.listener-enabled=false

EXIT_CODE=$?

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
