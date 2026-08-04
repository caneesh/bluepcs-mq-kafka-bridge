#!/bin/bash
# =============================================================================
# Smoke Test Script
# =============================================================================
# Starts the application with listener disabled and verifies health endpoints.
#
# Usage: ./smoke-test.sh [prod|test-env]
#   profile defaults to BRIDGE_PROFILE from .env, then test-env
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load .env if present (same convention as every sibling script) — the profiles
# declare secrets with no defaults, so without this the context dies on
# placeholder resolution and the failure masquerades as "health not responding".
if [ -f "${PROJECT_DIR}/.env" ]; then
    echo "Loading environment from ${PROJECT_DIR}/.env"
    set -a
    # shellcheck disable=SC1091
    source "${PROJECT_DIR}/.env"
    set +a
fi

# Profile: explicit argument > BRIDGE_PROFILE from .env > test-env
PROFILE="${1:-${BRIDGE_PROFILE:-test-env}}"
JAR_FILE="${PROJECT_DIR}/target/mq-kafka-bridge-*.jar"
PORT="${SERVER_PORT:-8080}"
HEALTH_URL="http://localhost:${PORT}/actuator/health"
MAX_WAIT=60
PID_FILE="/tmp/bridge-smoke-test.pid"

echo "============================================"
echo "MQ-Kafka Bridge - Smoke Test"
echo "============================================"
echo "Profile: ${PROFILE}"
echo "Port: ${PORT}"
echo ""

# Cleanup function
cleanup() {
    if [ -f "$PID_FILE" ]; then
        PID=$(cat "$PID_FILE")
        if kill -0 "$PID" 2>/dev/null; then
            echo "Stopping application (PID: $PID)..."
            kill "$PID"
            sleep 2
        fi
        rm -f "$PID_FILE"
    fi
}

trap cleanup EXIT

# Check if JAR exists
if ! ls ${JAR_FILE} 1> /dev/null 2>&1; then
    echo "ERROR: JAR file not found. Run 'mvn package' first."
    exit 1
fi

JAR_PATH=$(ls ${JAR_FILE} | head -1)

# Start application in background with listener disabled
echo "Starting application (listener disabled)..."
# require-listener-enabled=false: the smoke test deliberately starts without
# consuming; without this the prod profile's go-live gate would refuse to start
mkdir -p "${PROJECT_DIR}/logs"
# Own log file: must not share a running bridge's rolling log (see monitor.sh)
java -jar "${JAR_PATH}" \
    --spring.profiles.active="${PROFILE}" \
    --bridge.mq.listener-enabled=false \
    --bridge.mq.require-listener-enabled=false \
    --bridge.validate-only=false \
    --logging.file.name="${PROJECT_DIR}/logs/bridge-smoke-test.log" \
    --server.port="${PORT}" &

APP_PID=$!
echo $APP_PID > "$PID_FILE"
echo "Application PID: $APP_PID"
echo ""

# Wait for the health endpoint to RESPOND — any HTTP status counts as responding
# (a 503 means the app started with a DOWN indicator; that verdict belongs to the
# status check below, not to a misleading "not responding after 60s" error)
echo "Waiting for health endpoint..."
COUNTER=0
while [ $COUNTER -lt $MAX_WAIT ]; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "$HEALTH_URL") || HTTP_CODE="000"
    if [ "$HTTP_CODE" != "000" ]; then
        echo ""
        echo "Health endpoint responding (HTTP ${HTTP_CODE})"
        break
    fi
    echo -n "."
    sleep 1
    COUNTER=$((COUNTER + 1))
done

echo ""

if [ $COUNTER -ge $MAX_WAIT ]; then
    echo "ERROR: Health endpoint not responding after ${MAX_WAIT} seconds"
    exit 1
fi

# Check health details
echo "============================================"
echo "Health Check Response:"
echo "============================================"
curl -s "$HEALTH_URL" | python3 -m json.tool 2>/dev/null || curl -s "$HEALTH_URL"
echo ""
echo ""

# Verify health status
HEALTH_STATUS=$(curl -s "$HEALTH_URL" | grep -o '"status":"[^"]*"' | head -1 | cut -d'"' -f4)

echo "============================================"
if [ "$HEALTH_STATUS" == "UP" ]; then
    echo "SMOKE TEST: PASSED"
    echo "Application is healthy and ready for deployment."
    EXIT_CODE=0
else
    echo "SMOKE TEST: FAILED"
    echo "Health status: $HEALTH_STATUS"
    EXIT_CODE=1
fi
echo "============================================"

exit $EXIT_CODE
