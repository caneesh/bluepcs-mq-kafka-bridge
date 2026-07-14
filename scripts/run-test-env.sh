#!/bin/bash
# =============================================================================
# Test Environment Run Script
# =============================================================================
# Starts the application with the test-env profile. Sources .env from the
# project root if present, then verifies the required secrets are set.
#
# Usage: ./run-test-env.sh [--validate-only] [--listener-enabled] [--port PORT]
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

VALIDATE_ONLY=false
LISTENER_ENABLED=false
PORT="${SERVER_PORT:-8080}"

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --validate-only)
            VALIDATE_ONLY=true
            shift
            ;;
        --listener-enabled)
            LISTENER_ENABLED=true
            shift
            ;;
        --port)
            PORT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: ./run-test-env.sh [--validate-only] [--listener-enabled] [--port PORT]"
            exit 1
            ;;
    esac
done

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

# The test-env profile has no defaults for these; fail here with a clear
# message instead of a Spring placeholder-resolution error at startup.
# MQ_PASSWORD is intentionally NOT required: the test queue manager
# authenticates by user id / channel auth without a password.
MISSING_VARS=0
for var in KAFKA_TRUSTSTORE_PASSWORD OAUTH_CLIENT_SECRET; do
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

echo "============================================"
echo "MQ-Kafka Bridge - Test Environment"
echo "============================================"
echo "Profile: test-env"
echo "Port: ${PORT}"
echo "Validate Only: ${VALIDATE_ONLY}"
echo "Listener Enabled: ${LISTENER_ENABLED}"
echo ""

# Check if JAR exists, if not build
JAR_FILE="${PROJECT_DIR}/target/mq-kafka-bridge-*.jar"
if ! ls ${JAR_FILE} 1> /dev/null 2>&1; then
    echo "JAR not found. Building..."
    cd "$PROJECT_DIR"
    mvn package -DskipTests -q
    echo "Build complete."
    echo ""
fi

JAR_PATH=$(ls ${JAR_FILE} | head -1)
echo "JAR: ${JAR_PATH}"
echo ""

echo "Starting application..."
echo "============================================"
echo ""

java -jar "${JAR_PATH}" \
    --spring.profiles.active=test-env \
    --bridge.validate-only="${VALIDATE_ONLY}" \
    --bridge.mq.listener-enabled="${LISTENER_ENABLED}" \
    --server.port="${PORT}"
