#!/bin/bash
# =============================================================================
# Validate-Only Mode Script
# =============================================================================
# Runs the application in validate-only mode to verify configuration and
# connectivity without consuming any MQ messages.
#
# Usage: ./validate-only.sh [prod|test-env]
#
# Exit codes:
#   0 - All validation checks passed
#   1 - One or more validation checks failed
#   2 - Validation exception occurred
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

PROFILE="${1:-prod}"
JAR_FILE="${PROJECT_DIR}/target/mq-kafka-bridge-*.jar"

echo "============================================"
echo "MQ-Kafka Bridge - Validate Only Mode"
echo "============================================"
echo "Profile: ${PROFILE}"
echo "Project Dir: ${PROJECT_DIR}"
echo ""

# Check if JAR exists
if ! ls ${JAR_FILE} 1> /dev/null 2>&1; then
    echo "ERROR: JAR file not found. Run 'mvn package' first."
    exit 3
fi

JAR_PATH=$(ls ${JAR_FILE} | head -1)
echo "JAR: ${JAR_PATH}"
echo ""

# Check required environment variables
echo "Checking environment variables..."
MISSING_VARS=0

check_var() {
    if [ -z "${!1}" ]; then
        echo "  MISSING: $1"
        MISSING_VARS=$((MISSING_VARS + 1))
    else
        if [[ "$1" == *"PASSWORD"* ]] || [[ "$1" == *"SECRET"* ]]; then
            echo "  OK: $1 = ********"
        else
            echo "  OK: $1 = ${!1}"
        fi
    fi
}

check_var "MQ_HOST"
check_var "MQ_PASSWORD"
check_var "KAFKA_BOOTSTRAP_SERVERS"
check_var "KAFKA_TRUSTSTORE_PASSWORD"
check_var "HDFS_NAMENODE"
check_var "API_BASE_URL"
check_var "OAUTH_TOKEN_URL"
check_var "OAUTH_CLIENT_SECRET"

echo ""

if [ $MISSING_VARS -gt 0 ]; then
    echo "WARNING: $MISSING_VARS required environment variable(s) missing."
    echo "The application may fail to start."
    echo ""
fi

echo "Starting validation..."
echo "============================================"
echo ""

java -jar "${JAR_PATH}" \
    --spring.profiles.active="${PROFILE}" \
    --bridge.validate-only=true \
    --bridge.mq.listener-enabled=false

EXIT_CODE=$?

echo ""
echo "============================================"
case $EXIT_CODE in
    0) echo "RESULT: PASSED" ;;
    1) echo "RESULT: FAILED (checks failed)" ;;
    2) echo "RESULT: FAILED (exception)" ;;
    *) echo "RESULT: FAILED (exit code: $EXIT_CODE)" ;;
esac
echo "============================================"

exit $EXIT_CODE
