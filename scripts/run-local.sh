#!/bin/bash
# =============================================================================
# Local Development Run Script
# =============================================================================
# Starts the application in local profile with H2 database and mock services.
#
# Usage: ./run-local.sh [--validate-only] [--listener-enabled]
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
            echo "Usage: ./run-local.sh [--validate-only] [--listener-enabled] [--port PORT]"
            exit 1
            ;;
    esac
done

echo "============================================"
echo "MQ-Kafka Bridge - Local Development"
echo "============================================"
echo "Profile: local"
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
    --spring.profiles.active=local \
    --bridge.validate-only="${VALIDATE_ONLY}" \
    --bridge.mq.listener-enabled="${LISTENER_ENABLED}" \
    --server.port="${PORT}"
