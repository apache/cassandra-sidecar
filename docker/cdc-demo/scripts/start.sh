#!/usr/bin/env bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Builds the sidecar and starts the CDC demo stack.
#
# Usage (from anywhere in the repo):
#   ./scripts/start.sh                    # build + start in confluent mode (default)
#   ./scripts/start.sh --bytearray        # build + start in bytearray mode
#   ./scripts/start.sh --clean            # wipe all data volumes before starting
#   ./scripts/start.sh --skip-build       # reuse existing cassandra-sidecar:dev image
#   ./scripts/start.sh --clean --skip-build
set -euo pipefail

# ANSI color codes
BOLD='\033[1m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[0;33m'
UNDERLINE='\033[4m'
RESET='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CLEAN=false
SKIP_BUILD=false
SERIALIZER_MODE=confluent

usage() {
    printf "Usage: %s [OPTIONS]\n\n" "$(basename "$0")"
    printf "Builds the sidecar and starts the CDC demo stack.\n\n"
    printf "Options:\n"
    printf "  --confluent     Use Confluent Avro serializer (default)\n"
    printf "  --bytearray     Use byte-array serializer\n"
    printf "  --clean         Wipe all data volumes before starting\n"
    printf "  --skip-build    Reuse existing cassandra-sidecar:dev image\n"
    printf "  --help          Show this help message\n"
}

for arg in "$@"; do
    case "$arg" in
        --clean)       CLEAN=true ;;
        --skip-build)  SKIP_BUILD=true ;;
        --confluent)   SERIALIZER_MODE=confluent ;;
        --bytearray)   SERIALIZER_MODE=bytearray ;;
        --help)        usage; exit 0 ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

# ── Stop existing stack ───────────────────────────────────────────────────────
if $CLEAN; then
    bash "$SCRIPT_DIR/stop.sh" --clean
else
    bash "$SCRIPT_DIR/stop.sh"
fi

# ── Build ─────────────────────────────────────────────────────────────────────
if $SKIP_BUILD; then
    if ! docker image inspect cassandra-sidecar:dev > /dev/null 2>&1; then
        echo "ERROR: --skip-build specified but cassandra-sidecar:dev image not found." >&2
        echo "       Run without --skip-build to build the image first." >&2
        exit 1
    fi
    printf "${YELLOW}Skipping build — reusing existing cassandra-sidecar:dev image.${RESET}\n"
else
    printf "${BOLD}==> Building sidecar distribution (./gradlew installDist)...${RESET}\n"
    "$REPO_ROOT/gradlew" -p "$REPO_ROOT" installDist \
        -x test -x integrationTest -x containerTest \
        --parallel --quiet

    printf "${BOLD}==> Building sidecar Docker image...${RESET}\n"
    DOCKER_BUILDKIT=1 docker build \
        -f "$REPO_ROOT/docker/cdc-demo/Dockerfile.sidecar" \
        -t cassandra-sidecar:dev \
        "$REPO_ROOT"
fi

# ── Start stack ───────────────────────────────────────────────────────────────
printf "${BOLD}==> Starting stack (serializer-mode: ${SERIALIZER_MODE})...${RESET}\n"
cd "$DEMO_DIR"
export SERIALIZER_MODE
docker compose up -d

# ── Wait for sidecar ─────────────────────────────────────────────────────────
echo ""
echo "Waiting for sidecar to be ready (follow progress: docker compose logs -f cassandra-init sidecar)..."
until curl -sf http://localhost:9043/api/v1/__health > /dev/null 2>&1; do
    sleep 5
done

echo "Sidecar is up. Waiting for CDC iterators to start..."
CDC_TIMEOUT=360
docker compose logs -f sidecar 2>&1 | grep -m 1 "CDC iterators started successfully" > /dev/null &
LOG_PID=$!
ELAPSED=0
while kill -0 "$LOG_PID" 2>/dev/null; do
    if [ "$ELAPSED" -ge "$CDC_TIMEOUT" ]; then
        kill "$LOG_PID" 2>/dev/null || true
        echo "Warning: timed out after ${CDC_TIMEOUT}s waiting for CDC iterators — check: docker compose logs sidecar"
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

# ── Success banner ────────────────────────────────────────────────────────────
echo ""
printf "${GREEN}${BOLD}╔══════════════════════════════════════════════════════════╗${RESET}\n"
printf "${GREEN}${BOLD}║        Setup complete. CDC pipeline is running.          ║${RESET}\n"
printf "${GREEN}${BOLD}╚══════════════════════════════════════════════════════════╝${RESET}\n"
echo ""
printf "  ${BOLD}Serializer mode:${RESET} ${SERIALIZER_MODE}\n"
echo ""
printf "  ${BOLD}Step 1 — Insert a test mutation:${RESET}\n"
printf "  ${CYAN}\$ docker exec -it cdc-demo-cassandra-1 cqlsh -e \"INSERT INTO cdc_demo.events (id, msg, ts) VALUES (uuid(), 'hello', toTimestamp(now()));\"${RESET}\n"
echo ""
if [ "$SERIALIZER_MODE" = "confluent" ]; then
    printf "  ${BOLD}Step 2 — Inspect the registered Avro schema:${RESET}\n"
    printf "  ${UNDERLINE}http://localhost:8080/ui/clusters/local/schemas/cdc-mutations-value${RESET}\n"
    echo ""
    printf "  ${BOLD}Step 3 — View decoded messages in Kafka UI:${RESET}\n"
    printf "  ${UNDERLINE}http://localhost:8080/ui/clusters/local/all-topics/cdc-mutations/messages${RESET}\n"
    printf "  ${CYAN}(Set Key Serde → String, Value Serde → SchemaRegistry to view decoded messages)${RESET}\n"
else
    printf "  ${BOLD}Step 2 — View mutations in Kafka UI:${RESET}\n"
    printf "  ${UNDERLINE}http://localhost:8080/ui/clusters/local/all-topics/cdc-mutations/messages${RESET}\n"
fi
echo ""
printf "  ${BOLD}To stop:${RESET}       ${CYAN}\$ ./scripts/stop.sh${RESET}\n"
printf "  ${BOLD}To wipe data:${RESET}  ${CYAN}\$ ./scripts/stop.sh --clean${RESET}\n"
echo ""
