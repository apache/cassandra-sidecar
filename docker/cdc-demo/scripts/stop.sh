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
# Stops the CDC demo stack.
#
# Usage (from anywhere in the repo):
#   ./scripts/stop.sh           # stop containers, keep volumes (data preserved)
#   ./scripts/stop.sh --clean   # stop containers AND delete volumes (full wipe)
set -euo pipefail

BOLD='\033[1m'
YELLOW='\033[0;33m'
GREEN='\033[0;32m'
RESET='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEMO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLEAN=false

for arg in "$@"; do
    case "$arg" in
        --clean) CLEAN=true ;;
        *) echo "Unknown argument: $arg" >&2; exit 1 ;;
    esac
done

cd "$DEMO_DIR"

if $CLEAN; then
    printf "${YELLOW}${BOLD}Stopping stack and wiping all volumes...${RESET}\n"
    docker compose down -v --remove-orphans
    printf "${GREEN}Stack stopped. All data volumes removed.${RESET}\n"
else
    printf "${YELLOW}${BOLD}Stopping stack (volumes preserved)...${RESET}\n"
    docker compose down --remove-orphans
    printf "${GREEN}Stack stopped. Run with --clean to also remove data volumes.${RESET}\n"
fi
