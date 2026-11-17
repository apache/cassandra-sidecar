#!/bin/bash
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

# Publishes a SNAPSHOT build of the Cassandra Sidecar artifacts to the ASF Nexus
# snapshot repository.
set -e -o pipefail

SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
ROOT_DIR=$( dirname -- "${SCRIPT_DIR}"; )

MAVEN_REPOSITORY_URL="${MAVEN_REPOSITORY_URL:-https://repository.apache.org/content/repositories/snapshots}"
MAVEN_USERNAME="${MAVEN_USERNAME:-}"
MAVEN_PASSWORD="${MAVEN_PASSWORD:-}"

if [[ -z "${MAVEN_USERNAME}" || -z "${MAVEN_PASSWORD}" ]]; then
    echo "MAVEN_USERNAME and MAVEN_PASSWORD must both be set"
    exit 1
fi

version=$(grep '^version=' "${ROOT_DIR}/gradle.properties" | cut -d'=' -f2)

if [[ "${version}" != *-SNAPSHOT ]]; then
    echo "Refusing to publish: version '${version}' does not end in -SNAPSHOT."
    echo "This script only publishes development snapshots, never release versions."
    exit 1
fi

echo "Publishing artifacts for version ${version} to ${MAVEN_REPOSITORY_URL}"

"${ROOT_DIR}/gradlew" --no-daemon \
    -PskipSigning \
    -Pmaven.repository.url="${MAVEN_REPOSITORY_URL}" \
    -Pmaven.username="${MAVEN_USERNAME}" \
    -Pmaven.password="${MAVEN_PASSWORD}" \
    publish --stacktrace
