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

set -xe

REPO_DIR="${M2_HOME:-${HOME}/.m2/repository}"
SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
DTEST_JAR_DIR="$(dirname "${SCRIPT_DIR}/")/dtest-jars"
DTEST_ARTIFACT_ID=cassandra-dtest-local-all

function extract_version {
  echo "$1" | sed -n "s/^.*dtest-jars\/dtest-\(\S*\)\..*$/\1/p"
}

function install {
  CASSANDRA_VERSION="$1"
  "${SCRIPT_DIR}/mvnw" install:install-file                                     \
                       -Dfile="${DTEST_JAR_DIR}/dtest-${CASSANDRA_VERSION}.jar" \
                       -DgroupId=org.apache.cassandra                           \
                       -DartifactId="${DTEST_ARTIFACT_ID}"                      \
                       -Dversion="${CASSANDRA_VERSION}"                         \
                       -Dpackaging=jar                                          \
                       -DgeneratePom=true                                       \
                       -DlocalRepositoryPath="${REPO_DIR}"
}

for jar in "${DTEST_JAR_DIR}"/*.jar; do
  if [ -f "$jar" ]; then
    echo "Installing the jar: $jar"
    install "$(extract_version ${jar})"
  fi
done
