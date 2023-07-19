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
BRANCHES=${BRANCHES:-cassandra-4.0 trunk}
REPO=${REPO:-"git@github.com:apache/cassandra.git"}
SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
DTEST_JAR_DIR="$(dirname "${SCRIPT_DIR}/")/dtest-jars"
BUILD_DIR="${DTEST_JAR_DIR}/build"
mkdir -p "${BUILD_DIR}"

# host key verification
mkdir -p ~/.ssh
ssh-keyscan github.com >> ~/.ssh/known_hosts

for branch in $BRANCHES; do
  cd "${BUILD_DIR}"
  # check out the correct cassandra version:
  if [ ! -d "${branch}" ] ; then
    git clone --depth 1 --single-branch --branch "${branch}" "${REPO}" "${branch}"
    cd "${branch}"
  else
    cd "${branch}"
    git pull
  fi
  git checkout "${branch}"
  git clean -fd
  CASSANDRA_VERSION=$(cat build.xml | grep 'property name="base.version"' | awk -F "\"" '{print $4}')
  # Loop to prevent failure due to maven-ant-tasks not downloading a jar.
  for x in $(seq 1 3); do
      if [ -f "${DTEST_JAR_DIR}/dtest-${CASSANDRA_VERSION}.jar" ]; then
          RETURN="0"
          break
        else
          "${SCRIPT_DIR}/build-shaded-dtest-jar-local.sh"
          RETURN="$?"
          if [ "${RETURN}" -eq "0" ]; then
              break
          fi
      fi
  done
  # Exit, if we didn't build successfully
  if [ "${RETURN}" -ne "0" ]; then
      echo "Build failed with exit code: ${RETURN}"
      exit ${RETURN}
  fi
done
