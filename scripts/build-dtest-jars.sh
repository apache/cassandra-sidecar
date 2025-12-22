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
CANDIDATE_BRANCHES=(
  "cassandra-4.0:ec3b425c38d92d20d77d3a87c782ed9c072e1cd9"
  "cassandra-4.1:2d0fda4511003c883a6a682c1572f749f6d8da10"
  "cassandra-5.0:6fd83986e91a3ce369d1f2d01a04c8c68c319ae3"
  "trunk:66a7a366474cb9006e66737eac1746f0343db1e8"
)
BRANCHES=( ${BRANCHES:-cassandra-4.0 cassandra-4.1 cassandra-5.0 trunk} )
echo ${BRANCHES[*]}
REPO=${REPO:-"https://github.com/apache/cassandra.git"}
SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
DTEST_JAR_DIR="$(dirname "${SCRIPT_DIR}/")/dtest-jars"
DTEST_JAR_DIR=${CASSANDRA_DEP_DIR:-$DTEST_JAR_DIR}
BUILD_DIR="${DTEST_JAR_DIR}/build"

if [[ "x$CLEAN" != "x" ]]; then
  echo "Clean up $DTEST_JAR_DIR"
  rm -rf $DTEST_JAR_DIR
fi

source "$SCRIPT_DIR/functions.sh"
mkdir -p "${BUILD_DIR}"

# host key verification
mkdir -p ~/.ssh
REPO_HOST=$(get_hostname "${REPO}")
ssh-keyscan "${REPO_HOST}" >> ~/.ssh/known_hosts || true

for index in "${!CANDIDATE_BRANCHES[@]}"; do
  cd "${BUILD_DIR}"
  branchSha=(${CANDIDATE_BRANCHES[$index]//:/ })
  branch=${branchSha[0]}
  sha=${branchSha[1]}

  if ! [[ "${BRANCHES[@]}" =~ "$branch" ]]; then
    echo "branch ${branch} is not selected to build. The selected branches are ${BRANCHES[*]}"
    continue
  fi

  echo "index ${index} branch ${branch} sha ${sha}"
  # check out the correct cassandra version:
  if [ ! -d "${branch}" ] ; then
    if [ -n "${sha}" ] ; then
      mkdir -p "${branch}"
      cd "${branch}"
      git init
      git remote add upstream "${REPO}"
      git fetch --depth=1 upstream "${sha}"
      git reset --hard FETCH_HEAD
    else
      git clone --depth 1 --single-branch --branch "${branch}" "${REPO}" "${branch}"
      cd "${branch}"
    fi
  else
    cd "${branch}"
    if [ -z "${sha}" ] ; then
      git pull
    fi
  fi
  if [ -z "${sha}" ] ; then
    git checkout "${branch}"
  fi
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

# Remove the build directory enclosing all Cassandra branches just cloned,
# in order to not confuse IDE's indexing
rm -rf ${BUILD_DIR}
