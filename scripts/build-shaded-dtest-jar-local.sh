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

ARTIFACT_NAME=cassandra-dtest
REPO_DIR="$(pwd)/out"
SCRIPT_DIR=$( dirname -- "$( readlink -f -- "$0"; )"; )
CASSANDRA_VERSION=$(cat build.xml | grep 'property name="base.version"' | awk -F "\"" '{print $4}')
GIT_HASH=$(git rev-parse --short HEAD)
DTEST_ARTIFACT_ID=${ARTIFACT_NAME}-local
DTEST_JAR_DIR="$(dirname ${SCRIPT_DIR}/)/dtest-jars"

echo $CASSANDRA_VERSION
echo $GIT_HASH
echo $DTEST_ARTIFACT_ID

ant clean
ant dtest-jar -Dno-checkstyle=true

# Install the version that will be shaded
mvn install:install-file               \
   -Dfile=./build/dtest-${CASSANDRA_VERSION}.jar \
   -DgroupId=org.apache.cassandra      \
   -DartifactId=${DTEST_ARTIFACT_ID} \
   -Dversion=${CASSANDRA_VERSION}-${GIT_HASH}         \
   -Dpackaging=jar                     \
   -DgeneratePom=true                  \
   -DlocalRepositoryPath=${REPO_DIR}

# Create shaded artifact
mvn -f ${SCRIPT_DIR}/relocate-dtest-dependencies.pom package \
    -Drevision=${CASSANDRA_VERSION} \
    -DskipTests \
    -Ddtest.version=${CASSANDRA_VERSION}-${GIT_HASH} \
    -Dmaven.repo.local=${REPO_DIR} \
    -DoutputFilePath=${DTEST_JAR_DIR}/dtest-${CASSANDRA_VERSION}.jar \
    -Drelocation.prefix=shaded-${GIT_HASH} \
    -nsu -U

set +xe
