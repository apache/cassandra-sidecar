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
set -eu

SCRIPT_DIR=$(realpath "$(dirname "$0")")
CASSANDRA_VERSION=4.1.10
CASSANDRA_DIR="apache-cassandra-${CASSANDRA_VERSION}"
TARBALL_NAME="${CASSANDRA_DIR}-bin.tar.gz"
TARBALL_URL="https://dlcdn.apache.org/cassandra/${CASSANDRA_VERSION}/${TARBALL_NAME}"
NODE_DIR="${SCRIPT_DIR}/nodes/localhost"

SIDECAR_YAML="${SCRIPT_DIR}/conf/sidecar.yaml"
SIDECAR_YAML_TEMPLATE="${SCRIPT_DIR}/conf/sidecar.yaml.template"
CASSANDRA_HOME="${NODE_DIR}/opt/${CASSANDRA_DIR}"
CASSANDRA_LOG_DIR="${NODE_DIR}/var/log/cassandra"
CASSANDRA_CONF="${NODE_DIR}/etc/cassandra"
CASSANDRA_STORAGE_DIR="${NODE_DIR}/var/lib/cassandra"
SIDECAR_LIFECYCLE_DIR="${NODE_DIR}/var/lib/cassandra-sidecar/lifecycle"
TMP_DIR="${NODE_DIR}/tmp"

echo "Creating directories"
mkdir -p ${CASSANDRA_HOME} ${CASSANDRA_LOG_DIR} ${CASSANDRA_CONF} ${CASSANDRA_STORAGE_DIR} ${SIDECAR_LIFECYCLE_DIR} ${TMP_DIR}

if [ -f ${CASSANDRA_HOME}/bin/cassandra ]; then
  echo "Cassandra already installed at ${CASSANDRA_HOME}, skipping install"
else
  echo "Installing Cassandra at ${CASSANDRA_HOME}"
  echo "Downloading ${TARBALL_URL}"
  curl -L -o ${TMP_DIR}/$(basename ${TARBALL_URL}) ${TARBALL_URL}

  echo "Extracting Cassandra tarball"
  tar -xvzf ${TMP_DIR}/${TARBALL_NAME} -C $(dirname $CASSANDRA_HOME)
fi

echo "Creating Sidecar configuration"
cp -r ${CASSANDRA_HOME}/conf/* ${CASSANDRA_CONF}
sed "s#\$cassandraHome#${CASSANDRA_HOME}#g" ${SIDECAR_YAML_TEMPLATE} > ${SIDECAR_YAML}
sed -i '' "s#\$baseDir#${NODE_DIR}#g" ${SIDECAR_YAML}

echo "Setup complete!"
