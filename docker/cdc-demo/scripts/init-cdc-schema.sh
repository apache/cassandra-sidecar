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
# Creates the sidecar_internal schema and CDC demo keyspace/table.
set -euo pipefail

CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra}

cqlsh "${CASSANDRA_HOST}" <<'CQL'
CREATE KEYSPACE IF NOT EXISTS sidecar_internal
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS sidecar_internal.configs (
  service text,
  config  map<text, text>,
  PRIMARY KEY (service)
);

CREATE KEYSPACE IF NOT EXISTS cdc_demo
  WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1};

CREATE TABLE IF NOT EXISTS cdc_demo.events (
  id  uuid      PRIMARY KEY,
  msg text,
  ts  timestamp
) WITH cdc = true;
CQL

echo "Schema initialised."
