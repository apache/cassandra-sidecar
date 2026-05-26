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
# Seeds CDC and Kafka configuration into sidecar_internal.configs.
# IF NOT EXISTS makes each insert idempotent across restarts.
#
# Environment variables:
#   SERIALIZER_MODE       confluent (default) | bytearray
#   SCHEMA_REGISTRY_URL   http://schema-registry:8081 (default)
set -euo pipefail

CASSANDRA_HOST=${CASSANDRA_HOST:-cassandra}
KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}
CDC_TOPIC=${CDC_TOPIC:-cdc-mutations}
CDC_DATACENTER=${CDC_DATACENTER:-datacenter1}
SERIALIZER_MODE=${SERIALIZER_MODE:-confluent}
SCHEMA_REGISTRY_URL=${SCHEMA_REGISTRY_URL:-http://schema-registry:8081}

echo "Seeding CDC configs (serializer-mode: ${SERIALIZER_MODE})..."

cqlsh "${CASSANDRA_HOST}" <<CQL
INSERT INTO sidecar_internal.configs (service, config)
VALUES ('cdc', {
  'cdc_enabled':              'true',
  'topic':                    '${CDC_TOPIC}',
  'jobid':                    'docker-demo-job',
  'datacenter':               '${CDC_DATACENTER}',
  'watermark_seconds':        '3600',
  'micro_batch_delay_millis': '1000',
  'max_commit_logs':          '4',
  'persist_state':            'true',
  'fail_kafka_errors':        'true',
  'fail_kafka_too_large_errors': 'false'
}) IF NOT EXISTS;
CQL

if [ "${SERIALIZER_MODE}" = "confluent" ]; then
    cqlsh "${CASSANDRA_HOST}" <<CQL
INSERT INTO sidecar_internal.configs (service, config)
VALUES ('kafka', {
  'bootstrap.servers':  '${KAFKA_BOOTSTRAP}',
  'key.serializer':     'org.apache.kafka.common.serialization.StringSerializer',
  'value.serializer':   'io.confluent.kafka.serializers.KafkaAvroSerializer',
  'schema.registry.url': '${SCHEMA_REGISTRY_URL}',
  'acks':               'all',
  'retries':            '3',
  'linger.ms':          '5',
  'batch.size':         '16384'
}) IF NOT EXISTS;
CQL
else
    cqlsh "${CASSANDRA_HOST}" <<CQL
INSERT INTO sidecar_internal.configs (service, config)
VALUES ('kafka', {
  'bootstrap.servers': '${KAFKA_BOOTSTRAP}',
  'key.serializer':    'org.apache.kafka.common.serialization.StringSerializer',
  'value.serializer':  'org.apache.kafka.common.serialization.ByteArraySerializer',
  'acks':              'all',
  'retries':           '3',
  'linger.ms':         '5',
  'batch.size':        '16384'
}) IF NOT EXISTS;
CQL
fi

echo "Configs seeded."
