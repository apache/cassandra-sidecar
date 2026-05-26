<!--
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
-->
# CDC Demo — Docker Compose Setup

End-to-end demo that boots Cassandra, Cassandra Sidecar, Kafka, and Confluent
Schema Registry. Writes to a CDC-enabled Cassandra table are captured by the
sidecar, serialized as Avro (with schemas registered in Schema Registry), and
published to a Kafka topic.

## Architecture

```
┌──────────────┐   cdc_raw/commitlog   ┌──────────────────┐
│  Cassandra   │ ─────────────────────►│  Cassandra       │──► Kafka topic
│  (port 9042) │   (shared volume)     │  Sidecar         │    (cdc-mutations)
└──────────────┘                       │  (port 9043)     │
                                       └──────────────────┘
                                                │ KafkaAvroSerializer
                                                ▼
                                       ┌──────────────────┐
                                       │ Schema Registry  │
                                       │  (port 8081)     │
                                       └──────────────────┘

                                       ┌──────────────────┐
                                       │   Kafka UI       │
                                       │  (port 8080)     │
                                       └──────────────────┘
```

**Services:**
| Service | Image | Role |
|---|---|---|
| `kafka` | `confluentinc/cp-kafka:7.6.0` | KRaft broker (no ZooKeeper) |
| `schema-registry` | `confluentinc/cp-schema-registry:7.6.0` | Avro schema store |
| `cassandra` | `cassandra:5.0` | CDC-enabled Cassandra node |
| `cassandra-init` | `cassandra:5.0` | One-shot: seeds sidecar schema + configs |
| `sidecar` | `cassandra-sidecar:dev` | Reads commit logs, publishes to Kafka |
| `kafka-ui` | `ghcr.io/kafbat/kafka-ui:v1.5.0` | Browse topics + decoded Avro messages |

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker | 24+ | |
| Docker Compose | v2 (plugin) | |
| Java | 11 | Required on host for `./gradlew installDist` |
| Gradle | via wrapper | No installation needed — `./gradlew` is self-contained |

## Exposed ports

| Port | Service |
|---|---|
| `9042` | Cassandra CQL |
| `9043` | Cassandra Sidecar |
| `8080` | Kafka UI |
| `8081` | Confluent Schema Registry |

## Serializer modes

| Mode | `value.serializer` | Schema storage |
|---|---|---|
| `confluent` *(default)* | `KafkaAvroSerializer` | Confluent Schema Registry (port 8081) |
| `bytearray` | `ByteArraySerializer` | None — raw Avro bytes, no schema registry lookup |

## Quick Start

### Step 1 — Start the stack

From `docker/cdc-demo/`, run the start script. It builds the sidecar
distribution on the host, packages it into a Docker image, and starts all
services:

```bash
cd docker/cdc-demo
./scripts/start.sh
```

The script handles everything in order:
1. Stops any existing stack
2. Runs `./gradlew installDist` on the host
3. Builds the `cassandra-sidecar:dev` Docker image
4. Starts all services and waits for CDC iterators to be ready

**Common flags:**

```bash
./scripts/start.sh --clean        # wipe all data volumes before starting
./scripts/start.sh --skip-build   # reuse existing cassandra-sidecar:dev image (skip steps 2-3)
./scripts/start.sh --bytearray    # use ByteArraySerializer instead of Confluent Avro
```

> **`--skip-build`** is useful when you've only changed a config file or script
> and don't need to recompile Java. Requires a `cassandra-sidecar:dev` image
> from a prior run.

### Step 2 — Wait for CDC to be ready

`start.sh` automatically waits until the sidecar is up and CDC iterators have
started, then prints a **Setup complete** banner with next steps.

To follow progress in another terminal:

```bash
docker compose logs -f cassandra-init sidecar
```

### Step 3 — Write mutations to the CDC-enabled table

```bash
docker exec -it cdc-demo-cassandra-1 cqlsh -e "
  INSERT INTO cdc_demo.events (id, msg, ts)
  VALUES (uuid(), 'hello from CDC', toTimestamp(now()));
"
```

### Step 4 — View messages in Kafka UI

Open the topic in the Kafka UI:

```
http://localhost:8080/ui/clusters/local/all-topics/cdc-mutations/messages
```

**Confluent mode (default):** kafbat is pre-configured with the Schema Registry URL (`http://schema-registry:8081`).
To see human-readable messages, set the serde dropdowns at the top of the
Messages tab:

| Field | Serde to select | Why |
|---|---|---|
| **Key Serde** | `String` | CDC keys are plain UTF-8 strings (`keyspace:table:pk`) |
| **Value Serde** | `SchemaRegistry` | Values are Confluent Avro — kafbat fetches the schema by the embedded ID and renders the payload as JSON |

Once set, each message value displays as a decoded JSON object matching the
CDC-enabled table's schema, for example:

```json
{
  "operationType": "INSERT",
  "timestampMicros": 1746000000000000,
  "sourceKeyspace": "cdc_demo",
  "sourceTable": "events",
  "isPartial": false,
  "payload": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "msg": "hello from CDC",
    "ts": 1746000000000000
  }
}
```

**Bytearray mode:** values are raw Avro bytes with no schema registry lookup. Set
**Value Serde** to `Bytes` to inspect the raw payload.

### Step 5 — Inspect the registered Avro schema

The sidecar auto-registers one Avro schema per CDC-enabled table on first publish.
`KafkaAvroSerializer` uses the subject naming convention `{topic}-value`, so for
the `cdc-mutations` topic the subject is `cdc-mutations-value`.

Open the Kafka UI and navigate to the **Schema Registry** tab to browse the full
Avro schema:

```
http://localhost:8080/ui/clusters/local/schemas/cdc-mutations-value
```

## Supported Cassandra Versions

CDC is supported for **4.0, 4.1, 5.0, 5.1**. To use a different version:

```bash
CASSANDRA_VERSION=4.1 ./scripts/start.sh
```

The default is `5.0`. Note: the `cassandra:4.0` Docker image is `linux/amd64`
only — on Apple Silicon it runs under Rosetta emulation and may be slow to
start. Use `4.1` or later for ARM64 support.

## Configuration

`conf/sidecar.yaml` is volume-mounted into the sidecar container and can be
edited without rebuilding the image. Restart the sidecar to pick up changes:

```bash
docker compose restart sidecar
```

CDC and Kafka properties are stored in Cassandra and seeded automatically on
first boot by `scripts/seed-cdc-configs.sh`. To update them on a running cluster:

```bash
docker exec -it cdc-demo-cassandra-1 cqlsh -e "
  UPDATE sidecar_internal.configs
  SET config = config + {'micro_batch_delay_millis': '500'}
  WHERE service = 'cdc';
"
```

To switch serializer mode on a running cluster, delete the existing kafka config
row and restart the stack:

```bash
docker exec -it cdc-demo-cassandra-1 cqlsh -e "DELETE FROM sidecar_internal.configs WHERE service = 'kafka';"
./scripts/start.sh --bytearray --skip-build
```

## Persistence

All data is stored in named Docker volumes and survives `docker compose down`.

| Volume | Contents |
|---|---|
| `cassandra-varlib` | Cassandra data, commitlog, cdc_raw |
| `kafka-data` | Topic partitions + consumer offsets |

## Stopping

```bash
./scripts/stop.sh           # stop containers, keep volumes (data preserved)
./scripts/stop.sh --clean   # stop containers AND delete all data volumes
```

## Troubleshooting

**Sidecar keeps restarting**

The sidecar waits for `cassandra-init` to complete. Check its logs:

```bash
docker compose logs cassandra-init
```

**CDC events not arriving in Kafka**

1. Verify configs were seeded:
   ```bash
   docker exec cdc-demo-cassandra-1 cqlsh -e "SELECT * FROM sidecar_internal.configs;"
   ```
2. Check sidecar logs for `CDC iterators started successfully`
3. Confirm CDC is enabled on the table:
   ```bash
   docker exec cdc-demo-cassandra-1 cqlsh -e "DESCRIBE TABLE cdc_demo.events;"
   ```

**Schema Registry connection errors (confluent mode)**

Verify the registry is reachable and schemas are registered:

```bash
curl -s http://localhost:8081/subjects
docker compose logs schema-registry
```

If `seed-cdc-configs.sh` ran before the schema registry was healthy, the kafka
config row may be missing. Delete and re-run:

```bash
docker exec -it cdc-demo-cassandra-1 cqlsh -e "DELETE FROM sidecar_internal.configs WHERE service = 'kafka';"
docker compose run --rm cassandra-init
```

**JMX connection refused**

Remote JMX is enabled by the `LOCAL_JMX=no` env var on the Cassandra service,
which causes the stock Cassandra Docker entrypoint to set
`jmxremote.local.only=false`. The `JVM_EXTRA_OPTS` env var additionally sets
`-Djava.rmi.server.hostname=cassandra` so RMI binds to the right interface.
Verify the flags are active:

```bash
docker exec cdc-demo-cassandra-1 ps aux | grep jmxremote
```
