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

# Apache Cassandra Sidecar

This is a Sidecar for the highly scalable Apache Cassandra database.
For more information, see [the Apache Cassandra web site](http://cassandra.apache.org/) and [CIP-1](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=95652224).

We use the [Sidecar for Apache Cassandra JIRA](https://issues.apache.org/jira/projects/CASSSIDECAR) to track issues.

**This is project is still WIP.**

Requirements
------------
  1. Java >= 11<sup>1</sup> (OpenJDK or Oracle)
  2. Apache Cassandra 4.0.  We depend on virtual tables which is a 4.0 only feature.
  3. [Docker](https://www.docker.com/products/docker-desktop/) for running integration tests.

Build Prerequisites
-------------------
We depend on the Cassandra in-jvm dtest framework for testing. 
Because these jars are not published, you must manually build the dtest jars before you can build the project.

```shell
./scripts/build-dtest-jars.sh
```

The build script supports two parameters:
- `REPO` - the Cassandra git repository to use for the source files. This is helpful if you need to test with a fork of the Cassandra codebase.
    - default: `git@github.com:apache/cassandra.git`
- `BRANCHES` - a space-delimited list of branches to build.
  -default: `"cassandra-4.1 trunk"`

Remove any versions you may not want to test with. We recommend at least the latest (released) 4.X series and `trunk`.
See Testing for more details on how to choose which Cassandra versions to use while testing.

For multi-node in-jvm dtests, network aliases will need to be setup for each Cassandra node. The tests assume each node's 
ip address is 127.0.0.x, where x is the node id. 

For example if you populated your cluster with 3 nodes, create interfaces for 127.0.0.2 and 127.0.0.3 (the first node of course uses 127.0.0.1).

### macOS network aliases
To get up and running, create a temporary alias for every node except the first:

```
 for i in {2..20}; do sudo ifconfig lo0 alias "127.0.0.${i}"; done
```

Note that this does not persist across reboots, so you'll have to run it every time you restart.

### File descriptor limit
Integration tests open many sockets and kqueue selectors via Netty. The default macOS file descriptor limit (256) is too low and will cause `Too many open files` errors. Increase it before running tests:

```
ulimit -n 65536
```

Getting started: Running The Sidecar
--------------------------------------

After you clone the git repo, you can use the gradle wrapper to build and run the project. Make sure you have 
Apache Cassandra running on the host & port specified in `conf/sidecar.yaml`.

    $ ./gradlew run

Alternatively, you can run against a local CCM cluster. Cassandra Sidecar provides a configuration for a 3-node
CCM cluster named `sidecardemo`. You can use the gradle wrapper to run the project connected to a 3-node CCM cluster
as follows:

    $ ./gradlew run -Dsidecar.config=file:///$PWD/examples/conf/sidecar-ccm.yaml

Please see [samples](samples/README.md) for details.

Configuring Cassandra Instance
------------------------------

While setting up cassandra instance, make sure the data directories of cassandra are in the path stored in sidecar.yaml file, else modify data directories path to point to the correct directories for stream APIs to work.

Change Data Capture (CDC) Configuration
---------------------------------------

Apache Cassandra Sidecar supports Change Data Capture (CDC) to stream table mutations to Apache Kafka. This section describes how to configure and run Sidecar with CDC enabled.

### Prerequisites

1. Apache Cassandra 4.0+ with CDC support
2. Apache Kafka cluster
3. Sidecar configured with schema management enabled

### Configuration Steps

#### 1. Enable CDC in Cassandra

Edit your `cassandra.yaml` configuration file and enable CDC:

```yaml
cdc_enabled: true
```

Restart your Cassandra instance for this change to take effect.

#### 2. Configure Sidecar for CDC

Edit your `sidecar.yaml` configuration file with the following settings:

```yaml
sidecar:
  # Enable schema management (required for CDC)
  schema:
    is_enabled: true
    keyspace: sidecar_internal
    replication_strategy: SimpleStrategy
    replication_factor: 3

  # Enable CDC feature
  cdc:
    enabled: true
    config_refresh_time: 10s
    table_schema_refresh_time: 60s
    segment_hardlink_cache_expiry: 1m
```

**Configuration Parameters:**
- `schema.is_enabled`: **Must be `true`** for CDC to function. Creates the `sidecar_internal` keyspace for CDC state management.
- `cdc.enabled`: Enables the CDC feature in Sidecar.
- `cdc.config_refresh_time`: How frequently CDC configuration is refreshed from the database.
- `cdc.table_schema_refresh_time`: How frequently table schemas are refreshed for CDC-enabled tables.
- `cdc.segment_hardlink_cache_expiry`: Cache expiration time for CDC segment hard links.

#### 3. Enable CDC on Tables

For each table you want to capture changes from, enable the CDC property using CQL:

```cql
-- For a new table
CREATE TABLE my_keyspace.my_table (
    id text PRIMARY KEY,
    name text,
    value int
) WITH cdc = true;

-- For an existing table
ALTER TABLE my_keyspace.my_table WITH cdc = true;
```

#### 4. Configure CDC Service

Use the CDC configuration API endpoint to set up CDC parameters:

```bash
curl --request PUT \
  --url http://localhost:9043/api/v1/services/cdc/config \
  --header 'content-type: application/json' \
  --data '{
  "config": {
    "datacenter": "datacenter1",
    "env": "production",
    "topic_format_type": "STATIC",
    "topic": "cdc-events"
  }
}'
```

**CDC Configuration Parameters:**
- `datacenter`: The datacenter name for this Sidecar instance.
- `env`: Environment identifier (e.g., `production`, `staging`, `dev`).
- `topic_format_type`: Determines how Kafka topic names are generated. Options:
  - `STATIC`: Use a single fixed topic name specified in `topic` field
  - `KEYSPACE`: Format as `{topic}-{keyspace}`
  - `KEYSPACETABLE`: Format as `{topic}-{keyspace}-{table}`
  - `TABLE`: Format as `{topic}-{table}`
  - `MAP`: Use custom topic mapping (advanced)
- `topic`: Base Kafka topic name for CDC events.

#### 5. Configure Kafka Producer

Configure the Kafka producer settings using the Kafka configuration API endpoint:

```bash
curl --request PUT \
  --url http://localhost:9043/api/v1/services/kafka/config \
  --header 'content-type: application/json' \
  --data '{
  "config": {
    "bootstrap.servers": "localhost:9092",
    "key.serializer": "org.apache.kafka.common.serialization.StringSerializer",
    "value.serializer": "org.apache.kafka.common.serialization.ByteArraySerializer",
    "acks": "all",
    "retries": "3",
    "retry.backoff.ms": "200",
    "enable.idempotence": "true",
    "batch.size": "16384",
    "linger.ms": "5",
    "buffer.memory": "33554432",
    "compression.type": "snappy",
    "request.timeout.ms": "30000",
    "delivery.timeout.ms": "120000",
    "max.in.flight.requests.per.connection": "5",
    "client.id": "cdc-producer"
  }
}'
```

**Key Kafka Producer Parameters:**
- `bootstrap.servers`: Comma-separated list of Kafka broker addresses.
- `key.serializer`: Serializer for the message key (use `StringSerializer`).
- `value.serializer`: Serializer for the message value (use `ByteArraySerializer` for Avro).
- `acks`: Number of acknowledgments the producer requires (`all` for maximum durability).
- `enable.idempotence`: Ensures exactly-once semantics when set to `true`.
- `compression.type`: Compression algorithm (`snappy`, `gzip`, `lz4`, `zstd`, or `none`).

For a complete list of Kafka producer configurations, see the [Apache Kafka Producer Configuration Documentation](https://kafka.apache.org/documentation/#producerconfigs).

### Data Format and Serialization

CDC events are serialized in **Apache Avro** format. Sidecar includes a built-in schema store (`CachingSchemaStore`) that:
- Automatically tracks CDC-enabled table schemas
- Converts CQL schemas to Avro schemas
- Refreshes schemas based on `table_schema_refresh_time` configuration
- Caches Avro schemas for performance

Each CDC event published to Kafka contains:
- **Key**: Table identifier (keyspace + table name)
- **Value**: Avro-serialized mutation data containing:
  - Partition key
  - Clustering key (if applicable)
  - Mutation type (INSERT, UPDATE, DELETE)
  - Column values
  - Timestamp

### Verification

After completing the configuration:

1. **Check Sidecar Logs**: Verify CDC is enabled and connected to Kafka:
   ```
   grep -i "cdc" /path/to/sidecar.log
   ```

2. **Verify Configuration**: Retrieve current CDC and Kafka configurations:
   ```bash
   # Get CDC configuration
   curl http://localhost:9043/api/v1/services/cdc/config

   # Get Kafka configuration
   curl http://localhost:9043/api/v1/services/kafka/config

   # Get all service configurations
   curl http://localhost:9043/api/v1/services
   ```

### Advanced Configuration

#### Custom Schema Registry Integration

While Sidecar includes a built-in schema store, you can integrate with external schema registries by:
1. Implementing a custom `SchemaStore` interface
2. Registering your implementation via Guice dependency injection
3. Configuring your schema registry connection details in the Kafka producer configuration


### Troubleshooting

**CDC not starting:**
- Verify `schema.is_enabled: true` in `sidecar.yaml`
- Check Cassandra has `cdc_enabled: true`
- Ensure `sidecar_internal` keyspace exists and is accessible

**No messages in Kafka:**
- Verify tables have `cdc = true` property
- Check Kafka connectivity and broker availability
- Review Sidecar logs for errors: `grep -i "kafka\|cdc" /path/to/sidecar.log`
- Verify CDC and Kafka configurations are set via API endpoints

**Schema errors:**
- Ensure table schemas are stable (avoid frequent schema changes during CDC)
- Check `table_schema_refresh_time` is appropriate for your use case
- Review Sidecar logs for schema conversion errors


Testing
-------

The test framework is set up to run 4.1 and 5.1 (Trunk) tests (see `TestVersionSupplier.java`) by default.
You can change this via the Java property `cassandra.sidecar.versions_to_test` by supplying a comma-delimited string.
For example, `-Dcassandra.sidecar.versions_to_test=4.0,4.1,5.1`.

CircleCI Testing
-----------------

You will need to use the "Add Projects" function of CircleCI to set up CircleCI on your fork.  When promoted to create a branch, 
do not replace the CircleCI config, choose the option to do it manually.  CircleCI will pick up the in project configuration.

Contributing
------------

We warmly welcome and appreciate contributions from the community. Please see [CONTRIBUTING.md](CONTRIBUTING.md)
if you wish to submit pull requests.

Wondering where to go from here?
--------------------------------
  * Join us in #cassandra on [ASF Slack](https://s.apache.org/slack-invite) and ask questions 
  * Subscribe to the Users mailing list by sending a mail to
    user-subscribe@cassandra.apache.org
  * Visit the [community section](http://cassandra.apache.org/community/) of the Cassandra website for more information on getting involved.
  * Visit the [development section](http://cassandra.apache.org/doc/latest/development/index.html) of the Cassandra website for more information on how to contribute.
  * File issues with our [Sidecar JIRA](https://issues.apache.org/jira/projects/CASSANDRASC/issues/)

Notes
-----

<sup>1</sup> The Sidecar Client offers Java 1.8 compatibility, and produces artifacts for both Java 1.8 and Java 11.
