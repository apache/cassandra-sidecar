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

# Starting and stopping Cassandra via lifecycle APIs

In this guide we will show how to start and stop a local Cassandra instance via sidecar lifecycle APIs.

## Pre-requirements

- Configuring Cassandra
- Java 11

## Installing and configuring Cassandra

Use the setup.sh script to install and configure Cassandra and sidecar.

```shell
$ ./setup.sh
```

Once executed, the script should create the following directory structure, simulating a Cassandra host install:

```shell
$ ls -l nodes/localhost/
nodes/localhost/etc:
total 4
drwxr-xr-x 3 paulo paulo 4096 Aug 29 16:58 cassandra

nodes/localhost/opt:
total 4
drwxr-xr-x 8 paulo paulo 4096 Aug 29 16:58 apache-cassandra-4.1.9

nodes/localhost/tmp:
total 50672
-rw-r--r-- 1 paulo paulo 51883388 May 16 08:09 apache-cassandra-4.1.9-bin.tar.gz

nodes/localhost/var:
total 8
drwxr-xr-x 3 paulo paulo 4096 Aug 29 16:58 lib
drwxr-xr-x 3 paulo paulo 4096 Aug 29 16:58 log
```

## Starting sidecar

We can now start our Sidecar instance. The `setup.sh` has already configured `sidecar.yaml` configuration with the correct locations. Start sidecar with:

```shell
./gradlew run -Dsidecar.config=file:///$PWD/examples/lifecycle/conf/sidecar.yaml
```

Since Cassandra is not started yet, you should see the following sidecar logs indicating it's not able to reach Cassandra via JMX:

```
INFO  [sidecar-internal-worker-pool-1] 2025-08-29 17:10:33,441 JmxClient.java:197 - Could not connect to JMX on service:jmx:rmi://127.0.0.1:7199/jndi/rmi://127.0.0.1:7199/jmxrmi after 1 attempts. Will retry.
```

## Checking Cassandra lifecycle state via lifecycle API

Use the following command to check that the Cassandra instance is not running and CQL is not up:

```shell
# Check lifecycle state
$ curl localhost:9043/api/v1/cassandra/lifecycle
{"current_state":"STOPPED","desired_state":"UNKNOWN","status":"UNDEFINED","last_update":"No lifecycle task submitted for this instance yet."}

# Check CQL State
$ curl localhost:9043/api/v1/cassandra/native/__health
{"status":"NOT_OK"}
```

## Starting Cassandra via sidecar

Now let's try to start Cassandra:

```shell
$ curl -XPUT http://localhost:9043/api/v1/cassandra/lifecycle -d'{"state": "start"}'
{"current_state":"STOPPED","desired_state":"RUNNING","status":"CONVERGING","last_update":"Submitting start task for instance"}
```

If you see an error during this step, check the logs at `examples/lifecycle/nodes/localhost/var/lib/cassandra-sidecar/lifecycle/start-cassandra-1.out` (and corresponding `start-cassandra-1.err` file).

Query the lifecycle status until the instance is started:
```shell
$ curl localhost:9043/api/v1/cassandra/lifecycle
{"current_state":"RUNNING","desired_state":"RUNNING","status":"CONVERGED","last_update":"Instance has started"}
```

Query the CQL status until it's started. This might take some time since as the Cassandra process initializes.

```shell
$ curl localhost:9043/api/v1/cassandra/native/__health
{"status":"OK"}
```
You should see the following in the sidecar logs, indicating the Cassandra instance is started, and it's able to connect to it via CQL and JMX:

```shell
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:22,504 ProcessLifecycleProvider.java:118 - Starting Cassandra instance localhost with command: [/tmp/examples/lifecycle/nodes/localhost/opt/apache-cassandra-4.1.9/bin/cassandra, -p, /tmp/examples/lifecycle/nodes/localhost/var/lib/cassandra-sidecar/lifecycle/cassandra-localhost.pid, -Dcassandra.ring_delay_ms=5000, -D, cassandra.storagedir=/tmp/examples/lifecycle/nodes/localhost/var/lib/cassandra]
INFO  [vert.x-eventloop-thread-2] 2025-08-29 17:32:22,520 ?:? - 0:0:0:0:0:0:0:1 - - [Fri, 29 Aug 2025 21:32:22 GMT] "PUT /api/v1/cassandra/lifecycle HTTP/1.1" 202 126 "-" "curl/8.10.1"
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:25,365 ProcessLifecycleProvider.java:124 - Started Cassandra instance localhost with PID 882
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:48,745 JmxClient.java:215 - Connected to JMX server at service:jmx:rmi://127.0.0.1:7199/jndi/rmi://127.0.0.1:7199/jmxrmi after 1 attempt(s)
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:48,757 CassandraAdapterDelegate.java:225 - Cassandra version change detected (from=null to=4.1.9) for cassandraInstanceId=1. New adapter loaded=CassandraAdapter@694c957d
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:48,758 CassandraAdapterDelegate.java:520 - JMX connected to cassandraInstanceId=1
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:48,758 CQLSessionProviderImpl.java:186 - Connecting to cluster using contact points [/127.0.0.1:9042]
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:48,931 CQLSessionProviderImpl.java:225 - Successfully connected to Cassandra!
INFO  [sidecar-internal-worker-pool-2] 2025-08-29 17:32:48,948 CassandraAdapterDelegate.java:529 - CQL connected to cassandraInstanceId=1
INFO  [vert.x-eventloop-thread-0] 2025-08-29 17:32:48,951 Server.java:329 - CQL is ready for all Cassandra instances. [1]
```

Check that the Cassandra process ID matches the PID in the lifecycle process ID file:
```shell
$ ps aux | grep CassandraDaemon | grep -v grep | awk '{ print $2 }'
8821
$ cat nodes/localhost/var/lib/cassandra-sidecar/lifecycle/cassandra-1.pid
8821
```

At this stage, you may explore the cassandra logs at `examples/lifecycle/nodes/localhost/var/log/cassandra/system.log` or cassandra startup logs at `examples/lifecycle/nodes/localhost/var/lib/cassandra-sidecar/lifecycle/start-cassandra-1.out`.

## Stopping Cassandra via sidecar

Stop Cassandra via sidecar with the following command:
```shell
$ curl -XPUT http://localhost:9043/api/v1/cassandra/lifecycle -d'{"state": "stop"}'
{"current_state":"RUNNING","desired_state":"STOPPED","status":"CONVERGING","last_update":"Submitting stop task for instance"}
```

If you see an error during this step, check the logs at `examples/lifecycle/nodes/localhost/var/lib/cassandra-sidecar/lifecycle/cassandra-localhost.out` (and corresponding `cassandra-localhost.err` file).

Query the lifecycle status until the process is stopped:
```shell
$ curl http://localhost:9043/api/v1/cassandra/lifecycle
{"current_state":"STOPPED","desired_state":"STOPPED","status":"CONVERGED","last_update":"Instance has stopped"}
```

You should see the following in the sidecar logs, indicating the Cassandra instance is successfully stopped.

```shell
INFO  [sidecar-internal-worker-pool-11] 2025-08-29 18:03:05,957 ProcessLifecycleProvider.java:147 - Stopping Cassandra instance localhost with command: [/tmp/examples/lifecycle/nodes/localhost/opt/apache-cassandra-4.1.9/bin/stop-server, -p, /tmp/examples/lifecycle/nodes/localhost/var/lib/cassandra-sidecar/lifecycle/cassandra-localhost.pid]
INFO  [vert.x-eventloop-thread-2] 2025-08-29 18:03:05,958 ?:? - 0:0:0:0:0:0:0:1 - - [Fri, 29 Aug 2025 22:03:05 GMT] "PUT /api/v1/cassandra/lifecycle HTTP/1.1" 202 125 "-" "curl/8.10.1"
INFO  [sidecar-internal-worker-pool-11] 2025-08-29 18:03:05,960 ProcessLifecycleProvider.java:185 - Waiting for Cassandra instance localhost with PID 15652 to stop...
INFO  [cluster11-worker-0] 2025-08-29 18:03:05,969 CassandraAdapterDelegate.java:540 - CQL disconnection from cassandraInstanceId=1
INFO  [sidecar-internal-worker-pool-11] 2025-08-29 18:03:10,961 ProcessLifecycleProvider.java:159 - Stopped Cassandra instance localhost with PID 15652.
INFO  [sidecar-internal-worker-pool-19] 2025-08-29 18:06:30,985 CassandraAdapterDelegate.java:556 - JMX disconnection from cassandraInstanceId=1
```

