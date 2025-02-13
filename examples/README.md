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

# Running Cassandra Sidecar against a 3-node CCM cluster

To get quickly started with Cassandra Sidecar, we provide the following steps to start a CCM cluster, and run
Sidecar with the default configuration.

## Pre-requirements

- Configuring hosts
- [CCM](https://github.com/riptano/ccm)
- Java 11

## Configuring hosts

For this demo, we need to alias `127.0.0.1` -> `localhost1`, `127.0.0.2` -> `localhost2`, and
`127.0.0.3` -> `localhost3` in our local `/etc/hosts` file.

```
##
# Host Database
#
# localhost is used to configure the loopback interface
# when the system is booting.  Do not change this entry.
##
127.0.0.1	localhost
# ...
# Other entries
# ...
127.0.0.1	localhost1
127.0.0.2	localhost2
127.0.0.3	localhost3
```

## Configuring a 3-node CCM cluster

We will be creating a CCM cluster called `sidecardemo`. Let's run the following command to create the 3-node
Cassandra cluster and start it.

```shell
$ ccm create sidecardemo --version git:trunk --nodes=3 --datadirs=1
$ ccm start
```

Once started, let's ensure that the cluster is running. For example, we can check the ring of the cluster by
running

```shell
$ ccm node1 ring

Datacenter: datacenter1
==========
Address         Rack        Status State   Load            Owns                Token
                                                                               3074457345618258602
127.0.0.1       rack1       Up     Normal  76.87 KiB       66.67%              -9223372036854775808
127.0.0.2       rack1       Up     Normal  76.76 KiB       66.67%              -3074457345618258603
127.0.0.3       rack1       Up     Normal  66.59 KiB       66.67%              3074457345618258602
```

We can now start our Sidecar instance. A single Sidecar instance will manage all the 3 Cassandra instances
in our CCM cluster.

```shell
./gradlew run -Dsidecar.config=file:///$PWD/examples/conf/sidecar-ccm.yaml
```

We should see our Sidecar logs showing a successful connection to the all Cassandra instances

```
INFO  [sidecar-internal-worker-pool-5] 2025-02-13 06:49:09,039 CassandraAdapterDelegate.java:523 - CQL connected to cassandraInstanceId=1
INFO  [sidecar-internal-worker-pool-7] 2025-02-13 06:49:09,039 CassandraAdapterDelegate.java:523 - CQL connected to cassandraInstanceId=3
INFO  [sidecar-internal-worker-pool-6] 2025-02-13 06:49:09,042 CassandraAdapterDelegate.java:523 - CQL connected to cassandraInstanceId=2
INFO  [vert.x-eventloop-thread-0] 2025-02-13 06:49:09,043 Server.java:361 - CQL is ready for all Cassandra instances. [1, 2, 3]
```

### Creating a snapshot from Sidecar

Now let's try to create a snapshot using Sidecar endpoints. For that let's first create a keyspace and table. Let's
open a `cqlsh` session.

```shell
ccm node2 cqlsh
```

And create the following schema.

```cassandraql
CREATE KEYSPACE cycling
    WITH REPLICATION = {
        'class' : 'NetworkTopologyStrategy',
        'datacenter1' : 3
        };

CREATE TABLE cycling.cyclist_name
(
    id        UUID PRIMARY KEY,
    lastname  text,
    firstname text
);

-- INSERT SOME DATA

INSERT INTO cycling.cyclist_name (id, lastname, firstname)
VALUES (6ab09bec-e68e-48d9-a5f8-97e6fb4c9b47, 'KRUIKSWIJK', 'Steven');
```

And now we are ready to create a snapshot from Sidecar by running the following request. We need to give it a name,
so let's use `cyclyst_name_sidecar_demo_1` as the snapshot name

```shell
$ curl -XPUT "http://localhost1:9043/api/v1/keyspaces/cycling/tables/cyclist_name/snapshots/cyclyst_name_sidecar_demo_1"
```

We should see the success payload from the request as follows:

```json
{
  "result": "Success"
}
```

Let's verify that the snapshot has been created correctly for node1.

```shell
$ ls -l $HOME/.ccm/sidecardemo/node1/data0/cycling/cyclist_name-*/snapshots/cyclyst_name_sidecar_demo_1/
total 88
-rw-r--r--  1 fguerrero  fguerrero   129 Feb 13 09:10 manifest.json
-rw-r--r--  3 fguerrero  fguerrero    47 Feb 13 09:10 oa-1-big-CompressionInfo.db
-rw-r--r--  3 fguerrero  fguerrero    54 Feb 13 09:10 oa-1-big-Data.db
-rw-r--r--  3 fguerrero  fguerrero    10 Feb 13 09:10 oa-1-big-Digest.crc32
-rw-r--r--  3 fguerrero  fguerrero    16 Feb 13 09:10 oa-1-big-Filter.db
-rw-r--r--  3 fguerrero  fguerrero    20 Feb 13 09:10 oa-1-big-Index.db
-rw-r--r--  3 fguerrero  fguerrero  4889 Feb 13 09:10 oa-1-big-Statistics.db
-rw-r--r--  3 fguerrero  fguerrero    92 Feb 13 09:10 oa-1-big-Summary.db
-rw-r--r--  3 fguerrero  fguerrero    92 Feb 13 09:10 oa-1-big-TOC.txt
-rw-r--r--  1 fguerrero  fguerrero   994 Feb 13 09:10 schema.cql
```

But we shouldn't see the snapshot created for node2 or node3.

```shell
$ ls -l $HOME/.ccm/sidecardemo/node2/data0/cycling/cyclist_name-*/snapshots/cyclyst_name_sidecar_demo_1/
ls: /${HOME}/.ccm/sidecardemo/node2/data0/cycling/cyclist_name-*/snapshots/cyclyst_name_sidecar_demo_1/: No such file or directory
$ ls -l $HOME/.ccm/sidecardemo/node3/data0/cycling/cyclist_name-*/snapshots/cyclyst_name_sidecar_demo_1/
ls: /${HOME}/.ccm/sidecardemo/node3/data0/cycling/cyclist_name-*/snapshots/cyclyst_name_sidecar_demo_1/: No such file or directory
```

## Tear down

- Stop the Sidecar service
- Remove the CCM cluster

```
$ ccm remove sidecardemo
```
