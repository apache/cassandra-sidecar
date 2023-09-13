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

# Apache Cassandra Sidecar [WIP]

This is a Sidecar for the highly scalable Apache Cassandra database.
For more information, see [the Apache Cassandra web site](http://cassandra.apache.org/) and [CIP-1](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=95652224).

**This is project is still WIP.**

Requirements
------------
  1. Java >= 1.8 (OpenJDK or Oracle), or Java 11
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

Getting started: Running The Sidecar
--------------------------------------

After you clone the git repo, you can use the gradle wrapper to build and run the project. Make sure you have 
Apache Cassandra running on the host & port specified in `conf/sidecar.yaml`.

    $ ./gradlew run
  
Configuring Cassandra Instance
------------------------------

While setting up cassandra instance, make sure the data directories of cassandra are in the path stored in sidecar.yaml file, else modify data directories path to point to the correct directories for stream APIs to work.

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
