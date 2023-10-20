/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.config;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * The driver configuration to use when connecting to Cassandra
 */
public interface DriverConfiguration
{
    /**
     * A list of contact points to use for initial connection to Cassandra.
     * At least 2 non-replica nodes are recommended.
     * @return a list of contact points
     */
    List<InetSocketAddress> contactPoints();

    /**
     * The number of connections other than locally-managed nodes to use.
     * The minimum is 2 - if your value is &lt; 2, the Sidecar will use 2.
     * @return the number of connections to make to the cluster.
     */
    int numConnections();

    /**
     * The local datacenter to use for non-local queries to the cluster.
     * @return the local datacenter, or null if no local datacenter is specified.
     */
    String localDc();
}
