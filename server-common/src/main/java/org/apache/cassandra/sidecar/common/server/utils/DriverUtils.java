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

package org.apache.cassandra.sidecar.common.server.utils;

import java.net.InetSocketAddress;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;

/**
 * A shim layer that provides information from the Cassandra driver. Instead of accessing the
 * {@link com.datastax.driver.core.DriverUtils} directly, this acts as a proxy that can be swapped out
 * based on the specific Sidecar implementation. This can be useful if a different driver version is used
 */
public class DriverUtils
{
    /**
     * Start attempting to reconnect to the given host, as hosts with `IGNORED` distance aren't attempted
     * and the SidecarLoadBalancingPolicy marks non-selected nodes as IGNORED until they need to rotate in.
     *
     * @param cluster The cluster object
     * @param host    the host to which reconnect attempts will be made
     */
    public void startPeriodicReconnectionAttempt(Cluster cluster, Host host)
    {
        com.datastax.driver.core.DriverUtils.startPeriodicReconnectionAttempt(cluster, host);
    }

    /**
     * Gets a Host instance from metadata based on the native transport address
     *
     * @param metadata                    the {@link Metadata} instance to search for the host
     * @param localNativeTransportAddress the native transport ip address and port for the host to find
     * @return the {@link Host}           instance if found, else null
     */
    public Host getHost(Metadata metadata, InetSocketAddress localNativeTransportAddress)
    {
        return com.datastax.driver.core.DriverUtils.getHost(metadata, localNativeTransportAddress);
    }

    /**
     * Returns the address that the driver will use to connect to the node.
     *
     * @param host the host to which reconnect attempts will be made
     * @return the address.
     */
    public InetSocketAddress getSocketAddress(Host host)
    {
        return host.getEndPoint().resolve();
    }
}
