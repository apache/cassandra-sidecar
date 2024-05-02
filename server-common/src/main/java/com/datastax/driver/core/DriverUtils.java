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

package com.datastax.driver.core;

import java.net.InetSocketAddress;

import org.jetbrains.annotations.VisibleForTesting;

/**
 * A collection of methods that require access to package-private members in the datastax driver.
 */
public class DriverUtils
{
    /**
     * Check if a host has active connections.
     *
     * <p><b>Note:</b> This method should not be used directly, but should be proxied by
     * an implementation of {@link org.apache.cassandra.sidecar.common.server.utils.DriverUtils}.
     *
     * @param host the host to check
     * @return true if the host has active connections, false otherwise
     */
    @VisibleForTesting
    public static boolean hasActiveConnections(Host host)
    {
        return host.convictionPolicy.hasActiveConnections();
    }

    /**
     * Start attempting to reconnect to the given host, as hosts with `IGNORED` distance aren't attempted
     * and the SidecarLoadBalancingPolicy marks non-selected nodes as IGNORED until they need to rotate in.
     *
     * <p><b>Note:</b> This method should not be used directly, but should be proxied by
     * an implementation of {@link org.apache.cassandra.sidecar.common.server.utils.DriverUtils}.
     *
     * @param cluster The cluster object
     * @param host    the host to which reconnect attempts will be made
     */
    public static void startPeriodicReconnectionAttempt(Cluster cluster, Host host)
    {
        cluster.manager.startPeriodicReconnectionAttempt(host, false);
    }

    /**
     * Gets a Host instance from metadata based on the native transport address
     *
     * <p><b>Note:</b> This method should not be used directly, but should be proxied by
     * an implementation of {@link org.apache.cassandra.sidecar.common.server.utils.DriverUtils}.
     *
     * @param metadata                    the {@link Metadata} instance to search for the host
     * @param localNativeTransportAddress the native transport ip address and port for the host to find
     * @return the {@link Host}           instance if found, else null
     */
    public static Host getHost(Metadata metadata, InetSocketAddress localNativeTransportAddress)
    {
        // Because the driver can sometimes mess up the broadcast address, we need to search by endpoint
        // which is what it actually uses to connect to the cluster. Therefore, create a TranslatedAddressEndpoint
        // to use for searching. It has to be one of these because that's what the driver is using internally,
        // and the `.equals` method used when searching checks the type explicitly.
        TranslatedAddressEndPoint endPoint = new TranslatedAddressEndPoint(localNativeTransportAddress);
        return metadata.getHost(endPoint);
    }
}
