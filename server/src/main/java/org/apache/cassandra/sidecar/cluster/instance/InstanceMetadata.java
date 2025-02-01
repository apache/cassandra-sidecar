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

package org.apache.cassandra.sidecar.cluster.instance;

import java.net.UnknownHostException;
import java.util.List;

import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.metrics.instance.InstanceMetrics;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Metadata of an instance
 */
public interface InstanceMetadata
{
    /**
     * @return an identifier for the Cassandra instance
     */
    int id();

    /**
     * @return the hostname or IP address of the Cassandra instance
     */
    String host();

    /**
     * @return the IP address of the Cassandra instance. When no IP address is resolved, it returns null
     */
    @Nullable
    String ipAddress();

    /**
     * Resolve the ipAddress and update.
     * @return the IP address resolved
     * @throws UnknownHostException when hostname cannot be resolved to IP address
     */
    default String refreshIpAddress() throws UnknownHostException
    {
        return ipAddress();
    }

    /**
     * @return the native transport port number of the Cassandra instance
     */
    int port();

    /**
     * @return a list of data directories of cassandra instance
     */
    @NotNull
    List<String> dataDirs();

    /**
     * @return a staging directory of the cassandra instance
     */
    String stagingDir();

    /**
     * @return a {@link CassandraAdapterDelegate} specific for the instance, or throws when the delegate is unavailable
     * @throws CassandraUnavailableException when the Cassandra service is unavailable
     */
    @NotNull
    CassandraAdapterDelegate delegate() throws CassandraUnavailableException;

    /**
     * @return CDC directory of the cassandra instance, it can be configured as null when CDC is not enabled
     * for the Cassandra.
     * instance
     */
    @Nullable
    String cdcDir();

    /**
     * @return commitlog directory of the cassandra instance
     */
    @NotNull
    String commitlogDir();

    /**
     * @return hints directory of the Cassandra instance
     */
    @NotNull
    String hintsDir();

    /**
     * @return saved caches directory of the Cassandra instance
     */
    @NotNull
    String savedCachesDir();

    /**
     * @return local system data file directory of the cassandra instance
     */
    @Nullable
    String localSystemDataFileDir();

    /**
     * @return {@link InstanceMetrics} metrics specific for the Cassandra instance
     */
    @NotNull
    InstanceMetrics metrics();
}
