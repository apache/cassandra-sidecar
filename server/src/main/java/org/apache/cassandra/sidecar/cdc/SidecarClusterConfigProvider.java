/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.cdc;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import org.apache.cassandra.cdc.sidecar.ClusterConfigProvider;
import org.apache.cassandra.sidecar.common.response.NodeSettings;
import org.apache.cassandra.sidecar.common.server.utils.DriverUtils;
import org.apache.cassandra.sidecar.common.server.utils.TokenUtils;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.Partitioner;

/**
 * Provides cluster configuration information for CDC (Change Data Capture) operations by
 * interfacing with Cassandra sidecar instances.
 *
 * <p>This implementation of {@link ClusterConfigProvider} retrieves cluster topology and
 * configuration details from available Cassandra instances through the sidecar's metadata
 * fetcher. It serves as a bridge between the CDC framework and the sidecar's knowledge
 * of the Cassandra cluster.
 *
 */
public class SidecarClusterConfigProvider implements ClusterConfigProvider
{
    private final InstanceMetadataFetcher instanceMetadataFetcher;
    private final DriverUtils driverUtils;

    public SidecarClusterConfigProvider(InstanceMetadataFetcher instanceMetadataFetcher, DriverUtils driverUtils)
    {
        this.instanceMetadataFetcher = instanceMetadataFetcher;
        this.driverUtils = driverUtils;
    }

    public String dc()
    {
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        return nodeSettings.datacenter();
    }

    public Set<CassandraInstance> getCluster()
    {
        Metadata metadata = instanceMetadataFetcher.callOnFirstAvailableInstance(instance -> instance.delegate().metadata());
        TokenMap tokenMap = metadata.getTokenMap().get();
        Collection<Node> hosts = metadata.getNodes().values();
        return hosts.stream()
                    .filter(host -> host.getListenAddress().isPresent())
                    .flatMap(host -> tokenMap.getTokens(host).stream()
                                         .map(token -> new CassandraInstance(
                                         TokenUtils.tokenToBigInteger(token).toString(),
                                         driverUtils.getSocketAddress(host).getHostName(),
                                         host.getDatacenter()
                                         ))
                    ).collect(Collectors.toSet());
    }

    public Partitioner partitioner()
    {
        NodeSettings nodeSettings = instanceMetadataFetcher.callOnFirstAvailableInstance(instance-> instance.delegate().nodeSettings());
        String[] parts = nodeSettings.partitioner().split("\\.");
        return Partitioner.valueOf(parts[parts.length - 1]);
    }
}
