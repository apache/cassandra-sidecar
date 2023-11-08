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

package org.apache.cassandra.sidecar.cluster;


import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.jupiter.api.Assertions;

import com.datastax.driver.core.DriverExtensions;
import com.datastax.driver.core.Host;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A test for the SidecarLoadBalancingPolicy
 */
public class SidecarLoadBalancingPolicyTest extends IntegrationTestBase
{

    public static final int SIDECAR_MANAGED_INSTANCES = 2;

    private static List<Host> getConnectedHosts(Set<Host> hosts)
    {
        return hosts.stream()
                    .filter(DriverExtensions::hasActiveConnections)
                    .collect(Collectors.toList());
    }

    protected int getNumInstancesToManage(int clusterSize)
    {
        return SIDECAR_MANAGED_INSTANCES; // we only want to manage the first 2 instances in the "cluster"
    }

    @CassandraIntegrationTest(nodesPerDc = 6)
    public void shouldMaintainMinimumConnections() throws ExecutionException, InterruptedException
    {
        Set<Host> hosts = sidecarTestContext.session().getCluster().getMetadata().getAllHosts();
        List<Host> connectedHosts = getConnectedHosts(hosts);
        // We manage 2 hosts, and ask for an additional 4 (the default) for connections.
        // Therefore, we expect 4 hosts to have connections at startup.
        int expectedConnections = SIDECAR_MANAGED_INSTANCES + SidecarLoadBalancingPolicy.MIN_ADDITIONAL_CONNECTIONS;
        assertThat(connectedHosts.size()).isEqualTo(expectedConnections);
        // Now, shut down one of the hosts and make sure that we connect to a different node
        UpgradeableCluster cluster = sidecarTestContext.cluster();
        IUpgradeableInstance inst = shutDownNonLocalInstance(
        cluster,
        sidecarTestContext.instancesConfig().instances());
        assertThat(inst.isShutdown()).isTrue();
        InetSocketAddress downInstanceAddress = inst.broadcastAddress();
        assertConnectionsWithRetry(downInstanceAddress, expectedConnections);
    }

    private void assertConnectionsWithRetry(InetSocketAddress downInstanceAddress, int expectedConnections)
    {
        List<Host> connectedHosts = Collections.emptyList();
        int attempts = 0;
        // Retry for up to 2 minutes, but passes much more quickly most of the time, so this should be safe.
        while (attempts <= 24)
        {
            Set<Host> hosts = sidecarTestContext.session().getCluster().getMetadata().getAllHosts();
            connectedHosts = getConnectedHosts(hosts);
            List<InetSocketAddress> connectedAddresses = getAddresses(connectedHosts);
            assertThat(connectedAddresses).doesNotContain(downInstanceAddress);
            int connectedInstances = connectedHosts.size();
            if (connectedInstances == expectedConnections)
            {
                return;
            }
            attempts++;
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        }
        String message =
        String.format("Waited 2 minutes for connected hosts (%d) to be the expected number (%d) but failed",
                      connectedHosts.size(), expectedConnections);
        Assertions.fail(message);
    }

    private List<InetSocketAddress> getAddresses(List<Host> connectedHosts)
    {
        return connectedHosts.stream()
                             .map(h -> h.getEndPoint().resolve())
                             .collect(Collectors.toList());
    }

    private IUpgradeableInstance shutDownNonLocalInstance(UpgradeableCluster cluster,
                                                          List<InstanceMetadata> instances)
    throws ExecutionException, InterruptedException
    {
        Set<InetSocketAddress> localInstances = instances.stream().map(i -> new InetSocketAddress(i.host(), i.port()))
                                                         .collect(Collectors.toSet());
        for (IUpgradeableInstance inst : cluster)
        {
            InetSocketAddress nativeAddress = new InetSocketAddress(inst.config().broadcastAddress().getAddress(),
                                                                    inst.config().getInt("native_transport_port"));
            if (localInstances.contains(nativeAddress))
            {
                continue;
            }
            inst.shutdown(true).get();
            return inst;
        }
        throw new RuntimeException("Could not find instance to shut down");
    }
}
