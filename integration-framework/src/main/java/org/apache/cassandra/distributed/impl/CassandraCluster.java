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

package org.apache.cassandra.distributed.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.ICoordinator;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IMessage;
import org.apache.cassandra.distributed.api.IMessageFilters;
import org.apache.cassandra.distributed.api.IMessageSink;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Versions;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.apache.cassandra.testing.IClusterExtension;
import org.apache.cassandra.testing.Partitioner;
import org.apache.cassandra.testing.TestTokenSupplier;
import org.jetbrains.annotations.NotNull;

import static org.apache.cassandra.distributed.shared.NetworkTopology.dcAndRack;
import static org.apache.cassandra.distributed.shared.NetworkTopology.networkTopology;

/**
 * Implements the {@link IClusterExtension} interface, delegating the functionality to the
 * {@link AbstractCluster delegate}. This class is meant to be loaded in a different classloader.
 *
 * @param <I> the type of the cluster instance
 */
public class CassandraCluster<I extends IInstance> implements IClusterExtension<I>
{
    private final AbstractCluster<I> delegate;

    private static final Predicate<String> EXTRA = className -> {
        // Those classes can be reached by Spark SizeEstimator, when it estimates the broadcast variable.
        // In the test scenario containing cassandra instance shutdown, there is a chance that it pick the class
        // that is loaded by the closed instance classloader, causing the following exception.
        // java.lang.IllegalStateException: Can't load <CLASS>. Instance class loader is already closed.
        return className.equals("org.apache.cassandra.utils.concurrent.Ref$OnLeak")
               || className.startsWith("org.apache.cassandra.metrics.RestorableMeter")
               || className.equals("org.apache.logging.slf4j.EventDataConverter")
               || (className.startsWith("org.apache.cassandra.analytics.") && className.contains("BBHelper"));
    };

    public CassandraCluster(String versionString, ClusterBuilderConfiguration configuration) throws IOException
    {
        delegate = initializeCluster(versionString, configuration);
    }

    @SuppressWarnings("unchecked")
    public AbstractCluster<I> initializeCluster(String versionString,
                                                ClusterBuilderConfiguration configuration) throws IOException
    {
        // spin up a C* cluster using the in-jvm dtest
        Versions versions = Versions.find();
        Versions.Version requestedVersion = versions.getLatest(new Semver(versionString, Semver.SemverType.LOOSE));

        int nodesPerDc = configuration.nodesPerDc;
        int dcCount = configuration.dcCount;
        int newNodesPerDc = configuration.newNodesPerDc;
        Preconditions.checkArgument(newNodesPerDc >= 0, "newNodesPerDc cannot be a negative number");
        int originalNodeCount = nodesPerDc * dcCount;
        int finalNodeCount = dcCount * (nodesPerDc + newNodesPerDc);

        UpgradeableCluster.Builder clusterBuilder = UpgradeableCluster.build(originalNodeCount);
        clusterBuilder.withVersion(requestedVersion)
                      .withDynamicPortAllocation(configuration.dynamicPortAllocation) // to allow parallel test runs
                      .withSharedClasses(EXTRA.or(clusterBuilder.getSharedClasses()))
                      .withDCs(dcCount)
                      .withTokenCount(configuration.tokenCount)
                      .withDataDirCount(configuration.numDataDirsPerInstance);

        TokenSupplier tokenSupplier;
        Consumer<IInstanceConfig> instanceConfigUpdater;
        if (configuration.partitioner != null)
        {
            tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(Partitioner.fromClassName(configuration.partitioner),
                                                                      nodesPerDc, newNodesPerDc, dcCount, 1);
            instanceConfigUpdater = instanceConfig -> {
                instanceConfig.set("partitioner", configuration.partitioner);
                configuration.features.forEach(instanceConfig::with);
            };
        }
        else
        {
            tokenSupplier = TestTokenSupplier.evenlyDistributedTokens(nodesPerDc, newNodesPerDc, dcCount, 1);
            instanceConfigUpdater = config -> configuration.features.forEach(config::with);
        }
        if (configuration.additionalInstanceConfig != null)
        {
            instanceConfigUpdater = instanceConfigUpdater.andThen(config -> configuration.additionalInstanceConfig.forEach(config::set));
        }
        clusterBuilder.withTokenSupplier(tokenSupplier)
                      .withConfig(instanceConfigUpdater);

        if (dcCount > 1)
        {
            clusterBuilder.withNodeIdTopology(networkTopology(finalNodeCount,
                                                              (nodeId) -> nodeId % 2 != 0 ?
                                                                          dcAndRack("datacenter1", "rack1") :
                                                                          dcAndRack("datacenter2", "rack2")));
        }

        if (configuration.instanceInitializer != null)
        {
            clusterBuilder.withInstanceInitializer(configuration.instanceInitializer);
        }

        UpgradeableCluster cluster = clusterBuilder.start();
        if (cluster.size() > 1)
        {
            waitForHealthyRing(cluster);
            fixDistributedSchemas((AbstractCluster<I>) cluster);
        }
        return (AbstractCluster<I>) cluster;
    }

    // IClusterExtension methods

    @Override
    public void schemaChangeIgnoringStoppedInstances(String query)
    {
        delegate.schemaChangeIgnoringStoppedInstances(query);
    }

    @Override
    public I addInstance(String dc, String rack, Consumer<IInstanceConfig> fn)
    {
        return ClusterUtils.addInstance(delegate, dc, rack, fn);
    }

    @Override
    public I getFirstRunningInstance()
    {
        return delegate.getFirstRunningInstance();
    }

    @Override
    public IInstanceConfig newInstanceConfig()
    {
        return delegate.newInstanceConfig();
    }

    @Override
    public ICluster<I> delegate()
    {
        return delegate;
    }

    @Override
    public void awaitRingState(IInstance instance, IInstance expectedInRing, String state)
    {
        ClusterUtils.awaitRingState(instance, expectedInRing, state);
    }

    @Override
    public void awaitRingStatus(IInstance instance, IInstance expectedInRing, String status)
    {
        ClusterUtils.awaitRingStatus(instance, expectedInRing, status);
    }

    @Override
    public void awaitGossipStatus(IInstance instance, IInstance expectedInGossip, String targetStatus)
    {
        ClusterUtils.awaitGossipStatus(instance, expectedInGossip, targetStatus);
    }

    @Override
    public void stopUnchecked(IInstance instance)
    {
        ClusterUtils.stopUnchecked(instance);
    }

    // ICluster methods

    @Override
    public void startup()
    {
        delegate.startup();
    }

    @Override
    public I bootstrap(IInstanceConfig iInstanceConfig)
    {
        return delegate.bootstrap(iInstanceConfig);
    }

    @Override
    public I get(int i)
    {
        return delegate.get(i);
    }

    @Override
    public I get(InetSocketAddress inetSocketAddress)
    {
        return delegate.get(inetSocketAddress);
    }

    @Override
    public ICoordinator coordinator(int i)
    {
        return delegate.coordinator(i);
    }

    @Override
    public void schemaChange(String s)
    {
        delegate.schemaChange(s);
    }

    @Override
    public void schemaChange(String s, int i)
    {
        delegate.schemaChange(s, i);
    }

    @Override
    public int size()
    {
        return delegate.size();
    }

    @Override
    public Stream<I> stream()
    {
        return delegate.stream();
    }

    @Override
    public Stream<I> stream(String s)
    {
        return delegate.stream(s);
    }

    @Override
    public Stream<I> stream(String s, String s1)
    {
        return delegate.stream(s, s1);
    }

    @NotNull
    @Override
    public Iterator<I> iterator()
    {
        return delegate.iterator();
    }

    @Override
    public IMessageFilters filters()
    {
        return delegate.filters();
    }

    @Override
    public void setMessageSink(IMessageSink messageSink)
    {
        delegate.setMessageSink(messageSink);
    }

    @Override
    public void deliverMessage(InetSocketAddress to, IMessage msg)
    {
        delegate.deliverMessage(to, msg);
    }

    @Override
    public void setUncaughtExceptionsFilter(BiPredicate<Integer, Throwable> ignoreThrowable)
    {
        delegate.setUncaughtExceptionsFilter(ignoreThrowable);
    }

    @Override
    public void setUncaughtExceptionsFilter(Predicate<Throwable> ignoreThrowable)
    {
        delegate.setUncaughtExceptionsFilter(ignoreThrowable);
    }

    @Override
    public void checkAndResetUncaughtExceptions()
    {
        delegate.checkAndResetUncaughtExceptions();
    }

    @Override
    public void close() throws Exception
    {
        delegate.close();
    }

    @Override
    public void forEach(Consumer<? super I> action)
    {
        delegate.forEach(action);
    }

    @Override
    public Spliterator<I> spliterator()
    {
        return delegate.spliterator();
    }

    // Utility methods

    void fixDistributedSchemas(AbstractCluster<I> cluster)
    {
        // These keyspaces are under replicated by default, so must be updated when doing a multi-node cluster;
        // else bootstrap will fail with 'Unable to find sufficient sources for streaming range <range> in keyspace <name>'
        for (String ks : Arrays.asList("system_auth", "system_traces"))
        {
            cluster.schemaChange("ALTER KEYSPACE " + ks +
                                 " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': "
                                 + Math.min(cluster.size(), 3) + "}",
                                 true,
                                 cluster.getFirstRunningInstance());
        }

        // in real live repair is needed in this case, but in the test case it doesn't matter if the tables loose
        // anything, so ignoring repair to speed up the tests.
    }

    void waitForHealthyRing(ICluster<?> cluster)
    {
        for (IInstance inst : cluster)
        {
            ClusterUtils.awaitRingHealthy(inst);
        }
    }
}
