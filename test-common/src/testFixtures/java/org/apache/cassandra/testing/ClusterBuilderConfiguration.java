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

package org.apache.cassandra.testing;

import java.util.EnumSet;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.common.base.Preconditions;

import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.shared.AbstractBuilder;

/**
 * Defines the configuration to build the {@link IClusterExtension} cluster
 */
public class ClusterBuilderConfiguration
{
    public int nodesPerDc = 1;
    public int dcCount = 1;
    public int newNodesPerDc = 0;
    public int numDataDirsPerInstance = 1;
    public boolean dynamicPortAllocation = true;
    public final EnumSet<Feature> features = EnumSet.of(Feature.GOSSIP, Feature.JMX, Feature.NATIVE_PROTOCOL);
    public BiConsumer<ClassLoader, Integer> instanceInitializer = null;
    public String partitioner;
    public Map<String, Object> additionalInstanceConfig = null;
    public int tokenCount = 1;
    public boolean startCluster = true;
    public TokenSupplier tokenSupplier = null;
    public Consumer<AbstractBuilder<?, ?, ?>> clusterBuilderUpdater = null;

    /**
     * Adds a features to the list of default features.
     *
     * @param feature the {@code feature} to add
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration requestFeature(Feature feature)
    {
        features.add(feature);
        return this;
    }

    /**
     * Removes a feature to the list of requested features for the cluster.
     *
     * @param feature the {@code feature} to add
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration removeFeature(Feature feature)
    {
        features.remove(feature);
        return this;
    }

    /**
     * Sets the {@code nodesPerDc} and returns a reference to this Builder enabling method chaining.
     *
     * @param nodesPerDc the {@code nodesPerDc} to set
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration nodesPerDc(int nodesPerDc)
    {
        this.nodesPerDc = nodesPerDc;
        return this;
    }

    /**
     * Sets the {@code dcCount} and returns a reference to this Builder enabling method chaining.
     *
     * @param dcCount the {@code dcCount} to set
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration dcCount(int dcCount)
    {
        this.dcCount = dcCount;
        return this;
    }

    /**
     * Sets the {@code newNodesPerDc} and returns a reference to this Builder enabling method chaining.
     *
     * @param newNodesPerDc the {@code newNodesPerDc} to set
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration newNodesPerDc(int newNodesPerDc)
    {
        Preconditions.checkArgument(newNodesPerDc >= 0,
                                    "newNodesPerDc cannot be a negative number");
        this.newNodesPerDc = newNodesPerDc;
        return this;
    }

    /**
     * Sets the {@code numDataDirsPerInstance} and returns a reference to this Builder enabling method chaining.
     *
     * @param numDataDirsPerInstance the {@code numDataDirsPerInstance} to set
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration numDataDirsPerInstance(int numDataDirsPerInstance)
    {
        this.numDataDirsPerInstance = numDataDirsPerInstance;
        return this;
    }

    /**
     * Sets the {@code instanceInitializer} and returns a reference to this Builder enabling method chaining.
     *
     * @param instanceInitializer the {@code instanceInitializer} to set
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration instanceInitializer(BiConsumer<ClassLoader, Integer> instanceInitializer)
    {
        this.instanceInitializer = instanceInitializer;
        return this;
    }

    /**
     * Sets the {@code dynamicPortAllocation} and returns a reference to this Builder enabling method chaining.
     *
     * @param dynamicPortAllocation the {@code dynamicPortAllocation} to set
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration dynamicPortAllocation(boolean dynamicPortAllocation)
    {
        this.dynamicPortAllocation = dynamicPortAllocation;
        return this;
    }

    /**
     * Sets the {@code partitioner} and returns a reference to this Builder enabling method chaining.
     *
     * @param partitioner the {@code partitioner} to set
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration partitioner(String partitioner)
    {
        this.partitioner = partitioner;
        return this;
    }

    /**
     * Sets the {@code additionalInstanceConfig} and returns a reference to this Builder enabling method chaining.
     *
     * @param additionalInstanceConfig additional config to set for the instances
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration additionalInstanceConfig(Map<String, Object> additionalInstanceConfig)
    {
        this.additionalInstanceConfig = additionalInstanceConfig;
        return this;
    }

    /**
     * Sets the {@code tokenCount} of each instance in the cluster and returns a reference to this Builder enabling method chaining.
     *
     * @param tokenCount number of tokens
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration tokenCount(int tokenCount)
    {
        this.tokenCount = tokenCount;
        return this;
    }

    /**
     * Whether the cluster should be started when loaded
     *
     * @param startCluster {@code true} to start the cluster, {@code false} otherwise
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration startCluster(boolean startCluster)
    {
        this.startCluster = startCluster;
        return this;
    }

    /**
     * A token supplier used during the cluster build
     *
     * @param tokenSupplier the instance of the token supplier
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration tokenSupplier(TokenSupplier tokenSupplier)
    {
        this.tokenSupplier = tokenSupplier;
        return this;
    }

    /**
     * @param clusterBuilderUpdater a consumer that can update the cluster builder configuration with custom options
     * @return a reference to this Builder
     */
    public ClusterBuilderConfiguration clusterBuilderUpdater(Consumer<AbstractBuilder<?, ?, ?>> clusterBuilderUpdater)
    {
        this.clusterBuilderUpdater = clusterBuilderUpdater;
        return this;
    }
}
