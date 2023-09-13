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

package org.apache.cassandra.testing;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.UpgradeableCluster;

/**
 * A Cassandra Test Context implementation that allows advanced cluster configuration before cluster creation
 * by providing access to the cluster builder.
 */
public class ConfigurableCassandraTestContext extends AbstractCassandraTestContext
{
    public static final String BUILT_CLUSTER_CANNOT_BE_CONFIGURED_ERROR =
    "Cannot configure a cluster after it is built. Please set the buildCluster annotation attribute to false, "
    + "and do not call `getCluster` before calling this method.";

    private final UpgradeableCluster.Builder builder;

    public ConfigurableCassandraTestContext(SimpleCassandraVersion version,
                                            UpgradeableCluster.Builder builder,
                                            CassandraIntegrationTest annotation)
    {
        super(version, annotation);
        this.builder = builder;
    }

    public UpgradeableCluster configureCluster(Consumer<UpgradeableCluster.Builder> configurator) throws IOException
    {
        if (cluster != null)
        {
            throw new IllegalStateException(BUILT_CLUSTER_CANNOT_BE_CONFIGURED_ERROR);
        }
        configurator.accept(builder);
        cluster = builder.createWithoutStarting();
        return cluster;
    }

    public UpgradeableCluster configureAndStartCluster(Consumer<UpgradeableCluster.Builder> configurator)
    throws IOException
    {
        cluster = configureCluster(configurator);
        cluster.startup();
        return cluster;
    }

    @Override
    public String toString()
    {
        return "ConfigurableCassandraTestContext{"
               + ", version=" + version
               + ", builder=" + builder
               + '}';
    }
}
