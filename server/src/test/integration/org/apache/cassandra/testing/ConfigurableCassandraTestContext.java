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

import java.nio.file.Path;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.testing.utils.tls.CertificateBundle;

/**
 * A Cassandra Test Context implementation that allows advanced cluster configuration before cluster creation
 * by providing access to the cluster builder.
 */
public class ConfigurableCassandraTestContext extends AbstractCassandraTestContext
{
    private final ClusterBuilderConfiguration clusterConfiguration;
    private final IsolatedDTestClassLoaderWrapper classLoaderWrapper;
    private final String versionString;

    public ConfigurableCassandraTestContext(SimpleCassandraVersion version,
                                            ClusterBuilderConfiguration clusterConfiguration,
                                            CertificateBundle ca,
                                            Path serverKeystorePath,
                                            Path truststorePath,
                                            CassandraIntegrationTest annotation,
                                            IsolatedDTestClassLoaderWrapper classLoaderWrapper,
                                            String versionString)
    {
        super(version, ca, serverKeystorePath, truststorePath, annotation);
        this.clusterConfiguration = clusterConfiguration;
        this.classLoaderWrapper = classLoaderWrapper;
        this.versionString = versionString;
    }

    public IClusterExtension<? extends IInstance> configureAndStartCluster(Consumer<ClusterBuilderConfiguration> configurator)
    {
        configurator.accept(clusterConfiguration);
        IClusterExtension<? extends IInstance> cluster = CassandraTestTemplate.retriableStartCluster(classLoaderWrapper, versionString, clusterConfiguration, 3);
        setCluster(cluster);
        return cluster;
    }

    @Override
    public String toString()
    {
        return "ConfigurableCassandraTestContext{"
               + ", version=" + version
               + ", cluster=" + cluster()
               + ", builder=" + clusterConfiguration
               + '}';
    }
}
