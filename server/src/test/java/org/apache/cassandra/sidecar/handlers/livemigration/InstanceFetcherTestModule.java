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

package org.apache.cassandra.sidecar.handlers.livemigration;

import java.nio.file.Path;
import java.util.List;

import com.codahale.metrics.MetricRegistry;
import com.google.inject.AbstractModule;
import org.apache.cassandra.sidecar.cluster.InstancesMetadata;
import org.apache.cassandra.sidecar.cluster.InstancesMetadataImpl;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.common.server.dns.DnsResolver;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.mockito.Mockito.spy;

/**
 * Test module for injecting custom InstancesMetadata and InstanceMetadataFetcher for Live Migration.
 */
class InstanceFetcherTestModule extends AbstractModule
{
    private final Path storageDir;

    InstanceFetcherTestModule(Path storageDir)
    {
        this.storageDir = storageDir;
    }

    @Override
    protected void configure()
    {
        InstancesMetadata instancesMetadata = new InstancesMetadataImpl(
        List.of(
        getMockInstance(1, "127.0.0.1"),
        getMockInstance(2, "127.0.0.2"),
        getMockInstance(3, "127.0.0.3"),
        getMockInstance(4, "127.0.0.4")),
        new DnsResolver()
        {
            @Override
            public String resolve(String s)
            {
                return s;
            }

            @Override
            public String reverseResolve(String s)
            {
                return null;
            }
        });
        InstanceMetadataFetcher metadataFetcher = spy(new InstanceMetadataFetcher(instancesMetadata));
        bind(InstancesMetadata.class).toInstance(instancesMetadata);
        bind(InstanceMetadataFetcher.class).toInstance(metadataFetcher);
    }

    private InstanceMetadata getMockInstance(int id, String host)
    {
        return InstanceMetadataImpl.builder()
                                   .id(id)
                                   .host(host)
                                   .storagePort(7000)
                                   .storageDir(storageDir.toString())
                                   .stagingDir(storageDir.resolve("staging").toString())
                                   .metricRegistry(new MetricRegistry())
                                   .build();
    }
}
