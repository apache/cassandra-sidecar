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

package org.apache.cassandra.sidecar.common;

import java.util.concurrent.TimeUnit;

import com.datastax.driver.core.exceptions.TransportException;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.sidecar.adapters.base.CassandraFactory;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.dns.DnsResolver;
import org.apache.cassandra.sidecar.common.utils.SidecarVersionProvider;
import org.apache.cassandra.sidecar.testing.CassandraIntegrationTest;
import org.apache.cassandra.sidecar.testing.CassandraTestContext;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ensures the Delegate works correctly
 */
class DelegateTest
{
    private static CassandraAdapterDelegate getCassandraAdapterDelegate(CassandraTestContext context)
    {
        SidecarVersionProvider svp = new SidecarVersionProvider("/sidecar.version");
        CassandraVersionProvider versionProvider = new CassandraVersionProvider.Builder()
                                                       .add(new CassandraFactory(DnsResolver.DEFAULT,
                                                                                 svp.sidecarVersion()))
                                                       .build();
        InstanceMetadata instanceMetadata = context.instancesConfig.instances().get(0);
        CQLSessionProvider sessionProvider = new CQLSessionProvider(instanceMetadata.host(),
                                                                    instanceMetadata.port(),
                                                                    1000);
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(versionProvider,
                                                                         sessionProvider,
                                                                         context.jmxClient(),
                                                                         svp.sidecarVersion());
        return delegate;
    }

    @CassandraIntegrationTest
    void testCorrectVersionIsEnabled(CassandraTestContext context)
    {
        CassandraAdapterDelegate delegate = getCassandraAdapterDelegate(context);
        SimpleCassandraVersion version = delegate.version();
        assertThat(version).isNotNull();
        assertThat(version.major).isEqualTo(context.version.major);
        assertThat(version.minor).isEqualTo(context.version.minor);
        assertThat(version).isGreaterThanOrEqualTo(context.version);
    }

    @CassandraIntegrationTest
    void testHealthCheck(CassandraTestContext context) throws InterruptedException
    {
        CassandraAdapterDelegate delegate = getCassandraAdapterDelegate(context);

        delegate.healthCheck();

        assertThat(delegate.isUp()).as("health check succeeds").isTrue();

        NodeToolResult nodetoolResult = context.cluster.get(1).nodetoolResult("disablebinary");
        assertThat(nodetoolResult.getRc())
        .withFailMessage("Failed to disable binary:\nstdout:" + nodetoolResult.getStdout()
                         + "\nstderr: " + nodetoolResult.getStderr())
        .isEqualTo(0);

        for (int i = 0; i < 10; i++)
        {
            try
            {
                delegate.healthCheck();
                break;
            }
            catch (TransportException tex)
            {
                Thread.sleep(1000); // Give the delegate some time to recover
            }
        }
        assertThat(delegate.isUp()).as("health check fails after binary has been disabled").isFalse();

        context.cluster.get(1).nodetool("enablebinary");

        TimeUnit.SECONDS.sleep(1);
        delegate.healthCheck();

        assertThat(delegate.isUp()).as("health check succeeds after binary has been enabled").isTrue();
    }
}
