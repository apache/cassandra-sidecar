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
import org.apache.cassandra.sidecar.IntegrationTestBase;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ensures the Delegate works correctly
 */
class DelegateTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(jmx = false)
    void testCorrectVersionIsEnabled()
    {
        CassandraAdapterDelegate delegate = sidecarTestContext.instancesConfig().instances().get(0).delegate();
        SimpleCassandraVersion version = delegate.version();
        assertThat(version).isNotNull();
        assertThat(version.major).isEqualTo(sidecarTestContext.version.major);
        assertThat(version.minor).isEqualTo(sidecarTestContext.version.minor);
        assertThat(version).isGreaterThanOrEqualTo(sidecarTestContext.version);
    }

    @CassandraIntegrationTest(jmx = false)
    void testHealthCheck() throws InterruptedException
    {
        CassandraAdapterDelegate delegate = sidecarTestContext.instancesConfig().instances().get(0).delegate();

        delegate.healthCheck();

        assertThat(delegate.isUp()).as("health check succeeds").isTrue();

        NodeToolResult nodetoolResult = sidecarTestContext.cluster().get(1).nodetoolResult("disablebinary");
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

        sidecarTestContext.cluster().get(1).nodetool("enablebinary");

        TimeUnit.SECONDS.sleep(1);
        delegate.healthCheck();

        assertThat(delegate.isUp()).as("health check succeeds after binary has been enabled").isTrue();
    }
}
