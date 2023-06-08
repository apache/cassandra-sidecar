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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.sidecar.common.testing.CassandraIntegrationTest;
import org.apache.cassandra.sidecar.common.testing.CassandraTestContext;
import org.apache.cassandra.sidecar.mocks.V30;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ensures the Delegate works correctly
 */
class DelegateTest
{
    @CassandraIntegrationTest
    void testCorrectVersionIsEnabled(CassandraTestContext context)
    {
        CassandraVersionProvider provider = new CassandraVersionProvider.Builder().add(new V30()).build();
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(
                provider, context.session, context.jmxClient, "1.0-TEST");
        SimpleCassandraVersion version = delegate.version();
        assertThat(version).isNotNull();
    }

    @CassandraIntegrationTest
    void testHealthCheck(CassandraTestContext context) throws IOException, InterruptedException
    {
        CassandraVersionProvider provider = new CassandraVersionProvider.Builder().add(new V30()).build();
        CassandraAdapterDelegate delegate = new CassandraAdapterDelegate(
                provider, context.session, context.jmxClient, "1.0-TEST");

        delegate.healthCheck();

        assertThat(delegate.isUp()).as("health check succeeds").isTrue();

        context.container.execInContainer("nodetool", "disablebinary");

        delegate.healthCheck();
        assertThat(delegate.isUp()).as("health check fails after binary has been disabled").isFalse();

        context.container.execInContainer("nodetool", "enablebinary");

        TimeUnit.SECONDS.sleep(1);
        delegate.healthCheck();

        assertThat(delegate.isUp()).as("health check succeeds after binary has been enabled").isTrue();
    }
}
