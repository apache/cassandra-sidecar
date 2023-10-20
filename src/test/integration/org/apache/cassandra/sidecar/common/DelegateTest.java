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

import java.util.Set;
import java.util.stream.IntStream;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.testing.IntegrationTestBase;
import org.apache.cassandra.sidecar.utils.SimpleCassandraVersion;
import org.apache.cassandra.testing.CassandraIntegrationTest;

import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_ALL_CASSANDRA_CQL_READY;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_DISCONNECTED;
import static org.apache.cassandra.sidecar.server.SidecarServerEvents.ON_CASSANDRA_CQL_READY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ensures the Delegate works correctly
 */
@ExtendWith(VertxExtension.class)
class DelegateTest extends IntegrationTestBase
{
    @CassandraIntegrationTest(jmx = false)
    void testCorrectVersionIsEnabled()
    {
        CassandraAdapterDelegate delegate = sidecarTestContext.instancesConfig()
                                                              .instanceFromId(1)
                                                              .delegate();
        SimpleCassandraVersion version = delegate.version();
        assertThat(version).isNotNull();
        assertThat(version.major).isEqualTo(sidecarTestContext.version.major);
        assertThat(version.minor).isEqualTo(sidecarTestContext.version.minor);
        assertThat(version).isGreaterThanOrEqualTo(sidecarTestContext.version);
    }

    @CassandraIntegrationTest(jmx = false)
    void testHealthCheck(VertxTestContext context)
    {
        EventBus eventBus = vertx.eventBus();
        Checkpoint cqlReady = context.checkpoint();
        Checkpoint cqlDisconnected = context.checkpoint();

        CassandraAdapterDelegate adapterDelegate = sidecarTestContext.instancesConfig()
                                                                     .instanceFromId(1)
                                                                     .delegate();
        assertThat(adapterDelegate.isUp()).as("health check succeeds").isTrue();

        // Set up test listeners before disabling/enabling binary to avoid race conditions
        // where the event happens before the consumer is registered.
        eventBus.localConsumer(ON_CASSANDRA_CQL_DISCONNECTED.address(), (Message<JsonObject> message) -> {
            int instanceId = message.body().getInteger("cassandraInstanceId");
            CassandraAdapterDelegate delegate = sidecarTestContext.instancesConfig()
                                                                  .instanceFromId(instanceId)
                                                                  .delegate();

            assertThat(delegate.isUp()).as("health check fails after binary has been disabled").isFalse();
            cqlDisconnected.flag();
            sidecarTestContext.cluster().get(1).nodetool("enablebinary");
        });

        eventBus.localConsumer(ON_CASSANDRA_CQL_READY.address(), (Message<JsonObject> reconnectMessage) -> {
            int instanceId = reconnectMessage.body().getInteger("cassandraInstanceId");
            CassandraAdapterDelegate delegate = sidecarTestContext.instancesConfig()
                                                                  .instanceFromId(instanceId)
                                                                  .delegate();
            assertThat(delegate.isUp()).as("health check succeeds after binary has been enabled")
                                       .isTrue();
            cqlReady.flag();
        });

        // Disable binary
        NodeToolResult nodetoolResult = sidecarTestContext.cluster().get(1).nodetoolResult("disablebinary");
        assertThat(nodetoolResult.getRc())
        .withFailMessage("Failed to disable binary:\nstdout:" + nodetoolResult.getStdout()
                         + "\nstderr: " + nodetoolResult.getStderr())
        .isEqualTo(0);
        // NOTE: enable binary happens inside the disable binary handler above, which then will trigger the
        // cqlReady flag.
    }

    @CassandraIntegrationTest(jmx = false, nodesPerDc = 3)
    void testAllInstancesHealthCheck(VertxTestContext context)
    {
        EventBus eventBus = vertx.eventBus();
        Checkpoint allCqlReady = context.checkpoint();

        Set<Integer> expectedCassandraInstanceIds = ImmutableSet.of(1, 2, 3);
        eventBus.localConsumer(ON_ALL_CASSANDRA_CQL_READY.address(), (Message<JsonObject> message) -> {
            JsonArray cassandraInstanceIds = message.body().getJsonArray("cassandraInstanceIds");
            assertThat(cassandraInstanceIds).hasSize(3);
            assertThat(IntStream.rangeClosed(1, cassandraInstanceIds.size()))
            .allMatch(expectedCassandraInstanceIds::contains);

            allCqlReady.flag();
        });
    }
}
