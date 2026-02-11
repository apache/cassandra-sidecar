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

package org.apache.cassandra.sidecar.tasks;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.common.server.cluster.locator.TokenRange;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.PeriodicTaskConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.TokenRingProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link ClusterTopologyMonitor}
 */
@ExtendWith(VertxExtension.class)
class ClusterTopologyMonitorTest
{
    SidecarConfiguration mockConfiguration;
    PeriodicTaskConfiguration mockClusterTopologyMonitorConfiguration;
    TokenRingProvider mockTokenRingProvider;
    ClusterTopologyMonitor monitor;
    Vertx vertx;

    @BeforeEach
    void setup(Vertx vertx)
    {
        this.vertx = vertx;
        mockConfiguration = mock(SidecarConfiguration.class);
        mockClusterTopologyMonitorConfiguration = mock(PeriodicTaskConfiguration.class);
        mockTokenRingProvider = mock(TokenRingProvider.class);

        when(mockConfiguration.clusterTopologyMonitorConfiguration()).thenReturn(mockClusterTopologyMonitorConfiguration);
        when(mockClusterTopologyMonitorConfiguration.initialDelay()).thenReturn(MillisecondBoundConfiguration.parse("5s"));
        when(mockClusterTopologyMonitorConfiguration.executeInterval()).thenReturn(MillisecondBoundConfiguration.parse("500s"));

        monitor = new ClusterTopologyMonitor(vertx, mockTokenRingProvider, mockConfiguration);
    }

    @AfterEach
    void tearDown(VertxTestContext context)
    {
        vertx.close(context.succeedingThenComplete());
    }

    @Test
    void testConfiguration()
    {
        assertThat(monitor.initialDelay()).isEqualTo(MillisecondBoundConfiguration.parse("5s"));
        assertThat(monitor.delay()).isEqualTo(MillisecondBoundConfiguration.parse("500s"));

        when(mockClusterTopologyMonitorConfiguration.initialDelay()).thenReturn(MillisecondBoundConfiguration.parse("0s"));
        when(mockClusterTopologyMonitorConfiguration.executeInterval()).thenReturn(MillisecondBoundConfiguration.parse("2000s"));
        ClusterTopologyMonitor customMonitor = new ClusterTopologyMonitor(vertx, mockTokenRingProvider, mockConfiguration);

        assertThat(customMonitor.initialDelay()).isEqualTo(MillisecondBoundConfiguration.parse("0s"));
        assertThat(customMonitor.delay()).isEqualTo(MillisecondBoundConfiguration.parse("2000s"));
    }

    @Test
    void testConfigurationFromYaml() throws IOException
    {
        // Get project root from system property set by Gradle
        Path projectRoot = Paths.get(System.getProperty("project.root"));
        Path configPath = projectRoot.resolve("conf/sidecar.yaml");
        assertThat(configPath).exists();

        SidecarConfiguration config = SidecarConfigurationImpl.readYamlConfiguration(configPath);

        PeriodicTaskConfiguration clusterTopologyConfig = config.clusterTopologyMonitorConfiguration();

        assertThat(clusterTopologyConfig).isNotNull();
        assertThat(clusterTopologyConfig.enabled()).isTrue();
        assertThat(clusterTopologyConfig.initialDelay()).isEqualTo(MillisecondBoundConfiguration.parse("0s"));
        assertThat(clusterTopologyConfig.executeInterval()).isEqualTo(MillisecondBoundConfiguration.parse("1000ms"));
    }

    @Test
    void testInitialBootstrapPublishesEvent(VertxTestContext context)
    {
        Map<String, List<TokenRange>> topology = createTopology("instance1", 0, 100);
        setupMocksForSingleDc("dc1", topology);

        Checkpoint checkpoint = context.checkpoint();

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            ClusterTopologyMonitor.DcLocalTopologyChangeEvent event =
                (ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body();

            context.verify(() -> {
                assertThat(event.dc).isEqualTo("dc1");
                assertThat(event.prev).isNull();
                assertThat(event.curr).isNotNull().hasSize(1);
                assertThat(event.curr).containsKey("instance1");
                assertThat(event.curr.get("instance1")).hasSize(1);
            });
            checkpoint.flag();
        });

        monitor.execute(Promise.promise());
    }

    @Test
    void testNoTopologyChangeDoesNotPublishEvent(VertxTestContext context)
    {
        Map<String, List<TokenRange>> topology = createTopology("instance1", 0, 100);
        setupMocksForSingleDc("dc1", topology);

        List<ClusterTopologyMonitor.DcLocalTopologyChangeEvent> events = new ArrayList<>();

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            events.add((ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body());
        });

        // First execution - should publish bootstrap event
        monitor.execute(Promise.promise());

        // Wait a bit, then execute again with same topology - should not publish
        vertx.setTimer(200, id1 -> {
            monitor.execute(Promise.promise());

            // Wait again to ensure no second event
            vertx.setTimer(200, id2 -> {
                context.verify(() -> {
                    assertThat(events).hasSize(1); // Only bootstrap event
                    assertThat(events.get(0).prev).isNull();
                });
                context.completeNow();
            });
        });
    }

    @Test
    void testTopologyChangeWhenInstanceAdded(VertxTestContext context)
    {
        Map<String, List<TokenRange>> initial = createTopology("instance1", 0, 100);
        Map<String, List<TokenRange>> withNewInstance = new HashMap<>(initial);
        withNewInstance.put("instance2", List.of(new TokenRange(100, 200)));

        when(mockTokenRingProvider.dcs()).thenReturn(Set.of("dc1"));
        when(mockTokenRingProvider.getPrimaryTokenRanges("dc1"))
            .thenReturn(initial)
            .thenReturn(withNewInstance);

        List<ClusterTopologyMonitor.DcLocalTopologyChangeEvent> events = new ArrayList<>();

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            events.add((ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body());
        });

        // Bootstrap
        monitor.execute(Promise.promise());

        vertx.setTimer(200, id1 -> {
            // Add new instance
            monitor.execute(Promise.promise());

            vertx.setTimer(200, id2 -> {
                context.verify(() -> {
                    assertThat(events).hasSize(2);

                    // First event - bootstrap
                    assertThat(events.get(0).dc).isEqualTo("dc1");
                    assertThat(events.get(0).prev).isNull();
                    assertThat(events.get(0).curr).hasSize(1);

                    // Second event - instance added
                    assertThat(events.get(1).dc).isEqualTo("dc1");
                    assertThat(events.get(1).prev).isNotNull().hasSize(1);
                    assertThat(events.get(1).curr).hasSize(2);
                    assertThat(events.get(1).curr).containsKeys("instance1", "instance2");
                });
                context.completeNow();
            });
        });
    }

    @Test
    void testTopologyChangeWhenInstanceRemoved(VertxTestContext context)
    {
        Map<String, List<TokenRange>> initial = createTopology("instance1", 0, 100);
        initial.put("instance2", List.of(new TokenRange(100, 200)));

        Map<String, List<TokenRange>> afterRemoval = createTopology("instance1", 0, 100);

        when(mockTokenRingProvider.dcs()).thenReturn(Set.of("dc1"));
        when(mockTokenRingProvider.getPrimaryTokenRanges("dc1"))
            .thenReturn(initial)
            .thenReturn(afterRemoval);

        List<ClusterTopologyMonitor.DcLocalTopologyChangeEvent> events = new ArrayList<>();

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            events.add((ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body());
        });

        monitor.execute(Promise.promise());

        vertx.setTimer(200, id1 -> {
            monitor.execute(Promise.promise());

            vertx.setTimer(200, id2 -> {
                context.verify(() -> {
                    assertThat(events).hasSize(2);
                    assertThat(events.get(1).prev).hasSize(2);
                    assertThat(events.get(1).curr).hasSize(1);
                    assertThat(events.get(1).curr).containsKey("instance1");
                    assertThat(events.get(1).curr).doesNotContainKey("instance2");
                });
                context.completeNow();
            });
        });
    }

    @Test
    void testTopologyChangeWhenTokenRangesChange(VertxTestContext context)
    {
        Map<String, List<TokenRange>> initial = createTopology("instance1", 0, 100);
        Map<String, List<TokenRange>> updated = createTopology("instance1", 0, 150);

        when(mockTokenRingProvider.dcs()).thenReturn(Set.of("dc1"));
        when(mockTokenRingProvider.getPrimaryTokenRanges("dc1"))
            .thenReturn(initial)
            .thenReturn(updated);

        List<ClusterTopologyMonitor.DcLocalTopologyChangeEvent> events = new ArrayList<>();

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            events.add((ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body());
        });

        monitor.execute(Promise.promise());

        vertx.setTimer(200, id1 -> {
            monitor.execute(Promise.promise());

            vertx.setTimer(200, id2 -> {
                context.verify(() -> {
                    assertThat(events).hasSize(2);
                    // Verify token range changed
                    assertThat(events.get(1).prev).isNotNull();
                    assertThat(events.get(1).curr).isNotNull();
                    assertThat(events.get(1).prev).isNotEqualTo(events.get(1).curr);
                });
                context.completeNow();
            });
        });
    }

    @Test
    void testMultipleDatacenters(VertxTestContext context)
    {
        when(mockTokenRingProvider.dcs()).thenReturn(Set.of("dc1", "dc2"));
        when(mockTokenRingProvider.getPrimaryTokenRanges("dc1")).thenReturn(createTopology("instance1", 0, 100));
        when(mockTokenRingProvider.getPrimaryTokenRanges("dc2")).thenReturn(createTopology("instance2", 0, 100));

        List<ClusterTopologyMonitor.DcLocalTopologyChangeEvent> events = new ArrayList<>();
        Checkpoint checkpoint = context.checkpoint(2); // Expect 2 events

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            ClusterTopologyMonitor.DcLocalTopologyChangeEvent event =
                (ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body();
            events.add(event);
            checkpoint.flag();
        });

        monitor.execute(Promise.promise());

        // Give some time for events to be processed, then verify
        vertx.setTimer(500, id -> {
            context.verify(() -> {
                assertThat(events).hasSize(2);
                assertThat(events).anyMatch(e -> "dc1".equals(e.dc));
                assertThat(events).anyMatch(e -> "dc2".equals(e.dc));
            });
        });
    }

    @Test
    void testMultipleSequentialChanges(VertxTestContext context)
    {
        Map<String, List<TokenRange>> topology1 = createTopology("instance1", 0, 100);

        Map<String, List<TokenRange>> topology2 = new HashMap<>(topology1);
        topology2.put("instance2", List.of(new TokenRange(100, 200)));

        Map<String, List<TokenRange>> topology3 = new HashMap<>(topology2);
        topology3.put("instance3", List.of(new TokenRange(200, 300)));

        when(mockTokenRingProvider.dcs()).thenReturn(Set.of("dc1"));
        when(mockTokenRingProvider.getPrimaryTokenRanges("dc1"))
            .thenReturn(topology1)
            .thenReturn(topology2)
            .thenReturn(topology3);

        List<ClusterTopologyMonitor.DcLocalTopologyChangeEvent> events = new ArrayList<>();

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            events.add((ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body());
        });

        monitor.execute(Promise.promise());

        vertx.setTimer(200, id1 -> {
            monitor.execute(Promise.promise());

            vertx.setTimer(200, id2 -> {
                monitor.execute(Promise.promise());

                vertx.setTimer(200, id3 -> {
                    context.verify(() -> {
                        assertThat(events).hasSize(3);
                        assertThat(events.get(0).curr).hasSize(1);
                        assertThat(events.get(1).curr).hasSize(2);
                        assertThat(events.get(2).curr).hasSize(3);
                    });
                    context.completeNow();
                });
            });
        });
    }

    @Test
    void testExecuteHandlesExceptionGracefully(VertxTestContext context)
    {
        when(mockTokenRingProvider.dcs()).thenThrow(new RuntimeException("Test exception"));

        Promise<Void> promise = Promise.promise();
        monitor.execute(promise);

        promise.future().onComplete(ar -> {
            context.verify(() -> assertThat(ar.succeeded()).isTrue());
            context.completeNow();
        });
    }

    @Test
    void testChangeInInstanceTopologyDetectsChanges()
    {
        Map<String, List<TokenRange>> prev = createTopology("instance1", 0, 100);
        Map<String, List<TokenRange>> curr;

        // Adding instance
        curr = createTopology("instance1", 0, 100);
        curr.put("instance2", List.of(new TokenRange(100, 200)));
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology(prev, curr)).isTrue();

        // Removing instance
        prev = createTopology("instance1", 0, 100);
        prev.put("instance2", List.of(new TokenRange(100, 200)));
        curr = createTopology("instance1", 0, 100);
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology(prev, curr)).isTrue();

        // Token range change
        prev = createTopology("instance1", 0, 100);
        curr = createTopology("instance1", 0, 150);
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology(prev, curr)).isTrue();

        // No change
        prev = createTopology("instance1", 0, 100);
        curr = createTopology("instance1", 0, 100);
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology(prev, curr)).isFalse();

        // Bootstrap (prev is null)
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology(null, curr)).isTrue();
    }

    @Test
    void testChangeInInstanceTopologyDetectsTokenRangeChanges()
    {
        List<TokenRange> prev, curr;

        // Number of ranges changed
        prev = List.of(new TokenRange(0, 100));
        curr = List.of(new TokenRange(0, 50), new TokenRange(50, 100));
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology("instance1", prev, curr)).isTrue();

        // Lower endpoint changed
        prev = List.of(new TokenRange(0, 100));
        curr = List.of(new TokenRange(10, 100));
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology("instance1", prev, curr)).isTrue();

        // Upper endpoint changed
        prev = List.of(new TokenRange(0, 100));
        curr = List.of(new TokenRange(0, 150));
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology("instance1", prev, curr)).isTrue();

        // No change
        prev = List.of(new TokenRange(0, 100), new TokenRange(200, 300));
        curr = List.of(new TokenRange(0, 100), new TokenRange(200, 300));
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology("instance1", prev, curr)).isFalse();

        // Order doesn't matter
        prev = List.of(new TokenRange(200, 300), new TokenRange(0, 100));
        curr = List.of(new TokenRange(0, 100), new TokenRange(200, 300));
        assertThat(ClusterTopologyMonitor.changeInInstanceTopology("instance1", prev, curr)).isFalse();
    }

    @Test
    void testEmptyTopology(VertxTestContext context)
    {
        when(mockTokenRingProvider.dcs()).thenReturn(Set.of("dc1"));
        when(mockTokenRingProvider.getPrimaryTokenRanges("dc1")).thenReturn(new HashMap<>());

        List<ClusterTopologyMonitor.DcLocalTopologyChangeEvent> events = new ArrayList<>();

        vertx.eventBus().consumer(ClusterTopologyMonitor.ClusterTopologyEventType.ON_DC_TOPOLOGY_CHANGE.address(), message -> {
            events.add((ClusterTopologyMonitor.DcLocalTopologyChangeEvent) message.body());
        });

        monitor.execute(Promise.promise());

        vertx.setTimer(200, id -> {
            context.verify(() -> {
                // Should still publish bootstrap event even with empty topology
                assertThat(events).hasSize(1);
                assertThat(events.get(0).curr).isEmpty();
            });
            context.completeNow();
        });
    }

    @Test
    void testUpdateMethodBehavior()
    {
        String dc = "dc1";
        Map<String, List<TokenRange>> topology1 = createTopology("instance1", 0, 100);
        Map<String, List<TokenRange>> topology2 = createTopology("instance1", 0, 150);

        // First update (bootstrap) should succeed
        boolean result = monitor.update(dc, null, topology1);
        assertThat(result).isTrue();

        // Trying to update with same prev should succeed
        result = monitor.update(dc, topology1, topology2);
        assertThat(result).isTrue();

        // Trying to update with wrong prev should fail (concurrent modification)
        Map<String, List<TokenRange>> topology3 = createTopology("instance1", 0, 200);
        result = monitor.update(dc, topology1, topology3);
        assertThat(result).isFalse();
    }

    // Helper methods

    private Map<String, List<TokenRange>> createTopology(String instanceId, long start, long end)
    {
        Map<String, List<TokenRange>> topology = new HashMap<>();
        topology.put(instanceId, List.of(new TokenRange(start, end)));
        return topology;
    }

    private void setupMocksForSingleDc(String dc, Map<String, List<TokenRange>> topology)
    {
        when(mockTokenRingProvider.dcs()).thenReturn(Set.of(dc));
        when(mockTokenRingProvider.getPrimaryTokenRanges(dc)).thenReturn(topology);
    }
}
