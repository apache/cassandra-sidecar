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

package org.apache.cassandra.sidecar.metrics;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.NoopMetricRegistry;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Vertx;
import io.vertx.ext.dropwizard.ThroughputMeter;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.server.SidecarServerEvents;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Test for filtering of metrics
 */
@ExtendWith(VertxExtension.class)
public class FilteringMetricRegistryTest
{
    private static final MetricRegistry NO_OP_METRIC_REGISTRY = new NoopMetricRegistry();
    @TempDir
    private Path confPath;

    @Test
    void testNoopInstanceRetrieved()
    {
        MetricFilter.Equals testFilter = new MetricFilter.Equals("testMetric", true);
        MetricRegistryProvider registryProvider = new MetricRegistryProvider("cassandra_sidecar_" + UUID.randomUUID(),
                                                                             Collections.singletonList(testFilter));
        MetricRegistry metricRegistry = registryProvider.registry();

        assertThat(metricRegistry.timer("testMetric")).isSameAs(NO_OP_METRIC_REGISTRY.timer("any"));
        assertThat(metricRegistry.meter("testMetric")).isSameAs(NO_OP_METRIC_REGISTRY.meter("any"));
        assertThat(metricRegistry.counter("testMetric")).isSameAs(NO_OP_METRIC_REGISTRY.counter("any"));
        assertThat(metricRegistry.histogram("testMetric")).isSameAs(NO_OP_METRIC_REGISTRY.histogram("any"));

        assertThat(metricRegistry.gauge("testMetric", () -> new DefaultSettableGauge<>(0L)))
        .isInstanceOf(DefaultSettableGauge.class);
        assertThat(metricRegistry.getMetrics().containsKey("testMetric")).isFalse();

        metricRegistry.register("testMetric", new ThroughputMeter());
        assertThat(metricRegistry.getMetrics().containsKey("testMetric")).isFalse();
    }

    @Test
    void testOneMatchingFilter()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact", false);
        MetricFilter.Regex regexFilter = new MetricFilter.Regex("vertx.*", true);
        MetricRegistryProvider registryProvider = new MetricRegistryProvider("cassandra_sidecar_" + UUID.randomUUID(),
                                                                             Arrays.asList(exactFilter, regexFilter));
        MetricRegistry metricRegistry = registryProvider.registry();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getMetrics().containsKey("sidecar.metric.exact")).isTrue();
    }

    @Test
    void testMultipleMatchingFilter()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact", false);
        MetricFilter.Regex regexFilter = new MetricFilter.Regex("sidecar.*", false);
        MetricRegistryProvider registryProvider = new MetricRegistryProvider("cassandra_sidecar_" + UUID.randomUUID(),
                                                                             Arrays.asList(exactFilter, regexFilter));
        MetricRegistry metricRegistry = registryProvider.registry();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getMetrics().containsKey("sidecar.metric.exact")).isTrue();
    }

    @Test
    void testExcludingEqualsMetricFilter()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact", true);
        MetricRegistryProvider registryProvider = new MetricRegistryProvider("cassandra_sidecar_" + UUID.randomUUID(),
                                                                             Collections.singletonList(exactFilter));
        MetricRegistry metricRegistry = registryProvider.registry();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getMetrics().containsKey("sidecar.metric.exact")).isFalse();
    }

    @Test
    void testExcludingRegexMetricFilter()
    {
        MetricFilter.Regex vertxFilter = new MetricFilter.Regex("vertx.*", true);
        MetricFilter.Regex sidecarFilter = new MetricFilter.Regex("sidecar.*", false);
        MetricRegistryProvider registryProvider = new MetricRegistryProvider("cassandra_sidecar_" + UUID.randomUUID(),
                                                                             Arrays.asList(vertxFilter, sidecarFilter));
        MetricRegistry metricRegistry = registryProvider.registry();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getMetrics().containsKey("sidecar.metric.exact")).isTrue();
        metricRegistry.timer("vertx.eventbus.message_transfer_time");
        assertThat(metricRegistry.getMetrics().containsKey("vertx.eventbus.message_transfer_time")).isFalse();
    }

    @Test
    void testMultipleMatchingFilterWithOneInverse()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact", true);
        MetricFilter.Regex regexFilter = new MetricFilter.Regex("sidecar.*", false);
        MetricRegistryProvider registryProvider = new MetricRegistryProvider("cassandra_sidecar_" + UUID.randomUUID(),
                                                                             Arrays.asList(exactFilter, regexFilter));
        MetricRegistry metricRegistry = registryProvider.registry();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getMetrics().containsKey("sidecar.metric.exact")).isFalse();
    }

    @Test
    void testReconfiguringMetricFilters()
    {
        MetricFilter.Regex vertxFilterInverse = new MetricFilter.Regex("vertx.*", true);
        MetricRegistryProvider registryProvider = new MetricRegistryProvider("cassandra_sidecar_" + UUID.randomUUID(),
                                                                             Collections.singletonList(vertxFilterInverse));
        MetricRegistry metricRegistry = registryProvider.registry();

        metricRegistry.timer("vertx.eventbus.message_transfer_time");
        assertThat(metricRegistry.getMetrics().containsKey("vertx.eventbus.message_transfer_time")).isFalse();

        MetricFilter.Regex vertxFilter = new MetricFilter.Regex("vertx.*", false);
        ((FilteringMetricRegistry) metricRegistry).configureFilters(Collections.singletonList(vertxFilter));

        metricRegistry.timer("vertx.eventbus.message_transfer_time");
        assertThat(metricRegistry.getMetrics().containsKey("vertx.eventbus.message_transfer_time")).isTrue();
    }

    @Test
    void testWithServer(VertxTestContext context)
    {
        ClassLoader classLoader = FilteringMetricRegistryTest.class.getClassLoader();
        Path yamlPath = writeResourceToPath(classLoader, confPath, "config/sidecar_metrics.yaml");
        Injector injector = Guice.createInjector(new MainModule(yamlPath));
        Server server = injector.getInstance(Server.class);
        Vertx vertx = injector.getInstance(Vertx.class);

        Checkpoint serverStarted = context.checkpoint();
        Checkpoint waitUntilCheck = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());

        server.start()
              .onFailure(context::failNow)
              .onSuccess(v -> {
                  MetricRegistryProvider registryProvider = injector.getInstance(MetricRegistryProvider.class);
                  Pattern excludedPattern = Pattern.compile("vertx.eventbus.*");
                  MetricRegistry globalRegistry = registryProvider.registry();
                  assertThat(globalRegistry.getMetrics().size()).isGreaterThanOrEqualTo(1);
                  assertThat(globalRegistry.getMetrics()
                                           .keySet()
                                           .stream()
                                           .noneMatch(key -> excludedPattern.matcher(key).matches()))
                  .isTrue();
                  waitUntilCheck.flag();
                  context.completeNow();
              });
    }
}
