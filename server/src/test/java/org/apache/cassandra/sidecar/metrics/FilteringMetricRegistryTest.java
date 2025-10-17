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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.DefaultSettableGauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.NoopMetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.vertx.core.Vertx;
import io.vertx.ext.dropwizard.ThroughputMeter;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.modules.SidecarModules;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.server.SidecarServerEvents;

import static org.apache.cassandra.sidecar.common.ResourceUtils.writeResourceToPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test for filtering of metrics
 */
@ExtendWith(VertxExtension.class)
class FilteringMetricRegistryTest
{
    private static final MetricRegistry NO_OP_METRIC_REGISTRY = new NoopMetricRegistry();
    private static final List<BiConsumer<FilteringMetricRegistry, String>> METRIC_REGISTRY_STRING_BI_CONSUMER = List.of(
    (r, metricName) -> r.register(metricName, new ThroughputMeter()),
    FilteringMetricRegistry::meter,
    FilteringMetricRegistry::gauge,
    FilteringMetricRegistry::counter,
    FilteringMetricRegistry::histogram,
    FilteringMetricRegistry::timer
    );
    @TempDir
    private Path confPath;

    @AfterEach
    void cleanup()
    {
        SharedMetricRegistries.clear();
    }

    @Test
    void testNoopInstanceRetrieved()
    {
        MetricFilter.Regex testFilter = new MetricFilter.Regex("testMetric.*");
        MetricRegistryFactory registryFactory = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                          Collections.emptyList(),
                                                                          Collections.singletonList(testFilter));
        FilteringMetricRegistry metricRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();

        assertThat(metricRegistry.timer("testMetricTimer")).isSameAs(NO_OP_METRIC_REGISTRY.timer("any"));
        assertThat(metricRegistry.meter("testMetricMeter")).isSameAs(NO_OP_METRIC_REGISTRY.meter("any"));
        assertThat(metricRegistry.counter("testMetricCounter")).isSameAs(NO_OP_METRIC_REGISTRY.counter("any"));
        assertThat(metricRegistry.histogram("testMetricHistogram")).isSameAs(NO_OP_METRIC_REGISTRY.histogram("any"));

        metricRegistry.register("testMetricThroughputMeter", new ThroughputMeter());
        assertThat(metricRegistry.getIncludedMetrics()).doesNotContainKey("testMetricThroughputMeter");
    }

    @Test
    void testDuplicateMetricsNotAllowed()
    {
        MetricRegistry metricRegistry = new MetricRegistry();
        assertThat(metricRegistry.timer("testMetric")).isNotNull();
        assertThatThrownBy(() -> metricRegistry.meter("testMetric"))
        .isInstanceOf(IllegalArgumentException.class);

        MetricRegistryFactory registryFactory = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                          Collections.emptyList(),
                                                                          Collections.emptyList());
        FilteringMetricRegistry filteringMetricRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();

        filteringMetricRegistry.timer("testMetric");
        assertThatThrownBy(() -> filteringMetricRegistry.meter("testMetric"))
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testGaugeMetricExclusion()
    {
        MetricFilter.Regex testFilter = new MetricFilter.Regex("testMetric.*");
        MetricRegistryFactory registryFactory = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                          Collections.emptyList(),
                                                                          Collections.singletonList(testFilter));
        FilteringMetricRegistry metricRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();

        assertThat(metricRegistry.gauge("testMetricGauge", () -> new DefaultSettableGauge<>(0L)))
        .isInstanceOf(DefaultSettableGauge.class);
        assertThat(metricRegistry.getIncludedMetrics()).doesNotContainKey("testMetricGauge");

        metricRegistry.register("testMetricDefaultSettableGaugeLong", new DefaultSettableGauge<>(0L));
        assertThat(metricRegistry.getIncludedMetrics()).doesNotContainKey("testMetricDefaultSettableGaugeLong");

        metricRegistry.register("testMetricDefaultSettableGaugeDouble", new DefaultSettableGauge<>(0d));
        assertThat(metricRegistry.getIncludedMetrics()).doesNotContainKey("testMetricDefaultSettableGaugeDouble");
    }

    @Test
    void testOneMatchingFilter()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact");
        MetricFilter.Regex regexFilter = new MetricFilter.Regex("vertx.*");
        MetricRegistryFactory registryFactory = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                          Collections.singletonList(exactFilter),
                                                                          Collections.singletonList(regexFilter));
        FilteringMetricRegistry metricRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getIncludedMetrics()).containsKey("sidecar.metric.exact");
    }

    @Test
    void testMultipleMatchingFilter()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact");
        MetricFilter.Regex regexFilter = new MetricFilter.Regex("sidecar.*");
        MetricRegistryFactory registryFactory = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                          Arrays.asList(exactFilter, regexFilter),
                                                                          Collections.emptyList());
        FilteringMetricRegistry metricRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getIncludedMetrics()).containsKey("sidecar.metric.exact");
    }

    @Test
    void testExcludingEqualsMetricFilter()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact");
        MetricRegistryFactory registryFactory = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                          Collections.emptyList(),
                                                                          Collections.singletonList(exactFilter));
        FilteringMetricRegistry metricRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getIncludedMetrics()).doesNotContainKey("sidecar.metric.exact");
    }

    @Test
    void testExcludingRegexMetricFilter()
    {
        MetricFilter.Regex vertxFilter = new MetricFilter.Regex("vertx.*");
        MetricFilter.Regex sidecarFilter = new MetricFilter.Regex("sidecar.*");
        MetricRegistryFactory registryProvider = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                           Collections.singletonList(sidecarFilter),
                                                                           Collections.singletonList(vertxFilter));
        FilteringMetricRegistry metricRegistry = (FilteringMetricRegistry) registryProvider.getOrCreate();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getMetrics()).containsKey("sidecar.metric.exact");
        metricRegistry.timer("vertx.eventbus.message_transfer_time");
        assertThat(metricRegistry.getIncludedMetrics()).doesNotContainKey("vertx.eventbus.message_transfer_time");
    }

    @Test
    void testMultipleMatchingFilterWithOneExclude()
    {
        MetricFilter.Equals exactFilter = new MetricFilter.Equals("sidecar.metric.exact");
        MetricFilter.Regex regexFilter = new MetricFilter.Regex("sidecar.*");
        MetricRegistryFactory registryFactory = new MetricRegistryFactory("cassandra_sidecar_" + UUID.randomUUID(),
                                                                          Collections.singletonList(regexFilter),
                                                                          Collections.singletonList(exactFilter));
        FilteringMetricRegistry metricRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();

        metricRegistry.meter("sidecar.metric.exact");
        assertThat(metricRegistry.getIncludedMetrics()).doesNotContainKey("sidecar.metric.exact");
    }

    @Test
    void testExclusionsWithServer(VertxTestContext context)
    {
        ClassLoader classLoader = FilteringMetricRegistryTest.class.getClassLoader();
        Path yamlPath = writeResourceToPath(classLoader, confPath, "config/sidecar_metrics.yaml");
        Injector injector = Guice.createInjector(SidecarModules.all(yamlPath));
        Server server = injector.getInstance(Server.class);
        Vertx vertx = injector.getInstance(Vertx.class);

        Checkpoint serverStarted = context.checkpoint();
        Checkpoint waitUntilCheck = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());

        server.start()
              .onFailure(context::failNow)
              .onSuccess(v -> {
                  MetricRegistryFactory registryFactory = injector.getInstance(MetricRegistryFactory.class);
                  Pattern excludedPattern = Pattern.compile("vertx.eventbus.*");
                  FilteringMetricRegistry globalRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();
                  assertThat(globalRegistry.getIncludedMetrics().size()).isGreaterThanOrEqualTo(1);
                  assertThat(globalRegistry.getIncludedMetrics().keySet().stream())
                  .noneMatch(key -> excludedPattern.matcher(key).matches());
                  waitUntilCheck.flag();
                  context.completeNow();
              });
    }

    @Test
    void testNoFiltering(VertxTestContext context)
    {
        ClassLoader classLoader = FilteringMetricRegistryTest.class.getClassLoader();
        Path yamlPath = writeResourceToPath(classLoader, confPath, "config/sidecar_metrics_empty_filters.yaml");
        Injector injector = Guice.createInjector(SidecarModules.all(yamlPath));
        Server server = injector.getInstance(Server.class);
        Vertx vertx = injector.getInstance(Vertx.class);

        Checkpoint serverStarted = context.checkpoint();
        Checkpoint waitUntilCheck = context.checkpoint();

        vertx.eventBus().localConsumer(SidecarServerEvents.ON_SERVER_START.address(), message -> serverStarted.flag());

        server.start()
              .onFailure(context::failNow)
              .onSuccess(v -> {
                  MetricRegistryFactory registryFactory = injector.getInstance(MetricRegistryFactory.class);
                  FilteringMetricRegistry globalRegistry = (FilteringMetricRegistry) registryFactory.getOrCreate();
                  assertThat(globalRegistry.getIncludedMetrics().size()).isGreaterThanOrEqualTo(1);
                  waitUntilCheck.flag();
                  context.completeNow();
              });
    }

    @Test
    void testGetMetrics()
    {
        FilteringMetricRegistry registry = new FilteringMetricRegistry(s -> s.endsWith("Include"));

        registry.gauge("gaugeInclude", () -> new DefaultSettableGauge<>(0L));
        assertThat(registry.getMetrics()).hasSize(1)
                                         .containsKey("gaugeInclude");
        assertThat(registry.getIncludedMetrics()).hasSize(1)
                                                 .containsKey("gaugeInclude");
        registry.gauge("gaugeIgnore", () -> new DefaultSettableGauge<>(1L));
        assertThat(registry.getMetrics()).hasSize(2)
                                         .containsKey("gaugeIgnore");
        assertThat(registry.getIncludedMetrics()).hasSize(1)
                                                 .containsKey("gaugeInclude");

        registry.counter("counterInclude");
        assertThat(registry.getMetrics()).hasSize(3)
                                         .containsKey("counterInclude");
        assertThat(registry.getIncludedMetrics()).hasSize(2)
                                                 .containsKey("counterInclude");
        registry.counter("counterIgnore");
        assertThat(registry.getMetrics()).hasSize(4)
                                         .containsKey("counterIgnore");
        assertThat(registry.getIncludedMetrics()).hasSize(2)
                                                 .containsKey("counterInclude");

        registry.histogram("histogramInclude");
        assertThat(registry.getMetrics()).hasSize(5)
                                         .containsKey("histogramInclude");
        assertThat(registry.getIncludedMetrics()).hasSize(3)
                                                 .containsKey("histogramInclude");
        registry.histogram("histogramIgnore");
        assertThat(registry.getMetrics()).hasSize(6)
                                         .containsKey("histogramIgnore");
        assertThat(registry.getIncludedMetrics()).hasSize(3)
                                                 .containsKey("histogramInclude");

        registry.meter("meterInclude");
        assertThat(registry.getMetrics()).hasSize(7)
                                         .containsKey("meterInclude");
        assertThat(registry.getIncludedMetrics()).hasSize(4)
                                                 .containsKey("meterInclude");
        registry.meter("meterIgnore");
        assertThat(registry.getMetrics()).hasSize(8)
                                         .containsKey("meterIgnore");
        assertThat(registry.getIncludedMetrics()).hasSize(4)
                                                 .containsKey("meterInclude");

        registry.timer("timerInclude");
        assertThat(registry.getMetrics()).hasSize(9)
                                         .containsKey("timerInclude");
        assertThat(registry.getIncludedMetrics()).hasSize(5)
                                                 .containsKey("timerInclude");
        registry.timer("timerIgnore");
        assertThat(registry.getMetrics()).hasSize(10)
                                         .containsKey("timerIgnore");
        assertThat(registry.getIncludedMetrics()).hasSize(5)
                                                 .containsKey("timerInclude");

        registry.register("throughputInclude", new ThroughputMeter());
        assertThat(registry.getMetrics()).hasSize(11)
                                         .containsKey("throughputInclude");
        assertThat(registry.getIncludedMetrics()).hasSize(6)
                                                 .containsKey("throughputInclude");
        registry.register("throughputIgnore", new ThroughputMeter());
        assertThat(registry.getMetrics()).hasSize(12)
                                         .containsKey("throughputIgnore");
        assertThat(registry.getIncludedMetrics()).hasSize(6)
                                                 .containsKey("throughputInclude");
    }

    @Test
    void testGetMetricsWithConcurrentRegistration() throws InterruptedException
    {
        FilteringMetricRegistry registry = new FilteringMetricRegistry(s -> s.endsWith("odd"));

        // Let's get some concurrent updates to the registry
        int nThreads = 100;
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        CountDownLatch latch = new CountDownLatch(nThreads);
        for (int i = 0; i < nThreads; i++)
        {
            int finalI = i;
            pool.submit(() -> {
                try
                {
                    String metricName = "metric_" + finalI + "_" + ((finalI % 2 == 0) ? "even" : "odd");
                    BiConsumer<FilteringMetricRegistry, String> biConsumer = registerRandomMetric();
                    // Invoke register roughly at the same time
                    latch.countDown();
                    latch.await();

                    biConsumer.accept(registry, metricName);
                    assertThat(registry.getMetrics()).isNotEmpty();
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();

        // Let's make sure that all metrics are returned
        Map<String, Metric> allMetrics = registry.getMetrics();
        assertThat(allMetrics).hasSize(nThreads);
        assertThat(registry.getIncludedMetrics()).as("Our filter filters out half of the metrics, so we expect this value to be half")
                                                 .hasSize(nThreads / 2);

        // Validate that all metric names are in the set of all metric names
        Set<String> allMetricNames = allMetrics.keySet();
        for (int i = 0; i < nThreads; i++)
        {
            String expectedMetricName = "metric_" + i + "_" + ((i % 2 == 0) ? "even" : "odd");
            assertThat(allMetricNames).as("Expected metric %s", expectedMetricName).contains(expectedMetricName);
        }
    }

    @Test
    void testGetMetricsWithConcurrentRegistrationAndRemoval() throws InterruptedException
    {
        FilteringMetricRegistry registry = new FilteringMetricRegistry(s -> {
            int lastIndexOfUnderscore = s.lastIndexOf("_") + 1;
            int i = Integer.parseInt(s.substring(lastIndexOfUnderscore));
            return i % 4 != 0;
        });

        int nThreads = 100;

        // First populate the registry
        for (int i = 0; i < nThreads; i++)
        {
            String registryName = "testMetricThroughputMeter_" + i;
            registry.register(registryName, new ThroughputMeter());
            assertThat(registry.getMetrics()).hasSize(i + 1);
        }

        // Let's get some concurrent removals to the registry
        ExecutorService pool = Executors.newFixedThreadPool(nThreads);
        CountDownLatch latch = new CountDownLatch(nThreads);
        for (int i = 0; i < nThreads; i++)
        {
            int finalI = i;
            pool.submit(() -> {
                try
                {
                    String registryName = "testMetricThroughputMeter_" + finalI;
                    boolean removeFromRegistry = finalI % 2 == 0;
                    // Invoke register roughly at the same time
                    latch.countDown();
                    latch.await();

                    if (removeFromRegistry)
                    {
                        registry.remove(registryName);
                    }
                }
                catch (InterruptedException e)
                {
                    throw new RuntimeException(e);
                }
            });
        }

        pool.shutdown();
        assertThat(pool.awaitTermination(1, TimeUnit.MINUTES)).isTrue();

        // Let's make sure that all metrics are returned
        Map<String, Metric> allMetrics = registry.getMetrics();
        assertThat(allMetrics).as("About half the metrics are removed").hasSize(nThreads / 2);
        assertThat(registry.getIncludedMetrics()).hasSize(nThreads / 2);
    }

    BiConsumer<FilteringMetricRegistry, String> registerRandomMetric()
    {
        return METRIC_REGISTRY_STRING_BI_CONSUMER.get(ThreadLocalRandom.current().nextInt(METRIC_REGISTRY_STRING_BI_CONSUMER.size()));
    }
}
