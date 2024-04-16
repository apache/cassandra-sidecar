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

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.apache.cassandra.sidecar.cluster.CassandraAdapterDelegate;
import org.apache.cassandra.sidecar.cluster.InstancesConfig;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.HealthCheckConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.metrics.MetricRegistryFactory;
import org.apache.cassandra.sidecar.metrics.SidecarMetrics;
import org.apache.cassandra.sidecar.metrics.SidecarMetricsImpl;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.mockito.stubbing.Answer;

import static org.apache.cassandra.sidecar.utils.TestMetricUtils.registry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the {@link HealthCheckPeriodicTask}
 */
@ExtendWith(VertxExtension.class)
class HealthCheckPeriodicTaskTest
{
    SidecarConfiguration mockConfiguration;
    HealthCheckConfiguration mockHealthCheckConfiguration;
    HealthCheckPeriodicTask healthCheck;
    InstancesConfig mockInstancesConfig;
    SidecarMetrics metrics;

    @BeforeEach
    void setup()
    {
        mockConfiguration = mock(SidecarConfiguration.class);
        mockHealthCheckConfiguration = mock(HealthCheckConfiguration.class);
        when(mockConfiguration.healthCheckConfiguration()).thenReturn(mockHealthCheckConfiguration);
        when(mockHealthCheckConfiguration.initialDelayMillis()).thenReturn(10);
        when(mockHealthCheckConfiguration.checkIntervalMillis()).thenReturn(1000);

        mockInstancesConfig = mock(InstancesConfig.class);

        Vertx vertx = Vertx.vertx();
        MetricRegistryFactory mockRegistryFactory = mock(MetricRegistryFactory.class);
        when(mockRegistryFactory.getOrCreate()).thenReturn(registry());
        InstanceMetadataFetcher mockInstanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        metrics = new SidecarMetricsImpl(mockRegistryFactory, mockInstanceMetadataFetcher);
        ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
        healthCheck = new HealthCheckPeriodicTask(vertx, mockConfiguration, mockInstancesConfig,
                                                  executorPools, metrics);
    }

    @AfterEach
    void cleanup()
    {
        registry().removeMatching((name, metric) -> true);
    }

    @Test
    void testConfiguration()
    {
        assertThat(healthCheck.initialDelay()).isEqualTo(10);
        assertThat(healthCheck.delay()).isEqualTo(1000);
        assertThat(healthCheck.name()).isEqualTo("Health Check");
    }

    @Test
    void testHealthCheckPromiseCompletesWhenNoInstancesAreConfigured(VertxTestContext context)
    {
        int expectedUpInstances = 0;
        int expectedDownInstances = 0;
        List<InstanceMetadata> mockInstanceMetadata = Collections.emptyList();
        when(mockInstancesConfig.instances()).thenReturn(mockInstanceMetadata);
        Promise<Void> promise = Promise.promise();
        healthCheck.execute(promise);
        promise.future().onComplete(context.succeeding(v -> {
            assertThat(metrics.server().health().cassandraInstancesUp.metric.getValue()).isEqualTo(expectedUpInstances);
            assertThat(metrics.server().health().cassandraInstancesDown.metric.getValue()).isEqualTo(expectedDownInstances);
            context.completeNow();
        }));
    }

    @Test
    void testHealthCheckInvokedForAllInstances(VertxTestContext context)
    {
        int numberOfInstances = 5;
        int expectedUpInstances = 5;
        int expectedDownInstances = 0;
        Checkpoint healthCheckCheckPoint = context.checkpoint(numberOfInstances);
        List<InstanceMetadata> mockInstanceMetadata =
        buildMockInstanceMetadata(healthCheckCheckPoint, numberOfInstances);
        when(mockInstancesConfig.instances()).thenReturn(mockInstanceMetadata);
        Promise<Void> promise = Promise.promise();
        healthCheck.execute(promise);
        promise.future().onComplete(context.succeeding(v -> {
            assertThat(metrics.server().health().cassandraInstancesUp.metric.getValue()).isEqualTo(expectedUpInstances);
            assertThat(metrics.server().health().cassandraInstancesDown.metric.getValue()).isEqualTo(expectedDownInstances);
            context.completeNow();
        }));
    }

    @Test
    void testInstanceMetadataExceptionDoesntPreventChecksOnOtherInstances(VertxTestContext context)
    {
        int numberOfInstances = 5;
        int expectedUpInstances = 4;
        int expectedDownInstances = 1;
        Checkpoint healthCheckCheckPoint = context.checkpoint(numberOfInstances);
        List<InstanceMetadata> mockInstanceMetadata =
        buildMockInstanceMetadata(healthCheckCheckPoint, numberOfInstances);
        InstanceMetadata mockInstance = mock(InstanceMetadata.class);
        when(mockInstance.delegate()).thenThrow(new RuntimeException());
        mockInstanceMetadata.set(3, mockInstance);
        when(mockInstancesConfig.instances()).thenReturn(mockInstanceMetadata);
        Promise<Void> promise = Promise.promise();
        healthCheck.execute(promise);
        promise.future().onComplete(context.failing(v -> {
            assertThat(metrics.server().health().cassandraInstancesUp.metric.getValue()).isEqualTo(expectedUpInstances);
            assertThat(metrics.server().health().cassandraInstancesDown.metric.getValue()).isEqualTo(expectedDownInstances);
            context.completeNow();
        }));
    }

    @Test
    void testDelegateExceptionDoesntPreventChecksOnOtherInstances(VertxTestContext context)
    {
        int numberOfInstances = 5;
        int expectedUpInstances = 4;
        int expectedDownInstances = 1;
        Checkpoint healthCheckCheckPoint = context.checkpoint(numberOfInstances);
        List<InstanceMetadata> mockInstanceMetadata =
        buildMockInstanceMetadata(healthCheckCheckPoint, numberOfInstances);
        InstanceMetadata mockInstance = mock(InstanceMetadata.class);
        CassandraAdapterDelegate mockDelegate = mock(CassandraAdapterDelegate.class);
        when(mockInstance.delegate()).thenReturn(mockDelegate);
        doThrow(new RuntimeException()).when(mockDelegate).healthCheck();
        mockInstanceMetadata.set(3, mockInstance);
        when(mockInstancesConfig.instances()).thenReturn(mockInstanceMetadata);
        Promise<Void> promise = Promise.promise();
        healthCheck.execute(promise);
        promise.future().onComplete(context.failing(v -> {
            assertThat(metrics.server().health().cassandraInstancesUp.metric.getValue()).isEqualTo(expectedUpInstances);
            assertThat(metrics.server().health().cassandraInstancesDown.metric.getValue()).isEqualTo(expectedDownInstances);
            context.completeNow();
        }));
    }

    private List<InstanceMetadata> buildMockInstanceMetadata(Checkpoint healthCheckCheckPoint, int numberOfInstances)
    {
        return IntStream.range(0, numberOfInstances)
                        .mapToObj(i -> {
                            InstanceMetadata mockInstanceMetadata = mock(InstanceMetadata.class);
                            CassandraAdapterDelegate mockDelegate = mock(CassandraAdapterDelegate.class);

                            doAnswer((Answer<Void>) invocation -> {
                                healthCheckCheckPoint.flag();
                                return null;
                            }).when(mockDelegate).healthCheck();
                            when(mockInstanceMetadata.delegate()).thenReturn(mockDelegate);
                            return mockInstanceMetadata;
                        })
                        .collect(Collectors.toList());
    }
}
