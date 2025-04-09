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

package org.apache.cassandra.sidecar.cdc;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.cassandra.sidecar.TestResourceReaper;
import org.apache.cassandra.sidecar.common.server.ThrowingRunnable;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.config.yaml.ServiceConfigurationImpl;
import org.apache.cassandra.sidecar.coordination.ClusterLease;
import org.apache.cassandra.sidecar.db.CdcConfigAccessor;
import org.apache.cassandra.sidecar.db.KafkaConfigAccessor;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;

import static org.apache.cassandra.testing.utils.AssertionUtils.loopAssert;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CdcConfigImplTest
{
    private static final Vertx vertx = Vertx.vertx();
    private static final ExecutorPools executorPools = new ExecutorPools(vertx, new ServiceConfigurationImpl());
    private static final ClusterLease clusterLease = new ClusterLease();
    private PeriodicTaskExecutor executor = new PeriodicTaskExecutor(executorPools, clusterLease);

    @BeforeEach
    void beforeEach()
    {
        executor = new PeriodicTaskExecutor(executorPools, clusterLease);
    }

    @AfterEach
    void afterEach()
    {
        clusterLease.setOwnershipTesting(ClusterLease.Ownership.INDETERMINATE);
        executor.close(Promise.promise());
    }

    @AfterAll
    static void teardown()
    {
        TestResourceReaper.create().with(vertx).with(executorPools).close();
    }

    @Test
    void testIsConfigReadySchemaNotInitialized()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.isAvailable()).thenReturn(false);

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockSidecarConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testIsConfigReadyKafkaConfigsEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("k1", "v1"));
        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockSidecarConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testIsConfigReadyCdcConfigsEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("k1", "v1"));
        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockSidecarConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testReturnDefaultValuesWhenConfigsAreEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockSidecarConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.datacenter()).isEqualTo(null);
        assertThat(cdcConfig.env()).isEqualTo("");
        assertThat(cdcConfig.kafkaTopic()).isNull();
        assertThat(cdcConfig.logOnly()).isFalse();
        assertThat(cdcConfig.watermarkWindow()).isEqualTo(new SecondBoundConfiguration(72, TimeUnit.HOURS));
        assertThat(cdcConfig.persistDelay()).isEqualTo(new MillisecondBoundConfiguration(1, TimeUnit.SECONDS));
    }

    @Test
    void testConfigsWhenConfigsAreNotEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs())
                .thenReturn(Map.of("datacenter", "DC1",
                                   "env", "if",
                                   "log_only", "false",
                                   "topic", "topic1",
                                   "watermark_seconds", "120",
                                   "persist_delay_millis", "5000"));
        when(kafkaConfigAccessor.getConfig().getConfigs())
                .thenReturn(Map.of("k1", "v1"));

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockSidecarConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        loopAssert(5, ()-> assertThat(cdcConfig.isConfigReady()).isTrue());
        assertThat(cdcConfig.datacenter()).isEqualTo("DC1");
        assertThat(cdcConfig.env()).isEqualTo("if");
        assertThat(cdcConfig.kafkaTopic()).isEqualTo("topic1");
        assertThat(cdcConfig.logOnly()).isFalse();
        assertThat(cdcConfig.watermarkWindow()).isEqualTo(new SecondBoundConfiguration(2, TimeUnit.MINUTES));
        assertThat(cdcConfig.persistDelay()).isEqualTo(new MillisecondBoundConfiguration(5, TimeUnit.SECONDS));
    }

    @Test
    void testConfigChanged() throws Exception
    {
        ThrowingRunnable listener = mockRunnable();
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("dc", "DC1",
                "env", "if",
                "log_only", "false"));
        when(kafkaConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("topic", "topic1"));

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockSidecarConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        cdcConfig.registerConfigChangeListener(listener);

        // do not wait the periodic task execution, we force running it immediately.
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(1)).run();

        // run the task multiple times, the listener should still be invoked only once
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(1)).run();

        // update the config. The listener should be called
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("dc", "DC1",
                "env", "if",
                "log_only", "true"));
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(2)).run();

        // run the task multiple times, the listener should not be called since no more changes are made
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(2)).run();
    }

    @Test
    void testNotifierIsSkippedWhenCdcIsDisabled()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        when(cdcConfigAccessor.isAvailable()).thenReturn(true);

        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        CdcConfiguration cdcConfiguration = mockCdcConfiguration();
        when(cdcConfiguration.isEnabled()).thenReturn(false);

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockSidecarConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.configRefreshNotifier().scheduleDecision())
                .isEqualTo(ScheduleDecision.SKIP)
                .describedAs("When sidecarSchema is enabled but cdc is disabled, the refresh notifier should skip");
    }

    private CdcConfigAccessor mockCdcConfigAccessor()
    {
        CdcConfigAccessor cdcConfigAccessor = mock(CdcConfigAccessor.class, RETURNS_DEEP_STUBS);
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of());
        when(cdcConfigAccessor.isAvailable()).thenReturn(true);
        return cdcConfigAccessor;
    }

    private KafkaConfigAccessor mockKafkaConfigAccessor()
    {
        KafkaConfigAccessor kafkaConfigAccessor = mock(KafkaConfigAccessor.class, RETURNS_DEEP_STUBS);
        when(kafkaConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of());
        when(kafkaConfigAccessor.isAvailable()).thenReturn(true);
        return kafkaConfigAccessor;
    }

    private CdcConfiguration mockCdcConfiguration()
    {
        CdcConfiguration cdcConfiguration = mock(CdcConfiguration.class);
        when(cdcConfiguration.isEnabled()).thenReturn(true);
        when(cdcConfiguration.cdcConfigRefreshTime()).thenReturn(MillisecondBoundConfiguration.parse("1s"));
        return cdcConfiguration;
    }

    private SchemaKeyspaceConfiguration mockSchemaKeyspaceConfiguration()
    {
        SchemaKeyspaceConfiguration schemaKeyspaceConfiguration = mock(SchemaKeyspaceConfiguration.class);
        when(schemaKeyspaceConfiguration.isEnabled()).thenReturn(true);
        return schemaKeyspaceConfiguration;
    }

    private SidecarConfiguration mockSidecarConfiguration()
    {
        SidecarConfiguration sidecarConfiguration = mock(SidecarConfiguration.class, RETURNS_DEEP_STUBS);
        when(sidecarConfiguration.serviceConfiguration().cdcConfiguration()).thenAnswer(invocation -> mockCdcConfiguration());
        when(sidecarConfiguration.serviceConfiguration().schemaKeyspaceConfiguration()).thenAnswer(invocation -> mockSchemaKeyspaceConfiguration());
        return sidecarConfiguration;
    }

    private ThrowingRunnable mockRunnable()
    {
        return mock(ThrowingRunnable.class);
    }
}
