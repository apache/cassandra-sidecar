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
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Modules;
import org.apache.cassandra.sidecar.TestModule;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.CdcConfiguration;
import org.apache.cassandra.sidecar.config.SchemaKeyspaceConfiguration;
import org.apache.cassandra.sidecar.db.CdcConfigAccessor;
import org.apache.cassandra.sidecar.db.KafkaConfigAccessor;
import org.apache.cassandra.sidecar.server.MainModule;
import org.apache.cassandra.sidecar.tasks.PeriodicTaskExecutor;
import org.apache.cassandra.sidecar.tasks.ScheduleDecision;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CdcConfigImplTest
{
    private PeriodicTaskExecutor executor;

    @BeforeEach
    void setup()
    {
        Injector injector = Guice.createInjector(Modules.override(new MainModule()).with(new TestModule()));
        executor = injector.getInstance(PeriodicTaskExecutor.class);
    }

    @Test
    void testIsConfigReadySchemaNotInitialized()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.isSchemaInitialized()).thenReturn(false);

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockCdcConfiguration(), mockSchemaKeyspaceConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testIsConfigReadyKafkaConfigsEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("k1", "v1"));
        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockCdcConfiguration(), mockSchemaKeyspaceConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testIsConfigReadyCdcConfigsEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("k1", "v1"));
        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockCdcConfiguration(), mockSchemaKeyspaceConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.isConfigReady()).isFalse();
    }

    @Test
    void testReturnDefaultValuesWhenConfigsAreEmpty()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockCdcConfiguration(), mockSchemaKeyspaceConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.dc()).isEqualTo(null);
        assertThat(cdcConfig.env()).isEqualTo("");
        assertThat(cdcConfig.kafkaTopic()).isNull();
        assertThat(cdcConfig.logOnly()).isFalse();
    }

    @Test
    void testConfigsWhenConfigsAreNotEmpty() throws InterruptedException
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs())
                .thenReturn(Map.of("dc", "DC1", "env", "if", "log_only", "false", "topic", "topic1"));
        when(kafkaConfigAccessor.getConfig().getConfigs())
                .thenReturn(Map.of("k1", "v1"));

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockCdcConfiguration(), mockSchemaKeyspaceConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        while (!cdcConfig.isConfigReady())
        {
            Thread.sleep(1000);
        }
        assertThat(cdcConfig.dc()).isEqualTo("DC1");
        assertThat(cdcConfig.env()).isEqualTo("if");
        assertThat(cdcConfig.kafkaTopic()).isEqualTo("topic1");
        assertThat(cdcConfig.logOnly()).isFalse();
    }

    @Test
    void testConfigChanged() throws Exception
    {
        Callable listener = mockCallable();
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("dc", "DC1",
                "env", "if",
                "log_only", "false"));
        when(kafkaConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("topic", "topic1"));

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(mockCdcConfiguration(), mockSchemaKeyspaceConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        cdcConfig.registerConfigChangeListener(listener);

        // do not wait the periodic task execution, we force running it immediately.
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(1)).call();

        // run the task multiple times, the listener should still be invoked only once
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(1)).call();

        // update the config. The listener should be called
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of("dc", "DC1",
                "env", "if",
                "log_only", "true"));
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(2)).call();

        // run the task multiple times, the listener should not be called since no more changes are made
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        cdcConfig.forceExecuteNotifier();
        verify(listener, times(2)).call();
    }

    @Test
    void testNotifierIsSkippedWhenCdcIsDisabled()
    {
        CdcConfigAccessor cdcConfigAccessor = mockCdcConfigAccessor();
        when(cdcConfigAccessor.isSchemaInitialized()).thenReturn(true);

        KafkaConfigAccessor kafkaConfigAccessor = mockKafkaConfigAccessor();
        CdcConfiguration cdcConfiguration = mockCdcConfiguration();
        when(cdcConfiguration.isEnabled()).thenReturn(false);

        CdcConfigImpl cdcConfig =
                new CdcConfigImpl(cdcConfiguration, mockSchemaKeyspaceConfiguration(), cdcConfigAccessor, kafkaConfigAccessor, executor);
        assertThat(cdcConfig.configRefreshNotifier().scheduleDecision())
                .isEqualTo(ScheduleDecision.SKIP)
                .describedAs("When sidecarSchema is enabled but cdc is disabled, the refresh notifier should skip");
    }

    private CdcConfigAccessor mockCdcConfigAccessor()
    {
        CdcConfigAccessor cdcConfigAccessor = mock(CdcConfigAccessor.class, RETURNS_DEEP_STUBS);
        when(cdcConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of());
        when(cdcConfigAccessor.isSchemaInitialized()).thenReturn(true);
        return cdcConfigAccessor;
    }

    private KafkaConfigAccessor mockKafkaConfigAccessor()
    {
        KafkaConfigAccessor kafkaConfigAccessor = mock(KafkaConfigAccessor.class, RETURNS_DEEP_STUBS);
        when(kafkaConfigAccessor.getConfig().getConfigs()).thenReturn(Map.of());
        when(kafkaConfigAccessor.isSchemaInitialized()).thenReturn(true);
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

    private Callable mockCallable()
    {
        return mock(Callable.class);
    }
}
