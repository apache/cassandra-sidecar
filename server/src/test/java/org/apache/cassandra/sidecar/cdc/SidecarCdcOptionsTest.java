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

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.ProvisionException;
import org.apache.cassandra.bridge.CassandraBridge;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.bridge.CdcBridgeFactory;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.common.server.utils.SecondBoundConfiguration;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link SidecarCdcOptions}.
 *
 * <p>These specifically guard against the throughput/backpressure knobs silently falling back to
 * the hardcoded {@code CdcOptions} interface defaults (1000ms micro-batch delay, 4 commit logs
 * per instance, 200000 max state size, 1 hour max age) instead of the DB-backed {@link CdcConfig}
 * value an operator configures in the "configs" table. Every value asserted here is deliberately
 * chosen to differ from both the {@code CdcOptions} interface default and the {@link CdcConfig}
 * default, so a passing test proves real delegation rather than two defaults coincidentally
 * matching.
 */
class SidecarCdcOptionsTest
{
    private CdcConfig conf;
    private InstanceMetadataFetcher instanceMetadataFetcher;
    private Provider<CassandraBridge> cassandraBridgeProvider;
    private SidecarCdcOptions options;

    @BeforeEach
    void setUp()
    {
        conf = mock(CdcConfig.class);
        instanceMetadataFetcher = mock(InstanceMetadataFetcher.class);
        cassandraBridgeProvider = mock(Provider.class);
        options = new SidecarCdcOptions(instanceMetadataFetcher, conf, cassandraBridgeProvider);
    }

    @Test
    void versionIsTheVersionOfTheProvidedBridge()
    {
        CassandraBridge bridge = mock(CassandraBridge.class);
        when(bridge.getVersion()).thenReturn(CassandraVersion.SIXZERO);
        when(cassandraBridgeProvider.get()).thenReturn(bridge);

        assertThat(options.version()).isEqualTo(CassandraVersion.SIXZERO);
    }

    @Test
    void versionFallsBackToFourZeroForAnUnsupportedReleaseVersion()
    {
        // A Guice provider, as in Sidecar: it wraps what the binding throws in a ProvisionException
        Provider<CassandraBridge> provider = providerOf(() -> CdcBridgeFactory.get("9.9.9"));

        assertThat(new SidecarCdcOptions(instanceMetadataFetcher, conf, provider).version()).isEqualTo(CassandraVersion.FOURZERO);
    }

    @Test
    void versionPropagatesOtherBridgeFailures()
    {
        IllegalStateException failure = new IllegalStateException("No Cassandra instance is available");
        Provider<CassandraBridge> provider = providerOf(() -> {
            throw failure;
        });

        assertThatThrownBy(() -> new SidecarCdcOptions(instanceMetadataFetcher, conf, provider).version())
        .isInstanceOf(ProvisionException.class)
        .hasCause(failure);
    }

    @Test
    void minimumDelayBetweenMicroBatchesDelegatesToCdcConfig()
    {
        when(conf.minDelayBetweenMicroBatches()).thenReturn(new MillisecondBoundConfiguration(250, TimeUnit.MILLISECONDS));

        assertThat(options.minimumDelayBetweenMicroBatches()).isEqualTo(Duration.ofMillis(250));
    }

    @Test
    void maxCommitLogsPerInstanceDelegatesToCdcConfig()
    {
        when(conf.maxCommitLogsPerInstance()).thenReturn(16);

        assertThat(options.maxCommitLogsPerInstance()).isEqualTo(16);
    }

    @Test
    void maxCdcStateSizeDelegatesToCdcConfigMaxWatermarkerSize()
    {
        when(conf.maxWatermarkerSize()).thenReturn(12345);

        assertThat(options.maxCdcStateSize()).isEqualTo(12345);
    }

    @Test
    void maximumAgeDelegatesToCdcConfigWatermarkWindow()
    {
        // Regression guard: watermarkWindow() was previously read from the DB-backed config but
        // never consulted anywhere, so operators configuring it had no actual effect -- the CDC
        // engine silently used the 1-hour CdcOptions interface default instead.
        when(conf.watermarkWindow()).thenReturn(new SecondBoundConfiguration(120, TimeUnit.SECONDS));

        assertThat(options.maximumAge()).isEqualTo(Duration.ofSeconds(120));
    }

    private static Provider<CassandraBridge> providerOf(Supplier<CassandraBridge> bridge)
    {
        return Guice.createInjector(new AbstractModule()
        {
            @Provides
            CassandraBridge cassandraBridge()
            {
                return bridge.get();
            }
        }).getProvider(CassandraBridge.class);
    }
}
