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

package org.apache.cassandra.sidecar.configmanagement;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ConfigurationManager}
 */
class ConfigurationManagerTest
{
    private static final Path BASE_TEMPLATE = Paths.get("src/test/resources/configmanagement/cassandra_latest.yaml");

    private InMemoryConfigurationProvider provider;

    @BeforeEach
    void setUp()
    {
        provider = new InMemoryConfigurationProvider();
    }

    @Test
    void testGetEffectiveConfigurationNoOverlay()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        assertThat(result.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(result.configuration().cassandraYaml().getInteger("num_tokens")).isEqualTo(16);
        assertThat(result.configuration().extraJvmOpts()).isEmpty();
        assertThat(result.lastModified()).isNotNull();
        assertThat(result.hash()).startsWith("sha256:");
    }

    @Test
    void testGetEffectiveConfigurationWithOverlay()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject yamlOverlay = new JsonObject()
                                 .put("concurrent_reads", 128)
                                 .put("memtable_flush_writers", 8);
        Map<String, String> jvmOpts = new LinkedHashMap<>();
        jvmOpts.put("-Xmx", "4g");
        jvmOpts.put("-Dcassandra.ring_delay_ms", "60000");

        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yamlOverlay, jvmOpts);
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(Instant.now(), overlay);
        provider.storeOverlay(instance, null, snapshot);

        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        // Overlay values take precedence
        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
        assertThat(result.configuration().cassandraYaml().getInteger("memtable_flush_writers")).isEqualTo(8);
        // Base template values preserved
        assertThat(result.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(result.configuration().cassandraYaml().getInteger("num_tokens")).isEqualTo(16);
        // JVM opts carried through
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "4g");
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Dcassandra.ring_delay_ms", "60000");
    }

    @Test
    void testGetEffectiveConfigurationProviderFailure()
    {
        ConfigurationProvider failingProvider = new ConfigurationProvider()
        {
            @Override
            public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
            {
                throw new UncheckedIOException(new IOException("provider unavailable"));
            }

            @Override
            public boolean storeOverlay(InstanceMetadata instance, String originalHash,
                                        ConfigurationOverlaySnapshot newSnapshot)
            {
                throw new UnsupportedOperationException();
            }
        };

        ConfigurationManager manager = new ConfigurationManager(failingProvider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        assertThatThrownBy(() -> manager.getEffectiveConfiguration(instance))
                .isInstanceOf(ConfigurationManagerException.class)
                .hasMessageContaining("Failed to retrieve configuration overlay from provider")
                .hasCauseInstanceOf(UncheckedIOException.class);
    }

    @Test
    void testGetEffectiveConfigurationNullBaseTemplateNoOverlay()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, null);
        InstanceMetadata instance = mockInstance(1);

        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        assertThat(result.lastModified()).isEqualTo(Instant.EPOCH);
        assertThat(result.configuration().cassandraYaml()).isEmpty();
        assertThat(result.configuration().extraJvmOpts()).isEmpty();
        assertThat(result.hash()).startsWith("sha256:");
    }

    @Test
    void testGetEffectiveConfigurationNullBaseTemplateWithOverlay()
    {
        InstanceMetadata instance = mockInstance(1);

        JsonObject yamlOverlay = new JsonObject().put("concurrent_reads", 128);
        Map<String, String> jvmOpts = Collections.singletonMap("-Xmx", "4g");

        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yamlOverlay, jvmOpts);
        Instant overlayTime = Instant.parse("2026-06-01T00:00:00Z");
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(overlayTime, overlay);
        provider.storeOverlay(instance, null, snapshot);

        ConfigurationManager manager = new ConfigurationManager(provider, null);
        ConfigurationOverlaySnapshot result = manager.getEffectiveConfiguration(instance);

        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "4g");
        assertThat(result.lastModified()).isEqualTo(overlayTime);
    }

    @Test
    void testGetEffectiveConfigurationCachesBaseSnapshot()
    {
        ConfigurationManager manager = new ConfigurationManager(provider, BASE_TEMPLATE);
        InstanceMetadata instance = mockInstance(1);

        ConfigurationOverlaySnapshot first = manager.getEffectiveConfiguration(instance);
        ConfigurationOverlaySnapshot second = manager.getEffectiveConfiguration(instance);

        assertThat(second).isSameAs(first);
    }

    private static InstanceMetadata mockInstance(int id)
    {
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(id);
        return instance;
    }
}
