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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ConfigUtils}
 */
class ConfigUtilsTest
{
    @Test
    void testMergeOverlayWins()
    {
        JsonObject base = new JsonObject()
                          .put("concurrent_reads", 32)
                          .put("cluster_name", "original");

        JsonObject overlay = new JsonObject()
                             .put("concurrent_reads", 64);

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getInteger("concurrent_reads")).isEqualTo(64);
        assertThat(result.getString("cluster_name")).isEqualTo("original");
    }

    @Test
    void testMergeAddsNewKeys()
    {
        JsonObject base = new JsonObject()
                          .put("cluster_name", "test");

        JsonObject overlay = new JsonObject()
                             .put("concurrent_reads", 64)
                             .put("storage_compatibility_mode", "CASSANDRA_5");

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getString("cluster_name")).isEqualTo("test");
        assertThat(result.getInteger("concurrent_reads")).isEqualTo(64);
        assertThat(result.getString("storage_compatibility_mode")).isEqualTo("CASSANDRA_5");
    }

    @Test
    void testMergeDeepNestedValues()
    {
        JsonObject skiplist = new JsonObject().put("class_name", "SkipListMemtable");
        JsonObject defaultConfig = new JsonObject().put("inherits", "skiplist");
        JsonObject configurations = new JsonObject()
                                    .put("skiplist", skiplist.getMap())
                                    .put("default", defaultConfig.getMap());
        JsonObject memtable = new JsonObject().put("configurations", configurations.getMap());
        JsonObject base = new JsonObject()
                          .put("memtable", memtable.getMap())
                          .put("cluster_name", "test");

        JsonObject trie = new JsonObject().put("class_name", "TrieMemtable");
        JsonObject overlayDefault = new JsonObject().put("inherits", "trie");
        JsonObject overlayConfigurations = new JsonObject()
                                           .put("trie", trie.getMap())
                                           .put("default", overlayDefault.getMap());
        JsonObject overlayMemtable = new JsonObject().put("configurations", overlayConfigurations.getMap());
        JsonObject overlay = new JsonObject().put("memtable", overlayMemtable.getMap());

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        JsonObject resultConfigs = result.getJsonObject("memtable").getJsonObject("configurations");
        assertThat(resultConfigs.getJsonObject("skiplist").getString("class_name")).isEqualTo("SkipListMemtable");
        assertThat(resultConfigs.getJsonObject("trie").getString("class_name")).isEqualTo("TrieMemtable");
        assertThat(resultConfigs.getJsonObject("default").getString("inherits")).isEqualTo("trie");
        assertThat(result.getString("cluster_name")).isEqualTo("test");
    }

    @Test
    void testMergeOverlayOnlyOverridesSpecifiedKeys()
    {
        JsonObject base = new JsonObject()
                          .put("a", "a")
                          .put("b", "b")
                          .put("c", "c");

        JsonObject overlay = new JsonObject()
                             .put("b", "b2");

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getString("a")).isEqualTo("a");
        assertThat(result.getString("b")).isEqualTo("b2");
        assertThat(result.getString("c")).isEqualTo("c");
    }

    @Test
    void testMergeNullOverlayValueOverwritesBaseValue()
    {
        JsonObject base = new JsonObject()
                          .put("a", "a")
                          .put("b", "b")
                          .put("c", "c");

        JsonObject overlay = new JsonObject()
                             .put("b", (String) null);

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getString("a")).isEqualTo("a");
        assertThat(result.getValue("b")).isNull();
        assertThat(result.getString("c")).isEqualTo("c");
    }

    @Test
    void testMergeEmptyOverlay()
    {
        JsonObject base = new JsonObject()
                          .put("cluster_name", "test")
                          .put("concurrent_reads", 32);

        JsonObject overlay = new JsonObject();

        JsonObject result = ConfigUtils.mergeConfigurations(base, overlay);

        assertThat(result.getString("cluster_name")).isEqualTo("test");
        assertThat(result.getInteger("concurrent_reads")).isEqualTo(32);
    }

    @Test
    void testLoadYamlRealCassandraConfig()
    {
        Path yamlPath = Paths.get("src/test/resources/configmanagement/cassandra_latest.yaml");
        JsonObject config = ConfigUtils.loadYaml(yamlPath);

        assertThat(config).isNotNull();
        assertThat(config.getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(config.getInteger("num_tokens")).isEqualTo(16);
        assertThat(config.getString("commitlog_sync")).isEqualTo("periodic");
        assertThat(new JsonObject(config.getJsonObject("memtable").getMap())
                .getJsonObject("configurations")
                .getJsonObject("trie")
                .getString("class_name"))
                .isEqualTo("TrieMemtable");
        assertThat(config.containsKey("seed_provider")).isTrue();
    }

    @Test
    void testLoadConfiguration()
    {
        Path yamlPath = Paths.get("src/test/resources/configmanagement/cassandra_latest.yaml");

        ConfigurationOverlaySnapshot snapshot = ConfigUtils.loadConfiguration(yamlPath, null);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("Test Cluster");
        assertThat(snapshot.configuration().cassandraYaml().getInteger("num_tokens")).isEqualTo(16);
        assertThat(snapshot.configuration().extraJvmOpts()).isEmpty();
        assertThat(snapshot.lastModified()).isNotNull();
        assertThat(snapshot.hash()).startsWith("sha256:");
    }

    @Test
    void testLoadConfigurationNullPath()
    {
        ConfigurationOverlaySnapshot snapshot = ConfigUtils.loadConfiguration(null, null);

        assertThat(snapshot).isNotNull();
        assertThat(snapshot.lastModified()).isEqualTo(Instant.EPOCH);
        assertThat(snapshot.configuration().cassandraYaml()).isEmpty();
        assertThat(snapshot.configuration().extraJvmOpts()).isEmpty();
        assertThat(snapshot.hash()).startsWith("sha256:");
    }

    @Test
    void testLoadConfigurationReturnsCachedWhenUnmodified()
    {
        Path yamlPath = Paths.get("src/test/resources/configmanagement/cassandra_latest.yaml");

        ConfigurationOverlaySnapshot first = ConfigUtils.loadConfiguration(yamlPath, null);
        ConfigurationOverlaySnapshot second = ConfigUtils.loadConfiguration(yamlPath, first);

        assertThat(second).isSameAs(first);
    }
}
