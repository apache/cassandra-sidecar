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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.ADD;
import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.REMOVE;
import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.REPLACE;
import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ConfigurationPatchApplier}
 */
class ConfigurationPatchApplierTest
{
    private static final CassandraConfigurationOverlay EMPTY_OVERLAY = new CassandraConfigurationOverlay(null, null);

    private CassandraConfigurationOverlay effectiveConfig;

    @BeforeEach
    void setUp()
    {
        JsonObject effectiveYaml = new JsonObject()
                .put("cluster_name", "TestCluster")
                .put("concurrent_reads", 32)
                .put("memtable", new JsonObject()
                        .put("configurations", new JsonObject()
                                .put("trie", new JsonObject()
                                        .put("class_name", "TrieMemtable")
                                        .put("max_shard_count", 4))
                                .put("skiplist", new JsonObject()
                                        .put("class_name", "SkipListMemtable"))));

        Map<String, String> effectiveOpts = new LinkedHashMap<>();
        effectiveOpts.put("-Xmx", "4g");
        effectiveOpts.put("-Dcassandra.ring_delay_ms", "60000");

        effectiveConfig = new CassandraConfigurationOverlay(effectiveYaml, effectiveOpts);
    }

    // --- Top-level cassandraYaml operations ---

    @Test
    void testAddTopLevelKey()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/new_key", "new_value"));

        assertThat(newOverlay.cassandraYaml().getString("new_key")).isEqualTo("new_value");
    }

    @Test
    void testAddOverwritesExistingOverlayValue()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(
                new JsonObject().put("concurrent_reads", 64), null);

        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, overlay,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/concurrent_reads", 256));

        assertThat(newOverlay.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(256);
    }

    @Test
    void testRemoveTopLevelKey()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(
                new JsonObject().put("concurrent_reads", 64), null);

        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/cassandraYaml/concurrent_reads", null));

        assertThat(newOverlay.cassandraYaml().containsKey("concurrent_reads")).isFalse();
    }

    @Test
    void testRemoveTemplateOnlyKeyFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REMOVE, "/configuration/cassandraYaml/cluster_name", null)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in overlay");
    }

    @Test
    void testReplaceExistingKey()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/concurrent_reads", 128));

        assertThat(newOverlay.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
    }

    @Test
    void testReplaceAbsentKeyFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/nonexistent", 42)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in effective config");
    }

    @Test
    void testTestMatchingValue()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 32));

        assertThat(newOverlay.cassandraYaml()).isEmpty();
    }

    @Test
    void testTestMismatchFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 999)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed: expected 999 but found 32");
    }

    @Test
    void testTestAbsentPathFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/nonexistent", "x")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("path does not exist in effective config");
    }

    // --- Nested cassandraYaml operations (copy-siblings) ---

    @Test
    void testAddNestedKeyCopiesSiblings()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD,
                        "/configuration/cassandraYaml/memtable/configurations/trie/compression", "lz4"));

        JsonObject memtable = newOverlay.cassandraYaml().getJsonObject("memtable");
        JsonObject trie = memtable.getJsonObject("configurations").getJsonObject("trie");
        // New key added
        assertThat(trie.getString("compression")).isEqualTo("lz4");
        // Siblings copied from effective config
        assertThat(trie.getString("class_name")).isEqualTo("TrieMemtable");
        assertThat(trie.getInteger("max_shard_count")).isEqualTo(4);

        JsonObject skiplist = memtable.getJsonObject("configurations").getJsonObject("skiplist");
        assertThat(skiplist.getString("class_name")).isEqualTo("SkipListMemtable");
    }

    @Test
    void testReplaceNestedKeyCopiesSiblings()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "ShardedMemtable"));

        JsonObject trie = newOverlay.cassandraYaml().getJsonObject("memtable")
                                .getJsonObject("configurations").getJsonObject("trie");
        assertThat(trie.getString("class_name")).isEqualTo("ShardedMemtable");
        assertThat(trie.getInteger("max_shard_count")).isEqualTo(4);
    }

    @Test
    void testMultipleNestedOpsOnSameTopLevelKey()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "ShardedMemtable"),
                new ConfigurationPatchOperation(REPLACE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/max_shard_count", 8));

        JsonObject trie = newOverlay.cassandraYaml().getJsonObject("memtable")
                                .getJsonObject("configurations").getJsonObject("trie");
        assertThat(trie.getString("class_name")).isEqualTo("ShardedMemtable");
        assertThat(trie.getInteger("max_shard_count")).isEqualTo(8);

        JsonObject skiplist = newOverlay.cassandraYaml().getJsonObject("memtable")
                                    .getJsonObject("configurations").getJsonObject("skiplist");
        assertThat(skiplist.getString("class_name")).isEqualTo("SkipListMemtable");
    }

    @Test
    void testRemoveNestedKeyFromOverlay()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(new JsonObject()
                .put("memtable", new JsonObject()
                        .put("configurations", new JsonObject()
                                .put("trie", new JsonObject()
                                        .put("class_name", "ShardedMemtable")
                                        .put("max_shard_count", 8))
                                .put("skiplist", new JsonObject()
                                        .put("class_name", "SkipListMemtable")))), null);

        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, overlay,
                new ConfigurationPatchOperation(REMOVE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/max_shard_count", null));

        JsonObject trie = newOverlay.cassandraYaml().getJsonObject("memtable")
                                .getJsonObject("configurations").getJsonObject("trie");
        assertThat(trie.containsKey("max_shard_count")).isFalse();
        assertThat(trie.getString("class_name")).isEqualTo("ShardedMemtable");
    }

    @Test
    void testRemoveNestedKeyNotInOverlayFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REMOVE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", null)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in overlay");
    }

    @Test
    void testAddNestedKeyParentAbsentFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD,
                        "/configuration/cassandraYaml/nonexistent_parent/child/leaf", "value")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("parent path does not exist");
    }

    @Test
    void testTestNestedValue()
    {
        applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "TrieMemtable"));
    }

    @Test
    void testTestNestedValueMismatchFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "WrongValue")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    // --- Array-valued top-level keys ---

    @Test
    void testAddTopLevelArrayValue()
    {
        List<String> directories = List.of("/data1", "/data2");
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/data_file_directories", directories));

        assertThat(newOverlay.cassandraYaml().getJsonArray("data_file_directories"))
                .containsExactly("/data1", "/data2");
    }

    @Test
    void testReplaceTopLevelArrayValue()
    {
        JsonObject effectiveYaml = effectiveConfig.cassandraYaml().copy()
                .put("data_file_directories", List.of("/old_data"));
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveYaml, effectiveConfig.extraJvmOpts());

        CassandraConfigurationOverlay newOverlay = applyOps(effective, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/data_file_directories",
                        List.of("/new_data1", "/new_data2")));

        assertThat(newOverlay.cassandraYaml().getJsonArray("data_file_directories"))
                .containsExactly("/new_data1", "/new_data2");
    }

    @Test
    void testNestedPathIntoArrayValueFails()
    {
        JsonObject effectiveYaml = effectiveConfig.cassandraYaml().copy()
                .put("seed_provider", List.of(Map.of("class_name", "SimpleSeedProvider")));
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveYaml, effectiveConfig.extraJvmOpts());

        assertThatThrownBy(() -> applyOps(effective, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE,
                        "/configuration/cassandraYaml/seed_provider/0/class_name", "OtherProvider")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("intermediate value is not an object");
    }

    // --- extraJvmOpts operations ---

    @Test
    void testAddJvmOpt()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xms", "2g"));

        assertThat(newOverlay.extraJvmOpts()).containsEntry("-Xms", "2g");
    }

    @Test
    void testRemoveJvmOptFromOverlay()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null,
                new LinkedHashMap<>(Map.of("-Xmx", "8g")));

        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/extraJvmOpts/-Xmx", null));

        assertThat(newOverlay.extraJvmOpts()).doesNotContainKey("-Xmx");
    }

    @Test
    void testReplaceJvmOptAbsentFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/extraJvmOpts/-Xms", "2g")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in effective extraJvmOpts");
    }

    @Test
    void testTestJvmOptValue()
    {
        applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/extraJvmOpts/-Xmx", "4g"));
    }

    @Test
    void testTestJvmOptMismatchFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/extraJvmOpts/-Xmx", "16g")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    @Test
    void testAddInvalidJvmOptKeyFails()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/invalidKey", "value")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Invalid JVM option key 'invalidKey'");
    }

    @Test
    void testAddConflictingBooleanJvmOptFails()
    {
        Map<String, String> effectiveOpts = new LinkedHashMap<>();
        effectiveOpts.put("-Xmx", "4g");
        effectiveOpts.put("-XX:+UseG1GC", "");
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveConfig.cassandraYaml(), effectiveOpts);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null,
                new LinkedHashMap<>(Map.of("-XX:+UseG1GC", "")));

        assertThatThrownBy(() -> applyOps(effective, overlay,
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:-UseG1GC", "")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Conflicting boolean JVM option");
    }

    @Test
    void testAddConflictingBooleanJvmOptAgainstBaseFails()
    {
        // Conflicting option exists only in the effective config (i.e. base template), not the overlay.
        // The overlay-only check would miss this; the effective-config check must catch it.
        Map<String, String> effectiveOpts = new LinkedHashMap<>();
        effectiveOpts.put("-XX:+UseG1GC", "");
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveConfig.cassandraYaml(), effectiveOpts);

        assertThatThrownBy(() -> applyOps(effective, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:-UseG1GC", "")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Conflicting boolean JVM option");
    }

    @Test
    void testReplaceBooleanJvmOptByRemovingThenAdding()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null,
                new LinkedHashMap<>(Map.of("-XX:+UseG1GC", "")));

        Map<String, String> effectiveOpts = new LinkedHashMap<>();
        effectiveOpts.put("-Xmx", "4g");
        effectiveOpts.put("-XX:+UseG1GC", "");
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveConfig.cassandraYaml(), effectiveOpts);

        CassandraConfigurationOverlay newOverlay = applyOps(effective, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/extraJvmOpts/-XX:+UseG1GC", null),
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:-UseG1GC", ""));

        assertThat(newOverlay.extraJvmOpts()).doesNotContainKey("-XX:+UseG1GC");
        assertThat(newOverlay.extraJvmOpts()).containsEntry("-XX:-UseG1GC", "");
    }

    // --- Atomicity ---

    @Test
    void testTestFailurePreventsAllMutations()
    {
        assertThatThrownBy(() -> applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/new_key", "value"),
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 999)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    @Test
    void testMultipleOpsAppliedAtomically()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(effectiveConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/concurrent_writes", 64),
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xms", "2g"),
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 32));

        assertThat(newOverlay.cassandraYaml().getInteger("concurrent_writes")).isEqualTo(64);
        assertThat(newOverlay.extraJvmOpts()).containsEntry("-Xms", "2g");
    }

    private static CassandraConfigurationOverlay applyOps(CassandraConfigurationOverlay effective,
                                                          CassandraConfigurationOverlay overlay,
                                                          ConfigurationPatchOperation... ops)
    {
        List<ConfigurationPatchValidator.ParsedPatchOperation> parsed =
                ConfigurationPatchValidator.validate(List.of(ops));
        return ConfigurationPatchApplier.apply(parsed, effective, overlay);
    }
}
