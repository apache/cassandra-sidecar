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

import io.vertx.core.json.JsonArray;
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

    private CassandraConfigurationOverlay baseConfig;

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

        baseConfig = new CassandraConfigurationOverlay(effectiveYaml, effectiveOpts);
    }

    // --- Top-level cassandraYaml operations ---

    @Test
    void testAddTopLevelKey()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/new_key", "new_value"));

        assertThat(newOverlay.cassandraYaml().getString("new_key")).isEqualTo("new_value");
    }

    @Test
    void testAddOverwritesExistingOverlayValue()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(
                new JsonObject().put("concurrent_reads", 64), null);

        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, overlay,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/concurrent_reads", 256));

        assertThat(newOverlay.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(256);
    }

    @Test
    void testRemoveTopLevelKey()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(
                new JsonObject().put("concurrent_reads", 64), null);

        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/cassandraYaml/concurrent_reads", null));

        assertThat(newOverlay.cassandraYaml().containsKey("concurrent_reads")).isFalse();
    }

    @Test
    void testRemoveTemplateOnlyKeyFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REMOVE, "/configuration/cassandraYaml/cluster_name", null)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in overlay");
    }

    @Test
    void testReplaceExistingKey()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/concurrent_reads", 128));

        assertThat(newOverlay.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(128);
    }

    @Test
    void testReplaceAbsentKeyFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/nonexistent", 42)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in effective config");
    }

    @Test
    void testTestMatchingValue()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 32));

        assertThat(newOverlay.cassandraYaml()).isEmpty();
    }

    @Test
    void testTestMismatchFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 999)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed: expected 999 but found 32");
    }

    @Test
    void testTestAbsentPathFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/nonexistent", "x")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("path does not exist in effective config");
    }

    // --- Nested cassandraYaml operations (copy-siblings) ---

    @Test
    void testAddNestedKeyCopiesSiblings()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
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
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "ShardedMemtable"));

        JsonObject trie = newOverlay.cassandraYaml().getJsonObject("memtable")
                                .getJsonObject("configurations").getJsonObject("trie");
        assertThat(trie.getString("class_name")).isEqualTo("ShardedMemtable");
        assertThat(trie.getInteger("max_shard_count")).isEqualTo(4);
    }

    @Test
    void testNestedEditPinsSiblingsAgainstBaseDrift()
    {
        // Editing one nested leaf copies the whole top-level block's leaves into the overlay, pinning
        // the siblings that existed at edit time against later base-template drift. Keys added to the
        // base block afterwards are not pinned - the deep merge still surfaces them.
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "ShardedMemtable"));

        // Base template drifts after the overlay was written: an existing sibling changes value and a
        // brand-new sibling key appears.
        JsonObject driftedBaseYaml = baseConfig.cassandraYaml().copy();
        JsonObject driftedTrie = driftedBaseYaml.getJsonObject("memtable")
                                                .getJsonObject("configurations").getJsonObject("trie");
        driftedTrie.put("max_shard_count", 99);       // existing sibling drifts
        driftedTrie.put("new_base_field", "fromBase"); // new key added to base

        JsonObject effective = ConfigUtils.mergeConfigurations(driftedBaseYaml, newOverlay.cassandraYaml());
        JsonObject trie = effective.getJsonObject("memtable").getJsonObject("configurations").getJsonObject("trie");

        assertThat(trie.getString("class_name")).isEqualTo("ShardedMemtable"); // the edit
        assertThat(trie.getInteger("max_shard_count")).isEqualTo(4);           // pinned, not the drifted 99
        assertThat(trie.getString("new_base_field")).isEqualTo("fromBase");    // new base key still surfaces
    }

    @Test
    void testRemoveOverlaidLeafRevertsToCurrentBaseValue()
    {
        // trie/max_shard_count is overlaid at 8; removing it from the overlay lets the base value surface
        // again - and specifically the base value at merge time, not the value the overlay held.
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(new JsonObject()
                .put("memtable", new JsonObject()
                        .put("configurations", new JsonObject()
                                .put("trie", new JsonObject().put("max_shard_count", 8)))), null);

        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, overlay,
                new ConfigurationPatchOperation(REMOVE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/max_shard_count", null));

        // Base drifts to 16 after removal; the effective value tracks the current base, not the removed 8.
        JsonObject driftedBaseYaml = baseConfig.cassandraYaml().copy();
        driftedBaseYaml.getJsonObject("memtable").getJsonObject("configurations")
                       .getJsonObject("trie").put("max_shard_count", 16);

        JsonObject effective = ConfigUtils.mergeConfigurations(driftedBaseYaml, newOverlay.cassandraYaml());
        JsonObject trie = effective.getJsonObject("memtable").getJsonObject("configurations").getJsonObject("trie");

        assertThat(trie.getInteger("max_shard_count")).isEqualTo(16); // reverted to current base value
    }

    @Test
    void testMultipleNestedOpsOnSameTopLevelKey()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
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

        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, overlay,
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
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REMOVE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", null)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in overlay");
    }

    @Test
    void testAddNestedKeyParentAbsentFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD,
                        "/configuration/cassandraYaml/nonexistent_parent/child/leaf", "value")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("parent path does not exist");
    }

    @Test
    void testTestNestedValue()
    {
        applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "TrieMemtable"));
    }

    @Test
    void testTestObjectValue()
    {
        // TEST against an object-valued path. The request handler reads the value via JsonObject.getValue,
        // so the expected value is a JsonObject - the same type resolveValue returns.
        applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST,
                        "/configuration/cassandraYaml/memtable/configurations/trie",
                        new JsonObject().put("class_name", "TrieMemtable").put("max_shard_count", 4)));
    }

    @Test
    void testTestObjectValueMismatchFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST,
                        "/configuration/cassandraYaml/memtable/configurations/trie",
                        new JsonObject().put("class_name", "TrieMemtable").put("max_shard_count", 99))))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    @Test
    void testTestArrayValue()
    {
        // TEST against an array-valued path. The expected value is a JsonArray, matching what
        // resolveValue returns for array values.
        JsonObject effectiveYaml = baseConfig.cassandraYaml().copy()
                .put("data_file_directories", new JsonArray().add("/data1").add("/data2"));
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveYaml, baseConfig.extraJvmOpts());

        applyOps(effective, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST,
                        "/configuration/cassandraYaml/data_file_directories",
                        new JsonArray().add("/data1").add("/data2")));
    }

    @Test
    void testTestArrayValueMismatchFails()
    {
        JsonObject effectiveYaml = baseConfig.cassandraYaml().copy()
                .put("data_file_directories", new JsonArray().add("/data1").add("/data2"));
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveYaml, baseConfig.extraJvmOpts());

        assertThatThrownBy(() -> applyOps(effective, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST,
                        "/configuration/cassandraYaml/data_file_directories",
                        new JsonArray().add("/data1").add("/other"))))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    @Test
    void testTestNestedValueMismatchFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
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
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/data_file_directories", directories));

        assertThat(newOverlay.cassandraYaml().getJsonArray("data_file_directories"))
                .containsExactly("/data1", "/data2");
    }

    @Test
    void testReplaceTopLevelArrayValue()
    {
        JsonObject effectiveYaml = baseConfig.cassandraYaml().copy()
                .put("data_file_directories", List.of("/old_data"));
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveYaml, baseConfig.extraJvmOpts());

        CassandraConfigurationOverlay newOverlay = applyOps(effective, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/data_file_directories",
                        List.of("/new_data1", "/new_data2")));

        assertThat(newOverlay.cassandraYaml().getJsonArray("data_file_directories"))
                .containsExactly("/new_data1", "/new_data2");
    }

    @Test
    void testNestedPathIntoArrayValueFails()
    {
        JsonObject effectiveYaml = baseConfig.cassandraYaml().copy()
                .put("seed_provider", List.of(Map.of("class_name", "SimpleSeedProvider")));
        CassandraConfigurationOverlay effective = new CassandraConfigurationOverlay(
                effectiveYaml, baseConfig.extraJvmOpts());

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
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xms", "2g"));

        assertThat(newOverlay.extraJvmOpts()).containsEntry("-Xms", "2g");
    }

    @Test
    void testRemoveJvmOptFromOverlay()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null,
                new LinkedHashMap<>(Map.of("-Xmx", "8g")));

        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/extraJvmOpts/-Xmx", null));

        assertThat(newOverlay.extraJvmOpts()).doesNotContainKey("-Xmx");
    }

    @Test
    void testReplaceJvmOptAbsentFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/extraJvmOpts/-Xms", "2g")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in effective extraJvmOpts");
    }

    @Test
    void testTestJvmOptValue()
    {
        applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/extraJvmOpts/-Xmx", "4g"));
    }

    @Test
    void testTestJvmOptMismatchFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(TEST, "/configuration/extraJvmOpts/-Xmx", "16g")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    @Test
    void testAddInvalidJvmOptKeyFails()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
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
                baseConfig.cassandraYaml(), effectiveOpts);
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
                baseConfig.cassandraYaml(), effectiveOpts);

        assertThatThrownBy(() -> applyOps(effective, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:-UseG1GC", "")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Conflicting boolean JVM option");
    }

    @Test
    void testReplaceBooleanJvmOptByRemovingThenAdding()
    {
        // -XX:+UseG1GC lives only in the overlay (not the base template), so removing it from the
        // overlay clears it from the effective config, and the subsequent add of -XX:-UseG1GC applies
        // against the now-conflict-free effective configuration.
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null,
                new LinkedHashMap<>(Map.of("-XX:+UseG1GC", "")));

        Map<String, String> baseOpts = new LinkedHashMap<>();
        baseOpts.put("-Xmx", "4g");
        CassandraConfigurationOverlay base = new CassandraConfigurationOverlay(
                baseConfig.cassandraYaml(), baseOpts);

        CassandraConfigurationOverlay newOverlay = applyOps(base, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/extraJvmOpts/-XX:+UseG1GC", null),
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:-UseG1GC", ""));

        assertThat(newOverlay.extraJvmOpts()).doesNotContainKey("-XX:+UseG1GC");
        assertThat(newOverlay.extraJvmOpts()).containsEntry("-XX:-UseG1GC", "");
    }

    // --- Atomicity ---

    @Test
    void testTestFailurePreventsAllMutations()
    {
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/new_key", "value"),
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 999)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    @Test
    void testMultipleOpsAppliedAtomically()
    {
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/concurrent_writes", 64),
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xms", "2g"),
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 32));

        assertThat(newOverlay.cassandraYaml().getInteger("concurrent_writes")).isEqualTo(64);
        assertThat(newOverlay.extraJvmOpts()).containsEntry("-Xms", "2g");
    }

    // --- Sequential (RFC 6902 section 5) operation semantics ---

    @Test
    void testReplaceThenTestSeesUpdatedValue()
    {
        // "Change X, then assert X now holds the new value" - the test op must observe the prior replace.
        applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/concurrent_reads", 64),
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 64));
    }

    @Test
    void testTestAgainstStaleValueFails()
    {
        // After the replace, the value is 64, so a test for the original 32 must fail.
        assertThatThrownBy(() -> applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/concurrent_reads", 64),
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/concurrent_reads", 32)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Test failed");
    }

    @Test
    void testAddObjectThenAddChild()
    {
        // Build-up: create a new nested object, then add a child into it in the same patch.
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD,
                        "/configuration/cassandraYaml/memtable/configurations/custom", new JsonObject()),
                new ConfigurationPatchOperation(ADD,
                        "/configuration/cassandraYaml/memtable/configurations/custom/class_name", "CustomMemtable"));

        JsonObject custom = newOverlay.cassandraYaml().getJsonObject("memtable")
                                    .getJsonObject("configurations").getJsonObject("custom");
        assertThat(custom.getString("class_name")).isEqualTo("CustomMemtable");
    }

    @Test
    void testAddTopLevelThenReplaceChild()
    {
        // Create a new top-level section, then replace a field within it.
        CassandraConfigurationOverlay newOverlay = applyOps(baseConfig, EMPTY_OVERLAY,
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/new_section",
                        new JsonObject().put("a", 1)),
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/new_section/a", 2));

        assertThat(newOverlay.cassandraYaml().getJsonObject("new_section").getInteger("a")).isEqualTo(2);
    }

    @Test
    void testRemoveThenRemoveChildFails()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(new JsonObject()
                .put("memtable", new JsonObject()
                        .put("configurations", new JsonObject()
                                .put("trie", new JsonObject()
                                        .put("class_name", "ShardedMemtable")
                                        .put("max_shard_count", 8)))), null);

        // Removing 'trie' first makes the subsequent remove of trie/class_name reference a path that no
        // longer exists in the overlay - it must fail rather than silently no-op.
        assertThatThrownBy(() -> applyOps(baseConfig, overlay,
                new ConfigurationPatchOperation(REMOVE,
                        "/configuration/cassandraYaml/memtable/configurations/trie", null),
                new ConfigurationPatchOperation(REMOVE,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", null)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in overlay");
    }

    @Test
    void testRemoveOverlayOnlyTopLevelThenWriteChildFails()
    {
        // 'custom_section' exists only in the overlay. Removing it clears it from the effective config,
        // so replacing a child of it afterwards must fail instead of resurrecting it.
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(new JsonObject()
                .put("custom_section", new JsonObject().put("a", 1)), null);

        assertThatThrownBy(() -> applyOps(baseConfig, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/cassandraYaml/custom_section", null),
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/custom_section/a", 2)))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("does not exist in effective config");
    }

    @Test
    void testRemoveOverlayBooleanOptThenAddOppositeStillConflictsAgainstBase()
    {
        // +UseG1GC exists in BOTH the base and the overlay. Removing the overlay copy leaves the base's
        // +UseG1GC in the effective config, so adding -UseG1GC must still be rejected as a conflict.
        Map<String, String> baseOpts = new LinkedHashMap<>();
        baseOpts.put("-XX:+UseG1GC", "");
        CassandraConfigurationOverlay base = new CassandraConfigurationOverlay(
                baseConfig.cassandraYaml(), baseOpts);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null,
                new LinkedHashMap<>(Map.of("-XX:+UseG1GC", "")));

        assertThatThrownBy(() -> applyOps(base, overlay,
                new ConfigurationPatchOperation(REMOVE, "/configuration/extraJvmOpts/-XX:+UseG1GC", null),
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:-UseG1GC", "")))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Conflicting boolean JVM option");
    }

    private static CassandraConfigurationOverlay applyOps(CassandraConfigurationOverlay base,
                                                          CassandraConfigurationOverlay overlay,
                                                          ConfigurationPatchOperation... ops)
    {
        List<ConfigurationPatchValidator.ParsedPatchOperation> parsed =
                ConfigurationPatchValidator.validate(List.of(ops));
        return ConfigurationPatchApplier.apply(parsed, base, overlay);
    }
}
