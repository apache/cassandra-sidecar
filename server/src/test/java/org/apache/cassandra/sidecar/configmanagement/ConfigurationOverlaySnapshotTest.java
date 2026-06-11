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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ConfigurationOverlaySnapshot}
 */
class ConfigurationOverlaySnapshotTest
{
    @Test
    void testHashIsDeterministic()
    {
        JsonObject yaml1 = new JsonObject()
                           .put("concurrent_reads", 32)
                           .put("memtable_flush_writers", 4);

        JsonObject yaml2 = new JsonObject()
                           .put("concurrent_reads", 32)
                           .put("memtable_flush_writers", 4);

        CassandraConfigurationOverlay overlay1 = new CassandraConfigurationOverlay(yaml1, Map.of("-Xmx", "4G"));
        CassandraConfigurationOverlay overlay2 = new CassandraConfigurationOverlay(yaml2, Map.of("-Xmx", "4G"));

        ConfigurationOverlaySnapshot snapshot1 = new ConfigurationOverlaySnapshot(Instant.now(), overlay1);
        ConfigurationOverlaySnapshot snapshot2 = new ConfigurationOverlaySnapshot(Instant.now(), overlay2);

        assertThat(snapshot1.hash()).isEqualTo(snapshot2.hash());
    }

    @Test
    void testHashChangesWithDifferentContent()
    {
        JsonObject yaml1 = new JsonObject().put("concurrent_reads", 32);
        JsonObject yaml2 = new JsonObject().put("concurrent_reads", 64);

        CassandraConfigurationOverlay overlay1 = new CassandraConfigurationOverlay(yaml1, null);
        CassandraConfigurationOverlay overlay2 = new CassandraConfigurationOverlay(yaml2, null);

        ConfigurationOverlaySnapshot snapshot1 = new ConfigurationOverlaySnapshot(Instant.now(), overlay1);
        ConfigurationOverlaySnapshot snapshot2 = new ConfigurationOverlaySnapshot(Instant.now(), overlay2);

        assertThat(snapshot1.hash()).isNotEqualTo(snapshot2.hash());
    }

    @Test
    void testHashIsCached()
    {
        JsonObject yaml = new JsonObject().put("commitlog_sync", "periodic");

        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(Instant.now(), overlay);

        String firstCall = snapshot.hash();
        String secondCall = snapshot.hash();

        // Same String instance (referential equality) proves caching
        assertThat(firstCall).isSameAs(secondCall);
    }

    @Test
    void testHashHasSha256Prefix()
    {
        JsonObject yaml = new JsonObject().put("native_transport_port", 9042);

        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);
        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(Instant.now(), overlay);

        assertThat(snapshot.hash()).startsWith("sha256:");
        // SHA-256 produces 64 hex chars, plus "sha256:" prefix = 71 chars
        assertThat(snapshot.hash()).hasSize(71);
    }

    @Test
    void testOverlayMergesYamlKeys()
    {
        JsonObject baseYaml = new JsonObject()
                              .put("cluster_name", "test")
                              .put("concurrent_reads", 32);
        CassandraConfigurationOverlay baseOverlay = new CassandraConfigurationOverlay(baseYaml, null);
        ConfigurationOverlaySnapshot base = new ConfigurationOverlaySnapshot(Instant.parse("2026-01-01T00:00:00Z"),
                                                                             baseOverlay);

        JsonObject otherYaml = new JsonObject()
                               .put("concurrent_reads", 64)
                               .put("storage_compatibility_mode", "CASSANDRA_5");
        CassandraConfigurationOverlay otherOverlay = new CassandraConfigurationOverlay(otherYaml, null);
        ConfigurationOverlaySnapshot other = new ConfigurationOverlaySnapshot(Instant.parse("2026-02-01T00:00:00Z"),
                                                                              otherOverlay);

        ConfigurationOverlaySnapshot result = base.overlay(other, 1);

        assertThat(result.configuration().cassandraYaml().getString("cluster_name")).isEqualTo("test");
        assertThat(result.configuration().cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
        assertThat(result.configuration().cassandraYaml().getString("storage_compatibility_mode")).isEqualTo("CASSANDRA_5");
    }

    @Test
    void testOverlayMergesJvmOpts()
    {
        Map<String, String> baseOpts = new LinkedHashMap<>();
        baseOpts.put("-Xmx", "4g");
        baseOpts.put("-Dcassandra.jmx.local.port", "7199");
        CassandraConfigurationOverlay baseOverlay = new CassandraConfigurationOverlay(null, baseOpts);
        ConfigurationOverlaySnapshot base = new ConfigurationOverlaySnapshot(Instant.now(), baseOverlay);

        Map<String, String> otherOpts = new LinkedHashMap<>();
        otherOpts.put("-Xmx", "8g");
        otherOpts.put("-Dcassandra.ring_delay_ms", "60000");
        CassandraConfigurationOverlay otherOverlay = new CassandraConfigurationOverlay(null, otherOpts);
        ConfigurationOverlaySnapshot other = new ConfigurationOverlaySnapshot(Instant.now(), otherOverlay);

        ConfigurationOverlaySnapshot result = base.overlay(other, 1);

        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "8g");
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Dcassandra.jmx.local.port", "7199");
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Dcassandra.ring_delay_ms", "60000");
    }

    @Test
    void testOverlayPrefersBaseOnConflictingBooleanJvmOpts()
    {
        Map<String, String> baseOpts = new LinkedHashMap<>();
        baseOpts.put("-XX:+UseG1GC", "");
        baseOpts.put("-Xmx", "4g");
        CassandraConfigurationOverlay baseOverlay = new CassandraConfigurationOverlay(null, baseOpts);
        ConfigurationOverlaySnapshot base = new ConfigurationOverlaySnapshot(Instant.now(), baseOverlay);

        Map<String, String> otherOpts = new LinkedHashMap<>();
        otherOpts.put("-XX:-UseG1GC", "");
        otherOpts.put("-Xmx", "8g");
        CassandraConfigurationOverlay otherOverlay = new CassandraConfigurationOverlay(null, otherOpts);
        ConfigurationOverlaySnapshot other = new ConfigurationOverlaySnapshot(Instant.now(), otherOverlay);

        ConfigurationOverlaySnapshot result = base.overlay(other, 1);

        assertThat(result.configuration().extraJvmOpts()).containsEntry("-XX:+UseG1GC", "");
        assertThat(result.configuration().extraJvmOpts()).doesNotContainKey("-XX:-UseG1GC");
        assertThat(result.configuration().extraJvmOpts()).containsEntry("-Xmx", "8g");
    }

    @Test
    void testOverlayUsesMaxLastModified()
    {
        Instant older = Instant.parse("2026-01-01T00:00:00Z");
        Instant newer = Instant.parse("2026-06-01T00:00:00Z");

        CassandraConfigurationOverlay emptyOverlay = new CassandraConfigurationOverlay(null, null);

        ConfigurationOverlaySnapshot baseOlder = new ConfigurationOverlaySnapshot(older, emptyOverlay);
        ConfigurationOverlaySnapshot otherNewer = new ConfigurationOverlaySnapshot(newer, emptyOverlay);
        assertThat(baseOlder.overlay(otherNewer, 1).lastModified()).isEqualTo(newer);

        ConfigurationOverlaySnapshot baseNewer = new ConfigurationOverlaySnapshot(newer, emptyOverlay);
        ConfigurationOverlaySnapshot otherOlder = new ConfigurationOverlaySnapshot(older, emptyOverlay);
        assertThat(baseNewer.overlay(otherOlder, 1).lastModified()).isEqualTo(newer);
    }

    @Test
    void testOverlayDeepMergesNestedObjects()
    {
        JsonObject baseConfigs = new JsonObject()
                                 .put("skiplist", new JsonObject().put("class_name", "SkipListMemtable"))
                                 .put("default", new JsonObject().put("inherits", "skiplist"));
        JsonObject baseYaml = new JsonObject()
                              .put("memtable", new JsonObject().put("configurations", baseConfigs));
        CassandraConfigurationOverlay baseOverlay = new CassandraConfigurationOverlay(baseYaml, null);
        ConfigurationOverlaySnapshot base = new ConfigurationOverlaySnapshot(Instant.now(), baseOverlay);

        JsonObject otherConfigs = new JsonObject()
                                  .put("trie", new JsonObject().put("class_name", "TrieMemtable"))
                                  .put("default", new JsonObject().put("inherits", "trie"));
        JsonObject otherYaml = new JsonObject()
                               .put("memtable", new JsonObject().put("configurations", otherConfigs));
        CassandraConfigurationOverlay otherOverlay = new CassandraConfigurationOverlay(otherYaml, null);
        ConfigurationOverlaySnapshot other = new ConfigurationOverlaySnapshot(Instant.now(), otherOverlay);

        ConfigurationOverlaySnapshot result = base.overlay(other, 1);

        JsonObject resultConfigs = result.configuration().cassandraYaml()
                                         .getJsonObject("memtable")
                                         .getJsonObject("configurations");
        assertThat(resultConfigs.getJsonObject("skiplist").getString("class_name")).isEqualTo("SkipListMemtable");
        assertThat(resultConfigs.getJsonObject("trie").getString("class_name")).isEqualTo("TrieMemtable");
        assertThat(resultConfigs.getJsonObject("default").getString("inherits")).isEqualTo("trie");
    }

    @Test
    void testEmptySnapshot()
    {
        ConfigurationOverlaySnapshot snapshot = ConfigurationOverlaySnapshot.emptySnapshot();

        assertThat(snapshot.lastModified()).isEqualTo(Instant.EPOCH);
        assertThat(snapshot.configuration().cassandraYaml()).isEmpty();
        assertThat(snapshot.configuration().extraJvmOpts()).isEmpty();
        assertThat(snapshot.hash()).startsWith("sha256:");
        assertThat(snapshot.hash()).hasSize(71);
    }

    @Test
    void testToString()
    {
        JsonObject yaml = new JsonObject().put("concurrent_reads", 32);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, Map.of("-Xmx", "4G"));
        Instant timestamp = Instant.parse("2026-02-20T14:32:18Z");

        ConfigurationOverlaySnapshot snapshot = new ConfigurationOverlaySnapshot(timestamp, overlay);

        String expected = String.join("\n",
            "{",
            "  \"hash\" : \"" + snapshot.hash() + "\",",
            "  \"lastModified\" : \"2026-02-20T14:32:18Z\",",
            "  \"configuration\" : {",
            "    \"cassandraYaml\" : {",
            "      \"concurrent_reads\" : 32",
            "    },",
            "    \"extraJvmOpts\" : {",
            "      \"-Xmx\" : \"4G\"",
            "    }",
            "  }",
            "}");
        assertThat(snapshot.toString()).isEqualTo(expected);
    }
}
