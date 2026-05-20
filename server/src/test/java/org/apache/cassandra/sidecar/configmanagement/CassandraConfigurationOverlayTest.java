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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link CassandraConfigurationOverlay}
 */
class CassandraConfigurationOverlayTest
{
    @Test
    void testUpdatedAppliesCassandraYamlChanges()
    {
        JsonObject yaml = new JsonObject()
                          .put("concurrent_reads", 32)
                          .put("memtable_flush_writers", 4)
                          .put("storage_compatibility_mode", "CASSANDRA_4");
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);

        Map<String, Object> updates = new LinkedHashMap<>();
        updates.put("concurrent_reads", 64);
        updates.put("storage_compatibility_mode", null);

        CassandraConfigurationOverlay updated = overlay.updated(updates, null);

        // concurrent_reads updated
        assertThat(updated.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
        // storage_compatibility_mode removed
        assertThat(updated.cassandraYaml().containsKey("storage_compatibility_mode")).isFalse();
        // memtable_flush_writers preserved
        assertThat(updated.cassandraYaml().getInteger("memtable_flush_writers")).isEqualTo(4);
    }

    @Test
    void testUpdatedUpsertsAndRemovesJvmOpts()
    {
        Map<String, String> jvmOpts = new LinkedHashMap<>();
        jvmOpts.put("-Dcassandra.jmx.local.port", "7199");
        jvmOpts.put("-Xmx", "4G");
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null, jvmOpts);

        Map<String, String> updates = new LinkedHashMap<>();
        updates.put("-Xmx", "8G");

        CassandraConfigurationOverlay updated = overlay.updated(null, updates);

        Map<String, String> expected = new LinkedHashMap<>();
        expected.put("-Dcassandra.jmx.local.port", "7199");
        expected.put("-Xmx", "8G");
        assertThat(updated.extraJvmOpts()).containsExactlyEntriesOf(expected);
    }

    @Test
    void testUpdatedRemovesJvmOptWithNullValue()
    {
        Map<String, String> jvmOpts = new LinkedHashMap<>();
        jvmOpts.put("-Dcassandra.jmx.local.port", "7199");
        jvmOpts.put("-Xmx", "4G");
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null, jvmOpts);

        Map<String, String> updates = new LinkedHashMap<>();
        updates.put("-Xmx", null);

        CassandraConfigurationOverlay updated = overlay.updated(null, updates);

        assertThat(updated.extraJvmOpts()).containsExactlyEntriesOf(Map.of(
            "-Dcassandra.jmx.local.port", "7199"));
    }

    @Test
    void testUpdatedRejectsConflictingBooleanOpts()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null, Map.of("-XX:+UseG1GC", ""));

        Map<String, String> updates = new LinkedHashMap<>();
        updates.put("-XX:-UseG1GC", "");

        assertThatThrownBy(() -> overlay.updated(null, updates))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("-XX:+UseG1GC")
            .hasMessageContaining("-XX:-UseG1GC");
    }

    @Test
    void testUpdatedAllowsReplacingBooleanOpt()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null, Map.of("-XX:+UseG1GC", ""));

        Map<String, String> updates = new LinkedHashMap<>();
        updates.put("-XX:+UseG1GC", null);
        updates.put("-XX:-UseG1GC", "");

        CassandraConfigurationOverlay updated = overlay.updated(null, updates);

        assertThat(updated.extraJvmOpts()).containsExactlyEntriesOf(Map.of("-XX:-UseG1GC", ""));
    }

    @Test
    void testConstructorDeepCopiesCassandraYaml()
    {
        JsonObject yaml = new JsonObject().put("concurrent_reads", 32);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);

        yaml.put("concurrent_reads", 64);

        assertThat(overlay.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(32);
    }

    @Test
    void testUpdatedReturnsNewInstance()
    {
        JsonObject yaml = new JsonObject().put("concurrent_reads", 32);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, Map.of("-Xmx", "4G"));

        CassandraConfigurationOverlay updated = overlay.updated(
            Collections.singletonMap("concurrent_reads", 64),
            null);

        assertThat(updated).isNotSameAs(overlay);
        assertThat(updated.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(64);
        // Original is not modified by updated() — deep copy used internally
        assertThat(overlay.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(32);
    }

    @Test
    void testToString()
    {
        JsonObject yaml = new JsonObject()
                          .put("concurrent_reads", 32)
                          .put("commitlog_sync", "periodic");
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, Map.of("-Xmx", "4G"));

        assertThat(overlay.toString()).isEqualTo(String.join("\n",
            "{",
            "  \"cassandraYaml\" : {",
            "    \"concurrent_reads\" : 32,",
            "    \"commitlog_sync\" : \"periodic\"",
            "  },",
            "  \"extraJvmOpts\" : {",
            "    \"-Xmx\" : \"4G\"",
            "  }",
            "}"));
    }

    @Test
    void testToStringEmpty()
    {
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(null, null);

        assertThat(overlay.toString()).isEqualTo(String.join("\n",
            "{",
            "  \"cassandraYaml\" : { },",
            "  \"extraJvmOpts\" : { }",
            "}"));
    }
}
