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
            "  \"overlay\" : {",
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
