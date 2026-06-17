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

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.vertx.core.json.JsonObject;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link CassandraConfigurationOverlay}
 */
class CassandraConfigurationOverlayTest
{
    @Test
    void testConstructorDeepCopiesCassandraYaml()
    {
        JsonObject yaml = new JsonObject().put("concurrent_reads", 32);
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, null);

        yaml.put("concurrent_reads", 64);

        assertThat(overlay.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(32);
    }

    @Test
    void testFromJsonRoundTrip()
    {
        JsonObject yaml = new JsonObject()
                          .put("concurrent_reads", 32)
                          .put("commitlog_sync", "periodic");
        CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, Map.of("-Xmx", "4G"));

        CassandraConfigurationOverlay fromJson = CassandraConfigurationOverlay.fromJson(overlay.toJson());

        assertThat(fromJson.cassandraYaml().getInteger("concurrent_reads")).isEqualTo(32);
        assertThat(fromJson.cassandraYaml().getString("commitlog_sync")).isEqualTo("periodic");
        assertThat(fromJson.extraJvmOpts()).containsEntry("-Xmx", "4G");
    }

    @Test
    void testEqualsAndHashCode()
    {
        JsonObject yaml = new JsonObject().put("concurrent_reads", 32);
        CassandraConfigurationOverlay a = new CassandraConfigurationOverlay(yaml, Map.of("-Xmx", "4G"));
        CassandraConfigurationOverlay b = new CassandraConfigurationOverlay(yaml.copy(), Map.of("-Xmx", "4G"));

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
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
