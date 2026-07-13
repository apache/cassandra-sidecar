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

import java.util.List;

import org.junit.jupiter.api.Test;

import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.ADD;
import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.REMOVE;
import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.REPLACE;
import static org.apache.cassandra.sidecar.configmanagement.ConfigurationPatchOperation.Op.TEST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ConfigurationPatchValidator}
 */
class ConfigurationPatchValidatorTest
{
    @Test
    void testRejectsEmptyOperationsList()
    {
        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(List.of()))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void testRejectsRemoveWithValue()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(REMOVE, "/configuration/cassandraYaml/key", "value"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("REMOVE' must not have a value");
    }

    @Test
    void testRejectsAddWithoutValue()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/key", null));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("requires a value");
    }

    @Test
    void testRejectsReplaceWithoutValue()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/key", null));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("requires a value");
    }

    @Test
    void testRejectsTestWithoutValue()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/key", null));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("requires a value");
    }

    @Test
    void testRejectsInvalidPathPrefix()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/invalid/path", 42));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Path must start with");
    }

    @Test
    void testRejectsPathWithNoKeyAfterSection()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/", 42));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Path must specify at least one key after section");
    }

    @Test
    void testRejectsNestedExtraJvmOpts()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dfoo/nested", "bar"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("extraJvmOpts paths must be flat");
    }

    @Test
    void testRejectsEmptySegmentInPath()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml//key", "value"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("empty segment");
    }

    @Test
    void testRejectsDuplicateMutationPaths()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/key", "a"),
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/key", "b"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Duplicate path");
    }

    @Test
    void testAllowsTestAndMutationOnSamePath()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(TEST, "/configuration/cassandraYaml/key", "old"),
                new ConfigurationPatchOperation(REPLACE, "/configuration/cassandraYaml/key", "new"));

        List<ConfigurationPatchValidator.ParsedPatchOperation> parsed = ConfigurationPatchValidator.validate(ops);

        assertThat(parsed).hasSize(2);
    }

    @Test
    void testParsesTopLevelCassandraYamlPath()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/cassandraYaml/concurrent_reads", 64));

        List<ConfigurationPatchValidator.ParsedPatchOperation> parsed = ConfigurationPatchValidator.validate(ops);

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).section()).isEqualTo("cassandraYaml");
        assertThat(parsed.get(0).topLevelKey()).isEqualTo("concurrent_reads");
        assertThat(parsed.get(0).nestedSegments()).isEmpty();
        assertThat(parsed.get(0).isNested()).isFalse();
    }

    @Test
    void testParsesNestedCassandraYamlPath()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD,
                        "/configuration/cassandraYaml/memtable/configurations/trie/class_name", "value"));

        List<ConfigurationPatchValidator.ParsedPatchOperation> parsed = ConfigurationPatchValidator.validate(ops);

        assertThat(parsed.get(0).section()).isEqualTo("cassandraYaml");
        assertThat(parsed.get(0).topLevelKey()).isEqualTo("memtable");
        assertThat(parsed.get(0).nestedSegments()).containsExactly("configurations", "trie", "class_name");
        assertThat(parsed.get(0).isNested()).isTrue();
    }

    @Test
    void testParsesExtraJvmOptsPath()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xmx", "8g"));

        List<ConfigurationPatchValidator.ParsedPatchOperation> parsed = ConfigurationPatchValidator.validate(ops);

        assertThat(parsed.get(0).section()).isEqualTo("extraJvmOpts");
        assertThat(parsed.get(0).topLevelKey()).isEqualTo("-Xmx");
        assertThat(parsed.get(0).nestedSegments()).isEmpty();
    }

    // --- extraJvmOpts key allowlist tests ---

    @Test
    void testAcceptsValidSystemPropertyKey()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dcassandra.jmx.local.port", "7199"));

        List<ConfigurationPatchValidator.ParsedPatchOperation> parsed = ConfigurationPatchValidator.validate(ops);

        assertThat(parsed).hasSize(1);
        assertThat(parsed.get(0).topLevelKey()).isEqualTo("-Dcassandra.jmx.local.port");
    }

    @Test
    void testAcceptsValidXFlag()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xms", "2g"));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

    @Test
    void testAcceptsValidXXBooleanFlag()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:+UseG1GC", ""));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

    @Test
    void testAcceptsValidXXValueFlag()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:MaxGCPauseMillis", "200"));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

    @Test
    void testRejectsJavaAgentKey()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-javaagent", "/tmp/malicious.jar"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Invalid JVM option key '-javaagent'");
    }

    @Test
    void testRejectsAgentPathKey()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-agentpath", "/tmp/agent.so"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Invalid JVM option key '-agentpath'");
    }

    @Test
    void testRejectsBlockedOnOutOfMemoryError()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:OnOutOfMemoryError", "kill -9 %p"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessage("Blocked extraJvmOpts key '-XX:OnOutOfMemoryError': this JVM option is not allowed");
    }

    @Test
    void testRejectsBlockedOnError()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-XX:OnError", "rm -rf /"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessage("Blocked extraJvmOpts key '-XX:OnError': this JVM option is not allowed");
    }

    @Test
    void testRejectsPathBearingJvmOpts()
    {
        // Options that write to arbitrary filesystem paths must be blocked by key, since the value
        // pattern legitimately permits absolute paths for Cassandra system properties.
        List<String> blockedPathOpts = List.of(
                "-XX:ErrorFile", "-XX:HeapDumpPath", "-XX:LogFile",
                "-XX:FlightRecorderOptions", "-XX:StartFlightRecording",
                "-Xloggc", "-Xlog", "-Xbootclasspath");

        for (String key : blockedPathOpts)
        {
            List<ConfigurationPatchOperation> ops = List.of(
                    new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/" + key, "/tmp/evil"));

            assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                    .isInstanceOf(ConfigurationPatchException.class)
                    .hasMessage("Blocked extraJvmOpts key '" + key + "': this JVM option is not allowed");
        }
    }

    @Test
    void testRejectsKeyWithEqualsSign()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dfoo=bar", "value"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Invalid JVM option key '-Dfoo=bar'");
    }

    @Test
    void testRejectsKeyWithShellMetacharacters()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dfoo;echo pwned", "value"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("Invalid JVM option key '-Dfoo;echo pwned'");
    }

    // --- extraJvmOpts value validation tests ---

    @Test
    void testAcceptsValidJvmOptValue()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xmx", "4g"));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

    @Test
    void testAcceptsJvmOptValueWithAbsolutePath()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dcassandra.logdir", "/var/log/cassandra"));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

    @Test
    void testAcceptsJvmOptValueWithJson()
    {
        String jsonValue = "{\"class_name\":\"SizeTieredCompactionStrategy\","
                           + "\"parameters\":{\"min_threshold\":\"4\",\"max_threshold\":\"32\"}}";
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dcassandra.settings.default_compaction",
                                                jsonValue));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

    @Test
    void testAcceptsJvmOptValueWithCommas()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dcassandra.seed_provider",
                                                "org.apache.cassandra.locator.SimpleSeedProvider:127.0.0.1,127.0.0.2"));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

    @Test
    void testRejectsJvmOptValueWithWhitespace()
    {
        // Whitespace is rejected because bin/cassandra word-splits unquoted values during `eval`,
        // silently truncating a value like "hello world" to "hello".
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dfoo", "hello world"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("disallowed characters");
    }

    @Test
    void testRejectsJvmOptValueWithPathTraversal()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dcassandra.logdir", "/tmp/../../etc/cron.d"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("path traversal");
    }

    @Test
    void testRejectsJvmOptValueWithShellMetacharacters()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dfoo", "val;rm -rf /"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("disallowed characters");
    }

    @Test
    void testRejectsJvmOptValueWithBacktick()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Dfoo", "`whoami`"));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("disallowed characters");
    }

    @Test
    void testRejectsJvmOptValueExceedingMaxLength()
    {
        String longValue = "a".repeat(513);
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(ADD, "/configuration/extraJvmOpts/-Xmx", longValue));

        assertThatThrownBy(() -> ConfigurationPatchValidator.validate(ops))
                .isInstanceOf(ConfigurationPatchException.class)
                .hasMessageContaining("disallowed characters or exceeds 512");
    }

    @Test
    void testAllowsRemoveWithoutValueValidation()
    {
        List<ConfigurationPatchOperation> ops = List.of(
                new ConfigurationPatchOperation(REMOVE, "/configuration/extraJvmOpts/-Xmx", null));

        assertThat(ConfigurationPatchValidator.validate(ops)).hasSize(1);
    }

}
