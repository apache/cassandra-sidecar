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

package org.apache.cassandra.sidecar.lifecycle;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link ProcessRuntimeConfiguration}
 */
class ProcessRuntimeConfigurationTest
{
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @TempDir
    Path tempDir;

    private Path cassandraHome;
    private Path cassandraConfDir;
    private Path cassandraBin;
    private Path cassandraYaml;

    @BeforeEach
    void setup() throws IOException
    {
        cassandraHome = Files.createDirectories(tempDir.resolve("cassandra"));
        cassandraConfDir = Files.createDirectories(tempDir.resolve("conf"));

        // Create bin directory and cassandra executable
        Path binDir = Files.createDirectories(cassandraHome.resolve("bin"));
        cassandraBin = Files.createFile(binDir.resolve("cassandra"));
        Files.setPosixFilePermissions(cassandraBin, Set.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.OWNER_EXECUTE
        ));

        // Create cassandra.yaml
        cassandraYaml = Files.createFile(cassandraConfDir.resolve("cassandra.yaml"));
    }

    @Test
    void testValidateStartWithValidConfiguration()
    {
        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        assertThatCode(config::validateStart).doesNotThrowAnyException();
    }

    @Test
    void testValidateStartThrowsWhenCassandraHomeDoesNotExist()
    {
        Path nonExistentHome = tempDir.resolve("nonexistent");

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(nonExistentHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        assertThatThrownBy(config::validateStart)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra home does not exist or is not a directory");
    }

    @Test
    void testValidateStartThrowsWhenCassandraHomeIsFile() throws IOException
    {
        Path homeAsFile = Files.createFile(tempDir.resolve("homeAsFile"));

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(homeAsFile.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        assertThatThrownBy(config::validateStart)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra home does not exist or is not a directory");
    }

    @Test
    void testValidateStartThrowsWhenConfDirDoesNotExist()
    {
        Path nonExistentConfDir = tempDir.resolve("nonexistent-conf");

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(nonExistentConfDir.toString())
                                                                        .build();

        assertThatThrownBy(config::validateStart)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra configuration directory does not exist or is not a directory");
    }

    @Test
    void testValidateStartThrowsWhenCassandraYamlDoesNotExist() throws IOException
    {
        Files.delete(cassandraYaml);

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        assertThatThrownBy(config::validateStart)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra YAML configuration file does not exist");
    }

    @Test
    void testCustomCassandraYamlFile() throws IOException
    {
        Path customCassandraYamlPath = Files.createFile(cassandraConfDir.resolve("custom-cassandra.yaml"));

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraYamlPath(customCassandraYamlPath.toString())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();
        // validate the process runtime configuration
        config.validateStart();

        assertThat(config.cassandraYaml()).isEqualTo(tempDir.resolve("conf").resolve("custom-cassandra.yaml"));
    }

    @Test
    void testValidateStartThrowsWhenCassandraBinDoesNotExist() throws IOException
    {
        Files.delete(cassandraBin);

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        assertThatThrownBy(config::validateStart)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra binary does not exist or is not a regular file");
    }

    @Test
    void testValidateStartThrowsWhenCassandraBinNotExecutable() throws IOException
    {
        Files.setPosixFilePermissions(cassandraBin, Set.of(
        PosixFilePermission.OWNER_READ,
        PosixFilePermission.OWNER_WRITE
        ));

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        assertThatThrownBy(config::validateStart)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra binary is not executable");
    }

    @Test
    void testValidateStartThrowsWhenConfDirNotReadable() throws IOException
    {
        Files.setPosixFilePermissions(cassandraConfDir, Set.of(
        PosixFilePermission.OWNER_WRITE,
        PosixFilePermission.OWNER_EXECUTE
        ));

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        assertThatThrownBy(config::validateStart)
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra configuration directory is not readable");
    }

    @Test
    void testBuildStartCommand()
    {
        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .cassandraLogDir("/custom/log/dir")
                                                                        .storageDir("/custom/storage/dir")
                                                                        .build();

        String pidFile = "/tmp/cassandra.pid";
        Path stdoutFile = Path.of("/tmp/cassandra.out");
        Path stderrFile = Path.of("/tmp/cassandra.err");

        ProcessBuilder pb = config.buildStartCommand(pidFile, stdoutFile, stderrFile);

        // Verify command
        List<String> command = pb.command();
        assertThat(command).hasSize(5);
        assertThat(command.get(0)).isEqualTo(cassandraBin.toString());
        assertThat(command.get(1)).isEqualTo("-p");
        assertThat(command.get(2)).isEqualTo(pidFile);
        assertThat(command.get(3)).isEqualTo("-D");
        assertThat(command.get(4)).isEqualTo("cassandra.storagedir=/custom/storage/dir");

        // Verify environment variables
        Map<String, String> env = pb.environment();
        assertThat(env.get("CASSANDRA_HOME")).isEqualTo(cassandraHome.toString());
        assertThat(env.get("CASSANDRA_CONF")).isEqualTo(cassandraConfDir.toString());
        assertThat(env.get("CASSANDRA_LOG_DIR")).isEqualTo("/custom/log/dir");

        // Verify working directory
        assertThat(pb.directory()).isEqualTo(cassandraHome.toFile());

        // Verify redirects are configured (files don't need to exist for ProcessBuilder creation)
        assertThat(pb.redirectOutput().type()).isEqualTo(ProcessBuilder.Redirect.Type.WRITE);
        assertThat(pb.redirectError().type()).isEqualTo(ProcessBuilder.Redirect.Type.WRITE);
    }

    @Test
    void testBuildStartCommandWithoutStorageAndLogDir()
    {
        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .build();

        String pidFile = "/tmp/cassandra.pid";
        Path stdoutFile = Path.of("/tmp/cassandra.out");
        Path stderrFile = Path.of("/tmp/cassandra.err");

        ProcessBuilder pb = config.buildStartCommand(pidFile, stdoutFile, stderrFile);

        // Verify command - should not include storage dir parameters
        List<String> command = pb.command();
        assertThat(command).hasSize(3);
        assertThat(command.get(0)).isEqualTo(cassandraBin.toString());
        assertThat(command.get(1)).isEqualTo("-p");
        assertThat(command.get(2)).isEqualTo(pidFile);

        // Verify environment variables
        Map<String, String> env = pb.environment();
        assertThat(env.get("CASSANDRA_HOME")).isEqualTo(cassandraHome.toString());
        assertThat(env.get("CASSANDRA_CONF")).isEqualTo(cassandraConfDir.toString());

        // Verify working directory
        assertThat(pb.directory()).isEqualTo(cassandraHome.toFile());

        // Verify redirects are configured
        assertThat(pb.redirectOutput().type()).isEqualTo(ProcessBuilder.Redirect.Type.WRITE);
        assertThat(pb.redirectError().type()).isEqualTo(ProcessBuilder.Redirect.Type.WRITE);
    }

    @Test
    void testBuildStartCommandWithExtraJvmOptsAndEnvVars()
    {
        Map<String, String> extraJvmOpts = Map.of(
        "cassandra.jmx.local.port", "7199",
        "java.rmi.server.hostname", "localhost"
        );

        Map<String, String> extraEnvVars = Map.of(
        "JVM_OPTS", "-Xms1G -Xmx2G",
        "CUSTOM_VAR", "custom_value"
        );

        ProcessRuntimeConfiguration config = ProcessRuntimeConfiguration.builder()
                                                                        .instance(instanceMetadata())
                                                                        .cassandraHome(cassandraHome.toString())
                                                                        .cassandraConfDir(cassandraConfDir.toString())
                                                                        .extraJvmOptions(extraJvmOpts)
                                                                        .extraEnvironmentVariables(extraEnvVars)
                                                                        .build();

        String pidFile = "/tmp/cassandra.pid";
        Path stdoutFile = Path.of("/tmp/cassandra.out");
        Path stderrFile = Path.of("/tmp/cassandra.err");
        ProcessBuilder pb = config.buildStartCommand(pidFile, stdoutFile, stderrFile);

        // Verify JVM options are included (order may vary)
        List<String> command = pb.command();
        assertThat(command).contains("-Dcassandra.jmx.local.port=7199");
        assertThat(command).contains("-Djava.rmi.server.hostname=localhost");

        // Verify environment variables include both standard and extra vars
        Map<String, String> env = pb.environment();
        assertThat(env.get("JVM_OPTS")).isEqualTo("-Xms1G -Xmx2G");
        assertThat(env.get("CUSTOM_VAR")).isEqualTo("custom_value");
    }

    InstanceMetadata instanceMetadata()
    {
        return InstanceMetadataImpl.builder()
                                   .id(1)
                                   .storagePort(7000)
                                   .metricRegistry(METRIC_REGISTRY)
                                   .storageDir("/tmp/storage_dir")
                                   .build();
    }
}
