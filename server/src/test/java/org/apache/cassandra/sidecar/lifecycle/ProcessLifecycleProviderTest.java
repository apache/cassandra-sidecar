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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.codahale.metrics.MetricRegistry;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadataImpl;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.mockito.MockedStatic;

import static org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProvider.pidFileLocation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatRuntimeException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link ProcessLifecycleProvider}. These tests use mocking to simulate process behavior.
 * Actual process execution is tested by the {@link org.apache.cassandra.sidecar.lifecycle.ProcessLifecycleProviderIntegrationTest}
 */
public class ProcessLifecycleProviderTest
{
    private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @TempDir
    Path lifecycleStateDir;

    Path defaultCassandraHome;

    @BeforeEach
    void setUp() throws IOException
    {
        defaultCassandraHome = lifecycleStateDir.resolve("cassandra-home");
        Files.createDirectories(defaultCassandraHome);
    }

    /**
     * A fake implementation of ProcessLifecycleProvider for testing purposes.
     * This class overrides the buildCassandraConfig method to return a mock configuration to avoid actual process execution.
     * Also, this simulates starting and stopping a process by creating and deleting a PID file.
     */
    class FakeProcessLifecycleProvider extends ProcessLifecycleProvider
    {
        public FakeProcessLifecycleProvider(Map<String, String> params)
        {
            super(params);
        }

        protected ProcessRuntimeConfiguration getRuntimeConfiguration(InstanceMetadata instance)
        {
            try
            {
                Process mockProcess = mock(Process.class);
                when(mockProcess.waitFor()).thenReturn(0);

                ProcessBuilder startMock = mock(ProcessBuilder.class);
                when(startMock.start()).then(invocation -> {
                    String pidFileLocation = pidFileLocation(lifecycleStateDir.toString(), instance.id());
                    // create the pid file to simulate a started process
                    Path pidFile = Path.of(pidFileLocation);
                    Files.writeString(pidFile, "12345");
                    return mockProcess;
                });
                ProcessRuntimeConfiguration mockConfig = mock(ProcessRuntimeConfiguration.class);
                when(mockConfig.buildStartCommand(any(), any(), any())).thenReturn(startMock);

                when(mockConfig.instance()).thenReturn(instance);
                return mockConfig;
            }
            catch (InterruptedException | IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    void testStartStopIsRunning()
    {
        try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class))
        {
            // Mock ProcessHandle.of to simulate process running state
            ProcessHandle mockHandle = mock(ProcessHandle.class);
            ProcessHandle.Info mockInfo = mock(ProcessHandle.Info.class);
            when(mockHandle.isAlive()).thenReturn(true);
            when(mockHandle.info()).thenReturn(mockInfo);
            when(mockInfo.commandLine()).thenReturn(Optional.of("java -cp /path/to/cassandra org.apache.cassandra.service.CassandraDaemon"));
            when(mockHandle.onExit()).thenReturn(CompletableFuture.completedFuture(null));
            when(mockHandle.pid()).thenReturn(12345L);
            Optional<ProcessHandle> presentHandle = Optional.of(mockHandle);
            processHandleMock.when(() -> ProcessHandle.of(12345L))
                             .thenReturn(presentHandle);

            // Create provider with temporary lifecycle state directory
            Map<String, String> params = Map.of(
            ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
            ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
            );
            FakeProcessLifecycleProvider provider = new FakeProcessLifecycleProvider(params);

            InstanceMetadata instance = instanceMetadata(1);

            // Initially, instance should not be running (no PID file exists)
            String pidFileLocation = pidFileLocation(lifecycleStateDir.toString(), instance.id());
            Path pidFilePath = Path.of(pidFileLocation);
            assertThat(pidFilePath).doesNotExist();
            assertThat(provider.isRunning(instance)).isFalse();

            // Start the instance
            provider.start(instance);

            // After starting, instance should be running (PID file should exist)
            assertThat(pidFilePath).exists();
            assertThat(provider.isRunning(instance)).isTrue();

            // Stop the instance
            provider.stop(instance);

            // After stopping, instance should not be running (PID file should be deleted)
            assertThat(pidFilePath).doesNotExist();
            assertThat(provider.isRunning(instance)).isFalse();
        }
    }

    @Test
    void testBuildCassandraConfigWithCassandraHomeOverride()
    {
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, "/instance/cassandra/home",
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, "/instance/conf/dir",
        ProcessLifecycleProvider.OPT_CASSANDRA_LOG_DIR, "/instance/log/dir"
        );

        InstanceMetadata instance = instanceMetadata(1, lifecycleOptions);

        ProcessRuntimeConfiguration config = provider.getRuntimeConfiguration(instance);

        // Verify the configuration was built correctly
        assertThat(config.instance()).isEqualTo(instance);
        assertThat(config.cassandraHome()).isEqualTo(Path.of("/instance/cassandra/home"));
        assertThat(config.cassandraConfDir).isEqualTo(Path.of("/instance/conf/dir"));
        assertThat(config.cassandraLogDir).isEqualTo("/instance/log/dir");
        assertThat(config.storageDir).isEqualTo("/custom/storage/dir");
    }

    @Test
    void testBuildCassandraConfigWithDefaultCassandraHome()
    {
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        // Lifecycle options without CASSANDRA_HOME override
        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, "/instance/conf/dir",
        ProcessLifecycleProvider.OPT_CASSANDRA_LOG_DIR, "/instance/log/dir"
        );

        InstanceMetadata instance = instanceMetadata(1, lifecycleOptions);
        ProcessRuntimeConfiguration config = provider.getRuntimeConfiguration(instance);

        // Verify the configuration uses default Cassandra home
        assertThat(config.cassandraHome()).isEqualTo(defaultCassandraHome);
    }

    @Test
    void testBuildStartCommand() throws IOException
    {
        // Create temporary files to simulate the required directories and files first
        Path tempCassandraHome = lifecycleStateDir.resolve("cassandra");
        Path tempBinDir = tempCassandraHome.resolve("bin");
        Path tempConfDir = lifecycleStateDir.resolve("conf");
        Files.createDirectories(tempBinDir);
        Files.createDirectories(tempConfDir);

        Path cassandraBin = tempBinDir.resolve("cassandra");
        Path cassandraYaml = tempConfDir.resolve("cassandra.yaml");
        Files.createFile(cassandraBin);
        Files.createFile(cassandraYaml);
        cassandraBin.toFile().setExecutable(true);

        // Use the temp directory for Cassandra home instead of hardcoded path
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, tempCassandraHome.toString()
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        // Create mock instance metadata without storage dir
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.id()).thenReturn(5);
        when(instance.storageDir()).thenReturn(null);

        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, tempConfDir.toString()
        );
        when(instance.lifecycleOptions()).thenReturn(lifecycleOptions);

        // Build the config and test the start command using provider helper methods
        ProcessRuntimeConfiguration testConfig = provider.getRuntimeConfiguration(instance);
        String pidFileLocation = provider.pidFileLocation(instance);
        Path stdoutLocation = provider.stdoutLocation(instance);
        Path stderrLocation = provider.stderrLocation(instance);

        ProcessBuilder processBuilder = testConfig.buildStartCommand(pidFileLocation, stdoutLocation, stderrLocation);

        // Verify command arguments (no storage dir override)
        List<String> command = processBuilder.command();
        assertThat(command).containsExactly(
        cassandraBin.toString(),
        "-p",
        pidFileLocation
        );

        // Verify environment variables
        Map<String, String> env = processBuilder.environment();
        assertThat(env.get("CASSANDRA_HOME")).isEqualTo(tempCassandraHome.toString());
        assertThat(env.get("CASSANDRA_CONF")).isEqualTo(tempConfDir.toString());
        assertThat(env.get("CASSANDRA_LOG_DIR")).isNull();
    }

    @Test
    void testBuildStartCommandWithExtraJvmOptsAndEnvVars() throws IOException
    {
        // Create temporary files to simulate the required directories and files first
        Path tempCassandraHome = lifecycleStateDir.resolve("cassandra");
        Path tempBinDir = tempCassandraHome.resolve("bin");
        Path tempConfDir = lifecycleStateDir.resolve("conf");
        Files.createDirectories(tempBinDir);
        Files.createDirectories(tempConfDir);

        Path cassandraBin = tempBinDir.resolve("cassandra");
        Path cassandraYaml = tempConfDir.resolve("cassandra.yaml");
        Files.createFile(cassandraBin);
        Files.createFile(cassandraYaml);
        cassandraBin.toFile().setExecutable(true);

        // Create provider with extra JVM options and environment variables
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, tempCassandraHome.toString(),
        "sys.cassandra.max_queued_native_transport_requests", "1024",
        "env.JVM_OPTS", "-Xms1G -Xmx2G",
        "env.CUSTOM_VAR", "custom_value"
        );

        ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

        Map<String, String> lifecycleOptions = Map.of(
        ProcessLifecycleProvider.OPT_CASSANDRA_CONF_DIR, tempConfDir.toString()
        );

        InstanceMetadata instance = instanceMetadata(5, lifecycleOptions);

        // Build the config and test the start command
        ProcessRuntimeConfiguration testConfig = provider.getRuntimeConfiguration(instance);
        InstanceMetadata instanceMetadata = instanceMetadata(5);
        String pidFileLocation = provider.pidFileLocation(instanceMetadata);
        Path stdoutLocation = provider.stdoutLocation(instanceMetadata);
        Path stderrLocation = provider.stderrLocation(instanceMetadata);

        ProcessBuilder processBuilder = testConfig.buildStartCommand(pidFileLocation, stdoutLocation, stderrLocation);

        // Verify command includes JVM options as -D parameters
        List<String> command = processBuilder.command();
        assertThat(command).contains(cassandraBin.toString());
        assertThat(command).contains("-p");
        assertThat(command).contains(pidFileLocation);
        assertThat(command).contains("-Dcassandra.max_queued_native_transport_requests=1024");

        // Verify environment variables include both standard and extra vars
        Map<String, String> env = processBuilder.environment();
        assertThat(env.get("CASSANDRA_HOME")).isEqualTo(tempCassandraHome.toString());
        assertThat(env.get("CASSANDRA_CONF")).isEqualTo(tempConfDir.toString());
        assertThat(env.get("JVM_OPTS")).isEqualTo("-Xms1G -Xmx2G");
        assertThat(env.get("CUSTOM_VAR")).isEqualTo("custom_value");
    }

    @Test
    void testIsCassandraProcessRunningRemovesStalePidFile() throws IOException
    {
        try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class))
        {
            // Create provider
            Map<String, String> params = Map.of(
            ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
            ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
            );
            ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

            // Create mock instance metadata
            InstanceMetadata instance = mock(InstanceMetadata.class);
            when(instance.id()).thenReturn(1);
            when(instance.storageDir()).thenReturn("/storage/dir");
            when(instance.lifecycleOptions()).thenReturn(Map.of());

            // Create a PID file with a stale PID (process that doesn't exist)
            String pidFileLocation = pidFileLocation(lifecycleStateDir.toString(), 1);
            Path pidFilePath = Path.of(pidFileLocation);
            Files.writeString(pidFilePath, "99999");

            // Mock ProcessHandle.of to return empty (process doesn't exist)
            processHandleMock.when(() -> ProcessHandle.of(99999L))
                             .thenReturn(Optional.empty());

            // Verify PID file exists before the check
            assertThat(pidFilePath).exists();

            // Call isRunning - should return false and remove the stale PID file
            boolean isRunning = provider.isRunning(instance);

            // Verify process is not running
            assertThat(isRunning).isFalse();

            // Verify the stale PID file was removed
            assertThat(pidFilePath).doesNotExist();
        }
    }

    @Test
    void testIsCassandraProcessRunningRemovesStalePidFileWhenNotCassandra() throws IOException
    {
        try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class))
        {
            // Create provider
            Map<String, String> params = Map.of(
            ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
            ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
            );
            ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

            // Create mock instance metadata
            InstanceMetadata instance = mock(InstanceMetadata.class);
            when(instance.id()).thenReturn(1);
            when(instance.storageDir()).thenReturn("/storage/dir");
            when(instance.lifecycleOptions()).thenReturn(Map.of());

            // Create a PID file with a PID
            String pidFileLocation = pidFileLocation(lifecycleStateDir.toString(), 1);
            Path pidFilePath = Path.of(pidFileLocation);
            Files.writeString(pidFilePath, "55555");

            // Mock ProcessHandle that exists and is alive but is not a Cassandra process
            ProcessHandle mockHandle = mock(ProcessHandle.class);
            ProcessHandle.Info mockInfo = mock(ProcessHandle.Info.class);
            when(mockHandle.isAlive()).thenReturn(true);
            when(mockHandle.info()).thenReturn(mockInfo);
            when(mockInfo.commandLine()).thenReturn(Optional.of("/usr/bin/someothercommand"));
            when(mockHandle.pid()).thenReturn(55555L);

            processHandleMock.when(() -> ProcessHandle.of(55555L))
                             .thenReturn(Optional.of(mockHandle));

            // Verify PID file exists before the check
            assertThat(pidFilePath).exists();

            // Call isRunning - should return false and remove the stale PID file
            boolean isRunning = provider.isRunning(instance);

            // Verify process is not running
            assertThat(isRunning).isFalse();

            // Verify the stale PID file was removed
            assertThat(pidFilePath).doesNotExist();
        }
    }

    @Test
    void testIsCassandraProcessRunningDoesNotRemovePidFileWhenCassandraIsRunning() throws IOException
    {
        try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class))
        {
            // Create provider
            Map<String, String> params = Map.of(
            ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
            ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
            );
            ProcessLifecycleProvider provider = new ProcessLifecycleProvider(params);

            // Create mock instance metadata
            InstanceMetadata instance = mock(InstanceMetadata.class);
            when(instance.id()).thenReturn(1);
            when(instance.storageDir()).thenReturn("/storage/dir");
            when(instance.lifecycleOptions()).thenReturn(Map.of());

            // Create a PID file with a valid Cassandra process PID
            String pidFileLocation = pidFileLocation(lifecycleStateDir.toString(), 1);
            Path pidFilePath = Path.of(pidFileLocation);
            Files.writeString(pidFilePath, "77777");

            // Mock ProcessHandle that exists and is a Cassandra process
            ProcessHandle mockHandle = mock(ProcessHandle.class);
            ProcessHandle.Info mockInfo = mock(ProcessHandle.Info.class);
            when(mockHandle.isAlive()).thenReturn(true);
            when(mockHandle.info()).thenReturn(mockInfo);
            when(mockInfo.commandLine()).thenReturn(Optional.of("java -cp /path/to/cassandra org.apache.cassandra.service.CassandraDaemon"));
            when(mockHandle.pid()).thenReturn(77777L);

            processHandleMock.when(() -> ProcessHandle.of(77777L))
                             .thenReturn(Optional.of(mockHandle));

            // Verify PID file exists before the check
            assertThat(pidFilePath).exists();

            // Call isRunning
            boolean isRunning = provider.isRunning(instance);

            // Verify process is running
            assertThat(isRunning).isTrue();

            // Verify the PID file was NOT removed since process is running
            assertThat(pidFilePath).exists();
            assertThat(Files.readString(pidFilePath).trim()).isEqualTo("77777");
        }
    }

    @Test
    void testGetCommandLinePlatformIndependentReturnsFallbackWhenPsFails()
    {
        // Mock ProcessHandle
        ProcessHandle mockHandle = mock(ProcessHandle.class);
        ProcessHandle.Info mockInfo = mock(ProcessHandle.Info.class);
        when(mockHandle.info()).thenReturn(mockInfo);
        String expectedCommandLine = "java -cp /path/to/cassandra org.apache.cassandra.service.CassandraDaemon";
        when(mockInfo.commandLine()).thenReturn(Optional.of(expectedCommandLine));
        when(mockHandle.pid()).thenReturn(99999L);

        // Since ps will fail (PID doesn't exist), we should get the fallback command line
        Optional<String> result = ProcessLifecycleProvider.getCommandLinePlatformIndependent(mockHandle);

        // Verify that we get the fallback result (since ps will return empty for non-existent PID)
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(expectedCommandLine);
    }

    @Test
    void testReadPidFromFile() throws IOException
    {
        // Create a PID file with a valid PID
        Path pidFilePath = lifecycleStateDir.resolve("test.pid");
        Files.writeString(pidFilePath, "12345");

        // Read the PID from the file
        Long pid = ProcessLifecycleProvider.readPidFromFile(pidFilePath);

        // Verify the PID is read correctly
        assertThat(pid).isEqualTo(12345L);
    }

    @Test
    void testReadPidFromFileWithWhitespace() throws IOException
    {
        // Create a PID file with whitespace around the PID
        Path pidFilePath = lifecycleStateDir.resolve("test.pid");
        Files.writeString(pidFilePath, "  54321  \n");

        // Read the PID from the file
        Long pid = ProcessLifecycleProvider.readPidFromFile(pidFilePath);

        // Verify the PID is read correctly and whitespace is trimmed
        assertThat(pid).isEqualTo(54321L);
    }

    @Test
    void testReadPidFromFileThrowsExceptionForInvalidPid() throws IOException
    {
        // Create a PID file with invalid content
        Path pidFilePath = lifecycleStateDir.resolve("test.pid");
        assertThat(pidFilePath).doesNotExist();
        Files.writeString(pidFilePath, "invalid_pid");

        assertThatRuntimeException().isThrownBy(() -> ProcessLifecycleProvider.readPidFromFile(pidFilePath))
                                    .withCauseInstanceOf(NumberFormatException.class)
                                    .withMessageContaining("Unable to parse PID from file: ");
    }

    @Test
    void testReadPidFromFileThrowsExceptionForMissingFile()
    {
        // Try to read a PID file that doesn't exist
        Path pidFilePath = lifecycleStateDir.resolve("nonexistent.pid");

        try
        {
            ProcessLifecycleProvider.readPidFromFile(pidFilePath);

            // Should not reach here
            assertThat(false).as("Expected RuntimeException to be thrown").isTrue();
        }
        catch (RuntimeException e)
        {
            assertThat(e.getMessage()).contains("Failed to read PID from file");
            assertThat(e.getCause()).isInstanceOf(IOException.class);
        }
    }

    @Test
    void testDeletePidFile() throws IOException
    {
        // Create a mock instance
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.host()).thenReturn("testhost");

        // Create a PID file
        Path pidFilePath = lifecycleStateDir.resolve("cassandra-testhost.pid");
        Files.writeString(pidFilePath, "99999");

        // Verify the file exists
        assertThat(pidFilePath).exists();

        // Delete the PID file
        ProcessLifecycleProvider.deletePidFile(instance, pidFilePath);

        // Verify the file was deleted
        assertThat(pidFilePath).doesNotExist();
    }

    @Test
    void testDeletePidFileWhenFileDoesNotExist()
    {
        // Create a mock instance
        InstanceMetadata instance = mock(InstanceMetadata.class);
        when(instance.host()).thenReturn("testhost");

        // Try to delete a PID file that doesn't exist
        Path pidFilePath = lifecycleStateDir.resolve("nonexistent-cassandra-testhost.pid");

        // Verify the file doesn't exist
        assertThat(pidFilePath).doesNotExist();

        // Should not throw an exception when trying to delete a non-existent file
        ProcessLifecycleProvider.deletePidFile(instance, pidFilePath);

        // Verify the file still doesn't exist
        assertThat(pidFilePath).doesNotExist();
    }

    @Test
    void testStopCallsDestroyForciblyOnTimeout() throws Exception
    {
        try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class))
        {
            // Create provider
            Map<String, String> params = Map.of(
            ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
            ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
            );
            FakeProcessLifecycleProvider provider = new FakeProcessLifecycleProvider(params);

            InstanceMetadata instance = instanceMetadata(1);

            // Create a PID file
            String pidFileLocation = pidFileLocation(lifecycleStateDir.toString(), 1);
            Path pidFilePath = Path.of(pidFileLocation);
            Files.writeString(pidFilePath, "12345");

            // Mock ProcessHandle that times out on graceful termination but succeeds with force
            ProcessHandle mockHandle = mock(ProcessHandle.class);
            ProcessHandle.Info mockInfo = mock(ProcessHandle.Info.class);
            when(mockHandle.isAlive()).thenReturn(true);
            when(mockHandle.info()).thenReturn(mockInfo);
            when(mockInfo.commandLine()).thenReturn(Optional.of("java org.apache.cassandra.service.CassandraDaemon"));
            when(mockHandle.pid()).thenReturn(12345L);

            // Simulate timeout by mocking CompletableFuture.get() to throw TimeoutException
            @SuppressWarnings("unchecked")
            CompletableFuture<ProcessHandle> timeoutFuture = mock(CompletableFuture.class);
            when(timeoutFuture.get(anyLong(), any())).thenThrow(new TimeoutException("Simulated timeout"));
            when(mockHandle.onExit()).thenReturn(timeoutFuture);
            when(mockHandle.destroy()).thenReturn(true);
            when(mockHandle.destroyForcibly()).thenReturn(true);

            processHandleMock.when(() -> ProcessHandle.of(12345L))
                             .thenReturn(Optional.of(mockHandle));

            // Call stop - should call destroyForcibly after timeout
            provider.stop(instance);

            // Verify destroyForcibly was called
            verify(mockHandle).destroyForcibly();
        }
    }

    @Test
    void testStopThrowsExceptionWhenDestroyForciblyFails() throws Exception
    {
        try (MockedStatic<ProcessHandle> processHandleMock = mockStatic(ProcessHandle.class))
        {
            // Create provider
            Map<String, String> params = Map.of(
            ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
            ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
            );
            FakeProcessLifecycleProvider provider = new FakeProcessLifecycleProvider(params);

            // Create mock instance metadata
            InstanceMetadata instance = mock(InstanceMetadata.class);
            when(instance.id()).thenReturn(1);
            when(instance.storageDir()).thenReturn("/storage/dir");
            when(instance.lifecycleOptions()).thenReturn(Map.of());

            // Create a PID file
            String pidFileLocation = pidFileLocation(lifecycleStateDir.toString(), 1);
            Path pidFilePath = Path.of(pidFileLocation);
            Files.writeString(pidFilePath, "12345");

            // Mock ProcessHandle that times out and fails to force destroy
            ProcessHandle mockHandle = mock(ProcessHandle.class);
            ProcessHandle.Info mockInfo = mock(ProcessHandle.Info.class);
            when(mockHandle.isAlive()).thenReturn(true);
            when(mockHandle.info()).thenReturn(mockInfo);
            when(mockInfo.commandLine()).thenReturn(Optional.of("java org.apache.cassandra.service.CassandraDaemon"));
            when(mockHandle.pid()).thenReturn(12345L);

            // Simulate timeout by mocking CompletableFuture.get() to throw TimeoutException
            @SuppressWarnings("unchecked")
            CompletableFuture<ProcessHandle> timeoutFuture = mock(CompletableFuture.class);
            when(timeoutFuture.get(anyLong(), any())).thenThrow(new TimeoutException("Simulated timeout"));
            when(mockHandle.onExit()).thenReturn(timeoutFuture);
            when(mockHandle.destroy()).thenReturn(true);
            when(mockHandle.destroyForcibly()).thenReturn(false); // Force destroy fails

            processHandleMock.when(() -> ProcessHandle.of(12345L))
                             .thenReturn(Optional.of(mockHandle));

            // Call stop - should throw exception because destroyForcibly returned false
            assertThatThrownBy(() -> provider.stop(instance))
            .isInstanceOf(RuntimeException.class)
            .hasMessageContaining("Failed to forcibly destroy process");
        }
    }

    @Test
    void testThrowsExceptionWhenStateDirDoesNotExist()
    {
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, "/nonexistent/state/dir",
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, defaultCassandraHome.toString()
        );

        assertThatThrownBy(() -> new ProcessLifecycleProvider(params))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("State directory")
        .hasMessageContaining("does not exist or is not a directory");
    }

    @Test
    void testThrowsExceptionWhenCassandraHomeDoesNotExist()
    {
        Map<String, String> params = Map.of(
        ProcessLifecycleProvider.OPT_STATE_DIR, lifecycleStateDir.toString(),
        ProcessLifecycleProvider.OPT_CASSANDRA_HOME, "/nonexistent/cassandra/home"
        );

        assertThatThrownBy(() -> new ProcessLifecycleProvider(params))
        .isInstanceOf(ConfigurationException.class)
        .hasMessageContaining("Cassandra home")
        .hasMessageContaining("does not exist or is not a directory");
    }

    InstanceMetadata instanceMetadata(int instanceId)
    {
        return instanceMetadata(instanceId, Map.of());
    }

    InstanceMetadata instanceMetadata(int instanceId, Map<String, String> lifecycleOptions)
    {
        return InstanceMetadataImpl.builder()
                                   .id(instanceId)
                                   .storagePort(7000)
                                   .metricRegistry(METRIC_REGISTRY)
                                   .storageDir("/custom/storage/dir")
                                   .lifecycleOptions(lifecycleOptions)
                                   .build();
    }
}
