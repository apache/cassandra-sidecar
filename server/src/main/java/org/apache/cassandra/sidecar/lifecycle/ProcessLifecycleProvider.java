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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Manage the lifecycle of Cassandra instances running on local processes
 */
public class ProcessLifecycleProvider implements LifecycleProvider
{
    static final String OPT_CASSANDRA_HOME = "cassandra_home";
    static final String OPT_CASSANDRA_CONF_DIR = "cassandra_conf_dir";
    static final String OPT_CASSANDRA_LOG_DIR = "cassandra_log_dir";
    static final String OPT_CASSANDRA_YAML_PATH = "cassandra_yaml_path";
    static final String OPT_STATE_DIR = "state_dir";
    static final String OPT_PROCESS_TIMEOUT_SECONDS = "process_timeout_seconds";

    protected static final Logger LOGGER = LoggerFactory.getLogger(ProcessLifecycleProvider.class);
    public static final long DEFAULT_PROCESS_TIMEOUT_SECONDS = 120L;

    private final String lifecycleDir;
    private final String defaultCassandraHome;
    private final long processTimeoutSeconds;
    private final Map<String, String> defaultJvmProperties = new HashMap<>();
    private final Map<String, String> defaultEnvVars = new HashMap<>();

    public ProcessLifecycleProvider(@NotNull Map<String, String> params)
    {
        // Extract any JVM properties or environment variables from the params
        for (Map.Entry<String, String> entry : params.entrySet())
        {
            if (entry.getKey().startsWith("sys."))
            {
                defaultJvmProperties.put(entry.getKey().replaceFirst("^sys\\.", ""), entry.getValue());
            }
            else if (entry.getKey().startsWith("env."))
            {
                defaultEnvVars.put(entry.getKey().replaceFirst("^env\\.", ""), entry.getValue());
            }
        }
        this.lifecycleDir = params.get(OPT_STATE_DIR);
        this.defaultCassandraHome = params.get(OPT_CASSANDRA_HOME);
        String timeoutStr = params.get(OPT_PROCESS_TIMEOUT_SECONDS);
        this.processTimeoutSeconds = timeoutStr != null ? Long.parseLong(timeoutStr) : DEFAULT_PROCESS_TIMEOUT_SECONDS;
        validateConfiguration();
    }

    @Override
    public void start(InstanceMetadata instance)
    {
        if (isCassandraProcessRunning(instance))
        {
            LOGGER.info("Cassandra instance {} is already running.", instance);
            return;
        }
        startCassandra(instance);
    }

    @Override
    public void stop(InstanceMetadata instance)
    {
        if (!isCassandraProcessRunning(instance))
        {
            LOGGER.info("Cassandra instance {} is already stopped.", instance);
            return;
        }
        stopCassandra(instance);
    }

    @Override
    public boolean isRunning(InstanceMetadata instance)
    {
        return isCassandraProcessRunning(instance);
    }

    protected void validateConfiguration()
    {
        if (lifecycleDir == null || lifecycleDir.isEmpty())
        {
            throw new ConfigurationException("Configuration property '" + OPT_STATE_DIR + "' must be set for ProcessLifecycleProvider");
        }
        if (defaultCassandraHome == null || defaultCassandraHome.isEmpty())
        {
            throw new ConfigurationException("Configuration property '" + OPT_CASSANDRA_HOME + "' must be set for ProcessLifecycleProvider");
        }

        Path stateDir = Path.of(lifecycleDir);
        if (!Files.isDirectory(stateDir))
        {
            throw new ConfigurationException("State directory '" + lifecycleDir + "' does not exist or is not a directory");
        }
        if (!Files.isWritable(stateDir))
        {
            throw new ConfigurationException("State directory '" + lifecycleDir + "' is not writable");
        }

        Path cassandraHomePath = Path.of(defaultCassandraHome);
        if (!Files.isDirectory(cassandraHomePath))
        {
            throw new ConfigurationException("Cassandra home '" + defaultCassandraHome + "' does not exist or is not a directory");
        }
        if (!Files.isReadable(cassandraHomePath))
        {
            throw new ConfigurationException("Cassandra home '" + defaultCassandraHome + "' is not readable");
        }
    }

    protected void startCassandra(InstanceMetadata instance)
    {
        ProcessRuntimeConfiguration runtimeConfig = getRuntimeConfiguration(instance);
        try
        {
            Path stdoutLocation = stdoutLocation(runtimeConfig.instance());
            Path stderrLocation = stderrLocation(runtimeConfig.instance());
            String pidFileLocation = pidFileLocation(runtimeConfig.instance());
            ProcessBuilder processBuilder = runtimeConfig.buildStartCommand(pidFileLocation,
                                                                            stdoutLocation,
                                                                            stderrLocation);
            LOGGER.info("Starting Cassandra instance {} with command: {}", runtimeConfig.instance(), processBuilder.command());

            Process process = processBuilder.start();
            boolean completed = process.waitFor(processTimeoutSeconds, TimeUnit.SECONDS);

            if (!completed)
            {
                LOGGER.warn("Cassandra startup script for instance {} did not complete within {} seconds",
                            runtimeConfig.instance(), processTimeoutSeconds);
            }

            if (isCassandraProcessRunning(instance))
            {
                LOGGER.info("Started Cassandra instance {} with PID {}", runtimeConfig.instance(), readPidFromFile(Path.of(pidFileLocation)));
            }
            else
            {
                throw new RuntimeException("Failed to start Cassandra instance " + runtimeConfig.instance() +
                                           ". Check stdout at " + stdoutLocation + " and stderr at " + stderrLocation);
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to start Cassandra instance " + runtimeConfig.instance() + " due to " + t.getMessage(), t);
        }
    }

    protected void stopCassandra(InstanceMetadata instance)
    {
        ProcessRuntimeConfiguration casCfg = getRuntimeConfiguration(instance);
        try
        {
            String pidFileLocation = pidFileLocation(casCfg.instance());
            Path pidFilePath = Path.of(pidFileLocation);
            Long pid = readPidFromFile(pidFilePath);
            Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);
            if (processHandle.isPresent())
            {
                LOGGER.info("Stopping process of Cassandra instance {} with PID {}.", casCfg.instance(), pid);
                CompletableFuture<ProcessHandle> terminationFuture = processHandle.get().onExit();
                processHandle.get().destroy();
                try
                {
                    terminationFuture.get(processTimeoutSeconds, TimeUnit.SECONDS);
                }
                catch (TimeoutException e)
                {
                    LOGGER.warn("Process {} did not terminate within timeout, forcing destroy.", pid);
                    boolean destroyed = processHandle.get().destroyForcibly();
                    if (!destroyed)
                    {
                        throw new RuntimeException("Failed to forcibly destroy process " + pid +
                                                   " for Cassandra instance " + casCfg.instance(), e);
                    }
                    LOGGER.info("Process {} was forcibly destroyed for instance {}", pid, casCfg.instance());
                }
                Files.deleteIfExists(pidFilePath);
            }
            else
            {
                LOGGER.warn("No process running for Cassandra instance {} with PID {}.", casCfg.instance(), pid);
            }
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Failed to stop process for Cassandra instance " + casCfg.instance() + " due to " + t.getMessage(), t);
        }
    }

    @VisibleForTesting
    protected ProcessRuntimeConfiguration getRuntimeConfiguration(InstanceMetadata instance)
    {
        Map<String, String> options = instance.lifecycleOptions();
        String cassandraHome = options.getOrDefault(OPT_CASSANDRA_HOME, defaultCassandraHome);
        String cassandraConfDir = options.get(OPT_CASSANDRA_CONF_DIR);
        String cassandraLogDir = options.get(OPT_CASSANDRA_LOG_DIR);
        String cassandraYamlPath = options.get(OPT_CASSANDRA_YAML_PATH);
        return ProcessRuntimeConfiguration.builder()
                                          .instance(instance)
                                          .cassandraHome(cassandraHome)
                                          .cassandraConfDir(cassandraConfDir)
                                          .cassandraLogDir(cassandraLogDir)
                                          .cassandraYamlPath(cassandraYamlPath)
                                          .storageDir(instance.storageDir())
                                          .extraJvmOptions(defaultJvmProperties)
                                          .extraEnvironmentVariables(defaultEnvVars)
                                          .build();
    }

    /**
     * Checks whether a Cassandra instance is currently running as a local process
     * and automatically cleans up stale PID files.
     *
     * <p>Performs four validation steps:
     * <ol>
     * <li>Verifies the PID file exists and is readable. Returns false if not found.
     * <li>Reads the PID and checks if the process is alive. Returns false and deletes the
     * PID file if the process no longer exists or is not alive.
     * <li>Verifies the process is a Cassandra instance by checking for
     * {@code org.apache.cassandra.service.CassandraDaemon} in the command line. Returns true if
     * the command line contains the Cassandra daemon class or cannot be determined.
     * <li>If the process is running but is not a Cassandra process, returns false and deletes
     * the stale PID file.
     * </ol>
     *
     * @param instance the instance metadata containing host information
     * @return true if the instance is running as a Cassandra process, false otherwise
     */
    private boolean isCassandraProcessRunning(InstanceMetadata instance)
    {
        Path pidFilePath = Path.of(pidFileLocation(instance));
        try
        {
            // Case 1: PID file does not exist or is not readable
            if (!Files.isRegularFile(pidFilePath) || !Files.isReadable(pidFilePath))
            {
                LOGGER.warn("PID file does not exist or is not readable for instance {} at path {}", instance, pidFilePath);
                return false;
            }

            Long pid = readPidFromFile(pidFilePath);
            Optional<ProcessHandle> processHandle = ProcessHandle.of(pid);

            // Case 2: No process running with such PID or process is not alive
            if (processHandle.isEmpty() || !processHandle.get().isAlive())
            {
                LOGGER.warn("No running process found with PID {} for instance {}", pid, instance);
                deletePidFile(instance, pidFilePath);
                return false;
            }

            // Case 3: Process with such PID is running - check if it's a Cassandra process
            // If we can't determine the command line, we assume it's Cassandra
            Optional<String> cmdLine = getCommandLinePlatformIndependent(processHandle.get());
            if (cmdLine.isEmpty() || cmdLine.get().contains("org.apache.cassandra.service.CassandraDaemon"))
            {
                LOGGER.warn("Cassandra instance {} is running with PID {}", instance, pid);
                return true;
            }

            // Case 4: Process with such PID is running but it's not a Cassandra process
            LOGGER.warn("Process with PID {} for instance {} is not a Cassandra process (command line: {}).",
                        pid, instance, cmdLine);
            deletePidFile(instance, pidFilePath);
            return false;
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to read PID from file {} for instance {}", pidFilePath, instance, e);
            return false;
        }
    }

    protected static void deletePidFile(InstanceMetadata instance, Path pidFilePath)
    {
        try
        {
            LOGGER.info("Deleting stale PID file {} for instance {}", pidFilePath, instance);
            Files.delete(pidFilePath);
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to delete stale PID file {} for instance {}", pidFilePath, instance, e);
        }
    }

    /*
     * Due to JDK-8345117 java can sometimes truncate the command line returned by ProcessHandle.info().commandLine()
     * on some platforms (ie. Linux). To work around this, we use the 'ps' command to get the full command line.
     * This method should be platform-independent as it relies on the 'ps' command which is available on most Unix-like systems.
     * For non-Unix systems, we fall back to the default implementation.
     */
    protected static Optional<String> getCommandLinePlatformIndependent(ProcessHandle processHandle)
    {
        long pid = processHandle.pid();
        try
        {
            ProcessBuilder pb = new ProcessBuilder("ps", "-p", String.valueOf(pid), "-o", "args=");
            Process proc = pb.start();
            try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8)))
            {
                String line = reader.readLine();
                proc.waitFor(5, TimeUnit.SECONDS);
                if (line != null && !line.isEmpty())
                {
                    return Optional.of(line.trim());
                }
            }
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to get command line via ps for PID {}", pid, e);
        }
        // Fallback to default implementation
        return processHandle.info().commandLine();
    }

    @VisibleForTesting
    public static Long readPidFromFile(Path pidFilePath)
    {
        String pidFileContent = null;
        try
        {
            pidFileContent = Files.readString(pidFilePath, StandardCharsets.UTF_8).trim();
            return Long.parseLong(pidFileContent);
        }
        catch (NumberFormatException e)
        {
            throw new RuntimeException("Unable to parse PID from file: " + pidFilePath + " content: " + pidFileContent, e);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Failed to read PID from file: " + pidFilePath, e);
        }
    }

    protected String pidFileLocation(InstanceMetadata instance)
    {
        return pidFileLocation(lifecycleDir, instance.id());
    }

    protected Path stdoutLocation(InstanceMetadata instance)
    {
        return Path.of(lifecycleDir).resolve("start-cassandra-" + instance.id() + ".out");
    }

    protected Path stderrLocation(InstanceMetadata instance)
    {
        return Path.of(lifecycleDir).resolve("start-cassandra-" + instance.id() + ".err");
    }

    @VisibleForTesting
    public static String pidFileLocation(String lifecycleDir, int instanceId)
    {
        return Path.of(lifecycleDir).resolve("cassandra-" + instanceId + ".pid").toString();
    }
}
