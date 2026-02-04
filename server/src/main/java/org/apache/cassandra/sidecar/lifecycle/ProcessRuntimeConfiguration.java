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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.apache.cassandra.sidecar.common.DataObjectBuilder;
import org.apache.cassandra.sidecar.exceptions.ConfigurationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Represents the configuration for a Cassandra process instance.
 */
public class ProcessRuntimeConfiguration
{
    private final InstanceMetadata instance;
    @NotNull
    private final Path cassandraHome;
    private final Path cassandraYaml;
    @VisibleForTesting
    @NotNull
    final Path cassandraConfDir;
    @VisibleForTesting
    @Nullable
    final String cassandraLogDir;
    @VisibleForTesting
    @Nullable
    final String storageDir;
    private final Map<String, String> extraJvmOptions;
    private final Map<String, String> extraEnvironmentVariables;

    protected ProcessRuntimeConfiguration(Builder builder)
    {
        instance = builder.instance;
        cassandraHome = Path.of(builder.cassandraHome);
        cassandraConfDir = Path.of(builder.cassandraConfDir);
        cassandraLogDir = builder.cassandraLogDir;
        cassandraYaml = builder.cassandraYamlPath == null ? cassandraConfDir.resolve("cassandra.yaml") : Path.of(builder.cassandraYamlPath);
        extraEnvironmentVariables = builder.extraEnvironmentVariables == null ? Map.of() : Map.copyOf(builder.extraEnvironmentVariables);
        storageDir = builder.storageDir;
        extraJvmOptions = builder.extraJvmOptions == null ? Map.of() : Map.copyOf(builder.extraJvmOptions);
    }

    public InstanceMetadata instance()
    {
        return instance;
    }

    public Path cassandraHome()
    {
        return cassandraHome;
    }

    public String cassandraConf()
    {
        return cassandraConfDir.toString();
    }

    public Path cassandraBin()
    {
        return cassandraHome.resolve("bin").resolve("cassandra");
    }

    public Path cassandraYaml()
    {
        return cassandraYaml;
    }

    public void validateStart() throws ConfigurationException
    {
        // Check existence
        if (!Files.isDirectory(cassandraHome))
        {
            throw new ConfigurationException("Cassandra home does not exist or is not a directory: " + cassandraHome);
        }
        if (!Files.isDirectory(cassandraConfDir))
        {
            throw new ConfigurationException("Cassandra configuration directory does not exist or is not a directory: " + cassandraConfDir);
        }
        if (!Files.isRegularFile(cassandraYaml()))
        {
            throw new ConfigurationException("Cassandra YAML configuration file does not exist: " + cassandraYaml());
        }
        if (!Files.isRegularFile(cassandraBin()))
        {
            throw new ConfigurationException("Cassandra binary does not exist or is not a regular file: " + cassandraBin());
        }
        // Check permissions
        if (!Files.isExecutable(cassandraBin()))
        {
            throw new ConfigurationException("Cassandra binary is not executable: " + cassandraBin());
        }
        if (!Files.isReadable(cassandraConfDir))
        {
            throw new ConfigurationException("Cassandra configuration directory is not readable: " + cassandraConfDir);
        }
    }

    public ProcessBuilder buildStartCommand(String pidFileLocation, Path stdoutFileLocation, Path stderrFileLocation)
    {
        validateStart();

        List<String> startCassandraCmd = new ArrayList<>();
        startCassandraCmd.add(cassandraBin().toString());
        startCassandraCmd.add("-p");
        startCassandraCmd.add(pidFileLocation);
        for (Map.Entry<String, String> jvmOpt : extraJvmOptions.entrySet())
        {
            startCassandraCmd.add("-D" + jvmOpt.getKey() + "=" + jvmOpt.getValue());
        }

        // Override storage dir if present in sidecar configuration
        if (storageDir != null)
        {
            startCassandraCmd.add("-D");
            startCassandraCmd.add("cassandra.storagedir=" + storageDir);
        }

        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(startCassandraCmd);

        // Set environment variables
        Map<String, String> env = processBuilder.environment();
        env.put("CASSANDRA_HOME", cassandraHome().toString());
        env.put("CASSANDRA_CONF", cassandraConf());
        env.putAll(extraEnvironmentVariables);

        // Only override CASSANDRA_LOG_DIR if it is set in the configuration
        if (cassandraLogDir != null)
        {
            env.put("CASSANDRA_LOG_DIR", cassandraLogDir);
        }

        // Redirect output to logs
        processBuilder.redirectOutput(ProcessBuilder.Redirect.to(stdoutFileLocation.toFile()));
        processBuilder.redirectError(ProcessBuilder.Redirect.to(stderrFileLocation.toFile()));

        // Set working directory
        processBuilder.directory(cassandraHome().toFile());
        return processBuilder;
    }

    @Override
    public String toString()
    {
        return "ProcessRuntimeConfiguration{" +
               "instance=" + instance +
               ", cassandraHome=" + cassandraHome +
               ", cassandraConfDir=" + cassandraConfDir +
               ", cassandraLogDir='" + cassandraLogDir + '\'' +
               ", storageDir='" + storageDir + '\'' +
               ", extraJvmOpts=" + extraJvmOptions +
               ", extraEnvVars=" + extraEnvironmentVariables +
               '}';
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder for constructing {@link ProcessRuntimeConfiguration} instances.
     */
    public static final class Builder implements DataObjectBuilder<Builder, ProcessRuntimeConfiguration>
    {
        private InstanceMetadata instance;
        private String cassandraHome;
        private String cassandraConfDir;
        private @Nullable String cassandraLogDir;
        private @Nullable String cassandraYamlPath;
        private @Nullable String storageDir;
        private Map<String, String> extraJvmOptions;
        private Map<String, String> extraEnvironmentVariables;

        private Builder()
        {
        }

        @Override
        public Builder self()
        {
            return this;
        }

        public Builder instance(InstanceMetadata instance)
        {
            return update(b -> b.instance = Objects.requireNonNull(instance, "instance is required"));
        }

        public Builder cassandraHome(@NotNull String cassandraHome)
        {
            return update(b -> b.cassandraHome = Objects.requireNonNull(cassandraHome, "cassandraHome is required"));
        }

        public Builder cassandraConfDir(@NotNull String cassandraConfDir)
        {
            return update(b -> b.cassandraConfDir = Objects.requireNonNull(cassandraConfDir, "cassandraConfDir is required"));
        }

        public Builder cassandraLogDir(@Nullable String cassandraLogDir)
        {
            return update(b -> b.cassandraLogDir = cassandraLogDir);
        }

        public Builder cassandraYamlPath(@Nullable String cassandraYamlPath)
        {
            return update(b -> b.cassandraYamlPath = cassandraYamlPath);
        }

        public Builder storageDir(@Nullable String storageDir)
        {
            return update(b -> b.storageDir = storageDir);
        }

        public Builder extraJvmOptions(Map<String, String> extraJvmOptions)
        {
            return update(b -> b.extraJvmOptions = extraJvmOptions);
        }

        public Builder extraEnvironmentVariables(Map<String, String> extraEnvironmentVariables)
        {
            return update(b -> b.extraEnvironmentVariables = extraEnvironmentVariables);
        }

        @Override
        public ProcessRuntimeConfiguration build()
        {
            return new ProcessRuntimeConfiguration(this);
        }
    }
}
