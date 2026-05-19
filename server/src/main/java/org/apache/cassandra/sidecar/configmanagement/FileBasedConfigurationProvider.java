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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * File-based implementation of {@link ConfigurationProvider} that persists configuration
 * overlays as JSON files within a configuration store directory.
 *
 * <p>Each instance's overlay is stored at {@code {configDir}/{instanceId}/config.json}.
 * Writes are atomic (write to temp file, then rename) to prevent corruption from crashes.
 */
public class FileBasedConfigurationProvider implements ConfigurationProvider
{
    private static final String CONFIG_FILE_NAME = "overlay.json";
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            .enable(SerializationFeature.INDENT_OUTPUT);

    private final Path configDir;
    private final ConcurrentHashMap<Integer, Object> locks = new ConcurrentHashMap<>();

    public FileBasedConfigurationProvider(Path configDir)
    {
        this.configDir = Objects.requireNonNull(configDir, "configDir must not be null");
    }

    @Override
    @Nullable
    public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
    {
        Path configFile = resolveConfigFile(instance);
        if (!Files.exists(configFile))
        {
            return null;
        }
        try
        {
            return MAPPER.readValue(configFile.toFile(), ConfigurationOverlaySnapshot.class);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to read configuration overlay for instance " + instance.id(), e);
        }
    }

    @Override
    public boolean storeOverlay(InstanceMetadata instance,
                                @Nullable String originalHash,
                                @NotNull ConfigurationOverlaySnapshot newSnapshot)
    {
        Objects.requireNonNull(newSnapshot, "newSnapshot must not be null");
        Object lock = locks.computeIfAbsent(instance.id(), k -> new Object());
        synchronized (lock)
        {
            ConfigurationOverlaySnapshot current = getOverlay(instance);

            if (current == null && originalHash != null)
            {
                return false;
            }

            if (current != null && (originalHash == null || !current.hash().equals(originalHash)))
            {
                return false;
            }

            Path configFile = resolveConfigFile(instance);
            Path tempFile = null;
            try
            {
                Files.createDirectories(configFile.getParent());
                tempFile = Files.createTempFile(configFile.getParent(), "config", ".tmp");
                MAPPER.writeValue(tempFile.toFile(), newSnapshot);
                Files.move(tempFile, configFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                return true;
            }
            catch (IOException e)
            {
                if (tempFile != null)
                {
                    try
                    {
                        Files.deleteIfExists(tempFile);
                    }
                    catch (IOException suppressed)
                    {
                        e.addSuppressed(suppressed);
                    }
                }
                throw new UncheckedIOException("Failed to store configuration overlay for instance " + instance.id(), e);
            }
        }
    }

    private Path resolveConfigFile(InstanceMetadata instance)
    {
        return configDir.resolve(String.valueOf(instance.id())).resolve(CONFIG_FILE_NAME);
    }
}
