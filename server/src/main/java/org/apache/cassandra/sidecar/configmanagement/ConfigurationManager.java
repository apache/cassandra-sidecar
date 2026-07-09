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

import java.nio.file.Path;
import java.util.Objects;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Manages configuration of Cassandra instances via Sidecar
 */
public class ConfigurationManager
{
    private final ConfigurationProvider provider;
    @Nullable
    private final Path baseTemplatePath;

    @Nullable
    private volatile ConfigurationOverlaySnapshot cachedBaseSnapshot;

    /**
     * @param provider         the configuration provider for fetching overlays
     * @param baseTemplatePath path to the base cassandra.yaml template, or {@code null} for an empty base
     */
    public ConfigurationManager(ConfigurationProvider provider, @Nullable Path baseTemplatePath)
    {
        this.provider = Objects.requireNonNull(provider, "provider must not be null");
        this.baseTemplatePath = baseTemplatePath;
    }

    /**
     * Computes the effective configuration for the given instance by merging the base template
     * with the overlay from the {@link ConfigurationProvider}.
     *
     * @param instance the Cassandra instance metadata
     * @return a snapshot of the effective configuration with its SHA-256 hash and last modified timestamp
     * @throws ConfigurationManagerException if the provider fails to retrieve the overlay
     */
    @NotNull
    public ConfigurationOverlaySnapshot getEffectiveConfiguration(InstanceMetadata instance)
    {
        ConfigurationOverlaySnapshot baseSnapshot = getBaseSnapshot();

        ConfigurationOverlaySnapshot providerSnapshot;
        try
        {
            providerSnapshot = provider.getOverlay(instance);
        }
        catch (Exception e)
        {
            throw new ConfigurationManagerException(
                    "Failed to retrieve configuration overlay from provider", e);
        }

        if (providerSnapshot != null)
        {
            return baseSnapshot.overlay(providerSnapshot, instance.id());
        }
        return baseSnapshot;
    }

    private ConfigurationOverlaySnapshot getBaseSnapshot()
    {
        ConfigurationOverlaySnapshot snapshot = ConfigUtils.loadConfiguration(baseTemplatePath, cachedBaseSnapshot);
        cachedBaseSnapshot = snapshot;
        return snapshot;
    }
}
