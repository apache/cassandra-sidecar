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
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Manages configuration of Cassandra instances via Sidecar.
 *
 * <p>The <em>effective configuration</em> is the result of merging the base cassandra.yaml template
 * with the instance overlay retrieved from the {@link ConfigurationProvider}. It is what the instance
 * actually sees; the overlay is only the delta persisted on top of the base template.
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
     * Computes the effective configuration for the given instance.
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

    /**
     * Patches the effective configuration for the given instance using RFC 6902-inspired JSON Patch
     * operations. Validates the caller's expected hash against the current effective configuration,
     * validates and applies the patch operations to the overlay, persists via the
     * {@link ConfigurationProvider}, and returns the new effective configuration.
     *
     * <p>Paths reference the effective configuration structure, but mutations target the overlay.
     * For nested paths within a top-level {@code cassandraYaml} key,
     * the entire top-level key value is copied from the effective config into the overlay before
     * applying the leaf change (copy-siblings strategy). This ensures the overlay is always
     * self-contained at the top-level key granularity. As a consequence, editing a nested leaf pins
     * its sibling leaves against later base-template drift, and removing an overlaid leaf reverts it to
     * the current base value; see {@link ConfigurationPatchApplier} for details.
     *
     * <p>All operations are validated atomically: either all succeed or none are applied.
     *
     * @param instance     the Cassandra instance metadata
     * @param expectedHash the hash of the effective configuration as last seen by the caller
     * @param operations   the patch operations to apply
     * @return a snapshot of the new effective configuration with its SHA-256 hash and last modified timestamp
     * @throws ConfigurationConflictException if the expectedHash does not match the current effective hash
     * @throws ConfigurationPatchException    if any patch operation fails validation or application
     * @throws ConfigurationManagerException  if the provider fails during the operation
     */
    @NotNull
    public ConfigurationOverlaySnapshot patchConfiguration(@NotNull InstanceMetadata instance,
                                                           @NotNull String expectedHash,
                                                           @NotNull List<ConfigurationPatchOperation> operations)
    {
        Objects.requireNonNull(instance, "instance must not be null");
        Objects.requireNonNull(expectedHash, "expectedHash must not be null");
        Objects.requireNonNull(operations, "operations must not be null");

        try
        {
            // 1. Validate operations structure (path format, value presence, duplicates)
            List<ConfigurationPatchValidator.ParsedPatchOperation> parsedOps =
                    ConfigurationPatchValidator.validate(operations);

            // 2. Read current state and validate the caller's hash matches
            ConfigurationOverlaySnapshot baseSnapshot = getBaseSnapshot();
            ConfigurationOverlaySnapshot currentOverlay = provider.getOverlay(instance);

            ConfigurationOverlaySnapshot effectiveConfig = currentOverlay != null
                                                           ? baseSnapshot.overlay(currentOverlay, instance.id())
                                                           : baseSnapshot;

            if (!expectedHash.equals(effectiveConfig.hash()))
            {
                throw new ConfigurationConflictException(expectedHash, effectiveConfig.hash());
            }

            // 3. Apply patch operations to the overlay (precondition checks + mutations)
            CassandraConfigurationOverlay currentOverlayConfig = currentOverlay != null
                                                                 ? currentOverlay.configuration()
                                                                 : new CassandraConfigurationOverlay(null, null);
            CassandraConfigurationOverlay updatedOverlay =
                    ConfigurationPatchApplier.apply(parsedOps, baseSnapshot.configuration(), currentOverlayConfig);
            ConfigurationOverlaySnapshot newOverlaySnapshot = new ConfigurationOverlaySnapshot(Instant.now(),
                                                                                               updatedOverlay);

            // 4. CAS via provider — concurrent patches are resolved by the provider's own atomicity
            String currentOverlayHash = currentOverlay != null ? currentOverlay.hash() : null;
            boolean stored = provider.storeOverlay(instance, currentOverlayHash, newOverlaySnapshot);

            // 5. Store rejected — re-read to determine if this is a true conflict or an unexpected failure
            if (!stored)
            {
                ConfigurationOverlaySnapshot updatedCurrent = provider.getOverlay(instance);
                ConfigurationOverlaySnapshot newEffective = updatedCurrent != null
                                                            ? baseSnapshot.overlay(updatedCurrent, instance.id())
                                                            : baseSnapshot;
                if (!expectedHash.equals(newEffective.hash()))
                {
                    throw new ConfigurationConflictException(expectedHash, newEffective.hash());
                }
                throw new ConfigurationManagerException(
                        "Provider rejected the overlay store unexpectedly", null);
            }

            return baseSnapshot.overlay(newOverlaySnapshot, instance.id());
        }
        catch (ConfigurationManagerException e)
        {
            throw e;
        }
        catch (Exception e)
        {
            throw new ConfigurationManagerException("Failed to patch configuration", e);
        }
    }
}
