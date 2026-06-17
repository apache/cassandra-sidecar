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

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * In-memory implementation of {@link ConfigurationProvider} for testing and as a reference implementation.
 * Stores configuration overlays in a {@link ConcurrentHashMap} keyed by instance ID.
 */
public class InMemoryConfigurationProvider implements ConfigurationProvider
{
    private final ConcurrentHashMap<Integer, ConfigurationOverlaySnapshot> overlays = new ConcurrentHashMap<>();

    @Override
    @Nullable
    public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
    {
        return overlays.get(instance.id());
    }

    @Override
    public boolean storeOverlay(InstanceMetadata instance,
                                @Nullable String originalHash,
                                @NotNull ConfigurationOverlaySnapshot newSnapshot)
    {
        Objects.requireNonNull(newSnapshot, "newSnapshot must not be null");
        return overlays.compute(instance.id(), (k, current) -> {
            if (current == null && originalHash != null)
            {
                return null;
            }

            if (current != null && (originalHash == null || !current.hash().equals(originalHash)))
            {
                return current;
            }
            return newSnapshot;
        }) == newSnapshot;
    }
}
