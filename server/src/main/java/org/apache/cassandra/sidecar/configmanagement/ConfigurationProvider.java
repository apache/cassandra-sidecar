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

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Provides storage and retrieval of configuration overlays for Cassandra instances.
 *
 * <p>The provider is a pluggable abstraction that decouples configuration storage from the
 * Configuration Manager. Implementations may persist overlays locally (files), remotely
 * (etcd, Consul, HTTP APIs), or in-memory (for testing).
 *
 * <p>The provider stores version-agnostic overlays and does not perform version-specific
 * validation or merge logic. Validation against a version-aware schema and computing updated
 * overlays (via {@link ConfigurationPatchApplier}) are the responsibility of the
 * Configuration Manager.
 */
public interface ConfigurationProvider
{
    /**
     * Retrieve the configuration overlay for the given Cassandra instance.
     *
     * @param instance the Cassandra instance metadata
     * @return the configuration overlay snapshot, or {@code null} if no overlay exists for the instance
     */
    @Nullable
    ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance);

    /**
     * Atomically store a new configuration overlay snapshot for the given instance,
     * subject to hash-based optimistic concurrency control.
     *
     * <p>The caller is responsible for computing the new snapshot (via
     * {@link ConfigurationPatchApplier}). The provider only validates the original
     * hash against the currently stored version and persists the result.
     *
     * @param instance     the Cassandra instance metadata
     * @param originalHash the overlay hash from the previously read snapshot,
     *                     or {@code null} if no overlay existed at the time of the read
     * @param newSnapshot  the new snapshot to store
     * @return {@code true} if the snapshot was stored successfully (hash matched),
     *         {@code false} if a conflict was detected (hash mismatch)
     */
    boolean storeOverlay(InstanceMetadata instance,
                         @Nullable String originalHash,
                         @NotNull ConfigurationOverlaySnapshot newSnapshot);
}
