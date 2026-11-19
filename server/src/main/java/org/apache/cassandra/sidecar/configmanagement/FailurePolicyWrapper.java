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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.sidecar.cluster.instance.InstanceMetadata;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A decorator around {@link ConfigurationProvider} that applies a configurable
 * {@link FailurePolicy} when the downstream provider is unavailable.
 *
 * <p>On successful calls to the delegate, the result is cached locally via a
 * {@link FileBasedConfigurationProvider} writing to {@code cached_overlay.json}.
 * When the delegate fails, behavior is governed by the configured failure policy.
 *
 * <p>Only failures indicating the delegate could not be reached activate the failure policy; any
 * other failure is propagated to the caller unchanged. See {@link #isUnavailable(Throwable)}.
 */
public class FailurePolicyWrapper implements ConfigurationProvider
{
    private static final Logger LOGGER = LoggerFactory.getLogger(FailurePolicyWrapper.class);
    static final String CACHED_OVERLAY_FILE_NAME = "cached_overlay.json";

    private final ConfigurationProvider delegate;
    private final FileBasedConfigurationProvider cache;
    private final FailurePolicy failurePolicy;

    public FailurePolicyWrapper(ConfigurationProvider delegate,
                                Path cacheDir,
                                FailurePolicy failurePolicy)
    {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
        this.failurePolicy = Objects.requireNonNull(failurePolicy, "failurePolicy must not be null");
        Objects.requireNonNull(cacheDir, "cacheDir must not be null");
        this.cache = new FileBasedConfigurationProvider(cacheDir, CACHED_OVERLAY_FILE_NAME);
    }

    /**
     * Wraps a provider with failure policy handling. If the delegate is already a
     * {@link FileBasedConfigurationProvider}, it is returned directly (local providers
     * cannot be "unavailable" in the network sense).
     *
     * @param delegate      the downstream configuration provider
     * @param cacheDir      path to the configuration store directory for caching
     * @param failurePolicy the failure policy to apply
     * @return the delegate directly if it is file-based, or a wrapped instance otherwise
     */
    public static ConfigurationProvider wrap(ConfigurationProvider delegate,
                                             Path cacheDir,
                                             FailurePolicy failurePolicy)
    {
        if (delegate instanceof FileBasedConfigurationProvider)
        {
            return delegate;
        }
        return new FailurePolicyWrapper(delegate, cacheDir, failurePolicy);
    }

    @Override
    @Nullable
    public ConfigurationOverlaySnapshot getOverlay(InstanceMetadata instance)
    {
        ConfigurationOverlaySnapshot result;
        try
        {
            result = delegate.getOverlay(instance);
        }
        catch (Exception e)
        {
            return handleReadFailure(instance, e);
        }

        // A null result means the overlay was deleted upstream. Cache an empty overlay (rather
        // than leaving the stale entry) so a deleted overlay is not resurrected from cache during
        // a later outage. An empty overlay merges to a no-op, so it is equivalent to no overlay.
        updateCache(instance, result != null ? result : ConfigurationOverlaySnapshot.emptySnapshot());
        return result;
    }

    @Override
    public boolean storeOverlay(InstanceMetadata instance,
                                @Nullable String originalHash,
                                @NotNull ConfigurationOverlaySnapshot newSnapshot)
    {
        boolean stored;
        try
        {
            stored = delegate.storeOverlay(instance, originalHash, newSnapshot);
        }
        catch (Exception e)
        {
            return handleWriteFailure(instance, originalHash, newSnapshot, e);
        }

        if (stored)
        {
            updateCache(instance, newSnapshot);
        }
        return stored;
    }

    /**
     * @param failure the exception thrown by the delegate
     * @return {@code true} if the failure indicates the provider could not be reached
     */
    static boolean isUnavailable(Throwable failure)
    {
        // The provider's explicit outage signal.
        if (failure instanceof ConfigurationProviderUnavailableException)
        {
            return true;
        }

        // Domain errors and provider bugs must be surfaced to the client. Anything else — IOException,
        // socket and timeout failures, provider client exceptions — is considered an outage.
        return !(failure instanceof ConfigurationManagerException
                 || failure instanceof IllegalArgumentException
                 || failure instanceof NullPointerException
                 || failure instanceof IllegalStateException
                 || failure instanceof UnsupportedOperationException);
    }

    private static void rethrowUnlessUnavailable(Exception failure)
    {
        if (failure instanceof RuntimeException && !isUnavailable(failure))
        {
            throw (RuntimeException) failure;
        }
    }

    @NotNull
    private ConfigurationOverlaySnapshot handleReadFailure(InstanceMetadata instance, Exception cause)
    {
        rethrowUnlessUnavailable(cause);

        if (failurePolicy == FailurePolicy.FAIL)
        {
            throw new ConfigurationProviderUnavailableException(
                    "Configuration provider is unavailable; reads are rejected under FAIL policy", cause);
        }
        LOGGER.warn("Configuration provider unavailable for instance {}; falling back to cached overlay",
                    instance.id(), cause);

        ConfigurationOverlaySnapshot cached = cache.getOverlay(instance);
        if (cached == null)
        {
            // null means no read has been cached yet, so there is nothing to serve; a cached empty
            // overlay would instead mean the provider has no overlay for this instance.
            throw new ConfigurationProviderUnavailableException(
                    "Configuration provider is unavailable and no overlay is cached for instance "
                    + instance.id(), cause);
        }
        return cached;
    }

    private boolean handleWriteFailure(InstanceMetadata instance,
                                       @Nullable String originalHash,
                                       @NotNull ConfigurationOverlaySnapshot newSnapshot,
                                       Exception cause)
    {
        rethrowUnlessUnavailable(cause);

        switch (failurePolicy)
        {
            case FAIL:
                throw new ConfigurationProviderUnavailableException(
                        "Configuration provider is unavailable; writes are rejected under FAIL policy", cause);
            case CACHED_READ_ONLY:
                throw new ConfigurationProviderUnavailableException(
                        "Configuration provider is unavailable; writes are rejected under CACHED_READ_ONLY policy",
                        cause);
            case CACHED_READ_WRITE:
                LOGGER.warn("Configuration provider unavailable for instance {}; writing to cached overlay",
                            instance.id(), cause);
                return cache.storeOverlay(instance, originalHash, newSnapshot);
            default:
                throw new IllegalStateException("Unknown failure policy: " + failurePolicy);
        }
    }

    /**
     * Best-effort update of the local cache after a successful delegate operation.
     * Cache failures (e.g. disk full, permission errors, corrupt cache file) are logged
     * and swallowed so they do not fail an operation the delegate already completed.
     */
    private void updateCache(InstanceMetadata instance, ConfigurationOverlaySnapshot snapshot)
    {
        try
        {
            ConfigurationOverlaySnapshot cached = cache.getOverlay(instance);
            String cachedHash = cached != null ? cached.hash() : null;
            // Skip the disk write when the cache already holds the same content.
            if (snapshot.hash().equals(cachedHash))
            {
                return;
            }
            if (!cache.storeOverlay(instance, cachedHash, snapshot))
            {
                LOGGER.debug("Cache update skipped for instance {} due to concurrent modification", instance.id());
            }
        }
        catch (Exception e)
        {
            LOGGER.warn("Failed to update cached overlay for instance {}; delegate operation succeeded",
                        instance.id(), e);
        }
    }
}
