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

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;

/**
 * Represents a snapshot of a configuration with its metadata.
 * The SHA-256 hash is dynamically computed from the configuration contents and cached.
 *
 * <p>{@code lastModified} tracks when the configuration content was last updated at its source.
 * Merging two snapshots takes the maximum of both timestamps because merging combines existing
 * content without producing new configuration.
 */
public class ConfigurationOverlaySnapshot
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationOverlaySnapshot.class);

    @NotNull
    private final Instant lastModified;

    @NotNull
    private final CassandraConfigurationOverlay configuration;

    private volatile String hash;

    public ConfigurationOverlaySnapshot(@NotNull Instant lastModified,
                                        @NotNull CassandraConfigurationOverlay configuration)
    {
        this.lastModified = Objects.requireNonNull(lastModified, "lastModified must not be null");
        this.configuration = Objects.requireNonNull(configuration, "configuration must not be null");
    }

    /**
     * Merges another snapshot on top of this one, producing the effective configuration.
     * The other snapshot's values take precedence over this snapshot's values.
     *
     * <p>Deep-merges {@code cassandraYaml} (nested objects are recursively merged, all other
     * types are replaced by the other snapshot's values). Merges {@code extraJvmOpts} with the
     * other snapshot's entries overriding this snapshot's entries on key conflict.
     *
     * <p>Overlays may introduce keys not present in the base snapshot. New keys in
     * {@code cassandraYaml} are added to the result, and new keys in {@code extraJvmOpts}
     * are added alongside existing entries.
     *
     * <p>When a conflicting boolean JVM option is detected (e.g. {@code -XX:+UseG1GC} in the base
     * and {@code -XX:-UseG1GC} in the overlay), the base option is preserved and the overlay's
     * conflicting entry is skipped. A warning is logged to alert the operator.
     *
     * @param other      the overlay snapshot whose values take precedence
     * @param instanceId the Cassandra instance id, used for contextual log messages
     * @return a new snapshot with the merged configuration and the max of both lastModified timestamps
     */
    @NotNull
    public ConfigurationOverlaySnapshot overlay(@NotNull ConfigurationOverlaySnapshot other, int instanceId)
    {
        JsonObject mergedYaml = ConfigUtils.mergeConfigurations(configuration.cassandraYaml(),
                                                                other.configuration().cassandraYaml());

        Map<String, String> mergedOpts = new LinkedHashMap<>(configuration.extraJvmOpts());
        for (Map.Entry<String, String> entry : other.configuration().extraJvmOpts().entrySet())
        {
            String key = entry.getKey();
            if (CassandraConfigurationOverlay.hasConflictingBooleanOpt(mergedOpts, key))
            {
                LOGGER.warn("Instance {}: Conflicting boolean JVM option '{}' in overlay conflicts with base " +
                            "option '{}'. Preserving base option and skipping overlay entry.",
                            instanceId, key, CassandraConfigurationOverlay.conflictingBooleanOpt(key));
                continue;
            }
            mergedOpts.put(key, entry.getValue());
        }

        Instant mergedLastModified = lastModified.isAfter(other.lastModified)
                                     ? lastModified
                                     : other.lastModified;

        CassandraConfigurationOverlay mergedOverlay = new CassandraConfigurationOverlay(mergedYaml, mergedOpts);
        return new ConfigurationOverlaySnapshot(mergedLastModified, mergedOverlay);
    }

    /**
     * Returns the SHA-256 hash of the configuration contents, prefixed with "sha256:".
     * Computed on first access and cached for subsequent calls.
     *
     * @return the content hash in the form "sha256:&lt;64 hex chars&gt;"
     */
    @NotNull
    public String hash()
    {
        if (hash == null)
        {
            hash = computeHash();
        }
        return hash;
    }

    @NotNull
    public Instant lastModified()
    {
        return lastModified;
    }

    @NotNull
    public CassandraConfigurationOverlay configuration()
    {
        return configuration;
    }

    /**
     * Returns a JSON representation of this snapshot, suitable for persistence.
     *
     * @return a new {@link JsonObject} representing this snapshot
     */
    @NotNull
    public JsonObject toJson()
    {
        return new JsonObject()
               .put("hash", hash())
               .put("lastModified", lastModified.toString())
               .put("configuration", configuration.toJson());
    }

    /**
     * Returns an empty snapshot with {@link Instant#EPOCH} as the last modified time,
     * an empty {@code cassandraYaml}, and no extra JVM options.
     *
     * @return an empty configuration snapshot
     */
    @NotNull
    public static ConfigurationOverlaySnapshot emptySnapshot()
    {
        return new ConfigurationOverlaySnapshot(Instant.EPOCH, new CassandraConfigurationOverlay(null, null));
    }

    /**
     * Creates a {@link ConfigurationOverlaySnapshot} from its JSON representation.
     *
     * @param json the JSON object containing {@code lastModified} and {@code configuration}
     * @return a new snapshot instance
     */
    @NotNull
    public static ConfigurationOverlaySnapshot fromJson(@NotNull JsonObject json)
    {
        Instant lastModified = Instant.parse(json.getString("lastModified"));
        CassandraConfigurationOverlay configuration =
                CassandraConfigurationOverlay.fromJson(json.getJsonObject("configuration"));
        return new ConfigurationOverlaySnapshot(lastModified, configuration);
    }

    private String computeHash()
    {
        try
        {
            byte[] bytes = configuration.toJson().toBuffer().getBytes();
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashBytes = digest.digest(bytes);
            return "sha256:" + bytesToHex(hashBytes);
        }
        catch (NoSuchAlgorithmException e)
        {
            throw new RuntimeException("Failed to compute configuration hash", e);
        }
    }

    private static String bytesToHex(byte[] bytes)
    {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes)
        {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        ConfigurationOverlaySnapshot that = (ConfigurationOverlaySnapshot) o;
        return Objects.equals(lastModified, that.lastModified)
               && Objects.equals(configuration, that.configuration);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified, configuration);
    }

    @Override
    public String toString()
    {
        return toJson()
               .encodePrettily();
    }
}
