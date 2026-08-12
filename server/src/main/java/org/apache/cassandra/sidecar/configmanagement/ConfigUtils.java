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
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Utility methods for configuration operations: YAML loading and deep merge.
 */
public final class ConfigUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtils.class);

    static final YAMLFactory YAML_FACTORY = new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER);

    private ConfigUtils()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Loads configuration from the given YAML path, returning the cached snapshot if the file
     * has not been modified since it was last read.
     *
     * @param yamlPath the path to the YAML file, or {@code null} for an empty snapshot
     * @param cached   a previously loaded snapshot to reuse if the file is unchanged, or {@code null}
     * @return the cached snapshot if still valid, or a freshly loaded snapshot
     */
    public static ConfigurationOverlaySnapshot loadConfiguration(@Nullable Path yamlPath,
                                                                  @Nullable ConfigurationOverlaySnapshot cached)
    {
        if (yamlPath == null)
        {
            return ConfigurationOverlaySnapshot.emptySnapshot();
        }
        try
        {
            Instant lastModifiedBefore = Files.getLastModifiedTime(yamlPath).toInstant();
            if (cached != null && cached.lastModified().equals(lastModifiedBefore))
            {
                return cached;
            }
            JsonObject yaml = loadYaml(yamlPath);
            Instant lastModifiedAfter = Files.getLastModifiedTime(yamlPath).toInstant();
            if (!lastModifiedBefore.equals(lastModifiedAfter))
            {
                throw new IllegalStateException("File was modified while reading: " + yamlPath);
            }
            CassandraConfigurationOverlay overlay = new CassandraConfigurationOverlay(yaml, Collections.emptyMap());
            return new ConfigurationOverlaySnapshot(lastModifiedAfter, overlay);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to read modification time of " + yamlPath, e);
        }
    }

    /**
     * Loads a YAML file into a Vert.x {@link JsonObject}.
     *
     * @param yamlPath path to the YAML file
     * @return the parsed configuration as a JsonObject
     */
    @SuppressWarnings("unchecked")
    public static JsonObject loadYaml(Path yamlPath)
    {
        try (JsonParser parser = YAML_FACTORY.createParser(yamlPath.toFile()))
        {
            Map<String, Object> map = DatabindCodec.mapper().readValue(parser, Map.class);
            return map != null ? new JsonObject(map) : new JsonObject();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException("Failed to load YAML from " + yamlPath, e);
        }
    }

    /**
     * Deep-merges the overlay onto the base configuration. For nested objects both base and overlay
     * contain, fields are merged recursively. For all other node types (scalars, arrays, nulls),
     * the overlay value replaces the base value. The base node is not modified.
     *
     * <p>Overlays may introduce keys not present in the base configuration. Such keys are
     * added to the result as-is (scalars, arrays) or merged recursively (nested objects).
     *
     * @param base    the base configuration tree
     * @param overlay the overlay tree whose values take precedence
     * @return a new tree with the merged result
     */
    public static JsonObject mergeConfigurations(@NotNull JsonObject base, @NotNull JsonObject overlay)
    {
        Objects.requireNonNull(base, "base must not be null");
        Objects.requireNonNull(overlay, "overlay must not be null");
        JsonObject result = base.copy();
        result.mergeIn(overlay, true);
        return result;
    }

    /**
     * Merges overlay JVM options onto base JVM options, with overlay entries overriding base entries
     * on key conflict. When an overlay entry conflicts with a base boolean option (e.g. base
     * {@code -XX:+UseG1GC} and overlay {@code -XX:-UseG1GC}), the base option is preserved and the
     * overlay entry is skipped, matching {@link ConfigurationOverlaySnapshot#overlay}. Insertion order
     * is preserved. Neither input map is modified.
     *
     * @param base    the base JVM options
     * @param overlay the overlay JVM options whose values take precedence
     * @return a new map with the merged result
     */
    @NotNull
    public static Map<String, String> mergeOpts(@NotNull Map<String, String> base,
                                                @NotNull Map<String, String> overlay)
    {
        Objects.requireNonNull(base, "base must not be null");
        Objects.requireNonNull(overlay, "overlay must not be null");
        Map<String, String> merged = new LinkedHashMap<>(base);
        for (Map.Entry<String, String> entry : overlay.entrySet())
        {
            if (CassandraConfigurationOverlay.hasConflictingBooleanOpt(merged, entry.getKey()))
            {
                LOGGER.warn("Conflicting boolean JVM option '{}' in overlay conflicts with base option '{}'. " +
                            "Preserving base option and skipping overlay entry.",
                            entry.getKey(), CassandraConfigurationOverlay.conflictingBooleanOpt(entry.getKey()));
                continue; // preserve base boolean option, skip conflicting overlay entry
            }
            merged.put(entry.getKey(), entry.getValue());
        }
        return merged;
    }
}
