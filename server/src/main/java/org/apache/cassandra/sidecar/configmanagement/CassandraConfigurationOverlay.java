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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a configuration overlay - a sparse set of configuration values that overwrite base template
 * values or add new configuration attributes.
 *
 * <p>The {@code cassandraYaml} field is a version-agnostic JSON representation of {@code cassandra.yaml}
 * settings. It may contain settings from any Cassandra version supported by Sidecar (4.0, 4.1, 5.0, etc.).
 * No version-specific validation is performed by this class; validation against a version-aware schema is
 * the responsibility of the Configuration Manager.
 *
 * <p>The {@code extraJvmOpts} field contains JVM options that are appended to the Cassandra JVM startup
 * command. These are opaque strings not subject to schema validation.
 */
public class CassandraConfigurationOverlay
{
    @NotNull
    private final JsonObject cassandraYaml;

    @NotNull
    private final List<String> extraJvmOpts;

    public CassandraConfigurationOverlay(@Nullable JsonObject cassandraYaml,
                                         @Nullable List<String> extraJvmOpts)
    {
        this.cassandraYaml = cassandraYaml != null ? cassandraYaml.copy() : new JsonObject();
        this.extraJvmOpts = extraJvmOpts != null
                            ? List.copyOf(extraJvmOpts)
                            : Collections.emptyList();
    }

    /**
     * Returns the cassandra.yaml overlay as a version-agnostic JSON object. Callers must not mutate the
     * returned object; use {@link #updated} to produce a new overlay with changes applied.
     *
     * @return the cassandra.yaml overlay as a version-agnostic JSON object
     */
    @NotNull
    public JsonObject cassandraYaml()
    {
        return cassandraYaml;
    }

    /**
     * @return an unmodifiable list of extra JVM options
     */
    @NotNull
    public List<String> extraJvmOpts()
    {
        return extraJvmOpts;
    }

    /**
     * Returns a JSON representation of this overlay.
     *
     * @return a new {@link JsonObject} containing {@code cassandraYaml} and {@code extraJvmOpts}
     */
    @NotNull
    public JsonObject toJson()
    {
        return new JsonObject()
               .put("cassandraYaml", cassandraYaml.copy())
               .put("extraJvmOpts", new JsonArray(new ArrayList<>(extraJvmOpts)));
    }

    /**
     * Returns a new overlay with the given updates applied. The current instance is not modified.
     *
     * @param cassandraYamlUpdates field-level changes to cassandra.yaml: key = field name, value = new value.
     *                             A {@code null} value removes the field. Pass {@code null} for no yaml changes.
     * @param addJvmOpts           JVM options to append to the current list. Pass {@code null} for no additions.
     * @param removeJvmOpts        JVM options to remove by value. Pass {@code null} for no removals.
     * @return a new overlay with the updates applied
     */
    @NotNull
    public CassandraConfigurationOverlay updated(@Nullable Map<String, Object> cassandraYamlUpdates,
                                                 @Nullable List<String> addJvmOpts,
                                                 @Nullable List<String> removeJvmOpts)
    {
        JsonObject mergedYaml = cassandraYaml.copy();
        if (cassandraYamlUpdates != null)
        {
            for (Map.Entry<String, Object> entry : cassandraYamlUpdates.entrySet())
            {
                if (entry.getValue() == null)
                {
                    mergedYaml.remove(entry.getKey());
                }
                else
                {
                    mergedYaml.put(entry.getKey(), entry.getValue());
                }
            }
        }

        List<String> mergedOpts = new ArrayList<>(extraJvmOpts);
        if (removeJvmOpts != null)
        {
            mergedOpts.removeAll(removeJvmOpts);
        }
        if (addJvmOpts != null)
        {
            mergedOpts.addAll(addJvmOpts);
        }

        return new CassandraConfigurationOverlay(mergedYaml, mergedOpts);
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
        CassandraConfigurationOverlay that = (CassandraConfigurationOverlay) o;
        return Objects.equals(cassandraYaml, that.cassandraYaml)
               && Objects.equals(extraJvmOpts, that.extraJvmOpts);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cassandraYaml, extraJvmOpts);
    }

    @Override
    public String toString()
    {
        return toJson().encodePrettily();
    }
}
