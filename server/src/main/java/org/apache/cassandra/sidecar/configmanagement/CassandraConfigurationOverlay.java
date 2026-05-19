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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @NotNull
    private final JsonNode cassandraYaml;

    @NotNull
    private final List<String> extraJvmOpts;

    @JsonCreator
    public CassandraConfigurationOverlay(@JsonProperty("cassandraYaml") @Nullable JsonNode cassandraYaml,
                                         @JsonProperty("extraJvmOpts") @Nullable List<String> extraJvmOpts)
    {
        if (cassandraYaml == null)
        {
            this.cassandraYaml = MAPPER.createObjectNode();
        }
        else if (!cassandraYaml.isObject())
        {
            throw new IllegalArgumentException("cassandraYaml must be a JSON object, got " + cassandraYaml.getNodeType());
        }
        else
        {
            this.cassandraYaml = ((ObjectNode) cassandraYaml).deepCopy();
        }
        this.extraJvmOpts = extraJvmOpts != null
                            ? Collections.unmodifiableList(new ArrayList<>(extraJvmOpts))
                            : Collections.emptyList();
    }

    /**
     * Returns the cassandra.yaml overlay as a version-agnostic JSON object. Callers must not mutate the
     * returned node; use {@link #updated} to produce a new overlay with changes applied.
     *
     * @return the cassandra.yaml overlay as a version-agnostic JSON object
     */
    @JsonProperty("cassandraYaml")
    @NotNull
    public JsonNode cassandraYaml()
    {
        return cassandraYaml;
    }

    /**
     * @return an unmodifiable list of extra JVM options
     */
    @JsonProperty("extraJvmOpts")
    @NotNull
    public List<String> extraJvmOpts()
    {
        return extraJvmOpts;
    }

    /**
     * Returns a new overlay with the given updates applied. The current instance is not modified.
     *
     * @param cassandraYamlUpdates field-level changes to cassandra.yaml: key = field name, value = new value.
     *                             A null or {@link com.fasterxml.jackson.databind.node.NullNode} value removes
     *                             the field. Pass {@code null} for no yaml changes.
     * @param addJvmOpts           JVM options to append to the current list. Pass {@code null} for no additions.
     * @param removeJvmOpts        JVM options to remove by value. Pass {@code null} for no removals.
     * @return a new overlay with the updates applied
     */
    @NotNull
    public CassandraConfigurationOverlay updated(@Nullable Map<String, JsonNode> cassandraYamlUpdates,
                                                 @Nullable List<String> addJvmOpts,
                                                 @Nullable List<String> removeJvmOpts)
    {
        ObjectNode mergedYaml = ((ObjectNode) cassandraYaml).deepCopy();
        if (cassandraYamlUpdates != null)
        {
            for (Map.Entry<String, JsonNode> entry : cassandraYamlUpdates.entrySet())
            {
                if (entry.getValue() == null || entry.getValue().isNull())
                {
                    mergedYaml.remove(entry.getKey());
                }
                else
                {
                    mergedYaml.set(entry.getKey(), entry.getValue());
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
        ObjectNode node = MAPPER.createObjectNode();
        node.set("cassandraYaml", cassandraYaml);
        node.set("extraJvmOpts", MAPPER.valueToTree(extraJvmOpts));
        return node.toPrettyString();
    }
}
