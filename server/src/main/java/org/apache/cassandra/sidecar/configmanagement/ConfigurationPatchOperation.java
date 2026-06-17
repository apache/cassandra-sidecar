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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Represents a single JSON Patch operation (RFC 6902-inspired) for configuration updates.
 * Supported operations: {@code add}, {@code remove}, {@code replace}, {@code test}.
 *
 * <p>Paths use JSON Pointer syntax (RFC 6901) and reference the effective configuration structure.
 * Operations are interpreted as overlay mutations:
 * <ul>
 *   <li>{@code add} — upsert value in overlay; parent must exist in effective config</li>
 *   <li>{@code remove} — remove from overlay; fails if key only exists in base template</li>
 *   <li>{@code replace} — set value in overlay; fails if key absent from effective config</li>
 *   <li>{@code test} — assert effective config value matches; no mutation</li>
 * </ul>
 */
public class ConfigurationPatchOperation
{
    /**
     * The supported patch operations.
     */
    public enum Op
    {
        ADD,
        REMOVE,
        REPLACE,
        TEST
    }

    @NotNull
    private final Op op;

    @NotNull
    private final String path;

    @Nullable
    private final Object value;

    /**
     * @param op    the operation type
     * @param path  the JSON Pointer path (e.g. "/configuration/cassandraYaml/concurrent_reads")
     * @param value the value for add/replace/test operations; must be {@code null} for remove
     */
    public ConfigurationPatchOperation(@NotNull Op op, @NotNull String path, @Nullable Object value)
    {
        this.op = Objects.requireNonNull(op, "op must not be null");
        this.path = Objects.requireNonNull(path, "path must not be null");
        this.value = value;
    }

    @NotNull
    public Op op()
    {
        return op;
    }

    @NotNull
    public String path()
    {
        return path;
    }

    @Nullable
    public Object value()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return "ConfigurationPatchOperation{op=" + op + ", path='" + path + "', value=" + value + '}';
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
        ConfigurationPatchOperation that = (ConfigurationPatchOperation) o;
        return op == that.op && Objects.equals(path, that.path) && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(op, path, value);
    }
}
