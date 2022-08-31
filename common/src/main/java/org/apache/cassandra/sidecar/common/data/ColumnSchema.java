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

package org.apache.cassandra.sidecar.common.data;

import java.util.Objects;

import com.datastax.driver.core.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Represents a Cassandra table column. Used by {@link org.apache.cassandra.sidecar.routes.KeyspacesHandler}
 * to serialize keyspace/table responses.
 */
public class ColumnSchema
{
    private final String name;
    private final DataType type;
    private final boolean isStatic;

    /**
     * Constructs a non-static {@link ColumnSchema} object with the provided {@code name} and {@code type} parameters.
     *
     * @param name the name of the column
     * @param type the type of the column
     */
    @VisibleForTesting
    public ColumnSchema(String name, DataType type)
    {
        this(name, type, false);
    }

    /**
     * Constructs a {@link ColumnSchema} object with the provided {@code name}, {@code type}, and {@code isStatic}
     * parameters.
     *
     * @param name     the name of the column
     * @param type     the type of the column
     * @param isStatic true if the column is static, false otherwise
     */
    protected ColumnSchema(@JsonProperty("name") String name,
                           @JsonProperty("type") DataType type,
                           @JsonProperty("static") boolean isStatic)
    {
        this.name = name;
        this.type = type;
        this.isStatic = isStatic;
    }

    /**
     * @return the name of the column
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return the type of the column
     */
    public DataType getType()
    {
        return type;
    }

    /**
     * @return true if this column is static, false otherwise
     */
    public boolean isStatic()
    {
        return isStatic;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, isStatic);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnSchema that = (ColumnSchema) o;
        return isStatic == that.isStatic
               && Objects.equals(name, that.name)
               && Objects.equals(type, that.type);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "ColumnSchema{" +
               "name='" + name + '\'' +
               ", type=" + type +
               ", isStatic=" + isStatic +
               '}';
    }

    /**
     * Builds a {@link ColumnSchema} built from the given {@link ColumnMetadata columnMetadata}.
     *
     * @param columnMetadata the object that describes a column defined in a Cassandra table
     * @return a {@link ColumnSchema} built from the given {@link ColumnMetadata columnMetadata}
     */
    public static ColumnSchema of(ColumnMetadata columnMetadata)
    {
        return new ColumnSchema(columnMetadata.getName(),
                                DataType.of(columnMetadata.getType()),
                                columnMetadata.isStatic());
    }
}
