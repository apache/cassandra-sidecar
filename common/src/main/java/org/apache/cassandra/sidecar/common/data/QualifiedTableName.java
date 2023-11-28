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

import org.jetbrains.annotations.Nullable;

/**
 * Contains the keyspace and table name in Cassandra
 */
public class QualifiedTableName
{
    @Nullable
    private final Name keyspace;
    @Nullable
    private final Name table;

    /**
     * Constructs a qualified name with the given {@code keyspace} and {@code tableName}
     *
     * @param keyspace  the keyspace in Cassandra
     * @param tableName the table name in Cassandra
     */
    public QualifiedTableName(String keyspace, String tableName)
    {
        this(keyspace, tableName, true);
    }

    /**
     * Constructs a qualified name with the given {@code keyspace} and {@code tableName}. When {@code required}
     * is {@code false}, allow constructing the object with {@code null} {@code keyspace}/{@code tableName}.
     *
     * @param keyspace  the keyspace in Cassandra
     * @param tableName the table name in Cassandra
     * @param required  true if keyspace and table name are required, false if {@code null} is allowed
     */
    public QualifiedTableName(String keyspace, String tableName, boolean required)
    {
        if (required)
        {
            Objects.requireNonNull(keyspace, "keyspace must not be null");
            Objects.requireNonNull(tableName, "tableName must not be null");
        }
        this.keyspace = !required && keyspace == null ? null : new Name(keyspace);
        this.table = !required && tableName == null ? null : new Name(tableName);
    }

    public QualifiedTableName(@Nullable Name keyspace, @Nullable Name table)
    {
        this.keyspace = keyspace;
        this.table = table;
    }

    /**
     * @return the keyspace in Cassandra
     */
    public String keyspace()
    {
        return keyspace != null ? keyspace.name() : null;
    }

    /**
     * @return the keyspace in Cassandra, quoted if the original input was quoted and if
     * the unquoted keyspace needs to be quoted
     */
    public String maybeQuotedKeyspace()
    {
        return keyspace != null ? keyspace.maybeQuotedName() : null;
    }

    /**
     * @return the keyspace in Cassandra
     */
    public @Nullable Name getKeyspace()
    {
        return keyspace;
    }

    /**
     * @return the table name in Cassandra
     */
    public String tableName()
    {
        return table != null ? table.name() : null;
    }

    /**
     * @return the table name in Cassandra, quoted if the original input was quoted and if
     * the unquoted table needs to be quoted
     */
    public String maybeQuotedTableName()
    {
        return table != null ? table.maybeQuotedName() : null;
    }

    /**
     * @return the table name in Cassandra
     */
    public @Nullable Name table()
    {
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return maybeQuotedKeyspace() + "." + maybeQuotedTableName();
    }
}
