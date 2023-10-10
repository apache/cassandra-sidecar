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
 * Represents an SSTable component that includes a keyspace, table name and component name
 */
public class SSTableComponent
{
    private final QualifiedTableName qualifiedTableName;
    private final String componentName;
    @Nullable
    private final String indexName;

    /**
     * Constructor for the holder class
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param componentName      the name of the SSTable component
     */
    public SSTableComponent(QualifiedTableName qualifiedTableName, String componentName)
    {
        this(qualifiedTableName, null, componentName);
    }

    /**
     * Constructor for the holder class
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param indexName          the name of the index for the SSTable component
     * @param componentName      the name of the SSTable component
     */
    public SSTableComponent(QualifiedTableName qualifiedTableName, @Nullable String indexName, String componentName)
    {
        this.qualifiedTableName = Objects.requireNonNull(qualifiedTableName, "qualifiedTableName must not be null");
        this.indexName = indexName;
        this.componentName = Objects.requireNonNull(componentName, "componentName must not be null");
    }

    /**
     * @return the qualified table name in Cassandra
     */
    public QualifiedTableName qualifiedTableName()
    {
        return qualifiedTableName;
    }

    /**
     * @return the keyspace in Cassandra
     */
    public String keyspace()
    {
        return qualifiedTableName.keyspace();
    }

    /**
     * @return the table name in Cassandra
     */
    public String tableName()
    {
        return qualifiedTableName.tableName();
    }

    /**
     * @return the index name when the SSTable component is an index component, {@code null} otherwise
     */
    @Nullable
    public String indexName()
    {
        return indexName;
    }

    /**
     * @return the name of the SSTable component
     */
    public String componentName()
    {
        return componentName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "SSTableComponent{" +
               "qualifiedTableName='" + qualifiedTableName + '\'' +
               ", componentName='" + componentName + '\'' +
               '}';
    }
}
