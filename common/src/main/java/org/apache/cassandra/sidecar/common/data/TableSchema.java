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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ParseUtils;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.TableOptionsMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a Cassandra table schema. Used by {@link org.apache.cassandra.sidecar.routes.KeyspacesHandler}
 * to serialize keyspace/table responses.
 */
public class TableSchema
{
    private final String keyspaceName;
    private final String name;
    private final boolean isVirtual;
    private final boolean hasSecondaryIndexes;
    private final List<ColumnSchema> partitionKey;
    private final List<ColumnSchema> clusteringColumns;
    private final List<String> clusteringOrder;
    private final List<ColumnSchema> columns;
    private final TableOptionsMetadata options;
    protected final transient Map<String, ColumnSchema> columnsAsMap;

    protected TableSchema(@JsonProperty("keyspaceName") String keyspaceName,
                          @JsonProperty("name") String name,
                          @JsonProperty("virtual") boolean isVirtual,
                          @JsonProperty("secondaryIndexes") boolean hasSecondaryIndexes,
                          @JsonProperty("partitionKey") List<ColumnSchema> partitionKey,
                          @JsonProperty("clusteringColumns") List<ColumnSchema> clusteringColumns,
                          @JsonProperty("clusteringOrder") List<String> clusteringOrder,
                          @JsonProperty("columns") List<ColumnSchema> columns,
                          @JsonProperty("options") TableOptionsMetadata options)
    {
        this.keyspaceName = keyspaceName;
        this.name = name;
        this.isVirtual = isVirtual;
        this.hasSecondaryIndexes = hasSecondaryIndexes;
        this.partitionKey = partitionKey;
        this.clusteringColumns = clusteringColumns;
        this.clusteringOrder = clusteringOrder;
        this.columns = columns;
        this.options = options;
        this.columnsAsMap = buildColumnsMap();
    }

    /**
     * @return the name of the keyspace that owns this table
     */
    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    /**
     * @return the name of the table
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return true if the table belongs to a virtual keyspace, false otherwise
     */
    public boolean isVirtual()
    {
        return isVirtual;
    }

    /**
     * @return true if the table has secondary indexes, false otherwise
     */
    public boolean hasSecondaryIndexes()
    {
        return hasSecondaryIndexes;
    }

    /**
     * @return a list of {@link ColumnSchema columns} that represent the partition key
     */
    public List<ColumnSchema> getPartitionKey()
    {
        return partitionKey;
    }

    /**
     * @return a list of {@link ColumnSchema columns} that represents the set of clustering columns
     */
    public List<ColumnSchema> getClusteringColumns()
    {
        return clusteringColumns;
    }

    /**
     * @return a list of Strings representing the clustering order for a given clustering column
     */
    public List<String> getClusteringOrder()
    {
        return clusteringOrder;
    }

    /**
     * @return a list of {@link ColumnSchema regular columns}
     */
    public List<ColumnSchema> getColumns()
    {
        return columns;
    }


    /**
     * @return the table's primary key represented by the partition key columns and the clustering columns
     */
    @JsonIgnore
    public List<ColumnSchema> getPrimaryKey()
    {
        List<ColumnSchema> pk = new ArrayList<>(this.partitionKey.size() + this.clusteringColumns.size());
        pk.addAll(this.partitionKey);
        pk.addAll(this.clusteringColumns);
        return pk;
    }

    /**
     * Returns metadata on a column of this table.
     *
     * @param name the name of the column to retrieve ({@code name} will be interpreted as a
     *             case-insensitive identifier unless enclosed in double-quotes, see {@link Metadata#quote}).
     * @return the metadata for the column if it exists, or {@code null} otherwise.
     */
    public ColumnSchema getColumn(String name)
    {
        return columnsAsMap.get(handleId(name));
    }

    /**
     * @return the object representing this table's options metadata
     */
    public TableOptionsMetadata getOptions()
    {
        return options;
    }

    /**
     * Builds a {@link TableSchema} built from the given {@link TableMetadata tableMetadata}.
     *
     * @param tableMetadata the object that describes a Cassandra table
     * @return a {@link TableSchema} built from the given {@link TableMetadata tableMetadata}
     */
    public static TableSchema of(TableMetadata tableMetadata)
    {
        List<ColumnSchema> partitionKey = tableMetadata.getPartitionKey()
                                                       .stream()
                                                       .map(ColumnSchema::of)
                                                       .collect(Collectors.toList());
        List<ColumnSchema> clusteringColumns = tableMetadata.getClusteringColumns()
                                                            .stream()
                                                            .map(ColumnSchema::of)
                                                            .collect(Collectors.toList());
        List<String> clusteringOrder = tableMetadata.getClusteringOrder()
                                                    .stream()
                                                    .map(Enum::name)
                                                    .collect(Collectors.toList());
        List<ColumnSchema> columns = tableMetadata.getColumns()
                                                  .stream()
                                                  .map(ColumnSchema::of)
                                                  .collect(Collectors.toList());

        return new TableSchema(tableMetadata.getKeyspace().getName(),
                               tableMetadata.getName(),
                               tableMetadata.isVirtual(),
                               !tableMetadata.getIndexes().isEmpty(),
                               partitionKey,
                               clusteringColumns,
                               clusteringOrder,
                               columns,
                               tableMetadata.getOptions());
    }

    protected Map<String, ColumnSchema> buildColumnsMap()
    {
        // We use a linked hashmap because we will keep this in the order of a 'SELECT * FROM ...'.
        Map<String, ColumnSchema> columnsAsMap = new LinkedHashMap<>();
        for (ColumnSchema c : partitionKey) columnsAsMap.put(c.getName(), c);
        for (ColumnSchema c : clusteringColumns) columnsAsMap.put(c.getName(), c);
        for (ColumnSchema c : columns) columnsAsMap.put(c.getName(), c);
        return columnsAsMap;
    }

    protected String handleId(String id)
    {
        if (id == null)
        {
            return null;
        }

        boolean isAlphanumericLowCase = true;
        boolean isAlphanumeric = true;
        for (int i = 0; i < id.length(); i++)
        {
            char c = id.charAt(i);
            if (c >= 'A' && c <= 'Z')
            {
                isAlphanumericLowCase = false;
            }
            else if ((c < '0' || c > '9') && c != '_' && (c < 'a' || c > 'z'))
            {
                isAlphanumeric = false;
                isAlphanumericLowCase = false;
                break;
            }
        }

        if (isAlphanumericLowCase)
        {
            return id;
        }
        else
        {
            return isAlphanumeric ? id.toLowerCase() : ParseUtils.unDoubleQuote(id);
        }
    }
}
