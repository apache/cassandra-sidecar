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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.datastax.driver.core.KeyspaceMetadata;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a keyspace schema used by {@link org.apache.cassandra.sidecar.routes.KeyspacesHandler}
 * to serialize a keyspace response
 */
public class KeyspaceSchema
{
    private final String name;
    private final List<TableSchema> tables;
    private final Map<String, String> replication;
    private final boolean durableWrites;
    private final boolean virtual;

    protected KeyspaceSchema(@JsonProperty("name") String name,
                             @JsonProperty("tables") List<TableSchema> tables,
                             @JsonProperty("replication") Map<String, String> replication,
                             @JsonProperty("durableWrites") boolean durableWrites,
                             @JsonProperty("virtual") boolean virtual)
    {
        this.name = name;
        this.tables = tables;
        this.replication = replication;
        this.durableWrites = durableWrites;
        this.virtual = virtual;
    }

    /**
     * @return the name of the keyspace
     */
    public String getName()
    {
        return name;
    }

    /**
     * @return the replication information for the keyspace
     */
    public Map<String, String> getReplication()
    {
        return replication;
    }

    /**
     * @return true if durable writes, false otherwise
     */
    public boolean isDurableWrites()
    {
        return durableWrites;
    }

    /**
     * @return true for virtual tables, false otherwise
     */
    public boolean isVirtual()
    {
        return virtual;
    }

    /**
     * @return a list of tables available in this keyspace
     */
    public List<TableSchema> getTables()
    {
        return tables;
    }

    /**
     * Builds a {@link KeyspaceSchema} from the given {@link KeyspaceMetadata}
     *
     * @param keyspaceMetadata the keyspace metadata
     * @return a {@link KeyspaceSchema} built from the given {@link KeyspaceMetadata}
     */
    public static KeyspaceSchema of(KeyspaceMetadata keyspaceMetadata)
    {
        List<TableSchema> tables = keyspaceMetadata.getTables().stream().map(TableSchema::of)
                                                   .collect(Collectors.toList());
        return new KeyspaceSchema(keyspaceMetadata.getName(),
                                  tables,
                                  keyspaceMetadata.getReplication(),
                                  keyspaceMetadata.isDurableWrites(),
                                  keyspaceMetadata.isVirtual());
    }
}
