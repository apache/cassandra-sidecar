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

package org.apache.cassandra.sidecar.data;

import java.util.Objects;

import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.SSTableComponent;
import org.apache.cassandra.sidecar.utils.RequestUtils;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Holder class for the {@code org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler}
 * request parameters
 */
public class StreamSSTableComponentRequest extends SSTableComponent
{
    private final String tableUuid;
    private final String snapshotName;
    @Nullable
    private final Integer dataDirectoryIndex;

    /**
     * Constructor for the holder class
     *
     * @param keyspace      the keyspace in Cassandra
     * @param tableName     the table name in Cassandra
     * @param snapshotName  the name of the snapshot
     * @param componentName the name of the SSTable component
     */
    @VisibleForTesting
    public StreamSSTableComponentRequest(String keyspace, String tableName, String snapshotName, String componentName)
    {
        this(new QualifiedTableName(keyspace, tableName, true), snapshotName, null, componentName, null, null);
    }

    /**
     * Constructor for the holder class
     *
     * @param keyspace           the keyspace in Cassandra
     * @param tableName          the table name in Cassandra
     * @param snapshotName       the name of the snapshot
     * @param secondaryIndexName the name of the secondary index for the SSTable component
     * @param componentName      the name of the SSTable component
     */
    @VisibleForTesting
    public StreamSSTableComponentRequest(String keyspace, String tableName, String snapshotName,
                                         String secondaryIndexName, String componentName)
    {
        this(new QualifiedTableName(keyspace, tableName, true), snapshotName,
             secondaryIndexName, componentName, null, null);
    }

    /**
     * Constructor for the holder class
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param snapshotName       the name of the snapshot
     * @param secondaryIndexName the name of the secondary index for the SSTable component
     * @param componentName      the name of the SSTable component
     * @param tableUuid          the UUID for the Cassandra table
     * @param dataDirectoryIndex the index of the Cassandra data directory where the component resides
     */
    public StreamSSTableComponentRequest(QualifiedTableName qualifiedTableName,
                                         String snapshotName,
                                         @Nullable String secondaryIndexName,
                                         String componentName,
                                         @Nullable String tableUuid,
                                         @Nullable Integer dataDirectoryIndex)
    {
        super(qualifiedTableName, secondaryIndexName, componentName);
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshotName must not be null");
        this.tableUuid = tableUuid;
        this.dataDirectoryIndex = dataDirectoryIndex;
    }

    /**
     * @return the name of the snapshot
     */
    public String snapshotName()
    {
        return snapshotName;
    }

    /**
     * @return the Cassandra table UUID
     */
    public String tableUuid()
    {
        return tableUuid;
    }

    /**
     * @return the index of the Cassandra data directory where the component resides
     */
    public @Nullable Integer dataDirectoryIndex()
    {
        return dataDirectoryIndex;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "StreamSSTableComponentRequest{" +
               "keyspace='" + keyspace() + '\'' +
               ", tableName='" + tableName() + '\'' +
               ", snapshot='" + snapshotName + '\'' +
               ", secondaryIndexName='" + secondaryIndexName() + '\'' +
               ", componentName='" + componentName() + '\'' +
               ", dataDirectoryIndex='" + dataDirectoryIndex + '\'' +
               '}';
    }

    public static StreamSSTableComponentRequest from(QualifiedTableName qualifiedTableName, RoutingContext context)
    {
        String snapshotName = context.pathParam("snapshot");
        String secondaryIndexName = context.pathParam("index");
        String componentName = context.pathParam("component");
        String tableUuid = maybeGetTableUuid(context.pathParam("table"));
        Integer dataDirectoryIndex = RequestUtils.parseIntegerQueryParam(context.request(), "dataDirectory", null);

        return new StreamSSTableComponentRequest(qualifiedTableName,
                                                 snapshotName,
                                                 secondaryIndexName,
                                                 componentName,
                                                 tableUuid,
                                                 dataDirectoryIndex);
    }

    static String maybeGetTableUuid(String table)
    {
        if (table != null)
        {
            int index = table.indexOf("-");
            if (index > 0 && index + 1 < table.length())
            {
                return table.substring(index + 1);
            }
        }
        return null;
    }
}
