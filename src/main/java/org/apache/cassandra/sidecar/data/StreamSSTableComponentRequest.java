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
    private final String tableId;
    private final String snapshotName;
    private final int dataDirectoryIndex;

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
        this(new QualifiedTableName(keyspace, tableName, true), snapshotName, null, componentName, null, 0);
    }

    /**
     * Constructor for the holder class
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param snapshotName       the name of the snapshot
     * @param secondaryIndexName the name of the secondary index for the SSTable component
     * @param componentName      the name of the SSTable component
     * @param tableId            the UUID for the Cassandra table
     * @param dataDirectoryIndex the index of the Cassandra data directory where the component resides
     */
    public StreamSSTableComponentRequest(QualifiedTableName qualifiedTableName,
                                         String snapshotName,
                                         @Nullable String secondaryIndexName,
                                         String componentName,
                                         @Nullable String tableId,
                                         int dataDirectoryIndex)
    {
        super(qualifiedTableName, secondaryIndexName, componentName);
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshotName must not be null");
        this.tableId = tableId;
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
     * @return the Cassandra table ID
     */
    public String tableId()
    {
        return tableId;
    }

    /**
     * @return the index of the Cassandra data directory where the component resides
     */
    public int dataDirectoryIndex()
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
        String tableId = maybeGetTableId(context.pathParam("table"));
        int dataDirectoryIndex = RequestUtils.parseIntegerQueryParam(context.request(), "dataDirectoryIndex", 0);

        return new StreamSSTableComponentRequest(qualifiedTableName,
                                                 snapshotName,
                                                 secondaryIndexName,
                                                 componentName,
                                                 tableId,
                                                 dataDirectoryIndex);
    }

    static String maybeGetTableId(String table)
    {
        if (table != null)
        {
            // Cassandra disallows having '-' as part of the table name, even if the table name is quoted.
            // If the string contains '-', it is followed by the tableId.
            // See https://github.com/apache/cassandra/blob/c33c8ebab444209a9675f273448110afd0787faa/src/java/org/apache/cassandra/schema/TableId.java#L88
            int index = table.indexOf("-");
            if (index > 0 && index + 1 < table.length())
            {
                return table.substring(index + 1);
            }
        }
        return null;
    }
}
