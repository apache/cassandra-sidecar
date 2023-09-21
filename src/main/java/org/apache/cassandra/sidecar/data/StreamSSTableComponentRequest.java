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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.SSTableComponent;
import org.apache.cassandra.sidecar.common.utils.HttpEncodings;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Holder class for the {@code org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler}
 * request parameters
 */
public class StreamSSTableComponentRequest extends SSTableComponent
{
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamSSTableComponentRequest.class);

    private final String snapshotName;

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
        this(new QualifiedTableName(keyspace, tableName, true), snapshotName, componentName);
    }

    /**
     * Constructor for the holder class
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param snapshotName       the name of the snapshot
     * @param componentName      the name of the SSTable component
     */
    public StreamSSTableComponentRequest(QualifiedTableName qualifiedTableName,
                                         String snapshotName,
                                         String componentName)
    {
        super(qualifiedTableName, componentName);
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshotName must not be null");
    }

    public static StreamSSTableComponentRequest from(QualifiedTableName qualifiedTableName, RoutingContext context)
    {
        String component = context.pathParam("component");
        if (component == null)
        {
            component = context.pathParam("*");
            LOGGER.warn("Legacy client requests detected for component={}", component);
            component = Objects.requireNonNull(component, "path cannot be null").replaceFirst("components/", "");
        }
        // Decode slash to support streaming index files
        String componentName = HttpEncodings.decodeSSTableComponent(component);
        return new StreamSSTableComponentRequest(qualifiedTableName,
                                                 context.pathParam("snapshot"),
                                                 componentName);
    }

    /**
     * @return the name of the snapshot
     */
    public String snapshotName()
    {
        return snapshotName;
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
               ", componentName='" + componentName() + '\'' +
               '}';
    }
}
