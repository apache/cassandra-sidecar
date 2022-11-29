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

import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.SSTableComponent;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Holder class for the {@code org.apache.cassandra.sidecar.routes.StreamSSTableComponentHandler}
 * request parameters
 */
public class StreamSSTableComponentRequest extends SSTableComponent
{
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
