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

/**
 * Holder class for the {@link org.apache.cassandra.sidecar.routes.SnapshotsHandler}
 * request parameters
 */
public class SnapshotRequest
{
    private final String snapshotName;
    private final boolean includeSecondaryIndexFiles;
    private final QualifiedTableName qualifiedTableName;
    private final String ttl;

    /**
     * Constructor for the holder class
     *
     * @param keyspace                   the keyspace in Cassandra
     * @param tableName                  the table name in Cassandra
     * @param snapshotName               the name of the snapshot
     * @param includeSecondaryIndexFiles true if secondary index files are allowed, false otherwise
     * @param ttl                        an optional TTL for snapshot creation
     */
    public SnapshotRequest(String keyspace,
                           String tableName,
                           String snapshotName,
                           boolean includeSecondaryIndexFiles,
                           String ttl)
    {
        this(new QualifiedTableName(keyspace, tableName, true), snapshotName, includeSecondaryIndexFiles, ttl);
    }

    public SnapshotRequest(QualifiedTableName qualifiedTableName,
                           String snapshotName,
                           boolean includeSecondaryIndexFiles,
                           String ttl)
    {
        this.qualifiedTableName = qualifiedTableName;
        this.snapshotName = Objects.requireNonNull(snapshotName, "snapshotName must not be null");
        this.includeSecondaryIndexFiles = includeSecondaryIndexFiles;
        this.ttl = ttl;
    }

    /**
     * @return the {@link QualifiedTableName}
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
     * @return the name of the snapshot
     */
    public String snapshotName()
    {
        return snapshotName;
    }

    /**
     * @return true if secondary index files should be included, false otherwise
     */
    public boolean includeSecondaryIndexFiles()
    {
        return includeSecondaryIndexFiles;
    }

    /**
     * @return the TTL for snapshot creation if provided
     */
    public String ttl()
    {
        return ttl;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SnapshotRequest{" +
               "keyspace='" + keyspace() + '\'' +
               ", tableName='" + tableName() + '\'' +
               ", snapshotName='" + snapshotName + '\'' +
               ", includeSecondaryIndexFiles=" + includeSecondaryIndexFiles +
               ", ttl=" + ttl +
               '}';
    }
}
