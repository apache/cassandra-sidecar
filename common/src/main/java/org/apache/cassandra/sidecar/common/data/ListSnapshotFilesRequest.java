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

import org.apache.cassandra.sidecar.common.utils.ValidationUtils;

/**
 * Holder class for the {@link org.apache.cassandra.sidecar.routes.ListSnapshotFilesHandler}
 * request parameters
 */
public class ListSnapshotFilesRequest extends QualifiedTableName
{
    private final String snapshotName;
    private final boolean includeSecondaryIndexFiles;

    /**
     * Constructor for the holder class
     *
     * @param keyspace                   the keyspace in Cassandra
     * @param tableName                  the table name in Cassandra
     * @param snapshotName               the name of the snapshot
     * @param includeSecondaryIndexFiles true if secondary index files are allowed, false otherwise
     */
    public ListSnapshotFilesRequest(String keyspace,
                                    String tableName,
                                    String snapshotName,
                                    boolean includeSecondaryIndexFiles)
    {
        super(keyspace, tableName, true);
        this.snapshotName = ValidationUtils.validateSnapshotName(snapshotName);
        this.includeSecondaryIndexFiles = includeSecondaryIndexFiles;
    }

    /**
     * @return the name of the snapshot
     */
    public String getSnapshotName()
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
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ListSnapshotFilesRequest{" +
               "keyspace='" + getKeyspace() + '\'' +
               ", tableName='" + getTableName() + '\'' +
               ", snapshotName='" + snapshotName + '\'' +
               ", includeSecondaryIndexFiles=" + includeSecondaryIndexFiles +
               '}';
    }
}
