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

/**
 * A base class for the {@code SSTableImportRequest} and {@code SSTableUploadRequest}
 */
public class SSTableUploads
{
    private final QualifiedTableName qualifiedTableName;
    private final String uploadId;

    /**
     * Holds common request params needed for SSTable uploads
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param uploadId           an identifier for the upload
     */
    public SSTableUploads(QualifiedTableName qualifiedTableName, String uploadId)
    {
        this.qualifiedTableName = qualifiedTableName;
        this.uploadId = Objects.requireNonNull(uploadId, "uploadId should not be null");
    }

    /**
     * @return the keyspace in Cassandra
     */
    public Keyspace keyspace()
    {
        return qualifiedTableName.getKeyspace();
    }

    /**
     * @return the table name in Cassandra
     */
    public Table table()
    {
        return qualifiedTableName.table();
    }

    /**
     * @return an identifier for the upload session
     */
    public String uploadId()
    {
        return uploadId;
    }
}
