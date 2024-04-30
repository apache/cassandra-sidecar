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

package org.apache.cassandra.sidecar.common.response;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the response for an SSTable import status
 */
public class SSTableImportResponse
{
    private final boolean success;
    private final String uploadId;
    private final String keyspace;
    private final String tableName;

    public SSTableImportResponse(@JsonProperty("success") boolean success,
                                 @JsonProperty("uploadId") String uploadId,
                                 @JsonProperty("keyspace") String keyspace,
                                 @JsonProperty("tableName") String tableName)
    {
        this.success = success;
        this.uploadId = uploadId;
        this.keyspace = keyspace;
        this.tableName = tableName;
    }

    /**
     * @return true if the import was successful, false otherwise
     */
    @JsonProperty("success")
    public boolean success()
    {
        return success;
    }

    /**
     * @return an identifier for the upload session
     */
    @JsonProperty("uploadId")
    public String uploadId()
    {
        return uploadId;
    }

    /**
     * @return the name of the Cassandra keyspace
     */
    @JsonProperty("keyspace")
    public String keyspace()
    {
        return keyspace;
    }

    /**
     * @return the name of the Cassandra table
     */
    @JsonProperty("tableName")
    public String tableName()
    {
        return tableName;
    }
}
