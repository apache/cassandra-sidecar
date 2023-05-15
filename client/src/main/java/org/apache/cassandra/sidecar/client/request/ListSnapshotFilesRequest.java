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

package org.apache.cassandra.sidecar.client.request;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.data.ListSnapshotFilesResponse;

/**
 * Represents a request to list the files for a Cassandra snapshot
 */
public class ListSnapshotFilesRequest extends SnapshotRequest<ListSnapshotFilesResponse>
{
    /**
     * Constructs a request to list the Cassandra snapshot for the given {@code snapshotName} in the {@code keyspace}
     * and {@code table}, specifying whether secondary index files should be included as part of the response.
     *
     * @param keyspace                   the keyspace in Cassandra
     * @param table                      the table name in Cassandra
     * @param snapshotName               the name of the snapshot
     * @param includeSecondaryIndexFiles whether to include secondary index files
     */
    public ListSnapshotFilesRequest(String keyspace,
                                    String table,
                                    String snapshotName,
                                    boolean includeSecondaryIndexFiles)
    {
        super(keyspace, table, snapshotName, includeSecondaryIndexFiles);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }
}
