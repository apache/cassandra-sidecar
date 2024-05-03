/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.sidecar.common.request;

import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;
import org.apache.cassandra.sidecar.common.response.ListSnapshotFilesResponse;
import org.apache.cassandra.sidecar.common.utils.HttpRange;

/**
 * Represents a request to stream an SSTable component
 */
public class SSTableComponentRequest extends Request
{
    private final HttpRange range;

    /**
     * Constructs a Sidecar request with the given {@code requestURI}. Defaults to {@code ssl} enabled.
     *
     * @param keyspace      the keyspace in Cassandra
     * @param tableName     the table name in Cassandra
     * @param snapshotName  the name of the snapshot
     * @param componentName the name of the SSTable component
     * @param range         the HTTP range for the request
     */
    public SSTableComponentRequest(String keyspace,
                                   String tableName,
                                   String snapshotName,
                                   String componentName,
                                   HttpRange range)
    {
        super(requestURI(keyspace, tableName, snapshotName, componentName));
        this.range = range;
    }

    /**
     * Constructs a Sidecar request with the given {@code requestURI}. Defaults to {@code ssl} enabled.
     *
     * @param fileInfo contains information about the file to stream
     * @param range    the HTTP range for the request
     */
    public SSTableComponentRequest(ListSnapshotFilesResponse.FileInfo fileInfo, HttpRange range)
    {
        super(fileInfo.componentDownloadUrl());
        this.range = range;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected HttpRange range()
    {
        return range;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpMethod method()
    {
        return HttpMethod.GET;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "SSTableComponentRequest{" +
               "range=" + range +
               ", requestURI='" + requestURI + '\'' +
               '}';
    }

    static String requestURI(String keyspace, String table, String snapshot, String component)
    {
        return ApiEndpointsV1.COMPONENTS_ROUTE
               .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace)
               .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, table)
               .replaceAll(ApiEndpointsV1.SNAPSHOT_PATH_PARAM, snapshot)
               .replaceAll(ApiEndpointsV1.COMPONENT_PATH_PARAM, component);
    }
}
