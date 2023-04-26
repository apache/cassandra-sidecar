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

import io.netty.handler.codec.http.HttpHeaderNames;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.common.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.data.SSTableUploads;

/**
 * Holder class for the UploadHandler request parameters.
 */
public class SSTableUploadRequest extends SSTableUploads
{
    private final String component;
    private final String expectedChecksum;

    /**
     * Constructs an SSTableUploadRequest
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param uploadId           an identifier for the upload
     * @param component          SSTable component being uploaded
     * @param expectedChecksum   expected hash value to check integrity of SSTable component uploaded
     */
    public SSTableUploadRequest(QualifiedTableName qualifiedTableName,
                                String uploadId,
                                String component,
                                String expectedChecksum)
    {
        super(qualifiedTableName, uploadId);
        this.component = component;
        this.expectedChecksum = expectedChecksum;
    }

    /**
     * @return name of component being uploaded
     */
    public String component()
    {
        return this.component;
    }

    /**
     * @return expected checksum value of SSTable component
     */
    public String expectedChecksum()
    {
        return this.expectedChecksum;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "SSTableUploadRequest{" +
               "uploadId='" + uploadId() + '\'' +
               ", keyspace='" + keyspace() + '\'' +
               ", tableName='" + tableName() + '\'' +
               ", component='" + component + '\'' +
               ", expectedChecksum='" + expectedChecksum + '\'' +
               '}';
    }

    /**
     * Returns a new instance of the {@link SSTableUploadRequest} built from the {@link RoutingContext context}.
     *
     * @param qualifiedTableName the qualified table name in Cassandra
     * @param context            context from handler
     * @return SSTableUploadRequest created from params
     */
    public static SSTableUploadRequest from(QualifiedTableName qualifiedTableName, RoutingContext context)
    {
        return new SSTableUploadRequest(qualifiedTableName,
                                        context.pathParam("uploadId"),
                                        context.pathParam("component"),
                                        context.request().getHeader(HttpHeaderNames.CONTENT_MD5.toString()));
    }
}
