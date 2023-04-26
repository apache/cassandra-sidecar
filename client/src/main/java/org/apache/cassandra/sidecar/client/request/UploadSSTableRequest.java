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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.cassandra.sidecar.common.ApiEndpointsV1;

/**
 * Represents a request to upload an SSTable component
 */
public class UploadSSTableRequest extends Request implements UploadableRequest
{
    private final String expectedChecksum;
    private final String filename;

    /**
     * Constructs a request with the provided {@code requestURI}
     *
     * @param keyspace  the keyspace in Cassandra
     * @param table     the table name in Cassandra
     * @param uploadId  an identifier for the upload
     * @param component SSTable component being uploaded
     * @param checksum  hash value to check integrity of SSTable component uploaded
     * @param filename  the path to the file to be uploaded
     */
    public UploadSSTableRequest(String keyspace, String table, String uploadId, String component,
                                String checksum, String filename)
    {
        super(requestURI(keyspace, table, uploadId, component));
        this.expectedChecksum = checksum;
        this.filename = Objects.requireNonNull(filename, "the filename is must be non-null");

        if (!Files.exists(Paths.get(filename)))
        {
            throw new IllegalArgumentException("File '" + filename + "' does not exist");
        }
    }

    @Override
    public String filename()
    {
        return filename;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> headers()
    {
        if (expectedChecksum == null)
        {
            return super.headers();
        }
        Map<String, String> headers = new HashMap<>(super.headers());
        headers.put(HttpHeaderNames.CONTENT_MD5.toString(), expectedChecksum);
        return Collections.unmodifiableMap(headers);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public HttpMethod method()
    {
        return HttpMethod.PUT;
    }

    static String requestURI(String keyspace, String tableName, String uploadId, String component)
    {
        return ApiEndpointsV1.SSTABLE_UPLOAD_ROUTE
               .replaceAll(ApiEndpointsV1.UPLOAD_ID_PATH_PARAM, uploadId)
               .replaceAll(ApiEndpointsV1.KEYSPACE_PATH_PARAM, keyspace)
               .replaceAll(ApiEndpointsV1.TABLE_PATH_PARAM, tableName)
               .replaceAll(ApiEndpointsV1.COMPONENT_PATH_PARAM, component);
    }
}
