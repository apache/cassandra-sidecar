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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A class representing a response for the {@code SSTableUploadRequest}.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SSTableUploadResponse
{
    private final String uploadId;
    private final long uploadSizeBytes;
    private final long serviceTimeMillis;

    /**
     * Constructs upload response object with given params.
     * @param uploadId          an identifier for the upload
     * @param uploadSizeBytes   size of SSTable component uploaded
     * @param serviceTimeMillis time taken to complete upload job
     */
    @JsonCreator
    public SSTableUploadResponse(@JsonProperty("uploadId") String uploadId,
                                 @JsonProperty("uploadSizeBytes") long uploadSizeBytes,
                                 @JsonProperty("serviceTimeMillis") long serviceTimeMillis)
    {
        this.uploadId = uploadId;
        this.uploadSizeBytes = uploadSizeBytes;
        this.serviceTimeMillis = serviceTimeMillis;
    }

    /**
     * @return upload id passed along with upload request.
     */
    @JsonProperty("uploadId")
    public String uploadId()
    {
        return uploadId;
    }

    /**
     * @return size of uploaded SSTable component.
     */
    @JsonProperty("uploadSizeBytes")
    public long uploadSizeBytes()
    {
        return uploadSizeBytes;
    }

    /**
     * @return time taken in milliseconds for upload to complete.
     */
    @JsonProperty("serviceTimeMillis")
    public long serviceTimeMillis()
    {
        return serviceTimeMillis;
    }
}
