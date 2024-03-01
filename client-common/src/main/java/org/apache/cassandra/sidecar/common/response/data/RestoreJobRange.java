/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.common.response.data;

import java.math.BigInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.BUCKET_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_END_TOKEN;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_START_TOKEN;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_STORAGE_BUCKET;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_STORAGE_KEY;

/**
 * The minimum data presentation of a range of data in the restore job. The data structure is client-facing.
 * A range is fully enclosed in a slice, i.e. range covers the smaller or same amount of data of the enclosing slice.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RestoreJobRange
{
    private final String sliceId;
    private final int bucketId;
    private final String bucket;
    private final String key;
    private final BigInteger startToken;
    private final BigInteger endToken;

    @JsonCreator
    public RestoreJobRange(@JsonProperty(SLICE_ID) String sliceId,
                           @JsonProperty(BUCKET_ID) int bucketId,
                           @JsonProperty(SLICE_STORAGE_BUCKET) String bucket,
                           @JsonProperty(SLICE_STORAGE_KEY) String key,
                           @JsonProperty(SLICE_START_TOKEN) BigInteger startToken,
                           @JsonProperty(SLICE_END_TOKEN) BigInteger endToken)
    {
        this.sliceId = sliceId;
        this.bucketId = bucketId;
        this.bucket = bucket;
        this.key = key;
        this.startToken = startToken;
        this.endToken = endToken;
    }

    @JsonProperty(SLICE_ID)
    public String sliceId()
    {
        return this.sliceId;
    }

    @JsonProperty(BUCKET_ID)
    public int bucketId()
    {
        return this.bucketId;
    }

    @JsonProperty(SLICE_STORAGE_BUCKET)
    public String bucket()
    {
        return this.bucket;
    }

    @JsonProperty(SLICE_STORAGE_KEY)
    public String key()
    {
        return this.key;
    }

    @JsonProperty(SLICE_START_TOKEN)
    public BigInteger startToken()
    {
        return this.startToken;
    }

    @JsonProperty(SLICE_END_TOKEN)
    public BigInteger endToken()
    {
        return this.endToken;
    }
}
