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

import java.math.BigInteger;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.cassandra.sidecar.common.utils.Preconditions;

import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.BUCKET_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_CHECKSUM;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_COMPRESSED_SIZE;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_END_TOKEN;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_ID;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_START_TOKEN;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_STORAGE_BUCKET;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_STORAGE_KEY;
import static org.apache.cassandra.sidecar.common.data.RestoreJobConstants.SLICE_UNCOMPRESSED_SIZE;

/**
 * Request payload for creating a slice.
 * A slice is a blob object that contains SSTables to be imported into Cassandra.
 * The data covers a sub-range in the ring.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateSliceRequestPayload
{
    private final String sliceId;
    private final int bucketId;
    private final String bucket;
    private final String key;
    private final String checksum;
    private final BigInteger startToken;
    private final BigInteger endToken;
    // -- Optional fields - using Objects
    private final Long uncompressedSize;
    private final Long compressedSize;
    // ----

    @JsonCreator
    public CreateSliceRequestPayload(@JsonProperty(SLICE_ID) String sliceId,
                                     @JsonProperty(BUCKET_ID) int bucketId,
                                     @JsonProperty(SLICE_STORAGE_BUCKET) String bucket,
                                     @JsonProperty(SLICE_STORAGE_KEY) String key,
                                     @JsonProperty(SLICE_CHECKSUM) String checksum,
                                     @JsonProperty(SLICE_START_TOKEN) BigInteger startToken,
                                     @JsonProperty(SLICE_END_TOKEN) BigInteger endToken,
                                     @JsonProperty(SLICE_UNCOMPRESSED_SIZE) Long uncompressedSize,
                                     @JsonProperty(SLICE_COMPRESSED_SIZE) Long compressedSize)
    {
        Preconditions.checkArgument(sliceId != null
                                    && checksum != null
                                    && bucket != null
                                    && key != null
                                    && startToken != null
                                    && endToken != null,
                                    "Invalid create slice request payload");
        Preconditions.checkArgument(bucketId < Short.MAX_VALUE && bucketId >= 0,
                                    "Invalid bucketId. Valid range: [0, " + Short.MAX_VALUE + "), " +
                                    "but got " + bucketId);
        this.sliceId = sliceId;
        this.bucketId = bucketId;
        this.bucket = bucket;
        this.key = key;
        this.checksum = checksum;
        this.startToken = startToken;
        this.endToken = endToken;
        this.uncompressedSize = uncompressedSize;
        this.compressedSize = compressedSize;
    }

    /**
     * @return slice id to identify a slice uniquely.
     */
    @JsonProperty(SLICE_ID)
    public String sliceId()
    {
        return sliceId;
    }

    /**
     * @return bucket id to identify a bucket uniquely.
     */
    @JsonProperty(BUCKET_ID)
    public int bucketId()
    {
        return bucketId;
    }

    public short bucketIdAsShort()
    {
        return (short) bucketId;
    }

    /**
     * @return upload bucket used for slice upload
     */
    @JsonProperty(SLICE_STORAGE_BUCKET)
    public String bucket()
    {
        return bucket;
    }

    /**
     * @return upload key used for slice upload
     */
    @JsonProperty(SLICE_STORAGE_KEY)
    public String key()
    {
        return key;
    }

    /**
     * @return start token of slice
     */
    @JsonProperty(SLICE_START_TOKEN)
    public BigInteger startToken()
    {
        return startToken;
    }

    /**
     * @return end token of slice
     */
    @JsonProperty(SLICE_END_TOKEN)
    public BigInteger endToken()
    {
        return endToken;
    }

    /**
     * @return checksum of a slice
     */
    @JsonProperty(SLICE_CHECKSUM)
    public String checksum()
    {
        return checksum;
    }

    /**
     * @return the size of all files in the slice after unzipping, or null if not defined
     */
    @JsonProperty(SLICE_UNCOMPRESSED_SIZE)
    public Long uncompressedSize()
    {
        return uncompressedSize;
    }

    /**
     * @return the size of all files in the slice after unzipping, or 0 if not defined
     */
    public long uncompressedSizeOrZero()
    {
        return uncompressedSize == null ? 0 : uncompressedSize;
    }

    /**
     * @return the size of the slice, or null if not defined
     */
    @JsonProperty(SLICE_COMPRESSED_SIZE)
    public Long compressedSize()
    {
        return compressedSize;
    }

    /**
     * @return the size of the slice, or 0 if not defined
     */
    public long compressedSizeOrZero()
    {
        return compressedSize == null ? 0 : compressedSize;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "CreateSliceRequest{" + SLICE_ID + "='" + sliceId + "', " +
               BUCKET_ID + "='" + bucketId + "', " +
               SLICE_STORAGE_BUCKET + "='" + bucket + "', " +
               SLICE_STORAGE_KEY + "='" + key + "', " +
               SLICE_CHECKSUM + "='" + checksum + "', " +
               SLICE_START_TOKEN + "='" + startToken + "', " +
               SLICE_END_TOKEN + "='" + endToken + "', " +
               SLICE_COMPRESSED_SIZE + "='" + compressedSize + "', " +
               SLICE_UNCOMPRESSED_SIZE + "='" + uncompressedSize + "'}";
    }
}
